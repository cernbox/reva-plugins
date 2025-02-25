// Copyright 2018-2024 CERN
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// In applying this license, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.

package sql

import (
	"context"
	"fmt"
	"os/user"
	"strconv"
	"time"

	model "github.com/cernbox/reva-plugins/share"
	link "github.com/cs3org/go-cs3apis/cs3/sharing/link/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/pkg/appctx"
	conversions "github.com/cs3org/reva/pkg/cbox/utils"
	"github.com/cs3org/reva/pkg/errtypes"
	"github.com/cs3org/reva/pkg/publicshare"
	"github.com/cs3org/reva/pkg/utils"
	"github.com/pkg/errors"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	// Provides mysql drivers.
	_ "github.com/go-sql-driver/mysql"
)

// These follow the interface defined in github.com/cs3org/reva/pkg/publishare/publicshare.go

func (m *mgr) CreatePublicShare(ctx context.Context, u *user.User, md *provider.ResourceInfo, g *link.Grant, description string, internal bool, notifyUploads bool, notifyUploadsExtraRecipients string) (*link.PublicShare, error) {
	user := appctx.ContextMustGetUser(ctx)

	token := utils.RandString(15)

	quicklink, _ := strconv.ParseBool(md.ArbitraryMetadata.Metadata["quicklink"])
	displayName, ok := md.ArbitraryMetadata.Metadata["name"]
	if !ok {
		displayName = token
	}

	publiclink := &model.PublicLink{
		Quicklink:                    quicklink,
		Token:                        token,
		LinkName:                     displayName,
		NotifyUploads:                notifyUploads,
		NotifyUploadsExtraRecipients: notifyUploadsExtraRecipients,
	}

	publiclink.UIDOwner = conversions.FormatUserID(md.Owner)
	publiclink.UIDInitiator = conversions.FormatUserID(user.Id)
	publiclink.InitialPath = md.Path
	publiclink.ItemType = model.ItemType(conversions.ResourceTypeToItem(md.Type))
	publiclink.Inode = md.Id.OpaqueId
	publiclink.Instance = md.Id.StorageId
	publiclink.Permissions = uint8(conversions.SharePermToInt(g.Permissions.Permissions))
	publiclink.Orphan = false

	if g.Password != "" {
		hashedPassword, err := hashPassword(g.Password, m.c.LinkPasswordHashCost)
		if err != nil {
			return nil, errors.Wrap(err, "could not hash link password")

		}
		publiclink.Password = hashedPassword
	}

	if g.Expiration != nil && g.Expiration.Seconds != 0 {
		publiclink.Expiration = datatypes.NullTime{
			V:     time.Unix(int64(g.Expiration.Seconds), 0),
			Valid: true,
		}
	}

	res := m.db.Save(&publiclink)
	if res.Error != nil {
		return nil, res.Error
	}

	return publiclink.AsCS3PublicShare(), nil
}

func (m *mgr) UpdatePublicShare(ctx context.Context, u *user.User, req *link.UpdatePublicShareRequest, g *link.Grant) (*link.PublicShare, error) {
	var publiclink *model.PublicLink
	var err error

	if id := req.Ref.GetId(); id != nil {
		publiclink, err = emptyLinkWithId(id.OpaqueId)
	} else {
		publiclink, err = m.getLinkByToken(ctx, req.Ref.GetToken())
	}
	if err != nil {
		return nil, err
	}

	var res *gorm.DB
	switch req.GetUpdate().GetType() {
	case link.UpdatePublicShareRequest_Update_TYPE_DISPLAYNAME:
		//publiclink. req.Update.GetDisplayName()
		res = m.db.Model(&publiclink).Update("link_name", req.Update.GetDisplayName())
	case link.UpdatePublicShareRequest_Update_TYPE_PERMISSIONS:
		permissions := conversions.SharePermToInt(req.Update.GetGrant().GetPermissions().Permissions)
		res = m.db.Model(&publiclink).Update("permissions", uint8(permissions))
	case link.UpdatePublicShareRequest_Update_TYPE_EXPIRATION:
		res = m.db.Model(&publiclink).Update("expiration", time.Unix(int64(req.Update.GetGrant().Expiration.Seconds), 0))
	case link.UpdatePublicShareRequest_Update_TYPE_PASSWORD:
		if req.Update.GetGrant().Password == "" {
			// Remove the password
			res = m.db.Model(&publiclink).Update("password", "")
		} else {
			// Update the password
			hashedPwd, err := hashPassword(req.Update.GetGrant().Password, m.c.LinkPasswordHashCost)
			if err != nil {
				return nil, errors.Wrap(err, "could not hash share password")
			}
			res = m.db.Model(&publiclink).Update("password", hashedPwd)
		}
	case link.UpdatePublicShareRequest_Update_TYPE_DESCRIPTION:
		res = m.db.Model(&publiclink).Update("description", req.Update.GetDescription())
	case link.UpdatePublicShareRequest_Update_TYPE_NOTIFYUPLOADS:
		res = m.db.Model(&publiclink).Update("notify_uploads", req.Update.GetNotifyUploads())
	case link.UpdatePublicShareRequest_Update_TYPE_NOTIFYUPLOADSEXTRARECIPIENTS:
		res = m.db.Model(&publiclink).Update("notify_uploads_extra_recipients", req.Update.GetNotifyUploadsExtraRecipients())
	default:
		return nil, fmt.Errorf("invalid update type: %v", req.GetUpdate().GetType())
	}
	if res.Error != nil {
		return nil, res.Error
	}

	return m.GetPublicShare(ctx, u, req.Ref, true)

}

func (m *mgr) GetPublicShare(ctx context.Context, u *user.User, ref *link.PublicShareReference, sign bool) (*link.PublicShare, error) {
	var ln *model.PublicLink
	var err error
	switch {
	case ref.GetId() != nil:
		ln, err = m.getLinkByID(ctx, ref.GetId())
	case ref.GetToken() != "":
		ln, err = m.getLinkByToken(ctx, ref.GetToken())
	default:
		err = errtypes.NotFound(ref.String())
	}
	if err != nil {
		return nil, err
	}

	l := ln.AsCS3PublicShare()
	if ln.Password != "" && sign {
		if err := publicshare.AddSignature(l, ln.Password); err != nil {
			return nil, err
		}
	}

	return l, nil
}

func (m *mgr) ListPublicShares(ctx context.Context, u *user.User, filters []*link.ListPublicSharesRequest_Filter, md *provider.ResourceInfo, sign bool) ([]*link.PublicShare, error) {

	query := m.db.Model(&model.Share{}).
		Where("orphan = ?", false)

	// Append filters
	m.appendLinkFiltersToQuery(query, filters)

	var links []model.PublicLink
	var cs3links []*link.PublicShare
	res := query.Find(&links)
	if res.Error != nil {
		return nil, res.Error
	}

	for _, l := range links {
		cs3links = append(cs3links, l.AsCS3PublicShare())
	}

	return cs3links, nil
}

func (m *mgr) RevokePublicShare(ctx context.Context, u *user.User, ref *link.PublicShareReference) error {
	var err error
	var publiclink *model.PublicLink
	if id := ref.GetId(); id != nil {
		publiclink, err = emptyLinkWithId(id.OpaqueId)
	} else {
		publiclink, err = m.getLinkByToken(ctx, ref.GetToken())
	}
	if err != nil {
		return err
	}
	res := m.db.Delete(&publiclink)
	return res.Error

}

func (m *mgr) GetPublicShareByToken(ctx context.Context, token string, auth *link.PublicShareAuthentication, sign bool) (*link.PublicShare, error) {
	publiclink, err := m.getLinkByToken(ctx, token)
	if err != nil {
		return nil, err
	}

	cs3link := publiclink.AsCS3PublicShare()

	if sign {
		// TODO; what is the signature? Why does it require the password?
		if err := publicshare.AddSignature(cs3link, publiclink.Password); err != nil {
			return nil, err
		}
	}

	return cs3link, nil
}

// Get Link by ID. Does not return orphans or expired links.
func (m *mgr) getLinkByID(ctx context.Context, id *link.PublicShareId) (*model.PublicLink, error) {
	var link model.PublicLink
	res := m.db.First(&link, id.OpaqueId)

	if res.RowsAffected == 0 || link.Orphan || isExpired(link) {
		return nil, errtypes.NotFound(id.OpaqueId)
	}

	return &link, nil
}

// Get Link by token. Does not return orphans.
func (m *mgr) getLinkByToken(ctx context.Context, token string) (*model.PublicLink, error) {
	var link model.PublicLink
	res := m.db.First(&link).
		Where("token = ?", token)

	if res.RowsAffected == 0 || link.Orphan || isExpired(link) {
		return nil, errtypes.NotFound(token)
	}

	return &link, nil
}

func hashPassword(password string, cost int) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), cost)
	return "1|" + string(bytes), err
}

func isExpired(l model.PublicLink) bool {
	if l.Expiration.Valid {
		expTime := l.Expiration.V
		return time.Now().After(expTime)
	}
	return false
}

func emptyLinkWithId(id string) (*model.PublicLink, error) {
	intId, err := strconv.Atoi(id)
	if err != nil {
		return nil, err
	}
	link := &model.PublicLink{
		ProtoShare: model.ProtoShare{
			Model: gorm.Model{
				ID: uint(intId),
			},
		},
	}
	return link, nil
}

func (m *mgr) appendLinkFiltersToQuery(query *gorm.DB, filters []*link.ListPublicSharesRequest_Filter) {
	// We want to chain filters of different types with AND
	// and filters of the same type with OR
	// Therefore, we group them by type
	groupedFilters := groupFiltersByType(filters)

	for filtertype, filters := range groupedFilters {
		switch filtertype {
		case link.ListPublicSharesRequest_Filter_TYPE_RESOURCE_ID:
			innerQuery := m.db
			for i, filter := range filters {
				if i == 0 {
					innerQuery = innerQuery.Where("instance = ? and inode = ?", filter.GetResourceId().StorageId, filter.GetResourceId().OpaqueId)
				} else {
					innerQuery = innerQuery.Or("instance = ? and inode = ?", filter.GetResourceId().StorageId, filter.GetResourceId().OpaqueId)
				}
			}
			query = query.Where(innerQuery)
		case link.ListPublicSharesRequest_Filter_TYPE_OWNER:
			innerQuery := m.db
			for i, filter := range filters {
				if i == 0 {
					innerQuery = innerQuery.Where("uid_owner = ?", conversions.FormatUserID(filter.GetOwner()))
				} else {
					innerQuery = innerQuery.Or("uid_owner = ?", conversions.FormatUserID(filter.GetOwner()))
				}
			}
			query = query.Where(innerQuery)

		case link.ListPublicSharesRequest_Filter_TYPE_CREATOR:
			innerQuery := m.db
			for i, filter := range filters {
				if i == 0 {
					innerQuery = innerQuery.Where("uid_initiator = ?", conversions.FormatUserID(filter.GetCreator()))
				} else {
					innerQuery = innerQuery.Or("uid_initiator = ?", conversions.FormatUserID(filter.GetCreator()))
				}
			}
			query = query.Where(innerQuery)
		default:
			break
		}
	}
}

func groupFiltersByType(filters []*link.ListPublicSharesRequest_Filter) map[link.ListPublicSharesRequest_Filter_Type][]*link.ListPublicSharesRequest_Filter {
	grouped := make(map[link.ListPublicSharesRequest_Filter_Type][]*link.ListPublicSharesRequest_Filter)
	for _, f := range filters {
		grouped[f.Type] = append(grouped[f.Type], f)
	}
	return grouped
}

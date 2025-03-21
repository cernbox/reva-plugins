// Copyright 2018-2025 CERN
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
	"strconv"
	"strings"
	"time"

	model "github.com/cernbox/reva-plugins/share"
	user "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	link "github.com/cs3org/go-cs3apis/cs3/sharing/link/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva"
	"github.com/cs3org/reva/pkg/appctx"
	conversions "github.com/cs3org/reva/pkg/cbox/utils"
	"github.com/cs3org/reva/pkg/errtypes"
	"github.com/cs3org/reva/pkg/publicshare"
	"github.com/cs3org/reva/pkg/utils"
	"github.com/cs3org/reva/pkg/utils/cfg"
	"github.com/pkg/errors"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	// Provides mysql drivers.
	_ "github.com/go-sql-driver/mysql"
)

type publicShareMgr struct {
	c  *config
	db *gorm.DB
}

func (publicShareMgr) RevaPlugin() reva.PluginInfo {
	return reva.PluginInfo{
		ID:  "grpc.services.publicshareprovider.drivers.sql",
		New: NewPublicShareManager,
	}
}

func NewPublicShareManager(ctx context.Context, m map[string]interface{}) (publicshare.Manager, error) {
	var c config
	if err := cfg.Decode(m, &c); err != nil {
		return nil, err
	}

	db, err := getDb(c)
	if err != nil {
		return nil, err
	}

	// Migrate schemas
	err = db.AutoMigrate(&model.PublicLink{})

	if err != nil {
		return nil, err
	}

	mgr := &publicShareMgr{
		c:  &c,
		db: db,
	}
	return mgr, nil
}

// These follow the interface defined in github.com/cs3org/reva/pkg/publishare/publicshare.go

func (m *publicShareMgr) CreatePublicShare(ctx context.Context, u *user.User, md *provider.ResourceInfo, g *link.Grant, description string, internal bool, notifyUploads bool, notifyUploadsExtraRecipients string) (*link.PublicShare, error) {
	user := appctx.ContextMustGetUser(ctx)
	if user == nil {
		return nil, errors.New("no user found in context")
	}
	token := utils.RandString(15)

	quicklink := false
	var displayName string
	if md.ArbitraryMetadata != nil {
		quicklink, _ = strconv.ParseBool(md.ArbitraryMetadata.Metadata["quicklink"])
		displayName = md.ArbitraryMetadata.Metadata["name"]
	}

	publiclink := &model.PublicLink{
		Quicklink:                    quicklink,
		Token:                        token,
		LinkName:                     displayName,
		NotifyUploads:                notifyUploads,
		NotifyUploadsExtraRecipients: notifyUploadsExtraRecipients,
	}

	// Create Shared ID
	id, err := createID(m.db)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create id for PublicShare")
	}

	publiclink.BaseModel = model.BaseModel{
		Id:      id,
		ShareId: model.ShareID{ID: id},
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

func (m *publicShareMgr) UpdatePublicShare(ctx context.Context, u *user.User, req *link.UpdatePublicShareRequest, g *link.Grant) (*link.PublicShare, error) {
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
		res = m.db.Model(&publiclink).
			Where("id = ?", publiclink.Id).
			Update("link_name", req.Update.GetDisplayName())
	case link.UpdatePublicShareRequest_Update_TYPE_PERMISSIONS:
		permissions := conversions.SharePermToInt(req.Update.GetGrant().GetPermissions().Permissions)
		res = m.db.Model(&publiclink).
			Where("id = ?", publiclink.Id).
			Update("permissions", uint8(permissions))
	case link.UpdatePublicShareRequest_Update_TYPE_EXPIRATION:
		res = m.db.Model(&publiclink).
			Where("id = ?", publiclink.Id).
			Update("expiration", time.Unix(int64(req.Update.GetGrant().Expiration.Seconds), 0))
	case link.UpdatePublicShareRequest_Update_TYPE_PASSWORD:
		if req.Update.GetGrant().Password == "" {
			// Remove the password
			res = m.db.Model(&publiclink).
				Where("id = ?", publiclink.Id).
				Update("password", "")
		} else {
			// Update the password
			hashedPwd, err := hashPassword(req.Update.GetGrant().Password, m.c.LinkPasswordHashCost)
			if err != nil {
				return nil, errors.Wrap(err, "could not hash share password")
			}
			res = m.db.Model(&publiclink).
				Where("id = ?", publiclink.Id).
				Update("password", hashedPwd)
		}
	case link.UpdatePublicShareRequest_Update_TYPE_DESCRIPTION:
		res = m.db.Model(&publiclink).
			Where("id = ?", publiclink.Id).
			Update("description", req.Update.GetDescription())
	case link.UpdatePublicShareRequest_Update_TYPE_NOTIFYUPLOADS:
		res = m.db.Model(&publiclink).
			Where("id = ?", publiclink.Id).
			Update("notify_uploads", req.Update.GetNotifyUploads())
	case link.UpdatePublicShareRequest_Update_TYPE_NOTIFYUPLOADSEXTRARECIPIENTS:
		res = m.db.Model(&publiclink).
			Where("id = ?", publiclink.Id).
			Update("notify_uploads_extra_recipients", req.Update.GetNotifyUploadsExtraRecipients())
	default:
		return nil, fmt.Errorf("invalid update type: %v", req.GetUpdate().GetType())
	}
	if res.Error != nil {
		return nil, res.Error
	}

	return m.GetPublicShare(ctx, u, req.Ref, true)

}

func (m *publicShareMgr) GetPublicShare(ctx context.Context, u *user.User, ref *link.PublicShareReference, sign bool) (*link.PublicShare, error) {
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

// List public shares that match the given filters
func (m *publicShareMgr) ListPublicShares(ctx context.Context, u *user.User, filters []*link.ListPublicSharesRequest_Filter, md *provider.ResourceInfo, sign bool) ([]*link.PublicShare, error) {
	query := m.db.Model(&model.PublicLink{}).
		Where("orphan = ?", false)

	if u != nil {
		uid := conversions.FormatUserID(u.Id)
		query = query.Where("uid_owner = ? or uid_initiator = ?", uid, uid)
	}

	// Append filters
	m.appendLinkFiltersToQuery(query, filters)

	var links []model.PublicLink
	var cs3links []*link.PublicShare
	res := query.Find(&links)
	if res.Error != nil {
		return nil, res.Error
	}

	for _, l := range links {
		if !isExpired(l) {
			cs3links = append(cs3links, l.AsCS3PublicShare())
		}
	}

	return cs3links, nil
}

func (m *publicShareMgr) RevokePublicShare(ctx context.Context, u *user.User, ref *link.PublicShareReference) error {
	publiclink, err := m.getEmptyPLByRef(ctx, ref)
	if err != nil {
		return err
	}
	res := m.db.Where("id = ?", publiclink.Id).Delete(&publiclink)
	return res.Error

}

// Get a PublicShare identified by token. This function returns `errtypes.InvalidCredentials` if `auth` does not contain
// a valid password or signature in case the PublicShare is password-protected
func (m *publicShareMgr) GetPublicShareByToken(ctx context.Context, token string, auth *link.PublicShareAuthentication, sign bool) (*link.PublicShare, error) {
	publiclink, err := m.getLinkByToken(ctx, token)
	if err != nil {
		return nil, err
	}

	cs3link := publiclink.AsCS3PublicShare()

	// If the link has a password, check that it was provided correctly
	if publiclink.Password != "" {
		if !isValidAuthForLink(publiclink, auth) {
			return nil, errtypes.InvalidCredentials(token)
		}

		if sign {
			if err := publicshare.AddSignature(cs3link, publiclink.Password); err != nil {
				return nil, err
			}
		}

	}

	return cs3link, nil
}

// Get Link by ID. Does not return orphans or expired links.
func (m *publicShareMgr) getLinkByID(ctx context.Context, id *link.PublicShareId) (*model.PublicLink, error) {
	var link model.PublicLink
	res := m.db.Where("id = ?", id.OpaqueId).First(&link)

	if res.RowsAffected == 0 || link.Orphan || isExpired(link) {
		return nil, errtypes.NotFound(id.OpaqueId)
	}

	return &link, nil
}

// Get Link by token. Does not return orphans or expired links.
func (m *publicShareMgr) getLinkByToken(ctx context.Context, token string) (*model.PublicLink, error) {
	if token == "" {
		return nil, errors.New("no token provided to getLinkByToken")
	}

	var link model.PublicLink
	res := m.db.Model(&model.PublicLink{}).
		Where("token = ?", token).
		First(&link)

	if res.RowsAffected == 0 || link.Orphan || isExpired(link) {
		return nil, errtypes.NotFound(token)
	}

	if res.Error != nil {
		return nil, res.Error
	}

	return &link, nil
}

func hashPassword(password string, cost int) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), cost)
	return "1|" + string(bytes), err
}

func checkPasswordHash(password, hash string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(strings.TrimPrefix(hash, "1|")), []byte(password))
	return err == nil
}

func isValidAuthForLink(link *model.PublicLink, auth *link.PublicShareAuthentication) bool {
	switch {
	case auth.GetPassword() != "":
		return checkPasswordHash(auth.GetPassword(), link.Password)
	case auth.GetSignature() != nil:
		sig := auth.GetSignature()
		now := time.Now()
		expiration := time.Unix(int64(sig.GetSignatureExpiration().GetSeconds()), int64(sig.GetSignatureExpiration().GetNanos()))
		if now.After(expiration) {
			return false
		}
		s, err := publicshare.CreateSignature(link.Token, link.Password, expiration)
		if err != nil {
			// TODO(labkode): pass context to call to log err.
			// No we are blind
			return false
		}
		return sig.GetSignature() == s
	}
	return false
}

func isExpired(l model.PublicLink) bool {
	if l.Expiration.Valid {
		expTime := l.Expiration.V
		return time.Now().After(expTime)
	}
	return false
}

// Returns a Public Link containing at least the id field, but not necessarily more
func (m *publicShareMgr) getEmptyPLByRef(ctx context.Context, ref *link.PublicShareReference) (*model.PublicLink, error) {
	var err error
	var publiclink *model.PublicLink

	if id := ref.GetId(); id != nil {
		publiclink, err = emptyLinkWithId(id.OpaqueId)
	} else {
		publiclink, err = m.getLinkByToken(ctx, ref.GetToken())
	}
	return publiclink, err
}

func emptyLinkWithId(id string) (*model.PublicLink, error) {
	intId, err := strconv.Atoi(id)
	if err != nil {
		return nil, err
	}
	share := &model.PublicLink{
		ProtoShare: model.ProtoShare{
			BaseModel: model.BaseModel{
				Id: uint(intId),
			},
		},
	}
	return share, nil
}

func (m *publicShareMgr) appendLinkFiltersToQuery(query *gorm.DB, filters []*link.ListPublicSharesRequest_Filter) {
	// We want to chain filters of different types with AND
	// and filters of the same type with OR
	// Therefore, we group them by type
	groupedFilters := publicshare.GroupFiltersByType(filters)

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
		}
	}
}

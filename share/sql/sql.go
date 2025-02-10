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
	"strconv"
	"strings"

	model "github.com/cernbox/reva-plugins/share"
	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	collaboration "github.com/cs3org/go-cs3apis/cs3/sharing/collaboration/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva"
	"github.com/cs3org/reva/pkg/appctx"
	conversions "github.com/cs3org/reva/pkg/cbox/utils"
	"github.com/cs3org/reva/pkg/errtypes"
	"github.com/cs3org/reva/pkg/rgrpc/status"
	"github.com/cs3org/reva/pkg/rgrpc/todo/pool"
	revashare "github.com/cs3org/reva/pkg/share"
	"github.com/cs3org/reva/pkg/sharedconf"
	"github.com/cs3org/reva/pkg/utils"
	"github.com/cs3org/reva/pkg/utils/cfg"

	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	// Provides mysql drivers.
	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"google.golang.org/genproto/protobuf/field_mask"
)

const (
	projectInstancesPrefix        = "newproject"
	projectSpaceGroupsPrefix      = "cernbox-project-"
	projectSpaceAdminGroupsSuffix = "-admins"
	projectPathPrefix             = "/eos/project/"
)

func init() {
	reva.RegisterPlugin(mgr{})
}

func (mgr) RevaPlugin() reva.PluginInfo {
	return reva.PluginInfo{
		ID:  "grpc.services.usershareprovider.drivers.sql",
		New: New,
	}
}

type config struct {
	Engine     string `mapstructure:"engine"` // mysql | sqlite
	DBUsername string `mapstructure:"db_username"`
	DBPassword string `mapstructure:"db_password"`
	DBHost     string `mapstructure:"db_host"`
	DBPort     int    `mapstructure:"db_port"`
	DBName     string `mapstructure:"db_name"`
	GatewaySvc string `mapstructure:"gatewaysvc"`
}

type mgr struct {
	c  *config
	db *gorm.DB
}

func (c *config) ApplyDefaults() {
	c.GatewaySvc = sharedconf.GetGatewaySVC(c.GatewaySvc)
}

// New returns a new share manager.
func New(ctx context.Context, m map[string]interface{}) (revashare.Manager, error) {
	var c config
	if err := cfg.Decode(m, &c); err != nil {
		return nil, err
	}

	var db *gorm.DB
	var err error
	switch c.Engine {
	case "sqlite":
		db, err = gorm.Open(sqlite.Open(c.DBName), &gorm.Config{})
	case "mysql":
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", c.DBUsername, c.DBPassword, c.DBHost, c.DBPort, c.DBName)
		db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	default: // default is mysql
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", c.DBUsername, c.DBPassword, c.DBHost, c.DBPort, c.DBName)
		db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	}
	if err != nil {
		return nil, err
	}

	// Migrate schemas
	err = db.AutoMigrate(&model.Share{}, &model.PublicLink{}, &model.ShareState{})

	if err != nil {
		return nil, err
	}

	return &mgr{
		c:  &c,
		db: db,
	}, nil
}

func (m *mgr) Share(ctx context.Context, md *provider.ResourceInfo, g *collaboration.ShareGrant) (*collaboration.Share, error) {
	user := appctx.ContextMustGetUser(ctx)

	// do not allow share to myself or the owner if share is for a user
	// TODO(labkode): should not this be caught already at the gw level?
	if g.Grantee.Type == provider.GranteeType_GRANTEE_TYPE_USER &&
		(utils.UserEqual(g.Grantee.GetUserId(), user.Id) || utils.UserEqual(g.Grantee.GetUserId(), md.Owner)) {
		return nil, errors.New("sql: owner/creator and grantee are the same")
	}

	// check if share already exists.
	key := &collaboration.ShareKey{
		Owner:      md.Owner,
		ResourceId: md.Id,
		Grantee:    g.Grantee,
	}
	_, err := m.getByKey(ctx, key, true)
	// share already exists
	// TODO stricter error checking
	if err == nil {
		return nil, errors.New(errtypes.AlreadyExists(key.String()).Error())
	}

	var shareWith string
	if g.Grantee.Type == provider.GranteeType_GRANTEE_TYPE_USER {
		shareWith = conversions.FormatUserID(g.Grantee.GetUserId())
	} else if g.Grantee.Type == provider.GranteeType_GRANTEE_TYPE_GROUP {
		// ShareWith is a group
		shareWith = g.Grantee.GetGroupId().OpaqueId
	} else {
		return nil, errors.New("Unsuppored grantee type passed to Share()")
	}

	share := &model.Share{
		ShareWith:         shareWith,
		SharedWithIsGroup: g.Grantee.Type == provider.GranteeType_GRANTEE_TYPE_GROUP,
	}
	share.UIDOwner = conversions.FormatUserID(md.Owner)
	share.UIDInitiator = conversions.FormatUserID(user.Id)
	share.InitialPath = md.Path
	share.ItemType = model.ItemType(conversions.ResourceTypeToItem(md.Type))
	share.Inode = md.Id.OpaqueId
	share.Instance = md.Id.StorageId
	share.Permissions = uint8(conversions.SharePermToInt(g.Permissions.Permissions))
	share.Orphan = false

	res := m.db.Save(&share)
	if res.Error != nil {
		return nil, res.Error
	}

	granteeType, _ := m.getUserType(ctx, share.ShareWith)
	return share.AsCS3Share(granteeType), nil
}

// Get Share by ID. Does not return orphans.
func (m *mgr) getByID(ctx context.Context, id *collaboration.ShareId) (*model.Share, error) {
	var share model.Share
	res := m.db.First(&share, id.OpaqueId)

	if res.RowsAffected == 0 || share.Orphan {
		return nil, errtypes.NotFound(id.OpaqueId)
	}

	return &share, nil
}

// Get Share by Key. Does not return orphans.
func (m *mgr) getByKey(ctx context.Context, key *collaboration.ShareKey, checkOwner bool) (*model.Share, error) {
	owner := conversions.FormatUserID(key.Owner)

	var share model.Share
	_, shareWith := conversions.FormatGrantee(key.Grantee)

	query := m.db.Model(&share).
		Where("orphan = ?", false).
		Where("uid_owner = ?", owner).
		Where("instance = ?", key.ResourceId.StorageId).
		Where("inode = ?", key.ResourceId.OpaqueId).
		Where("shared_with_is_group = ?", key.Grantee.Type == provider.GranteeType_GRANTEE_TYPE_GROUP).
		Where("share_with = ?", strings.ToLower(shareWith))

	uid := conversions.FormatUserID(appctx.ContextMustGetUser(ctx).Id)
	// In case the user is not the owner (i.e. in the case of projects)
	if checkOwner && owner != uid {
		query = query.Where("uid_initiator = ?", uid)
	}

	res := query.First(&share)

	if res.RowsAffected == 0 {
		return nil, errtypes.NotFound(key.String())
	}

	return &share, nil
}

func (m *mgr) getShare(ctx context.Context, ref *collaboration.ShareReference) (*model.Share, error) {
	var s *model.Share
	var err error
	switch {
	case ref.GetId() != nil:
		s, err = m.getByID(ctx, ref.GetId())
	case ref.GetKey() != nil:
		s, err = m.getByKey(ctx, ref.GetKey(), false)
	default:
		return nil, errtypes.NotFound(ref.String())
	}
	if err != nil {
		return nil, err
	}

	user := appctx.ContextMustGetUser(ctx)
	if s.UIDOwner == user.Id.OpaqueId && s.UIDInitiator == user.Id.OpaqueId {
		return s, nil
	}

	path, err := m.getPath(ctx, &provider.ResourceId{
		StorageId: s.Instance,
		OpaqueId:  s.Inode,
	})
	if err != nil {
		return nil, err
	}

	if m.isProjectAdmin(user, path) {
		return s, nil
	}

	return nil, errtypes.NotFound(ref.String())
}

func (m *mgr) GetShare(ctx context.Context, ref *collaboration.ShareReference) (*collaboration.Share, error) {
	share, err := m.getShare(ctx, ref)
	if err != nil {
		return nil, err
	}

	granteeType, _ := m.getUserType(ctx, share.ShareWith)
	cs3share := share.AsCS3Share(granteeType)

	return cs3share, nil
}

func (m *mgr) Unshare(ctx context.Context, ref *collaboration.ShareReference) error {
	var share *model.Share
	var err error
	if id := ref.GetId(); id != nil {
		share, err = emptyShareWithId(id.OpaqueId)
	} else {
		share, err = m.getShare(ctx, ref)
	}
	if err != nil {
		return err
	}
	res := m.db.Delete(&share)
	return res.Error
}

func (m *mgr) UpdateShare(ctx context.Context, ref *collaboration.ShareReference, p *collaboration.SharePermissions) (*collaboration.Share, error) {
	var share *model.Share
	var err error
	if id := ref.GetId(); id != nil {
		share, err = emptyShareWithId(id.OpaqueId)
	} else {
		share, err = m.getShare(ctx, ref)
	}
	if err != nil {
		return nil, err
	}

	permissions := conversions.SharePermToInt(p.Permissions)
	res := m.db.Model(&share).Update("permissions", uint8(permissions))
	if res.Error != nil {
		return nil, res.Error
	}

	return m.GetShare(ctx, ref)
}

func (m *mgr) getPath(ctx context.Context, resID *provider.ResourceId) (string, error) {
	client, err := pool.GetGatewayServiceClient(pool.Endpoint(m.c.GatewaySvc))
	if err != nil {
		return "", err
	}

	res, err := client.GetPath(ctx, &provider.GetPathRequest{
		ResourceId: resID,
	})

	if err != nil {
		return "", err
	}

	if res.Status.Code == rpc.Code_CODE_OK {
		return res.GetPath(), nil
	} else if res.Status.Code == rpc.Code_CODE_NOT_FOUND {
		return "", errtypes.NotFound(resID.OpaqueId)
	}
	return "", errors.New(res.Status.Code.String() + ": " + res.Status.Message)
}

func (m *mgr) isProjectAdmin(u *userpb.User, path string) bool {
	if strings.HasPrefix(path, projectPathPrefix) {
		// The path will look like /eos/project/c/cernbox, we need to extract the project name
		parts := strings.SplitN(path, "/", 6)
		if len(parts) < 5 {
			return false
		}

		adminGroup := projectSpaceGroupsPrefix + parts[4] + projectSpaceAdminGroupsSuffix
		for _, g := range u.Groups {
			if g == adminGroup {
				// User belongs to the admin group, list all shares for the resource

				return true
			}
		}
	}
	return false
}

func (m *mgr) ListShares(ctx context.Context, filters []*collaboration.Filter) ([]*collaboration.Share, error) {
	uid := conversions.FormatUserID(appctx.ContextMustGetUser(ctx).Id)

	query := m.db.Model(&model.Share{}).
		Where("uid_owner = ? or uid_initiator = ?", uid, uid).
		Where("orphan = ?", false)

	// Append filters
	m.appendFiltersToQuery(query, filters)

	var shares []model.Share
	var cs3shares []*collaboration.Share
	res := query.Find(&shares)
	if res.Error != nil {
		return nil, res.Error
	}

	for _, s := range shares {
		granteeType, _ := m.getUserType(ctx, s.ShareWith)
		cs3share := s.AsCS3Share(granteeType)
		cs3shares = append(cs3shares, cs3share)
	}

	return cs3shares, nil
}

// we list the shares that are targeted to the user in context or to the user groups.
func (m *mgr) ListReceivedShares(ctx context.Context, filters []*collaboration.Filter) ([]*collaboration.ReceivedShare, error) {
	user := appctx.ContextMustGetUser(ctx)

	// We need to do this to parse the result
	// Normally, GORM would be able to fill in the Share that is referenced in ShareState
	// However, in GORM's docs: "Join Preload will loads association data using left join"
	// Because we do a RIGHT JOIN, GORM cannot load the data into shareState.Share (in case that ShareState is empty)
	// So we load them both separately, and then set ShareState.Share = Share ourselves
	var results []struct {
		model.ShareState
		model.Share
	}

	query := m.db.Model(&model.ShareState{}).
		Select("share_states.*, shares.*").
		Joins("RIGHT OUTER JOIN shares ON shares.id = share_states.share_id and share_states.user = ?", user.Username).
		Where("shares.orphan = ?", false)

	// Also search by all the groups the user is a member of
	innerQuery := m.db.Where("shares.share_with = ? and shares.shared_with_is_group = ?", user.Username, false)
	for _, group := range user.Groups {
		innerQuery = innerQuery.Or("shares.share_with = ? and shares.shared_with_is_group = ?", group, true)
	}
	query = query.Where(innerQuery)

	// Append filters
	m.appendFiltersToQuery(query, filters)

	// Get the shares + states
	res := query.Find(&results)
	if res.Error != nil {
		return nil, res.Error
	}

	var receivedShares []*collaboration.ReceivedShare

	// Now we parse everything into the CS3 definition of a CS3ReceivedShare
	for _, res := range results {
		shareState := res.ShareState
		shareState.Share = res.Share
		granteeType, _ := m.getUserType(ctx, res.Share.ShareWith)

		receivedShares = append(receivedShares, res.Share.AsCS3ReceivedShare(&shareState, granteeType))
	}

	return receivedShares, nil
}

func (m *mgr) getShareState(ctx context.Context, share *model.Share, user *userpb.User) (*model.ShareState, error) {
	var shareState model.ShareState
	query := m.db.Model(&shareState).
		Where("share_id = ?", share.ID).
		Where("user = ?", user.Username)

	res := query.First(&shareState)

	if res.RowsAffected == 0 {
		// If no share state has been created yet, we create it now using these defaults
		shareState = model.ShareState{
			Share:  *share,
			Hidden: false,
			Synced: false,
			User:   user.Username,
		}
	}

	return &shareState, nil
}

func emptyShareWithId(id string) (*model.Share, error) {
	intId, err := strconv.Atoi(id)
	if err != nil {
		return nil, err
	}
	share := &model.Share{
		ProtoShare: model.ProtoShare{
			Model: gorm.Model{
				ID: uint(intId),
			},
		},
	}
	return share, nil
}

func (m *mgr) getReceivedByID(ctx context.Context, id *collaboration.ShareId, gtype userpb.UserType) (*collaboration.ReceivedShare, error) {
	user := appctx.ContextMustGetUser(ctx)
	share, err := m.getByID(ctx, id)
	if err != nil {
		return nil, err
	}

	shareState, err := m.getShareState(ctx, share, user)
	if err != nil {
		return nil, err
	}

	receivedShare := share.AsCS3ReceivedShare(shareState, gtype)
	return receivedShare, nil
}

func (m *mgr) getReceivedByKey(ctx context.Context, key *collaboration.ShareKey, gtype userpb.UserType) (*collaboration.ReceivedShare, error) {
	user := appctx.ContextMustGetUser(ctx)
	share, err := m.getByKey(ctx, key, false)
	if err != nil {
		return nil, err
	}

	shareState, err := m.getShareState(ctx, share, user)
	if err != nil {
		return nil, err
	}

	receivedShare := share.AsCS3ReceivedShare(shareState, gtype)
	return receivedShare, nil
}

func (m *mgr) GetReceivedShare(ctx context.Context, ref *collaboration.ShareReference) (*collaboration.ReceivedShare, error) {
	var s *collaboration.ReceivedShare
	var err error
	switch {
	case ref.GetId() != nil:
		s, err = m.getReceivedByID(ctx, ref.GetId(), userpb.UserType_USER_TYPE_INVALID)
	case ref.GetKey() != nil:
		s, err = m.getReceivedByKey(ctx, ref.GetKey(), userpb.UserType_USER_TYPE_INVALID)
	default:
		err = errtypes.NotFound(ref.String())
	}

	if err != nil {
		return nil, err
	}

	// resolve grantee's user type if applicable
	if s.Share.Grantee.Type == provider.GranteeType_GRANTEE_TYPE_USER {
		s.Share.Grantee.GetUserId().Type, _ = m.getUserType(ctx, s.Share.Grantee.GetUserId().OpaqueId)
	}

	return s, nil
}

func (m *mgr) UpdateReceivedShare(ctx context.Context, recvShare *collaboration.ReceivedShare, fieldMask *field_mask.FieldMask) (*collaboration.ReceivedShare, error) {

	user := appctx.ContextMustGetUser(ctx)

	rs, err := m.getReceivedByID(ctx, recvShare.Share.Id, user.Id.Type)
	if err != nil {
		return nil, err
	}

	share, err := emptyShareWithId(recvShare.Share.Id.OpaqueId)
	if err != nil {
		return nil, err
	}

	shareState, err := m.getShareState(ctx, share, user)
	if err != nil {
		return nil, err
	}

	// FieldMask determines which parts of the share we actually update
	for _, path := range fieldMask.Paths {
		switch path {
		case "state":
			rs.State = recvShare.State
			switch rs.State {
			case collaboration.ShareState_SHARE_STATE_ACCEPTED:
				shareState.Hidden = false
			case collaboration.ShareState_SHARE_STATE_REJECTED:
				shareState.Hidden = true
			}
		case "hidden":
			rs.Hidden = recvShare.Hidden
		default:
			return nil, errtypes.NotSupported("updating " + path + " is not supported")
		}
	}

	// Now we do the actual update to the db model

	res := m.db.Save(&shareState)
	if res.Error != nil {
		return nil, res.Error
	}

	return rs, nil
}

func (m *mgr) getUserType(ctx context.Context, username string) (userpb.UserType, error) {
	client, err := pool.GetGatewayServiceClient(pool.Endpoint(m.c.GatewaySvc))
	if err != nil {
		return userpb.UserType_USER_TYPE_PRIMARY, err
	}
	userRes, err := client.GetUserByClaim(ctx, &userpb.GetUserByClaimRequest{
		Claim: "username",
		Value: username,
	})
	if err != nil {
		return userpb.UserType_USER_TYPE_PRIMARY, errors.Wrapf(err, "error getting user by username '%v'", username)
	}
	if userRes.Status.Code != rpc.Code_CODE_OK {
		return userpb.UserType_USER_TYPE_PRIMARY, status.NewErrorFromCode(userRes.Status.Code, "oidc")
	}

	return userRes.GetUser().Id.Type, nil
}

func (m *mgr) appendFiltersToQuery(query *gorm.DB, filters []*collaboration.Filter) {
	// We want to chain filters of different types with AND
	// and filters of the same type with OR
	// Therefore, we group them by type
	groupedFilters := revashare.GroupFiltersByType(filters)

	for filtertype, filters := range groupedFilters {
		switch filtertype {
		case collaboration.Filter_TYPE_RESOURCE_ID:
			innerQuery := m.db
			for i, filter := range filters {
				if i == 0 {
					innerQuery = innerQuery.Where("instance = ? and inode = ?", filter.GetResourceId().StorageId, filter.GetResourceId().OpaqueId)
				} else {
					innerQuery = innerQuery.Or("instance = ? and inode = ?", filter.GetResourceId().StorageId, filter.GetResourceId().OpaqueId)
				}
			}
			query = query.Where(innerQuery)
		case collaboration.Filter_TYPE_EXCLUDE_DENIALS:
			query = query.Where("permissions > ?", 0)
		case collaboration.Filter_TYPE_GRANTEE_TYPE:
			innerQuery := m.db
			for i, filter := range filters {
				isGroup := filter.GetGranteeType() == provider.GranteeType_GRANTEE_TYPE_GROUP
				if i == 0 {
					innerQuery = innerQuery.Where("shared_with_is_group = ?", isGroup)
				} else {
					innerQuery = innerQuery.Or("shared_with_is_group = ? ", isGroup)
				}
			}
			query = query.Where(innerQuery)
		default:
			break
		}
	}
}

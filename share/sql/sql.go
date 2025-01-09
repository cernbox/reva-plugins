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
	c *config
	//db *sql.DB
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
	case "mysql":
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", c.DBUsername, c.DBPassword, c.DBHost, c.DBPort, c.DBName)
		db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	case "sqlite":
		db, err = gorm.Open(sqlite.Open(c.DBName), &gorm.Config{})
	default:
		return nil, errors.New("ShareManager SQL: unsupported database type " + c.Engine)
	}
	if err != nil {
		return nil, err
	}

	// Migrate schemas
	db.AutoMigrate(&model.Share{}, &model.PublicLink{}, &model.ShareState{})

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

	if checkOwner {
		uid := conversions.FormatUserID(appctx.ContextMustGetUser(ctx).Id)
		query = query.
			Where("uid_owner = ? or uid_initiator = ?", uid, uid)
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
		err = errtypes.NotFound(ref.String())
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
	share, err := m.getShare(ctx, ref)
	if err != nil {
		return err
	}
	res := m.db.Delete(&share)
	return res.Error
}

func (m *mgr) UpdateShare(ctx context.Context, ref *collaboration.ShareReference, p *collaboration.SharePermissions) (*collaboration.Share, error) {
	permissions := conversions.SharePermToInt(p.Permissions)
	share, err := m.getShare(ctx, ref)
	if err != nil {
		return nil, err
	}

	share.Permissions = uint8(permissions)

	res := m.db.Save(&share)
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

	return res.GetPath(), nil
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

				// TODO: this only works if shares for a single project are requested.
				// If shares for multiple projects are requested, then we're not checking if the
				// user is an admin for all of those. We can append the query ` or uid_owner=?`
				// for all the project owners, which works fine for new reva
				// but won't work for revaold since there, we store the uid of the share creator as uid_owner.
				// For this to work across the two versions, this change would have to be made in revaold
				// but it won't be straightforward as there, the storage provider doesn't return the
				// resource owners.
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

	query := m.db.Model(&model.Share{}).
		Where("orphan = ?", false)

	// Also search by all the groups the user is a member of
	innerQuery := m.db.Where("share_with = ? and shared_with_is_group = ?", user.Username, false)
	for _, group := range user.Groups {
		innerQuery = innerQuery.Or("share_with = ? and shared_with_is_group = ?", group, true)
	}
	query = query.Where(innerQuery)

	// Append filters
	m.appendFiltersToQuery(query, filters)

	// Get the shares
	var shares []model.Share
	res := query.Find(&shares)
	if res.Error != nil {
		return nil, res.Error
	}

	// Now that we have the shares, we fetch the share state for every share
	var receivedShares []*collaboration.ReceivedShare

	for _, s := range shares {
		shareId := &collaboration.ShareId{
			OpaqueId: strconv.FormatUint(uint64(s.ID), 10),
		}
		shareState, err := m.getShareState(ctx, shareId, user)
		if err != nil {
			return nil, err
		}

		granteeType, _ := m.getUserType(ctx, s.ShareWith)

		receivedShares = append(receivedShares, s.AsCS3ReceivedShare(shareState, granteeType))
	}

	return receivedShares, nil
}

func (m *mgr) getShareState(ctx context.Context, shareId *collaboration.ShareId, user *userpb.User) (*model.ShareState, error) {
	var shareState model.ShareState
	query := m.db.Model(&shareState).
		Where("share_id = ?", shareId.OpaqueId).
		Where("user = ?", user.Username)

	res := query.First(&shareState)

	if res.RowsAffected == 0 {
		shareIdInt, err := strconv.Atoi(shareId.OpaqueId)
		if err != nil {
			return nil, errors.New("Failed to fetch shareState, and failed to create one (share_id is not an int)")
		}
		// If no share state has been created yet, we create it now using these defaults
		shareState = model.ShareState{
			ShareID: uint(shareIdInt),
			Hidden:  false,
			Synced:  false,
			User:    user.Username,
		}
		// Does not really matter if it fails, next time the user
		// lists his shares this will just be called again
		m.db.Save(&shareState)
	}

	return &shareState, nil
}

func (m *mgr) getReceivedByID(ctx context.Context, id *collaboration.ShareId, gtype userpb.UserType) (*collaboration.ReceivedShare, error) {
	user := appctx.ContextMustGetUser(ctx)
	share, err := m.getByID(ctx, id)
	if err != nil {
		return nil, err
	}

	shareState, err := m.getShareState(ctx, id, user)
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

	shareId := &collaboration.ShareId{
		OpaqueId: strconv.Itoa(int(share.ID)),
	}

	shareState, err := m.getShareState(ctx, shareId, user)
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

func (m *mgr) UpdateReceivedShare(ctx context.Context, share *collaboration.ReceivedShare, fieldMask *field_mask.FieldMask) (*collaboration.ReceivedShare, error) {

	user := appctx.ContextMustGetUser(ctx)

	rs, err := m.GetReceivedShare(ctx, &collaboration.ShareReference{Spec: &collaboration.ShareReference_Id{Id: share.Share.Id}})
	if err != nil {
		return nil, err
	}

	shareState, err := m.getShareState(ctx, share.Share.Id, user)
	if err != nil {
		return nil, err
	}

	// FieldMask determines which parts of the share we actually update
	// Right now, only updating the state is supported
	for _, path := range fieldMask.Paths {
		switch path {
		case "state":
			rs.State = share.State
		case "hidden":
			rs.Hidden = share.Hidden
		default:
			return nil, errtypes.NotSupported("updating " + path + " is not supported")
		}
	}

	// Now we do the actual update to the db model
	switch rs.State {
	case collaboration.ShareState_SHARE_STATE_ACCEPTED:
		shareState.Hidden = false
	case collaboration.ShareState_SHARE_STATE_REJECTED:
		shareState.Hidden = true
	}

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

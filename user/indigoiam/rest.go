// Copyright 2018-2026 CERN
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

// Package indigoiam implements a Reva user provider driver backed by the
// Indigo IAM REST API
// (https://indigo-iam.github.io/v/current/docs/reference/api/account-api/).
package indigoiam

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/cernbox/reva-plugins/cache"
	redispools "github.com/cernbox/reva-plugins/redispools"
	"github.com/cernbox/reva-plugins/utils"
	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	"github.com/cs3org/reva/v3"
	"github.com/cs3org/reva/v3/pkg/appctx"
	"github.com/cs3org/reva/v3/pkg/user"
	"github.com/cs3org/reva/v3/pkg/utils/cfg"
)

func init() {
	reva.RegisterPlugin(manager{})
}

type manager struct {
	conf         *config
	cache        *cache.UserCache
	tokenManager *utils.APITokenManager
}

type primaryUser struct {
	Username  string `mapstructure:"username"`
	UIDNumber int64  `mapstructure:"uid_number"`
	GIDNumber int64  `mapstructure:"gid_number"`
}

func (manager) RevaPlugin() reva.PluginInfo {
	return reva.PluginInfo{
		ID:  "grpc.services.userprovider.drivers.indigo-iam",
		New: New,
	}
}

type config struct {
	RedisAddress         string `mapstructure:"redis_address"          docs:"localhost:6379"`
	RedisSentinelAddress string `mapstructure:"redis_sentinel_address" docs:""`
	RedisUsername        string `mapstructure:"redis_username"         docs:""`
	RedisPassword        string `mapstructure:"redis_password"         docs:""`
	RedisMasterName      string `mapstructure:"redis_master_name"      docs:""`
	RedisSentinelMode    bool   `mapstructure:"redis_sentinel_mode"    docs:"false"`

	IAMBaseURL string `mapstructure:"iam_base_url" docs:"https://iam.example.org"`
	IDProvider string `mapstructure:"id_provider" docs:"https://iam.example.org"`

	// Credentials for the client_credentials exchange yielding the admin token
	// for the IAM REST API. Consumed by utils.InitAPITokenManager, which handles
	// retrieval, caching and renewal.
	ClientID          string `mapstructure:"client_id"           docs:"-"`
	ClientSecret      string `mapstructure:"client_secret"       docs:"-"`
	OIDCTokenEndpoint string `mapstructure:"oidc_token_endpoint" docs:"https://iam.example.org/token"`
	TargetAPI         string `mapstructure:"target_api"          docs:"scim"`
	Scope             string `mapstructure:"scope"               docs:"iam:admin.read"`

	UserFetchInterval         int `mapstructure:"user_fetch_interval"          docs:"3600"`
	UserGroupsCacheExpiration int `mapstructure:"user_groups_cache_expiration" docs:"5"`
	PageSize                  int `mapstructure:"page_size"                    docs:"100"`

	// PrimaryUsers maps an IAM account UUID to a username, uid and gid, and marks
	// that account USER_TYPE_PRIMARY; every other one is USER_TYPE_LIGHTWEIGHT.
	// IAM has no native notion of the distinction, so it takes an allowlist.
	PrimaryUsers map[string]primaryUser `mapstructure:"primary_users" docs:"{}"`
}

func (c *config) ApplyDefaults() {
	if c.RedisAddress == "" {
		c.RedisAddress = ":6379"
	}
	if c.RedisSentinelAddress == "" {
		c.RedisSentinelAddress = c.RedisAddress
	}
	if c.IAMBaseURL == "" {
		c.IAMBaseURL = "https://iam.example.org"
	}
	if c.IDProvider == "" {
		c.IDProvider = c.IAMBaseURL
	}
	if c.UserFetchInterval == 0 {
		c.UserFetchInterval = 3600
	}
	if c.UserGroupsCacheExpiration == 0 {
		c.UserGroupsCacheExpiration = 5
	}
	if c.PageSize == 0 {
		c.PageSize = 100
	}
}

// ---------------------------------------------------------------------------
// Indigo IAM JSON shapes
// ---------------------------------------------------------------------------

type IndigoIAMName struct {
	Formatted  string `json:"formatted"`
	GivenName  string `json:"givenName"`
	FamilyName string `json:"familyName"`
}

type IndigoIAMEmail struct {
	Value   string `json:"value"`
	Primary bool   `json:"primary"`
}

type IndigoIAMGroupRef struct {
	Display string `json:"display"`
	Value   string `json:"value"`
}

// IndigoIAMAccount is the shape of a single resource in /iam/account/search.
type IndigoIAMAccount struct {
	ID          string              `json:"id"`
	UserName    string              `json:"userName"`
	DisplayName string              `json:"displayName"`
	Name        IndigoIAMName       `json:"name"`
	Active      bool                `json:"active"`
	Emails      []IndigoIAMEmail    `json:"emails"`
	Groups      []IndigoIAMGroupRef `json:"groups"`
}

func (a *IndigoIAMAccount) primaryEmail() string {
	for _, e := range a.Emails {
		if e.Primary {
			return e.Value
		}
	}
	if len(a.Emails) > 0 {
		return a.Emails[0].Value
	}
	return ""
}

// IndigoIAMAccountList is the paginated list returned by /iam/account/search.
type IndigoIAMAccountList struct {
	TotalResults int                `json:"totalResults"`
	ItemsPerPage int                `json:"itemsPerPage"`
	StartIndex   int                `json:"startIndex"`
	Resources    []IndigoIAMAccount `json:"Resources"`
}

// IndigoIAMGroupResource is the shape of a group in /iam/account/{id}/groups.
type IndigoIAMGroupResource struct {
	UUID        string `json:"uuid"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

// IndigoIAMGroupList is the paginated list returned by /iam/account/{id}/groups.
type IndigoIAMGroupList struct {
	TotalResults int                      `json:"totalResults"`
	ItemsPerPage int                      `json:"itemsPerPage"`
	StartIndex   int                      `json:"startIndex"`
	Resources    []IndigoIAMGroupResource `json:"Resources"`
}

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

// New creates and returns a user.Manager backed by Indigo IAM.
func New(ctx context.Context, m map[string]interface{}) (user.Manager, error) {
	mgr := &manager{}
	if err := mgr.Configure(m, ctx); err != nil {
		return nil, err
	}
	return mgr, nil
}

func (m *manager) Configure(ml map[string]interface{}, ctx context.Context) error {
	var c config
	if err := cfg.Decode(ml, &c); err != nil {
		return err
	}
	c.ApplyDefaults()

	pools, err := redispools.NewRedisPoolsWithSentinelAddress(
		ctx,
		c.RedisAddress, c.RedisSentinelAddress,
		c.RedisUsername, c.RedisPassword,
		c.RedisSentinelMode, c.RedisMasterName,
	)
	if err != nil {
		appctx.GetLogger(ctx).Error().Err(err).Msg("indigoiam user: failed to initialise Redis pools")
		pools = &redispools.RedisPools{}
	}

	tokenManager, err := utils.InitAPITokenManager(ml)
	if err != nil {
		return err
	}

	m.conf = &c
	m.cache = cache.NewUserCache(pools, c.UserFetchInterval, c.UserGroupsCacheExpiration)
	m.tokenManager = tokenManager

	go m.fetchAllUsers(context.Background())
	return nil
}

// ---------------------------------------------------------------------------
// Background bulk-fetch loop
// ---------------------------------------------------------------------------

func (m *manager) fetchAllUsers(ctx context.Context) {
	log := appctx.GetLogger(ctx)
	if err := m.fetchAllUserAccounts(ctx); err != nil {
		log.Error().Err(err).Msg("indigoiam user: initial bulk fetch failed")
	}

	ticker := time.NewTicker(time.Duration(m.conf.UserFetchInterval) * time.Second)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT)

	for {
		select {
		case <-sigs:
			return
		case <-ticker.C:
			if err := m.fetchAllUserAccounts(ctx); err != nil {
				log.Error().Err(err).Msg("indigoiam user: periodic bulk fetch failed")
			}
		}
	}
}

func (m *manager) fetchAllUserAccounts(ctx context.Context) error {
	log := appctx.GetLogger(ctx)
	startIndex := 1

	for {
		url := fmt.Sprintf(
			"%s/iam/account/search?startIndex=%d&count=%d&sortBy=name&sortDirection=asc",
			m.conf.IAMBaseURL, startIndex, m.conf.PageSize,
		)

		var list IndigoIAMAccountList
		if err := m.tokenManager.SendAPIGetRequest(ctx, url, false, &list); err != nil {
			return err
		}

		for i := range list.Resources {
			acc := &list.Resources[i]
			if !acc.Active {
				continue
			}
			u, remapped := m.accountToProto(acc)
			if err := m.cache.StoreUser(u); err != nil {
				log.Error().Err(err).Str("uuid", acc.ID).Msg("indigoiam user: cache error")
			}
			// Lightweight users already have OpaqueId == IAM UUID; only a remap
			// needs the reverse index, plus an eviction of the pre-remap record —
			// which StoreUser above cannot have overwritten, its keys differ.
			if remapped {
				if err := m.cache.EvictLightweightRecord(ctx, acc.ID, acc.UserName); err != nil {
					log.Error().Err(err).Str("uuid", acc.ID).Msg("indigoiam user: failed to evict stale lightweight record")
				}
				if err := m.cache.StoreIAMUUID(u.Id.OpaqueId, acc.ID); err != nil {
					log.Error().Err(err).Str("uuid", acc.ID).Msg("indigoiam user: failed to cache IAM UUID mapping")
				}
			}
		}

		nextStart := startIndex + list.ItemsPerPage
		if list.ItemsPerPage == 0 || nextStart > list.TotalResults {
			break
		}
		startIndex = nextStart
	}
	return nil
}

// accountToProto converts an IndigoIAMAccount to the CS3 userpb.User type.
// Accounts in conf.PrimaryUsers become PRIMARY, identified by their mapped CERN
// login and carrying its uid/gid; every other account is LIGHTWEIGHT, identified
// by its IAM account UUID with no uid/gid. OpaqueId and Username always hold the
// same value. The bool reports the PRIMARY case, which needs extra cache writes.
func (m *manager) accountToProto(acc *IndigoIAMAccount) (*userpb.User, bool) {
	// IAM sets displayName to the account UUID for federated accounts.
	displayName := acc.DisplayName
	if acc.Name.Formatted != "" {
		displayName = acc.Name.Formatted
	}

	// Lightweight is the default: the IAM UUID is both OpaqueId and Username,
	// the invariant CERNBox relies on (user/rest sets Username = FormatUserID).
	u := &userpb.User{
		Id: &userpb.UserId{
			OpaqueId: acc.ID,
			Idp:      m.conf.IDProvider,
			Type:     userpb.UserType_USER_TYPE_LIGHTWEIGHT,
		},
		Username:    acc.ID,
		Mail:        acc.primaryEmail(),
		DisplayName: displayName,
	}

	mapped, ok := m.conf.PrimaryUsers[acc.ID]
	if !ok {
		return u, false
	}
	u.Id.OpaqueId = mapped.Username
	u.Id.Type = userpb.UserType_USER_TYPE_PRIMARY
	u.Username = mapped.Username
	u.UidNumber = mapped.UIDNumber
	u.GidNumber = mapped.GIDNumber
	return u, true
}

// ---------------------------------------------------------------------------
// user.Manager interface
// ---------------------------------------------------------------------------

func (m *manager) GetUser(ctx context.Context, uid *userpb.UserId, skipFetchingGroups bool) (*userpb.User, error) {
	u, err := m.cache.GetByID(ctx, uid.OpaqueId)
	if err != nil {
		return nil, err
	}
	if !skipFetchingGroups {
		groups, err := m.GetUserGroups(ctx, uid)
		if err != nil {
			return nil, err
		}
		u.Groups = groups
	}
	return u, nil
}

func (m *manager) GetUserByClaim(ctx context.Context, claim, value string, skipFetchingGroups bool) (*userpb.User, error) {
	var u *userpb.User
	var err error
	switch claim {
	case "username":
		u, err = m.cache.GetByUsername(ctx, value)
		if err != nil {
			// IAM tokens carry only `sub` (the account UUID) and reva's oidc manager
			// always calls GetUserByClaim("username", <sub>). Lightweight users are
			// indexed under that UUID, so reaching here means a primary user: map
			// the UUID to their login via the reverse index.
			opaqueID := value
			if mapped, e := m.cache.GetOpaqueIDByIAMUUID(ctx, value); e == nil {
				opaqueID = mapped
			}
			u, err = m.cache.GetByID(ctx, opaqueID)
		}
	case "mail":
		u, err = m.cache.GetByMail(ctx, value)
	case "uid":
		// EOS resolves file ownership by uid; only primary users have one.
		u, err = m.cache.GetByUID(ctx, value)
	default:
		u, err = m.cache.GetByID(ctx, value)
	}
	if err != nil {
		return nil, err
	}

	if !skipFetchingGroups {
		groups, err := m.GetUserGroups(ctx, u.Id)
		if err != nil {
			return nil, err
		}
		u.Groups = groups
	}
	return u, nil
}

func (m *manager) FindUsers(ctx context.Context, query string, filters []*userpb.Filter, skipFetchingGroups bool) ([]*userpb.User, error) {
	users, err := m.cache.Find(ctx, query)
	if err != nil {
		return nil, err
	}

	if filters == nil {
		return users, nil
	}

	result := make([]*userpb.User, 0, len(users))
	for _, u := range users {
		ok := true
		for _, f := range filters {
			if !user.DoesUserFulfillFilterCriteria(u, f) {
				ok = false
				break
			}
		}
		if ok {
			result = append(result, u)
		}
	}
	return result, nil
}

func (m *manager) GetUserGroups(ctx context.Context, uid *userpb.UserId) ([]string, error) {
	if cached, err := m.cache.GetGroups(ctx, uid.OpaqueId); err == nil {
		return cached, nil
	}

	iamUUID, err := m.cache.GetIAMUUID(ctx, uid.OpaqueId)
	if err != nil {
		// Lightweight users are never remapped, so OpaqueId already is the UUID.
		iamUUID = uid.OpaqueId
	}

	// The endpoint defaults to count=10, so paging is explicit.
	startIndex := 1
	var groups []string
	for {
		url := fmt.Sprintf(
			"%s/iam/account/%s/groups?startIndex=%d&count=%d",
			m.conf.IAMBaseURL, iamUUID, startIndex, m.conf.PageSize,
		)
		var list IndigoIAMGroupList
		if err := m.tokenManager.SendAPIGetRequest(ctx, url, false, &list); err != nil {
			return nil, err
		}

		for _, g := range list.Resources {
			// Lower-cased to match both group drivers' opaque ids, since the
			// membership checks downstream are exact-match.
			groups = append(groups, strings.ToLower(g.Name))
		}

		nextStart := startIndex + list.ItemsPerPage
		if list.ItemsPerPage == 0 || nextStart > list.TotalResults {
			break
		}
		startIndex = nextStart
	}

	if err := m.cache.StoreGroups(uid, groups); err != nil {
		appctx.GetLogger(ctx).Error().Err(err).Msg("indigoiam user: failed to cache user groups")
	}
	return groups, nil
}

func (m *manager) IsInGroup(ctx context.Context, uid *userpb.UserId, group string) (bool, error) {
	groups, err := m.GetUserGroups(ctx, uid)
	if err != nil {
		return false, err
	}
	return slices.Contains(groups, group), nil
}

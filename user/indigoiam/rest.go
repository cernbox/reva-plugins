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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cernbox/reva-plugins/cache"
	redispools "github.com/cernbox/reva-plugins/redispools"
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
	conf       *config
	cache      *cache.UserCache
	httpClient *http.Client
}

type primaryUser struct {
	username   string
	uid_number int64
	gid_number int64
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

	IAMBaseURL         string `mapstructure:"iam_base_url"         docs:"https://iam.example.org"`
	AdminToken         string `mapstructure:"admin_token"          docs:"-"`
	IDProvider         string `mapstructure:"id_provider"          docs:"https://iam.example.org"`
	HTTPTimeoutSeconds int    `mapstructure:"http_timeout_seconds" docs:"30"`
	Insecure           bool   `mapstructure:"insecure"             docs:"false"`

	UserFetchInterval         int `mapstructure:"user_fetch_interval"          docs:"3600"`
	UserGroupsCacheExpiration int `mapstructure:"user_groups_cache_expiration" docs:"5"`
	PageSize                  int `mapstructure:"page_size"                    docs:"100"`

	// PrimaryUsers maps an IAM account UUID to an internal user with the given
	// username, uid and gid, and marks that user as USER_TYPE_PRIMARY. Every other
	// user is treated as USER_TYPE_LIGHTWEIGHT. This is needed because IAM
	// has no native concept of primary vs. lightweight accounts: by default
	// every account looks the same, so an explicit allowlist is required to
	// recognize the subset of "real" organization members.
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
	if c.HTTPTimeoutSeconds == 0 {
		c.HTTPTimeoutSeconds = 30
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

type iamName struct {
	Formatted  string `json:"formatted"`
	GivenName  string `json:"givenName"`
	FamilyName string `json:"familyName"`
}

type iamEmail struct {
	Value   string `json:"value"`
	Primary bool   `json:"primary"`
}

type iamGroupRef struct {
	Display string `json:"display"`
	Value   string `json:"value"`
}

// iamAccount is the shape of a single resource in /iam/account/search.
type iamAccount struct {
	ID          string        `json:"id"`
	UserName    string        `json:"userName"`
	DisplayName string        `json:"displayName"`
	Name        iamName       `json:"name"`
	Active      bool          `json:"active"`
	Emails      []iamEmail    `json:"emails"`
	Groups      []iamGroupRef `json:"groups"`
}

func (a *iamAccount) primaryEmail() string {
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

// iamAccountList is the paginated list returned by /iam/account/search.
type iamAccountList struct {
	TotalResults int          `json:"totalResults"`
	ItemsPerPage int          `json:"itemsPerPage"`
	StartIndex   int          `json:"startIndex"`
	Resources    []iamAccount `json:"Resources"`
}

// iamGroupResource is the shape of a group in /iam/account/{id}/groups.
type iamGroupResource struct {
	UUID        string `json:"uuid"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

// iamGroupList is the paginated list returned by /iam/account/{id}/groups.
type iamGroupList struct {
	TotalResults int                `json:"totalResults"`
	Resources    []iamGroupResource `json:"Resources"`
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

	tr := http.DefaultTransport
	if c.Insecure {
		tr = &http.Transport{}
	}

	m.conf = &c
	m.cache = cache.NewUserCache(pools, c.UserFetchInterval, c.UserGroupsCacheExpiration)
	m.httpClient = &http.Client{
		Transport: tr,
		Timeout:   time.Duration(c.HTTPTimeoutSeconds) * time.Second,
	}

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

		var list iamAccountList
		if err := m.iamGET(ctx, url, &list); err != nil {
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
			// Only the exceptional, remapped case needs a reverse-index
			// entry; lightweight users already have OpaqueId == IAM UUID.
			if remapped {
				if err := m.cache.StoreIAMUUID(u.Id.OpaqueId, acc.ID); err != nil {
					log.Error().Err(err).Str("uuid", acc.ID).Msg("indigoiam user: failed to cache IAM UUID mapping")
				}
			}
		}

		nextStart := startIndex + list.ItemsPerPage
		if nextStart > list.TotalResults {
			break
		}
		startIndex = nextStart
	}
	return nil
}

// accountToProto converts an iamAccount to the CS3 userpb.User type.
// If acc.ID is a key in conf.PrimaryUsers, the OpaqueId is replaced by the
// mapped value and the user is marked PRIMARY; otherwise the user is
// LIGHTWEIGHT and keeps its original IAM UUID as OpaqueId.
func (m *manager) accountToProto(acc *iamAccount) (*userpb.User, bool) {
	opaqueID := acc.ID
	userType := userpb.UserType_USER_TYPE_LIGHTWEIGHT
	uid := int64(0)
	gid := int64(0)
	remapped := false

	if mapped, remapped := m.conf.PrimaryUsers[acc.ID]; remapped {
		opaqueID = mapped.username
		uid = int64(mapped.uid_number)
		gid = int64(mapped.gid_number)
		userType = userpb.UserType_USER_TYPE_PRIMARY
	}

	return &userpb.User{
		Id: &userpb.UserId{
			OpaqueId: opaqueID,
			Idp:      m.conf.IDProvider,
			Type:     userType,
		},
		Username:    acc.UserName,
		Mail:        acc.primaryEmail(),
		DisplayName: acc.DisplayName,
		UidNumber:   uid,
		GidNumber:   gid,
	}, remapped
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
	case "mail":
		u, err = m.cache.GetByMail(ctx, value)
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
		// No mapping found — assume the OpaqueId is already the IAM UUID
		// (this is always true for lightweight users, since they are never
		// remapped).
		iamUUID = uid.OpaqueId
	}

	url := fmt.Sprintf("%s/iam/account/%s/groups", m.conf.IAMBaseURL, iamUUID)
	var list iamGroupList
	if err := m.iamGET(ctx, url, &list); err != nil {
		return nil, err
	}

	groups := make([]string, 0, len(list.Resources))
	for _, g := range list.Resources {
		groups = append(groups, g.Name)
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
	for _, g := range groups {
		if g == group {
			return true, nil
		}
	}
	return false, nil
}

// ---------------------------------------------------------------------------
// IAM HTTP helper
// ---------------------------------------------------------------------------

func (m *manager) iamGET(ctx context.Context, url string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+m.conf.AdminToken)
	req.Header.Set("Accept", "application/json")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("indigoiam: GET %s returned %s: %s", url, resp.Status, string(body))
	}

	return json.NewDecoder(resp.Body).Decode(v)
}

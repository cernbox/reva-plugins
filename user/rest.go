// Copyright 2018-2023 CERN
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

package rest

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	"github.com/cs3org/reva/v3"
	"github.com/cs3org/reva/v3/pkg/appctx"
	utils "github.com/cs3org/reva/v3/pkg/cbox/utils"
	"github.com/cs3org/reva/v3/pkg/user"
	"github.com/cs3org/reva/v3/pkg/utils/cfg"
	"github.com/cs3org/reva/v3/pkg/utils/list"
	"github.com/gomodule/redigo/redis"
	"github.com/rs/zerolog/log"
)

func init() {
	reva.RegisterPlugin(manager{})
}

type manager struct {
	conf            *config
	redisPool       *redis.Pool
	apiTokenManager *utils.APITokenManager
}

func (manager) RevaPlugin() reva.PluginInfo {
	return reva.PluginInfo{
		ID:  "grpc.services.userprovider.drivers.rest",
		New: New,
	}
}

type config struct {
	// The address at which the redis server is running
	RedisAddress string `mapstructure:"redis_address" docs:"localhost:6379"`
	// The username for connecting to the redis server
	RedisUsername string `mapstructure:"redis_username" docs:""`
	// The password for connecting to the redis server
	RedisPassword string `mapstructure:"redis_password" docs:""`
	// The time in minutes for which the groups to which a user belongs would be cached
	UserGroupsCacheExpiration int `mapstructure:"user_groups_cache_expiration" docs:"5"`
	// The OIDC Provider
	IDProvider string `mapstructure:"id_provider" docs:"http://cernbox.cern.ch"`
	// Base API Endpoint
	APIBaseURL string `mapstructure:"api_base_url" docs:"https://authorization-service-api-dev.web.cern.ch"`
	// Client ID needed to authenticate
	ClientID string `mapstructure:"client_id" docs:"-"`
	// Client Secret
	ClientSecret string `mapstructure:"client_secret" docs:"-"`

	// Endpoint to generate token to access the API
	OIDCTokenEndpoint string `mapstructure:"oidc_token_endpoint" docs:"https://keycloak-dev.cern.ch/auth/realms/cern/api-access/token"`
	// The target application for which token needs to be generated
	TargetAPI string `mapstructure:"target_api" docs:"authorization-service-api"`
	// The time in seconds between bulk fetch of user accounts
	UserFetchInterval int `mapstructure:"user_fetch_interval" docs:"3600"`
}

func (c *config) ApplyDefaults() {
	if c.UserGroupsCacheExpiration == 0 {
		c.UserGroupsCacheExpiration = 5
	}
	if c.RedisAddress == "" {
		c.RedisAddress = ":6379"
	}
	if c.APIBaseURL == "" {
		c.APIBaseURL = "https://authorization-service-api-dev.web.cern.ch"
	}
	if c.TargetAPI == "" {
		c.TargetAPI = "authorization-service-api"
	}
	if c.OIDCTokenEndpoint == "" {
		c.OIDCTokenEndpoint = "https://keycloak-dev.cern.ch/auth/realms/cern/api-access/token"
	}
	if c.IDProvider == "" {
		c.IDProvider = "http://cernbox.cern.ch"
	}
	if c.UserFetchInterval == 0 {
		c.UserFetchInterval = 3600
	}
}

// New returns a user manager implementation that makes calls to the GRAPPA API.
func New(ctx context.Context, m map[string]interface{}) (user.Manager, error) {
	mgr := &manager{}
	err := mgr.Configure(m)
	if err != nil {
		return nil, err
	}
	return mgr, err
}

func (m *manager) Configure(ml map[string]interface{}) error {
	var c config
	if err := cfg.Decode(ml, &c); err != nil {
		return err
	}
	redisPool := initRedisPool(c.RedisAddress, c.RedisUsername, c.RedisPassword)
	apiTokenManager, err := utils.InitAPITokenManager(ml)
	if err != nil {
		return err
	}
	m.conf = &c
	m.redisPool = redisPool
	m.apiTokenManager = apiTokenManager

	// Since we're starting a subroutine which would take some time to execute,
	// we can't wait to see if it works before returning the user.Manager object
	// TODO: return err if the fetch fails
	go m.fetchAllUsers(context.Background())
	return nil
}

func (m *manager) fetchAllUsers(ctx context.Context) {
	_ = m.fetchAllUserAccounts(ctx)
	ticker := time.NewTicker(time.Duration(m.conf.UserFetchInterval) * time.Second)
	work := make(chan os.Signal, 1)
	signal.Notify(work, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT)

	for {
		select {
		case <-work:
			return
		case <-ticker.C:
			_ = m.fetchAllUserAccounts(ctx)
		}
	}
}

// Identity contains the information of a single user.
type Identity struct {
	PrimaryAccountEmail string `json:"primaryAccountEmail,omitempty"`
	Type                string `json:"type,omitempty"`
	Upn                 string `json:"upn"`
	DisplayName         string `json:"displayName"`
	Source              string `json:"source,omitempty"`
	ActiveUser          bool   `json:"activeUser,omitempty"`
	UID                 int    `json:"uid,omitempty"`
	GID                 int    `json:"gid,omitempty"`
}

// IdentitiesResponse contains the expected response from grappa
// when getting the list of users.
type IdentitiesResponse struct {
	Pagination struct {
		Next *string `json:"next"`
	} `json:"pagination"`
	Data []*Identity `json:"data"`
}

// UserType convert the user type in grappa to CS3APIs.
func (i *Identity) UserType() userpb.UserType {
	switch i.Type {
	case "Application":
		return userpb.UserType_USER_TYPE_APPLICATION
	case "Service":
		return userpb.UserType_USER_TYPE_SERVICE
	case "Secondary":
		return userpb.UserType_USER_TYPE_SECONDARY
	case "Person":
		if i.Source == "cern" && i.UID > 0 {
			// this is a CERN account; here we should check if i.ActiveUser = true,
			// but users that have just left the Organization have ActiveUser = false,
			// whereas users with UID = 0 are definitely non-primary.
			return userpb.UserType_USER_TYPE_PRIMARY
		}
		return userpb.UserType_USER_TYPE_LIGHTWEIGHT // external user
	default:
		return userpb.UserType_USER_TYPE_INVALID
	}
}

func (m *manager) fetchAllUserAccounts(ctx context.Context) error {
	url := fmt.Sprintf("%s/api/v1.0/Identity?filter=unconfirmed%%3Afalse&field=upn&field=primaryAccountEmail&field=displayName&field=uid&field=gid&field=type&field=source&field=activeUser", m.conf.APIBaseURL)

	for {
		var r IdentitiesResponse
		if err := m.apiTokenManager.SendAPIGetRequest(ctx, url, false, &r); err != nil {
			return err
		}

		for _, usr := range r.Data {
			if _, err := m.parseAndCacheUser(ctx, usr); err != nil {
				continue
			}
		}

		if r.Pagination.Next == nil {
			break
		}
		url = fmt.Sprintf("%s%s", m.conf.APIBaseURL, *r.Pagination.Next)
	}

	return nil
}

func (m *manager) parseAndCacheUser(ctx context.Context, i *Identity) (*userpb.User, error) {
	u := &userpb.User{
		Id: &userpb.UserId{
			OpaqueId: i.Upn,
			Idp:      m.conf.IDProvider,
			Type:     i.UserType(),
		},
		Mail:        i.PrimaryAccountEmail,
		DisplayName: i.DisplayName,
		UidNumber:   int64(i.UID),
		GidNumber:   int64(i.GID),
	}
	u.Username = utils.FormatUserID(u.Id)

	if err := m.cacheUserDetails(u); err != nil {
		log.Error().Err(err).Msg("rest: error caching user details")
	}

	return u, nil
}

func (m *manager) GetUser(ctx context.Context, uid *userpb.UserId, skipFetchingGroups bool) (*userpb.User, error) {
	u, err := m.fetchCachedUserDetails(uid)
	if err != nil {
		return nil, err
	}

	if !skipFetchingGroups {
		userGroups, err := m.GetUserGroups(ctx, uid)
		if err != nil {
			return nil, err
		}
		u.Groups = userGroups
	}

	return u, nil
}

func (m *manager) GetUserByClaim(ctx context.Context, claim, value string, skipFetchingGroups bool) (*userpb.User, error) {
	u, err := m.fetchCachedUserByParam(claim, value)
	if err != nil {
		return nil, err
	}

	if !skipFetchingGroups {
		userGroups, err := m.GetUserGroups(ctx, u.Id)
		if err != nil {
			return nil, err
		}
		u.Groups = userGroups
	}

	return u, nil
}

func (m *manager) FindUsers(ctx context.Context, query string, skipFetchingGroups bool) ([]*userpb.User, error) {
	// Look at namespaces filters. If the query starts with:
	// "a" => look into primary/secondary/service accounts
	// "l" => look into lightweight/federated accounts
	// none => look into primary

	parts := strings.SplitN(query, ":", 2)

	var namespace string
	if len(parts) == 2 {
		// the query contains a namespace filter
		namespace, query = parts[0], parts[1]
	}

	users, err := m.findCachedUsers(query)
	if err != nil {
		return nil, err
	}

	userSlice := []*userpb.User{}

	var accountsFilters []userpb.UserType
	switch namespace {
	case "":
		accountsFilters = []userpb.UserType{userpb.UserType_USER_TYPE_PRIMARY}
	case "a":
		accountsFilters = []userpb.UserType{userpb.UserType_USER_TYPE_PRIMARY, userpb.UserType_USER_TYPE_SECONDARY, userpb.UserType_USER_TYPE_SERVICE}
	case "l":
		accountsFilters = []userpb.UserType{userpb.UserType_USER_TYPE_LIGHTWEIGHT, userpb.UserType_USER_TYPE_FEDERATED}
	}

	for _, u := range users {
		if isUserAnyType(u, accountsFilters) {
			userSlice = append(userSlice, u)
		}
	}

	return userSlice, nil
}

// isUserAnyType returns true if the user's type is one of types list.
func isUserAnyType(user *userpb.User, types []userpb.UserType) bool {
	for _, t := range types {
		if user.GetId().Type == t {
			return true
		}
	}
	return false
}

// Group contains the information about a group.
type Group struct {
	DisplayName string `json:"displayName"`
}

// GroupsResponse contains the expected response from grappa
// when getting the list of groups.
type GroupsResponse struct {
	Pagination struct {
		Next *string `json:"next"`
	} `json:"pagination"`
	Data []Group `json:"data"`
}

func (m *manager) GetUserGroups(ctx context.Context, uid *userpb.UserId) ([]string, error) {
	groups, err := m.fetchCachedUserGroups(uid)
	if err == nil {
		return groups, nil
	}

	// TODO (gdelmont): support pagination! we may have problems with users having more than 1000 groups
	url := fmt.Sprintf("%s/api/v1.0/Identity/%s/groups/recursive?field=displayName", m.conf.APIBaseURL, uid.OpaqueId)

	var r GroupsResponse
	if err := m.apiTokenManager.SendAPIGetRequest(ctx, url, false, &r); err != nil {
		return nil, err
	}

	groups = list.Map(r.Data, func(g Group) string { return strings.ToLower(g.DisplayName) })

	if err = m.cacheUserGroups(uid, groups); err != nil {
		log := appctx.GetLogger(ctx)
		log.Error().Err(err).Msg("rest: error caching user groups")
	}

	return groups, nil
}

func (m *manager) IsInGroup(ctx context.Context, uid *userpb.UserId, group string) (bool, error) {
	// TODO (gdelmont): this can be improved storing the groups a user belong to as a list in redis
	// and, instead of returning all the groups, use the redis apis to check if the group is in the list.
	userGroups, err := m.GetUserGroups(ctx, uid)
	if err != nil {
		return false, err
	}

	for _, g := range userGroups {
		if group == g {
			return true, nil
		}
	}
	return false, nil
}

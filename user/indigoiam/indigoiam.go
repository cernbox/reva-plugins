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

// A user provider following the Indigo IAM REST API
// See https://indigo-iam.github.io/v/v1.14.0/docs/reference/api/account-api/

package indigoiam

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	"github.com/cs3org/reva/v3"
	"github.com/cs3org/reva/v3/pkg/user"
	"github.com/go-redis/redis/v8"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

func init() {
	reva.RegisterPlugin(manager{})
}

type manager struct {
	conf        *config
	client      *http.Client
	redisClient *redis.Client
}

func (manager) RevaPlugin() reva.PluginInfo {
	return reva.PluginInfo{
		ID:  "grpc.services.userprovider.drivers.indigoiam",
		New: New,
	}
}

// config maps the configuration fields for the Indigo IAM user provider
type config struct {
	Endpoint      string        `mapstructure:"endpoint"`
	ClientToken   string        `mapstructure:"client_token";doc:"Admin token with iam:admin.read scope"`
	Idp           string        `mapstructure:"idp"`
	PrimaryUsers  []string      `mapstructure:"primary_users";default:"[]";doc:"A list of user IDs that should be treated as primary users. If empty, everyone is considered primary."`
	RedisAddress  string        `mapstructure:"redis_address"`
	RedisPassword string        `mapstructure:"redis_password"`
	RedisDB       int           `mapstructure:"redis_db"`
	CacheTTL      time.Duration `mapstructure:"cache_ttl"`
}

// IAMAccount represents the JSON payload structure from the Indigo IAM account/search API
type IAMAccount struct {
	ID       string `json:"id"`
	Username string `json:"username"`
	Name     string `json:"name"`
	Email    string `json:"email"`
}

// IAMGroup represents the group payload from /iam/account/{id}/groups
type IAMGroup struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// New returns a new user.Manager interacting with Indigo IAM
func New(ctx context.Context, m map[string]any) (user.Manager, error) {
	c := &config{}
	if err := mapstructure.Decode(m, c); err != nil {
		return nil, errors.Wrap(err, "error decoding configuration")
	}

	if c.Endpoint == "" {
		return nil, errors.New("indigo iam provider: missing endpoint configuration")
	}
	if c.ClientToken == "" {
		return nil, errors.New("indigo iam provider: missing client token configuration")
	}
	if c.CacheTTL == 0 {
		c.CacheTTL = 5 * time.Minute
	}

	var rClient *redis.Client
	if c.RedisAddress != "" {
		rClient = redis.NewClient(&redis.Options{
			Addr:     c.RedisAddress,
			Password: c.RedisPassword,
			DB:       c.RedisDB,
		})
	}

	return &manager{
		conf:        c,
		client:      &http.Client{Timeout: 10 * time.Second},
		redisClient: rClient,
	}, nil
}

func (m *manager) Configure(ml map[string]interface{}) error {
	return nil
}

// GetUser fetches user information by unique account identifier
func (m *manager) GetUser(ctx context.Context, uid *userpb.UserId, skipFetchingGroups bool) (*userpb.User, error) {
	if uid == nil || uid.OpaqueId == "" {
		return nil, errors.New("indigo iam provider: empty user id")
	}

	cacheKey := "user:id:" + uid.OpaqueId
	if m.redisClient != nil {
		if val, err := m.redisClient.Get(ctx, cacheKey).Result(); err == nil {
			var u userpb.User
			if err := json.Unmarshal([]byte(val), &u); err == nil {
				if !skipFetchingGroups && len(u.Groups) == 0 {
					groups, err := m.fetchUserGroups(ctx, u.Id.OpaqueId)
					if err == nil {
						u.Groups = groups
					}
				}
				return &u, nil
			}
		}
	}

	// Leverage /iam/account/search endpoint targeting the primary unique ID
	url := fmt.Sprintf("%s/iam/account/search?Id=%s", m.conf.Endpoint, uid.OpaqueId)
	var accounts []IAMAccount
	if err := m.doRequest(ctx, url, &accounts); err != nil {
		return nil, err
	}

	if len(accounts) == 0 {
		return nil, errors.New("user not found")
	}

	u := m.iamAccountToUser(&accounts[0])

	if !skipFetchingGroups {
		groups, err := m.fetchUserGroups(ctx, u.Id.OpaqueId)
		if err == nil {
			u.Groups = groups
		}
	}

	m.storeInCache(ctx, cacheKey, u)
	return u, nil
}

// GetUserByClaim looks up a user matching properties like username or mail fields
func (m *manager) GetUserByClaim(ctx context.Context, claim, value, tenantID string, skipFetchingGroups bool) (*userpb.User, error) {
	if claim == "" || value == "" {
		return nil, errors.New("indigo iam provider: empty claim or value")
	}

	var searchParam string
	switch claim {
	case "username":
		searchParam = "username"
	case "mail", "email":
		searchParam = "email"
	default:
		return nil, fmt.Errorf("indigo iam provider: unsupported lookup claim: %s", claim)
	}

	cacheKey := fmt.Sprintf("user:%s:%s", searchParam, value)
	if m.redisClient != nil {
		if val, err := m.redisClient.Get(ctx, cacheKey).Result(); err == nil {
			var u userpb.User
			if err := json.Unmarshal([]byte(val), &u); err == nil {
				if !skipFetchingGroups && len(u.Groups) == 0 {
					groups, err := m.fetchUserGroups(ctx, u.Id.OpaqueId)
					if err == nil {
						u.Groups = groups
					}
				}
				return &u, nil
			}
		}
	}

	url := fmt.Sprintf("%s/iam/account/search?%s=%s", m.conf.Endpoint, searchParam, value)
	var accounts []IAMAccount
	if err := m.doRequest(ctx, url, &accounts); err != nil {
		return nil, err
	}

	if len(accounts) == 0 {
		return nil, fmt.Errorf("user not found with %s: %s", claim, value)
	}

	u := m.iamAccountToUser(&accounts[0])

	if !skipFetchingGroups {
		groups, err := m.fetchUserGroups(ctx, u.Id.OpaqueId)
		if err == nil {
			u.Groups = groups
		}
	}

	m.storeInCache(ctx, cacheKey, u)
	return u, nil
}

// FindUsers searches for subsets of accounts matching names, mail handles or usernames
func (m *manager) FindUsers(ctx context.Context, query, tenantID string, skipFetchingGroups bool) ([]*userpb.User, error) {
	// Attempt matching on full/display names first
	url := fmt.Sprintf("%s/iam/account/search?name=%s", m.conf.Endpoint, query)
	var accounts []IAMAccount
	if err := m.doRequest(ctx, url, &accounts); err != nil {
		return nil, err
	}

	// Fallback lookup strategy by username if generic query yielded nothing
	if len(accounts) == 0 {
		url = fmt.Sprintf("%s/iam/account/search?username=%s", m.conf.Endpoint, query)
		if err := m.doRequest(ctx, url, &accounts); err != nil {
			return nil, err
		}
	}

	users := make([]*userpb.User, 0, len(accounts))
	for _, acc := range accounts {
		u := m.iamAccountToUser(&acc)
		if !skipFetchingGroups {
			groups, err := m.fetchUserGroups(ctx, u.Id.OpaqueId)
			if err == nil {
				u.Groups = groups
			}
		}
		users = append(users, u)
	}

	return users, nil
}

// GetUserGroups resolves user identities directly to assigned string groups
func (m *manager) GetUserGroups(ctx context.Context, uid *userpb.UserId) ([]string, error) {
	if uid == nil || uid.OpaqueId == "" {
		return nil, errors.New("indigo iam provider: empty user id")
	}
	return m.fetchUserGroups(ctx, uid.OpaqueId)
}

// Internal implementation helpers

func (m *manager) doRequest(ctx context.Context, url string, target interface{}) error {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return errors.Wrap(err, "failed to build request context")
	}

	req.Header.Set("Authorization", "Bearer "+m.conf.ClientToken)
	req.Header.Set("Accept", "application/json")

	resp, err := m.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "http execution failed against endpoint")
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return errors.New("identity entity not found mapping resource")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected upstream api reply status: %d", resp.StatusCode)
	}

	return json.NewDecoder(resp.Body).Decode(target)
}

func (m *manager) fetchUserGroups(ctx context.Context, userID string) ([]string, error) {
	cacheKey := "user:groups:" + userID
	if m.redisClient != nil {
		if groups, err := m.redisClient.SMembers(ctx, cacheKey).Result(); err == nil && len(groups) > 0 {
			return groups, nil
		}
	}

	url := fmt.Sprintf("%s/iam/account/%s/groups", m.conf.Endpoint, userID)
	var iamGroups []IAMGroup
	if err := m.doRequest(ctx, url, &iamGroups); err != nil {
		return nil, err
	}

	groups := make([]string, len(iamGroups))
	for i, g := range iamGroups {
		groups[i] = g.Name
	}

	if m.redisClient != nil && len(groups) > 0 {
		pipe := m.redisClient.Pipeline()
		pipe.SAdd(ctx, cacheKey, groups)
		pipe.Expire(ctx, cacheKey, m.conf.CacheTTL)
		_, _ = pipe.Exec(ctx)
	}

	return groups, nil
}

func (m *manager) iamAccountToUser(account *IAMAccount) *userpb.User {
	var ut userpb.UserType
	if m.conf.PrimaryUsers == nil || stringInSlice(account.ID, m.conf.PrimaryUsers) {
		ut = userpb.UserType_USER_TYPE_PRIMARY
	} else {
		ut = userpb.UserType_USER_TYPE_LIGHTWEIGHT
	}
	return &userpb.User{
		Id: &userpb.UserId{
			OpaqueId: account.ID,
			Idp:      m.conf.Idp,
			Type:     ut,
		},
		Username:    account.Username,
		Mail:        account.Email,
		DisplayName: account.Name,
	}
}

func (m *manager) storeInCache(ctx context.Context, directKey string, u *userpb.User) {
	if m.redisClient == nil {
		return
	}
	data, err := json.Marshal(u)
	if err != nil {
		return
	}

	pipe := m.redisClient.Pipeline()
	pipe.Set(ctx, directKey, data, m.conf.CacheTTL)
	pipe.Set(ctx, "user:id:"+u.Id.OpaqueId, data, m.conf.CacheTTL)
	pipe.Set(ctx, "user:username:"+u.Username, data, m.conf.CacheTTL)
	if u.Mail != "" {
		pipe.Set(ctx, "user:email:"+u.Mail, data, m.conf.CacheTTL)
	}
	_, _ = pipe.Exec(ctx)
}

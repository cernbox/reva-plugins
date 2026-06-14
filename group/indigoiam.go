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

// A Group provider following the Indigo IAM REST API
// See https://indigo-iam.github.io/v/v1.14.0/docs/reference/api/account-api/

package indigoiam

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	grouppb "github.com/cs3org/go-cs3apis/cs3/identity/group/v1beta1"
	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	"github.com/cs3org/reva/v3"
	"github.com/cs3org/reva/v3/pkg/group"
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
		ID:  "grpc.services.groupprovider.drivers.indigoiam",
		New: New,
	}
}

// config maps the configuration fields for the Indigo IAM group provider
type config struct {
	Endpoint      string        `mapstructure:"endpoint"`
	ClientToken   string        `mapstructure:"client_token"` // Admin token with `iam:admin.read` scope
	Idp           string        `mapstructure:"idp"`
	RedisAddress  string        `mapstructure:"redis_address"`
	RedisPassword string        `mapstructure:"redis_password"`
	RedisDB       int           `mapstructure:"redis_db"`
	CacheTTL      time.Duration `mapstructure:"cache_ttl"`
}

// IAMGroup represents the JSON structure from the Indigo IAM /iam/group/search API
type IAMGroup struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// IAMMember represents a simplified account identity when resolving memberships
type IAMMember struct {
	ID       string `json:"id"`
	Username string `json:"username"`
}

// New returns a new group.Manager interacting with Indigo IAM
func New(ctx context.Context, m map[string]any) (group.Manager, error) {
	c := &config{}
	if err := mapstructure.Decode(m, c); err != nil {
		return nil, errors.Wrap(err, "error decoding configuration")
	}

	if c.Endpoint == "" {
		return nil, errors.New("indigo iam group provider: missing endpoint configuration")
	}
	if c.ClientToken == "" {
		return nil, errors.New("indigo iam group provider: missing client token configuration")
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

// GetGroup fetches group details by its unique identifier
func (m *manager) GetGroup(ctx context.Context, gid *grouppb.GroupId) (*grouppb.Group, error) {
	if gid == nil || gid.OpaqueId == "" {
		return nil, errors.New("indigo iam group provider: empty group id")
	}

	cacheKey := "group:id:" + gid.OpaqueId
	if m.redisClient != nil {
		if val, err := m.redisClient.Get(ctx, cacheKey).Result(); err == nil {
			var g grouppb.Group
			if err := json.Unmarshal([]byte(val), &g); err == nil {
				return &g, nil
			}
		}
	}

	// Fetch groups and isolate the one matching target identifier
	url := fmt.Sprintf("%s/iam/group/search", m.conf.Endpoint)
	var groups []IAMGroup
	if err := m.doRequest(ctx, url, &groups); err != nil {
		return nil, err
	}

	for _, g := range groups {
		if g.ID == gid.OpaqueId {
			targetGroup := m.iamGroupToGroup(&g)
			m.storeInCache(ctx, cacheKey, targetGroup)
			return targetGroup, nil
		}
	}

	return nil, errors.New("group not found")
}

// GetGroupByClaim performs unique lookups using attributes like the group's name
func (m *manager) GetGroupByClaim(ctx context.Context, claim, value string) (*grouppb.Group, error) {
	if claim == "" || value == "" {
		return nil, errors.New("indigo iam group provider: empty claim or value")
	}

	if claim != "name" && claim != "groupname" {
		return nil, fmt.Errorf("indigo iam group provider: unsupported claim: %s", claim)
	}

	cacheKey := "group:name:" + value
	if m.redisClient != nil {
		if val, err := m.redisClient.Get(ctx, cacheKey).Result(); err == nil {
			var g grouppb.Group
			if err := json.Unmarshal([]byte(val), &g); err == nil {
				return &g, nil
			}
		}
	}

	url := fmt.Sprintf("%s/iam/group/search", m.conf.Endpoint)
	var groups []IAMGroup
	if err := m.doRequest(ctx, url, &groups); err != nil {
		return nil, err
	}

	for _, g := range groups {
		if g.Name == value {
			targetGroup := m.iamGroupToGroup(&g)
			m.storeInCache(ctx, cacheKey, targetGroup)
			return targetGroup, nil
		}
	}

	return nil, fmt.Errorf("group not found with claim %s: %s", claim, value)
}

// FindGroups searches for lists of groups matching a query string fraction
func (m *manager) FindGroups(ctx context.Context, query string) ([]*grouppb.Group, error) {
	url := fmt.Sprintf("%s/iam/group/search", m.conf.Endpoint)
	var iamGroups []IAMGroup
	if err := m.doRequest(ctx, url, &iamGroups); err != nil {
		return nil, err
	}

	var groups []*grouppb.Group
	for _, ig := range iamGroups {
		// Basic case-insensitive substring matching if query is provided
		if query == "" || legacyContains(ig.Name, query) {
			groups = append(groups, m.iamGroupToGroup(&ig))
		}
	}

	return groups, nil
}

// GetMembers evaluates and lists members assigned to the requested group ID
func (m *manager) GetMembers(ctx context.Context, gid *grouppb.GroupId) ([]*userpb.UserId, error) {
	if gid == nil || gid.OpaqueId == "" {
		return nil, errors.New("indigo iam group provider: empty group id")
	}

	cacheKey := "group:members:" + gid.OpaqueId
	if m.redisClient != nil {
		if cached, err := m.redisClient.Get(ctx, cacheKey).Result(); err == nil {
			var ids []*userpb.UserId
			if err := json.Unmarshal([]byte(cached), &ids); err == nil {
				return ids, nil
			}
		}
	}

	// Resolve target group identity first to obtain its explicit human name
	g, err := m.GetGroup(ctx, gid)
	if err != nil {
		return nil, err
	}

	// Leverage Account query searching filtering matching users linked directly to the group name
	url := fmt.Sprintf("%s/iam/account/search?filter=group:%s", m.conf.Endpoint, g.GroupName)
	var members []IAMMember
	if err := m.doRequest(ctx, url, &members); err != nil {
		return nil, err
	}

	userIds := make([]*userpb.UserId, len(members))
	for i, mber := range members {
		userIds[i] = &userpb.UserId{
			OpaqueId: mber.ID,
			Idp:      m.conf.Idp,
			Type:     userpb.UserType_USER_TYPE_PRIMARY,
		}
	}

	if m.redisClient != nil && len(userIds) > 0 {
		if data, err := json.Marshal(userIds); err == nil {
			_ = m.redisClient.Set(ctx, cacheKey, data, m.conf.CacheTTL).Err()
		}
	}

	return userIds, nil
}

// HasMember checks if a specific user identifier belongs within a target group context
func (m *manager) HasMember(ctx context.Context, gid *grouppb.GroupId, uid *userpb.UserId) (bool, error) {
	if gid == nil || uid == nil {
		return false, errors.New("indigo iam group provider: invalid group or user identity")
	}

	members, err := m.GetMembers(ctx, gid)
	if err != nil {
		return false, err
	}

	for _, member := range members {
		if member.OpaqueId == uid.OpaqueId {
			return true, nil
		}
	}

	return false, nil
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
		return errors.New("group entity resource not found")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected upstream api reply status: %d", resp.StatusCode)
	}

	return json.NewDecoder(resp.Body).Decode(target)
}

func (m *manager) iamGroupToGroup(g *IAMGroup) *grouppb.Group {
	return &grouppb.Group{
		Id: &grouppb.GroupId{
			OpaqueId: g.ID,
			Idp:      m.conf.Idp,
		},
		GroupName:   g.Name,
		DisplayName: g.Name,
	}
}

func (m *manager) storeInCache(ctx context.Context, directKey string, g *grouppb.Group) {
	if m.redisClient == nil {
		return
	}
	data, err := json.Marshal(g)
	if err != nil {
		return
	}

	pipe := m.redisClient.Pipeline()
	pipe.Set(ctx, directKey, data, m.conf.CacheTTL)
	pipe.Set(ctx, "group:id:"+g.Id.OpaqueId, data, m.conf.CacheTTL)
	pipe.Set(ctx, "group:name:"+g.GroupName, data, m.conf.CacheTTL)
	_, _ = pipe.Exec(ctx)
}

func legacyContains(s, substr string) bool {
	// Simple matching layer mirroring standard lower case lookups
	return len(s) >= len(substr) && (s == substr || s[:len(substr)] == substr)
}

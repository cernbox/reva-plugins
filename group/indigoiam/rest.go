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

// Package indigoiam implements a Reva group provider driver backed by the
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
	"strings"
	"syscall"
	"time"

	"github.com/cernbox/reva-plugins/cache"
	redispools "github.com/cernbox/reva-plugins/redispools"
	grouppb "github.com/cs3org/go-cs3apis/cs3/identity/group/v1beta1"
	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	"github.com/cs3org/reva/v3"
	"github.com/cs3org/reva/v3/pkg/appctx"
	"github.com/cs3org/reva/v3/pkg/group"
	"github.com/cs3org/reva/v3/pkg/utils/cfg"
)

func init() {
	reva.RegisterPlugin(manager{})
}

type manager struct {
	conf       *config
	cache      *cache.GroupCache
	httpClient *http.Client
}

func (manager) RevaPlugin() reva.PluginInfo {
	return reva.PluginInfo{
		ID:  "grpc.services.groupprovider.drivers.indigo-iam",
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

	GroupFetchInterval          int `mapstructure:"group_fetch_interval"           docs:"3600"`
	GroupMembersCacheExpiration int `mapstructure:"group_members_cache_expiration" docs:"5"`
	PageSize                    int `mapstructure:"page_size"                      docs:"100"`
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
	if c.GroupFetchInterval == 0 {
		c.GroupFetchInterval = 3600
	}
	if c.GroupMembersCacheExpiration == 0 {
		c.GroupMembersCacheExpiration = 5
	}
	if c.PageSize == 0 {
		c.PageSize = 100
	}
}

// ---------------------------------------------------------------------------
// Indigo IAM JSON shapes
// ---------------------------------------------------------------------------

// iamGroup is the shape of a single resource in /iam/group/search.
type iamGroup struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
	IndigoGroup struct {
		ParentGroup *struct {
			ID          string `json:"id"`
			DisplayName string `json:"displayName"`
		} `json:"parentGroup"`
		Description string `json:"description"`
	} `json:"urn:indigo-dc:scim:schemas:IndigoGroup"`
}

// iamGroupList is the paginated list returned by /iam/group/search.
type iamGroupList struct {
	TotalResults int        `json:"totalResults"`
	ItemsPerPage int        `json:"itemsPerPage"`
	StartIndex   int        `json:"startIndex"`
	Resources    []iamGroup `json:"Resources"`
}

// iamAccount is the shape of a user returned by /iam/account/find/bygroup/{id}.
type iamAccount struct {
	ID       string `json:"id"`
	UserName string `json:"userName"`
	Active   bool   `json:"active"`
}

// iamAccountList is the paginated list returned by the bygroup filter endpoint.
type iamAccountList struct {
	TotalResults int          `json:"totalResults"`
	ItemsPerPage int          `json:"itemsPerPage"`
	StartIndex   int          `json:"startIndex"`
	Resources    []iamAccount `json:"Resources"`
}

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

// New creates and returns a group.Manager backed by Indigo IAM.
func New(ctx context.Context, m map[string]interface{}) (group.Manager, error) {
	var c config
	if err := cfg.Decode(m, &c); err != nil {
		return nil, err
	}
	c.ApplyDefaults()

	pools, err := redispools.NewRedisPoolsWithSentinelAddress(
		ctx,
		c.RedisAddress, c.RedisSentinelAddress,
		c.RedisUsername, c.RedisPassword,
		c.RedisSentinelMode, c.RedisMasterName,
	)
	if err != nil {
		appctx.GetLogger(ctx).Error().Err(err).Msg("indigoiam group: failed to initialise Redis pools")
		pools = &redispools.RedisPools{}
	}

	tr := http.DefaultTransport
	if c.Insecure {
		tr = &http.Transport{}
	}

	mgr := &manager{
		conf:  &c,
		cache: cache.NewGroupCache(pools, c.GroupFetchInterval, c.GroupMembersCacheExpiration),
		httpClient: &http.Client{
			Transport: tr,
			Timeout:   time.Duration(c.HTTPTimeoutSeconds) * time.Second,
		},
	}

	go mgr.fetchAllGroups(context.Background())
	return mgr, nil
}

// ---------------------------------------------------------------------------
// Background bulk-fetch loop
// ---------------------------------------------------------------------------

func (m *manager) fetchAllGroups(ctx context.Context) {
	log := appctx.GetLogger(ctx)
	if err := m.fetchAllGroupAccounts(ctx); err != nil {
		log.Error().Err(err).Msg("indigoiam group: initial bulk fetch failed")
	}

	ticker := time.NewTicker(time.Duration(m.conf.GroupFetchInterval) * time.Second)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT)

	for {
		select {
		case <-sigs:
			return
		case <-ticker.C:
			if err := m.fetchAllGroupAccounts(ctx); err != nil {
				log.Error().Err(err).Msg("indigoiam group: periodic bulk fetch failed")
			}
		}
	}
}

func (m *manager) fetchAllGroupAccounts(ctx context.Context) error {
	log := appctx.GetLogger(ctx)
	startIndex := 1

	for {
		url := fmt.Sprintf(
			"%s/iam/group/search?startIndex=%d&count=%d",
			m.conf.IAMBaseURL, startIndex, m.conf.PageSize,
		)

		var list iamGroupList
		if err := m.iamGET(ctx, url, &list); err != nil {
			return err
		}

		for i := range list.Resources {
			g := m.iamGroupToProto(&list.Resources[i])
			if err := m.cache.StoreGroup(g); err != nil {
				log.Error().Err(err).Str("uuid", list.Resources[i].ID).Msg("indigoiam group: cache error")
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

func (m *manager) iamGroupToProto(g *iamGroup) *grouppb.Group {
	return &grouppb.Group{
		Id: &grouppb.GroupId{
			OpaqueId: g.ID,
			Idp:      m.conf.IDProvider,
		},
		GroupName:   g.DisplayName,
		DisplayName: g.DisplayName,
	}
}

// ---------------------------------------------------------------------------
// group.Manager interface
// ---------------------------------------------------------------------------

func (m *manager) GetGroup(ctx context.Context, gid *grouppb.GroupId, skipFetchingMembers bool) (*grouppb.Group, error) {
	g, err := m.cache.GetByID(ctx, gid.OpaqueId)
	if err != nil {
		return nil, err
	}
	if !skipFetchingMembers {
		members, err := m.GetMembers(ctx, gid)
		if err != nil {
			return nil, err
		}
		g.Members = members
	}
	return g, nil
}

func (m *manager) GetGroupByClaim(ctx context.Context, claim, value string, skipFetchingMembers bool) (*grouppb.Group, error) {
	var g *grouppb.Group
	var err error
	switch claim {
	case "group_name", "name":
		g, err = m.cache.GetByName(ctx, value)
	default:
		g, err = m.cache.GetByID(ctx, value)
	}
	if err != nil {
		return nil, err
	}

	if !skipFetchingMembers {
		members, err := m.GetMembers(ctx, g.Id)
		if err != nil {
			return nil, err
		}
		g.Members = members
	}
	return g, nil
}

func (m *manager) FindGroups(ctx context.Context, query string, skipFetchingMembers bool) ([]*grouppb.Group, error) {
	parts := strings.SplitN(query, ":", 2)
	if len(parts) == 2 {
		if parts[0] == "a" {
			query = parts[1]
		} else {
			return []*grouppb.Group{}, nil
		}
	}
	return m.cache.Find(ctx, query)
}

func (m *manager) GetMembers(ctx context.Context, gid *grouppb.GroupId) ([]*userpb.UserId, error) {
	if cached, err := m.cache.GetMembers(ctx, gid.OpaqueId); err == nil {
		return cached, nil
	}

	startIndex := 1
	var members []*userpb.UserId

	for {
		url := fmt.Sprintf(
			"%s/iam/account/find/bygroup/%s?startIndex=%d&count=%d",
			m.conf.IAMBaseURL, gid.OpaqueId, startIndex, m.conf.PageSize,
		)

		var list iamAccountList
		if err := m.iamGET(ctx, url, &list); err != nil {
			return nil, err
		}

		for _, acc := range list.Resources {
			if !acc.Active {
				continue
			}
			members = append(members, &userpb.UserId{
				OpaqueId: acc.ID,
				Idp:      m.conf.IDProvider,
				Type:     userpb.UserType_USER_TYPE_PRIMARY,
			})
		}

		nextStart := startIndex + list.ItemsPerPage
		if nextStart > list.TotalResults {
			break
		}
		startIndex = nextStart
	}

	if err := m.cache.StoreMembers(gid, members); err != nil {
		appctx.GetLogger(ctx).Error().Err(err).Msg("indigoiam group: failed to cache members")
	}
	return members, nil
}

func (m *manager) HasMember(ctx context.Context, gid *grouppb.GroupId, uid *userpb.UserId) (bool, error) {
	members, err := m.GetMembers(ctx, gid)
	if err != nil {
		return false, err
	}
	for _, u := range members {
		if u.OpaqueId == uid.OpaqueId {
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

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

package rest

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	grouppb "github.com/cs3org/go-cs3apis/cs3/identity/group/v1beta1"
	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	"github.com/cs3org/reva/v3/pkg/appctx"
	"github.com/gomodule/redigo/redis"
)

const (
	groupPrefix           = "group:"
	idPrefix              = "id:"
	namePrefix            = "name:"
	gidPrefix             = "gid:"
	groupMembersPrefix    = "members:"
	groupInternalIDPrefix = "internal:"
)

func (m *manager) findCachedGroups(ctx context.Context, query string) ([]*grouppb.Group, error) {
	query = fmt.Sprintf("%s*%s*", groupPrefix, strings.ReplaceAll(strings.ToLower(query), " ", "_"))
	log := appctx.GetLogger(ctx)

	raw, err := m.redisPools.DoWithReadFallback(ctx, func(conn redis.Conn) (interface{}, error) {
		keys, err := redis.Strings(conn.Do("KEYS", query))
		if err != nil {
			return nil, err
		}
		if len(keys) == 0 {
			return []string{}, nil
		}
		args := make([]interface{}, len(keys))
		for i, k := range keys {
			args[i] = k
		}
		return redis.Strings(conn.Do("MGET", args...))
	})
	if err != nil {
		return nil, err
	}

	groupStrings := raw.([]string)
	groupMap := make(map[string]*grouppb.Group)
	for _, group := range groupStrings {
		g := grouppb.Group{}
		if err = json.Unmarshal([]byte(group), &g); err == nil {
			groupMap[g.Id.OpaqueId] = &g
		}
	}

	groups := make([]*grouppb.Group, 0, len(groupMap))
	for _, g := range groupMap {
		groups = append(groups, g)
	}
	log.Debug().Any("query", query).Int("results", len(groups)).Msg("rest: successfully found cached groups")
	return groups, nil
}

func (m *manager) fetchCachedGroupDetails(ctx context.Context, gid *grouppb.GroupId) (*grouppb.Group, error) {
	group, err := m.redisPools.GetVal(ctx, groupPrefix+idPrefix+gid.OpaqueId)
	if err != nil {
		return nil, err
	}

	g := grouppb.Group{}
	if err = json.Unmarshal([]byte(group), &g); err != nil {
		return nil, err
	}
	return &g, nil
}

func (m *manager) cacheGroupDetails(g *grouppb.Group) error {
	encodedGroup, err := json.Marshal(&g)
	if err != nil {
		return err
	}
	if err = m.redisPools.SetVal(groupPrefix+idPrefix+strings.ToLower(g.Id.OpaqueId), string(encodedGroup), 5*m.conf.GroupFetchInterval); err != nil {
		return err
	}

	if g.GidNumber != 0 {
		if err = m.redisPools.SetVal(groupPrefix+gidPrefix+strconv.FormatInt(g.GidNumber, 10), g.Id.OpaqueId, 5*m.conf.GroupFetchInterval); err != nil {
			return err
		}
	}
	if g.DisplayName != "" {
		if err = m.redisPools.SetVal(groupPrefix+namePrefix+g.Id.OpaqueId+"_"+strings.ToLower(g.DisplayName), g.Id.OpaqueId, 5*m.conf.GroupFetchInterval); err != nil {
			return err
		}
	}
	return nil
}

func (m *manager) fetchCachedGroupByParam(ctx context.Context, field, claim string) (*grouppb.Group, error) {
	group, err := m.redisPools.GetVal(ctx, groupPrefix+field+":"+strings.ToLower(claim))
	if err != nil {
		return nil, err
	}

	g := grouppb.Group{}
	if err = json.Unmarshal([]byte(group), &g); err != nil {
		return nil, err
	}
	return &g, nil
}

func (m *manager) fetchCachedGroupMembers(ctx context.Context, gid *grouppb.GroupId) ([]*userpb.UserId, error) {
	members, err := m.redisPools.GetVal(ctx, groupPrefix+groupMembersPrefix+strings.ToLower(gid.OpaqueId))
	if err != nil {
		return nil, err
	}
	u := []*userpb.UserId{}
	if err = json.Unmarshal([]byte(members), &u); err != nil {
		return nil, err
	}
	return u, nil
}

func (m *manager) cacheGroupMembers(gid *grouppb.GroupId, members []*userpb.UserId) error {
	u, err := json.Marshal(&members)
	if err != nil {
		return err
	}
	return m.redisPools.SetVal(groupPrefix+groupMembersPrefix+strings.ToLower(gid.OpaqueId), string(u), m.conf.GroupMembersCacheExpiration*60)
}

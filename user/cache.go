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

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	"github.com/cs3org/reva/v3/pkg/appctx"
	"github.com/cs3org/reva/v3/pkg/errtypes"
	"github.com/gomodule/redigo/redis"
)

const (
	userPrefix       = "user:"
	usernamePrefix   = "username:"
	namePrefix       = "name:"
	mailPrefix       = "mail:"
	uidPrefix        = "uid:"
	userGroupsPrefix = "groups:"
)

func (m *manager) findCachedUsers(ctx context.Context, query string) ([]*userpb.User, error) {
	query = fmt.Sprintf("%s*%s*", userPrefix, strings.ReplaceAll(strings.ToLower(query), " ", "_"))
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

	userStrings := raw.([]string)
	userMap := make(map[string]*userpb.User)
	for _, user := range userStrings {
		u := userpb.User{}
		// Only keep users that have no "inactive" status set (cf. parseAndCacheUser)
		if err = json.Unmarshal([]byte(user), &u); err == nil {
			if u.Status != userpb.UserStatus_USER_STATUS_INACTIVE {
				userMap[u.Id.OpaqueId] = &u
			}
		}
	}

	users := make([]*userpb.User, 0, len(userMap))
	for _, u := range userMap {
		users = append(users, u)
	}
	log.Debug().Any("query", query).Int("results", len(users)).Msg("rest: successfully found cached users")
	return users, nil
}

func (m *manager) fetchCachedUserDetails(ctx context.Context, uid *userpb.UserId) (*userpb.User, error) {
	user, err := m.redisPools.GetVal(ctx, userPrefix+usernamePrefix+strings.ToLower(uid.OpaqueId))
	if err != nil {
		return nil, err
	}

	u := userpb.User{}
	if err = json.Unmarshal([]byte(user), &u); err != nil {
		return nil, err
	}
	return &u, nil
}

func (m *manager) cacheUserDetails(u *userpb.User) error {
	encodedUser, err := json.Marshal(&u)
	if err != nil {
		return err
	}
	if err = m.redisPools.SetVal(userPrefix+usernamePrefix+strings.ToLower(u.Id.OpaqueId), string(encodedUser), 5*m.conf.UserFetchInterval); err != nil {
		return err
	}

	if u.Mail != "" {
		if err = m.redisPools.SetVal(userPrefix+mailPrefix+strings.ToLower(u.Mail), string(encodedUser), 5*m.conf.UserFetchInterval); err != nil {
			return err
		}
	}
	if u.DisplayName != "" {
		if err = m.redisPools.SetVal(userPrefix+namePrefix+u.Id.OpaqueId+"_"+strings.ReplaceAll(strings.ToLower(u.DisplayName), " ", "_"), string(encodedUser), 5*m.conf.UserFetchInterval); err != nil {
			return err
		}
	}
	if u.UidNumber != 0 {
		if err = m.redisPools.SetVal(userPrefix+uidPrefix+strconv.FormatInt(u.UidNumber, 10), string(encodedUser), 5*m.conf.UserFetchInterval); err != nil {
			return err
		}
	}
	return nil
}

func (m *manager) fetchCachedUserByParam(ctx context.Context, field, claim string) (*userpb.User, error) {
	// We do not want to support linked external accounts
	// These can be identified by having username equal to a CERN email
	if field == "username" && strings.HasSuffix(claim, "@cern.ch") {
		return nil, errtypes.Conflict("linked external accounts are not supported")
	}

	user, err := m.redisPools.GetVal(ctx, userPrefix+field+":"+strings.ToLower(claim))
	if err != nil {
		return nil, err
	}

	u := userpb.User{}
	if err = json.Unmarshal([]byte(user), &u); err != nil {
		return nil, err
	}
	return &u, nil
}

func (m *manager) fetchCachedUserGroups(ctx context.Context, uid *userpb.UserId) ([]string, error) {
	groups, err := m.redisPools.GetVal(ctx, userPrefix+userGroupsPrefix+strings.ToLower(uid.OpaqueId))
	if err != nil {
		return nil, err
	}
	g := []string{}
	if err = json.Unmarshal([]byte(groups), &g); err != nil {
		return nil, err
	}
	return g, nil
}

func (m *manager) cacheUserGroups(uid *userpb.UserId, groups []string) error {
	g, err := json.Marshal(&groups)
	if err != nil {
		return err
	}
	return m.redisPools.SetVal(userPrefix+userGroupsPrefix+strings.ToLower(uid.OpaqueId), string(g), m.conf.UserGroupsCacheExpiration*60)
}

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
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cernbox/reva-plugins/cache"
	"github.com/cernbox/reva-plugins/redispools"
	"github.com/cernbox/reva-plugins/user/rest/api"
	revautils "github.com/cs3org/reva/v3/pkg/cbox/utils"
	"github.com/cs3org/reva/v3/pkg/errtypes"
	"github.com/cs3org/reva/v3/pkg/invoke"
	"github.com/gomodule/redigo/redis"
)

var _ invoke.Invokable = (*manager)(nil)

var identities = map[string]map[string]any{
	"svcbox": {
		"id": "11111111-0000-0000-0000-000000000001", "upn": "svcbox", "type": "Service", "source": "cern",
		"displayName": "CERNBox service", "primaryAccountEmail": "svcbox@cern.ch", "uid": 1001, "gid": 2763,
		"activeUser": true, "blocked": false, "blockingTime": nil, "ownerId": "11111111-0000-0000-0000-000000000002",
	},
	"11111111-0000-0000-0000-000000000002": {
		"id": "11111111-0000-0000-0000-000000000002", "upn": "jdoe", "type": "Person", "source": "cern",
		"displayName": "John Doe", "firstName": "John", "lastName": "Doe", "primaryAccountEmail": "john.doe@cern.ch",
		"uid": 1000, "gid": 1028, "cernDepartment": "IT", "cernGroup": "SD", "cernSection": "GSS",
		"telephone1": "+41227670000", "portablePhone": "+41754110000", "activeUser": true,
		"ownerId": "11111111-0000-0000-0000-000000000002",
	},
	"left": {
		"id": "11111111-0000-0000-0000-000000000003", "upn": "left", "type": "Person", "source": "cern",
		"displayName": "Gone User", "uid": 1002, "gid": 1028, "activeUser": false, "disabled": true,
		"blocked": true, "blockingReason": "Left CERN", "blockingTime": "2026-03-01T08:30:00.123456",
		"blockingDeadline": "2026-09-01T00:00:00Z",
	},
}

var byEmail = map[string][]map[string]any{
	"guest@example.org": {
		{"upn": "guest", "type": "Person", "source": "cern", "uid": 1003, "primaryAccountEmail": "guest@example.org"},
		{"id": "11111111-0000-0000-0000-000000000004", "upn": "guest@example.org", "type": "Person", "source": "edugain",
			"displayName": "Guest", "primaryAccountEmail": "guest@example.org", "activeUser": true},
	},
}

func newTestManager(t *testing.T) *manager {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "tok", "expires_in": 1200})
			return
		}
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if !slices.Contains(r.URL.Query()["field"], "ownerId") {
			http.Error(w, "fields not selected", http.StatusBadRequest)
			return
		}
		if email, ok := strings.CutPrefix(r.URL.Path, "/api/v1.0/Identity/by_email/"); ok {
			_ = json.NewEncoder(w).Encode(map[string]any{"data": byEmail[email]})
			return
		}
		id, _ := strings.CutPrefix(r.URL.Path, "/api/v1.0/Identity/")
		i, ok := identities[id]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": i})
	}))
	t.Cleanup(srv.Close)

	tm, err := revautils.InitAPITokenManager(map[string]any{
		"oidc_token_endpoint": srv.URL + "/token", "target_api": "authorization-service-api",
		"client_id": "id", "client_secret": "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Without Redis every lookup is a cache miss, which the invocation must handle.
	noRedis := &redis.Pool{Dial: func() (redis.Conn, error) { return nil, errors.New("no redis") }}
	m := &manager{
		conf:            &config{APIBaseURL: srv.URL},
		cache:           cache.NewUserCache(&redispools.RedisPools{Read: noRedis, Write: noRedis}, 3600, 5),
		apiTokenManager: tm,
	}
	m.registerInvocations()
	return m
}

func invokeAccount(t *testing.T, m *manager, user string) (*api.Account, error) {
	t.Helper()
	r, err := m.Invoke(context.Background(), api.Name, map[string]any{api.ArgUser: user})
	if err != nil {
		return nil, err
	}
	// The result goes through JSON in the Admin API.
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(b, &wire); err != nil {
		t.Fatal(err)
	}
	a, err := api.FromResult(wire)
	if err != nil {
		t.Fatal(err)
	}
	return a, nil
}

func TestAccountCatalogue(t *testing.T) {
	specs := newTestManager(t).Invocations()
	if len(specs) != 1 || specs[0].Name != api.Name || specs[0].Kind != invoke.KindReadonly || specs[0].Scope != invoke.ScopeShared {
		t.Fatalf("specs = %+v", specs)
	}
	if len(specs[0].Args) != 1 || specs[0].Args[0].Name != api.ArgUser || !specs[0].Args[0].Required {
		t.Fatalf("args = %+v", specs[0].Args)
	}
}

func TestAccountServiceWithOwner(t *testing.T) {
	a, err := invokeAccount(t, newTestManager(t), "svcbox")
	if err != nil {
		t.Fatal(err)
	}
	if a.Username != "svcbox" || a.Type != "service" || a.UIDNumber != 1001 || a.Blocked || a.BlockingTime != nil {
		t.Fatalf("account = %+v", a)
	}
	o := a.Owner
	if o == nil {
		t.Fatal("no owner")
	}
	want := api.Account{
		Username: "jdoe", Type: "primary", Source: "cern", DisplayName: "John Doe", FirstName: "John", LastName: "Doe",
		Mail: "john.doe@cern.ch", UIDNumber: 1000, GIDNumber: 1028, Department: "IT", Group: "SD", Section: "GSS",
		Phone: "+41227670000", Mobile: "+41754110000", Active: true,
	}
	if *o != want {
		t.Fatalf("owner = %+v, want %+v", *o, want)
	}
}

func TestAccountBlockedPrimary(t *testing.T) {
	a, err := invokeAccount(t, newTestManager(t), "left")
	if err != nil {
		t.Fatal(err)
	}
	wantTime := time.Date(2026, 3, 1, 8, 30, 0, 123456000, time.UTC)
	wantDeadline := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	if a.Type != "primary" || a.Active || !a.Disabled || !a.Blocked || a.BlockingReason != "Left CERN" || a.Owner != nil {
		t.Fatalf("account = %+v", a)
	}
	if a.BlockingTime == nil || !a.BlockingTime.Equal(wantTime) || a.BlockingDeadline == nil || !a.BlockingDeadline.Equal(wantDeadline) {
		t.Fatalf("blocking time = %v, deadline = %v", a.BlockingTime, a.BlockingDeadline)
	}
}

func TestAccountLightweight(t *testing.T) {
	a, err := invokeAccount(t, newTestManager(t), "guest@example.org")
	if err != nil {
		t.Fatal(err)
	}
	if a.Username != "guest@example.org" || a.Type != "lightweight" || a.Source != "edugain" || a.Owner != nil {
		t.Fatalf("account = %+v", a)
	}
}

func TestAccountNotFound(t *testing.T) {
	m := newTestManager(t)
	for _, user := range []string{"nobody", "nobody@example.org"} {
		_, err := invokeAccount(t, m, user)
		if _, ok := errors.AsType[errtypes.NotFound](err); !ok {
			t.Errorf("%s: err = %v, want not found", user, err)
		}
	}
}

func TestAccountNeedsUser(t *testing.T) {
	if _, err := newTestManager(t).Invoke(context.Background(), api.Name, map[string]any{}); err == nil {
		t.Fatal("no error without user")
	}
}

func TestAccountResultKeys(t *testing.T) {
	r, err := (&api.Account{}).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"username", "type", "department", "blocked", "blocking_time", "owner"} {
		if _, ok := r[k]; !ok {
			t.Errorf("no key %q in %v", k, r)
		}
	}
}

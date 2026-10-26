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

package user_test

import (
	"encoding/json"
	"maps"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/cernbox/reva-plugins/cli/internal/fakeadmin"
	_ "github.com/cernbox/reva-plugins/cli/user"
	"github.com/cernbox/reva-plugins/user/rest/api"
	revacmd "github.com/cs3org/reva/v3/cmd/reva/app"
	"github.com/cs3org/reva/v3/pkg/admin/adminpb"
	"github.com/cs3org/reva/v3/pkg/cli"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TODO(T31): remove the stand-in when reva has the core command `user`.
func TestMain(m *testing.M) {
	if c, _, err := revacmd.NewRoot().Find([]string{"user"}); err != nil || c.Name() != "user" {
		cli.RegisterCommand("user", func(*cli.Runtime) *cobra.Command { return &cobra.Command{Use: "user"} })
	}
	m.Run()
}

var service = &api.Account{
	Username:         "svc-backup",
	Type:             "service",
	Source:           "cern",
	DisplayName:      "Backup service",
	Mail:             "svc-backup@cern.ch",
	UIDNumber:        12345,
	GIDNumber:        2766,
	Department:       "IT",
	Group:            "SD",
	Active:           true,
	Blocked:          true,
	BlockingReason:   "owner left CERN",
	BlockingTime:     new(time.Date(2026, 9, 1, 8, 0, 0, 0, time.UTC)),
	BlockingDeadline: new(time.Date(2026, 12, 1, 8, 0, 0, 0, time.UTC)),
	Owner: &api.Account{
		Username:    "jdoe",
		Type:        "primary",
		DisplayName: "John Doe",
		Mail:        "john.doe@cern.ch",
		Blocked:     true,
	},
}

func accountServer(t *testing.T) *fakeadmin.Server {
	return fakeadmin.Start(t, func(req *adminpb.InvokeRequest) (any, error) {
		if req.Args[api.ArgUser] != service.Username {
			return nil, status.Error(codes.NotFound, "user not found")
		}
		return service.Result()
	})
}

func TestAccount(t *testing.T) {
	s := accountServer(t)
	out, err := s.Run(t, "user", "account", "svc-backup")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"service",
		"jdoe (John Doe, john.doe@cern.ch)",
		"not active, blocked",
		"IT-SD",
		"12345/2766",
		"owner left CERN",
		"2026-",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("no %q in:\n%s", want, out)
		}
	}
	calls := s.Calls()
	if len(calls) != 1 {
		t.Fatalf("%d calls", len(calls))
	}
	c := calls[0]
	if c.Service != "localhost:1/"+api.Target || c.Invocation != api.Invocation || !maps.Equal(c.Args, map[string]string{api.ArgUser: "svc-backup"}) {
		t.Errorf("call %s %s %v", c.Service, c.Invocation, c.Args)
	}
}

func TestAccountJSON(t *testing.T) {
	out, err := accountServer(t).Run(t, "-o", "json", "user", "account", "svc-backup")
	if err != nil {
		t.Fatal(err)
	}
	var got api.Account
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(&got, service) {
		t.Errorf("got %+v, want %+v", got, *service)
	}
}

func TestPrimaryAccountHasNoOwner(t *testing.T) {
	s := fakeadmin.Start(t, func(*adminpb.InvokeRequest) (any, error) {
		return (&api.Account{Username: "jdoe", Type: "primary", Active: true}).Result()
	})
	out, err := s.Run(t, "user", "account", "jdoe")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "Owner state") || strings.Contains(out, "Blocking") {
		t.Errorf("rows without a value are shown:\n%s", out)
	}
	for _, want := range []string{"Owner", "Blocked"} {
		if !strings.Contains(out, want) {
			t.Errorf("no row %q in:\n%s", want, out)
		}
	}
}

func TestAccountErrors(t *testing.T) {
	s := accountServer(t)
	if _, err := s.Run(t, "user", "account", "nobody"); status.Code(err) != codes.NotFound {
		t.Errorf("err %v, want not found", err)
	}
	if _, err := s.Run(t, "user", "account"); err == nil {
		t.Error("no error without a user")
	}
	if _, err := s.Run(t, "--as", "alice", "user", "account", "jdoe"); err == nil || !strings.Contains(err.Error(), "--as") {
		t.Errorf("err %v, want a refusal of --as", err)
	}
	if n := len(s.Calls()); n != 1 {
		t.Errorf("%d calls, want 1", n)
	}
}

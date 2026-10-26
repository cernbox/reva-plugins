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

package otg_test

import (
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cernbox/reva-plugins/cli/internal/fakeadmin"
	_ "github.com/cernbox/reva-plugins/cli/otg"
	"github.com/cernbox/reva-plugins/otg/api"
	revacmd "github.com/cs3org/reva/v3/cmd/reva/app"
	"github.com/cs3org/reva/v3/pkg/admin/adminpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// otgServer keeps the message in memory, as the otg service does in its table.
func otgServer(t *testing.T, st api.Status) *fakeadmin.Server {
	return fakeadmin.Start(t, func(req *adminpb.InvokeRequest) (any, error) {
		switch req.Invocation {
		case api.Get:
		case api.Set:
			st = api.Status{Present: true, Number: req.Args[api.ArgNumber], Message: req.Args[api.ArgMessage]}
		case api.Clear:
			st = api.Status{}
		default:
			return nil, status.Error(codes.NotFound, "no invocation "+req.Invocation)
		}
		return st, nil
	})
}

func onlyCall(t *testing.T, s *fakeadmin.Server, invocation string, args map[string]string) {
	t.Helper()
	calls := s.Calls()
	if len(calls) != 1 {
		t.Fatalf("%d calls, want 1: %v", len(calls), calls)
	}
	c := calls[0]
	if c.Service != "localhost:1/"+api.Service || c.Invocation != invocation || !maps.Equal(c.Args, args) {
		t.Errorf("call %s %s %v, want %s %v", c.Service, c.Invocation, c.Args, invocation, args)
	}
}

func TestGet(t *testing.T) {
	s := otgServer(t, api.Status{Present: true, Number: "OTG0060602", Message: "EOS is <b>slow</b>"})
	out, err := s.Run(t, "otg", "get")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"NUMBER", "OTG0060602", "EOS is <b>slow</b>"} {
		if !strings.Contains(out, want) {
			t.Errorf("no %q in:\n%s", want, out)
		}
	}
	onlyCall(t, s, api.Get, nil)
}

func TestGetJSON(t *testing.T) {
	want := api.Status{Present: true, Number: "OTG0060602", Message: "EOS is <b>slow</b>"}
	s := otgServer(t, want)
	out, err := s.Run(t, "-o", "json", "otg", "get")
	if err != nil {
		t.Fatal(err)
	}
	var got api.Status
	if err := json.Unmarshal([]byte(out), &got); err != nil || got != want {
		t.Errorf("got %+v (%v), want %+v", got, err, want)
	}
}

func TestGetNone(t *testing.T) {
	out, err := otgServer(t, api.Status{}).Run(t, "otg", "get")
	if err != nil || out != "No OTG message.\n" {
		t.Errorf("out %q, err %v", out, err)
	}
}

func TestSet(t *testing.T) {
	msg := `EOS is <b>slow</b>, see <a href="https://cern.service-now.com/x">the SSB</a>`
	s := otgServer(t, api.Status{})
	out, err := s.Run(t, "--yes", "otg", "set", "--number", "OTG0060602", "--message", msg)
	if err != nil {
		t.Fatal(err)
	}
	onlyCall(t, s, api.Set, map[string]string{api.ArgNumber: "OTG0060602", api.ArgMessage: msg})
	if !strings.Contains(out, "OTG0060602") {
		t.Errorf("the new message is not shown:\n%s", out)
	}
}

func TestSetMessageFile(t *testing.T) {
	msg := "EOS is <b>slow</b>.\nSee the SSB."
	file := filepath.Join(t.TempDir(), "otg.html")
	if err := os.WriteFile(file, []byte(msg+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s := otgServer(t, api.Status{})
	if _, err := s.Run(t, "--yes", "otg", "set", "--number", "OTG1", "--message-file", file); err != nil {
		t.Fatal(err)
	}
	onlyCall(t, s, api.Set, map[string]string{api.ArgNumber: "OTG1", api.ArgMessage: msg})

	s = otgServer(t, api.Status{})
	if _, err := s.RunIn(t, strings.NewReader(msg), "--yes", "otg", "set", "--number", "OTG1", "--message-file", "-"); err != nil {
		t.Fatal(err)
	}
	onlyCall(t, s, api.Set, map[string]string{api.ArgNumber: "OTG1", api.ArgMessage: msg})
}

func TestSetBadUsage(t *testing.T) {
	for name, args := range map[string][]string{
		"no number":     {"--message", "m"},
		"no message":    {"--number", "OTG1"},
		"two messages":  {"--number", "OTG1", "--message", "m", "--message-file", "f"},
		"empty message": {"--number", "OTG1", "--message", "  "},
		"an argument":   {"--number", "OTG1", "--message", "m", "extra"},
	} {
		t.Run(name, func(t *testing.T) {
			s := otgServer(t, api.Status{})
			if _, err := s.Run(t, append([]string{"--yes", "otg", "set"}, args...)...); err == nil {
				t.Error("no error")
			}
			if n := len(s.Calls()); n != 0 {
				t.Errorf("%d calls", n)
			}
		})
	}
}

// Without --yes and without a terminal, set and clear change nothing.
func TestConfirmation(t *testing.T) {
	for _, args := range [][]string{{"otg", "set", "--number", "OTG1", "--message", "m"}, {"otg", "clear"}} {
		s := otgServer(t, api.Status{Present: true, Number: "OTG1", Message: "m"})
		if _, err := s.Run(t, args...); err == nil || !strings.Contains(err.Error(), "--yes") {
			t.Errorf("%v: err %v, want a refusal", args, err)
		}
		if n := len(s.Calls()); n != 0 {
			t.Errorf("%v: %d calls", args, n)
		}
	}
}

func TestClear(t *testing.T) {
	s := otgServer(t, api.Status{Present: true, Number: "OTG1", Message: "m"})
	out, err := s.Run(t, "--yes", "otg", "clear")
	if err != nil || out != "No OTG message.\n" {
		t.Errorf("out %q, err %v", out, err)
	}
	onlyCall(t, s, api.Clear, nil)
}

func TestServerError(t *testing.T) {
	s := fakeadmin.Start(t, func(*adminpb.InvokeRequest) (any, error) {
		return nil, status.Error(codes.Internal, "otg: database is down")
	})
	_, err := s.Run(t, "otg", "get")
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "database is down") {
		t.Errorf("err %v", err)
	}
}

func TestOperatorCommand(t *testing.T) {
	root := revacmd.NewRoot()
	c, _, err := root.Find([]string{"otg", "set"})
	if err != nil || c.Name() != "set" {
		t.Fatalf("no otg set: %v", err)
	}
	if otg := c.Parent(); otg.GroupID != "fleet" || !strings.HasSuffix(otg.Short, "(admin)") {
		t.Errorf("otg: group %q, short %q", otg.GroupID, otg.Short)
	}
	// --as is for the user plane only.
	_, err = otgServer(t, api.Status{}).Run(t, "--as", "alice", "otg", "get")
	if err == nil || !strings.Contains(err.Error(), "--as") {
		t.Errorf("err %v, want a refusal of --as", err)
	}
}

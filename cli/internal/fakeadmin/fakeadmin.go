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

// Package fakeadmin runs the CLI against a fake Admin API, for the tests of
// the CLI plugins.
package fakeadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	revacmd "github.com/cs3org/reva/v3/cmd/reva/app"
	"github.com/cs3org/reva/v3/pkg/admin/adminpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// Handler answers an invocation with a result, which is sent as JSON, or
// with an error.
type Handler func(req *adminpb.InvokeRequest) (any, error)

// Server is an Admin API with one ready instance of every service.
type Server struct {
	adminpb.UnimplementedAdminAPIServer
	addr   string
	handle Handler

	mu    sync.Mutex
	calls []*adminpb.InvokeRequest
}

// Start serves a fake Admin API on a Unix socket. The CLI needs no token for
// a socket, so the tests need no sign-in.
func Start(t *testing.T, handle Handler) *Server {
	t.Helper()
	// t.TempDir can be longer than the limit of a socket path.
	dir, err := os.MkdirTemp("", "fakeadmin")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "admin.sock")
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{addr: "unix://" + sock, handle: handle}
	g := grpc.NewServer()
	adminpb.RegisterAdminAPIServer(g, s)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)
	return s
}

// Calls returns the invocations that the server received.
func (s *Server) Calls() []*adminpb.InvokeRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*adminpb.InvokeRequest(nil), s.calls...)
}

// Run runs the command line args of the CLI, with the plugins that the test
// binary links, against the server. It returns what the command writes to
// stdout.
func (s *Server) Run(t *testing.T, args ...string) (string, error) {
	t.Helper()
	return s.RunIn(t, strings.NewReader(""), args...)
}

// RunIn is Run with in as stdin.
func (s *Server) RunIn(t *testing.T, in io.Reader, args ...string) (string, error) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(dir, ".config"))
	for _, env := range []string{"REVA_GATEWAY", "REVA_HOST", "REVA_ADMIN_HOST", "REVA_TOKEN", "REVA_NO_ELEVATE", "SUDO_USER"} {
		t.Setenv(env, "")
	}
	root := revacmd.NewRoot()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(io.Discard)
	root.SetIn(in)
	root.SetArgs(append([]string{"--admin-host", s.addr}, args...))
	err := root.Execute()
	return out.String(), err
}

func (s *Server) ListServices(_ context.Context, req *adminpb.ListServicesRequest) (*adminpb.ListServicesResponse, error) {
	return &adminpb.ListServicesResponse{Services: []*adminpb.Service{{
		Name:  req.Service,
		Nodes: []*adminpb.Node{{Id: "localhost:1/" + req.Service, State: "ready"}},
	}}}, nil
}

func (s *Server) Invoke(_ context.Context, req *adminpb.InvokeRequest) (*adminpb.InvokeResponse, error) {
	s.mu.Lock()
	s.calls = append(s.calls, req)
	s.mu.Unlock()
	res := &adminpb.NodeResult{Node: req.Service}
	v, err := s.handle(req)
	if err != nil {
		st := status.Convert(err)
		res.Error, res.Code = st.Message(), int32(st.Code())
	} else {
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		res.ResultJson = string(b)
	}
	return &adminpb.InvokeResponse{Results: []*adminpb.NodeResult{res}}, nil
}

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


package cback

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	gateway "github.com/cs3org/go-cs3apis/cs3/gateway/v1beta1"
	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	storage "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/v3/pkg/appctx"
	"github.com/cs3org/reva/v3/pkg/service"
	"google.golang.org/grpc"
)

type fakeGateway struct {
	gateway.GatewayAPIClient
	stat func(*storage.StatRequest) (*storage.StatResponse, error)
}

func (g *fakeGateway) Stat(_ context.Context, req *storage.StatRequest, _ ...grpc.CallOption) (*storage.StatResponse, error) {
	return g.stat(req)
}

type fakeResolver struct {
	service.Clients
	gw gateway.GatewayAPIClient
}

func (r fakeResolver) Gateway(context.Context) (gateway.GatewayAPIClient, error) {
	return r.gw, nil
}

var testGateway = &fakeGateway{}

// The global resolver can be set only once per process.
func TestMain(m *testing.M) {
	service.SetGlobal(fakeResolver{gw: testGateway})
	os.Exit(m.Run())
}

func TestCreateRestoreStatsThroughGateway(t *testing.T) {
	tests := []struct {
		name string
		resp *storage.StatResponse
		err  error
		want int
	}{
		{
			name: "gateway error",
			err:  errors.New("unavailable"),
			want: http.StatusInternalServerError,
		},
		{
			name: "not found",
			resp: &storage.StatResponse{Status: &rpc.Status{Code: rpc.Code_CODE_NOT_FOUND}},
			want: http.StatusNotFound,
		},
		{
			name: "other storage",
			resp: &storage.StatResponse{
				Status: &rpc.Status{Code: rpc.Code_CODE_OK},
				Info:   &storage.ResourceInfo{Id: &storage.ResourceId{StorageId: "eos"}},
			},
			want: http.StatusBadRequest,
		},
	}

	s, err := New(context.Background(), map[string]any{"storage_id": "cback"})
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotPath string
			testGateway.stat = func(req *storage.StatRequest) (*storage.StatResponse, error) {
				gotPath = req.Ref.Path
				return tt.resp, tt.err
			}

			r := httptest.NewRequest(http.MethodPost, "/restores?path=/eos/user/a/alice", nil)
			r = r.WithContext(appctx.ContextSetUser(r.Context(), &userpb.User{Username: "alice"}))
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)

			if w.Code != tt.want {
				t.Errorf("status = %d, want %d", w.Code, tt.want)
			}
			if gotPath != "/eos/user/a/alice" {
				t.Errorf("stat path = %q, want /eos/user/a/alice", gotPath)
			}
		})
	}
}

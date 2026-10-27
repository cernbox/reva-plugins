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

package thumbnails

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	gateway "github.com/cs3org/go-cs3apis/cs3/gateway/v1beta1"
	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/v3/pkg/errtypes"
	"github.com/cs3org/reva/v3/pkg/httpclient"
	"github.com/cs3org/reva/v3/pkg/service"
	"google.golang.org/grpc"
)

type fakeGateway struct {
	gateway.GatewayAPIClient
	stat     func(*provider.StatRequest) (*provider.StatResponse, error)
	download func(*provider.InitiateFileDownloadRequest) (*gateway.InitiateFileDownloadResponse, error)
}

func (g *fakeGateway) Stat(_ context.Context, req *provider.StatRequest, _ ...grpc.CallOption) (*provider.StatResponse, error) {
	return g.stat(req)
}

func (g *fakeGateway) InitiateFileDownload(_ context.Context, req *provider.InitiateFileDownloadRequest, _ ...grpc.CallOption) (*gateway.InitiateFileDownloadResponse, error) {
	return g.download(req)
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

func TestStatResThroughGateway(t *testing.T) {
	s := &Thumbnails{}
	ref := &provider.Reference{Path: "/eos/user/a/alice/pic.png"}

	testGateway.stat = func(req *provider.StatRequest) (*provider.StatResponse, error) {
		if req.Ref.Path != ref.Path {
			t.Errorf("stat path = %q, want %q", req.Ref.Path, ref.Path)
		}
		return &provider.StatResponse{
			Status: &rpc.Status{Code: rpc.Code_CODE_OK},
			Info:   &provider.ResourceInfo{Path: ref.Path},
		}, nil
	}
	info, err := s.statRes(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if info.Path != ref.Path {
		t.Errorf("info path = %q, want %q", info.Path, ref.Path)
	}

	testGateway.stat = func(*provider.StatRequest) (*provider.StatResponse, error) {
		return &provider.StatResponse{Status: &rpc.Status{Code: rpc.Code_CODE_NOT_FOUND}}, nil
	}
	var nf errtypes.IsNotFound
	if _, err := s.statRes(context.Background(), ref); !errors.As(err, &nf) {
		t.Errorf("err = %v, want not found", err)
	}
}

func TestGatewayDownloader(t *testing.T) {
	data := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("image"))
	}))
	defer data.Close()

	testGateway.download = func(req *provider.InitiateFileDownloadRequest) (*gateway.InitiateFileDownloadResponse, error) {
		if req.Ref.Path != "/eos/pic.png" {
			t.Errorf("download path = %q, want /eos/pic.png", req.Ref.Path)
		}
		return &gateway.InitiateFileDownloadResponse{
			Status:    &rpc.Status{Code: rpc.Code_CODE_OK},
			Protocols: []*gateway.FileDownloadProtocol{{Protocol: "simple", DownloadEndpoint: data.URL}},
		}, nil
	}

	r, err := gatewayDownloader{client: httpclient.New()}.Download(context.Background(), "/eos/pic.png", "")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "image" {
		t.Errorf("body = %q, want image", b)
	}
}

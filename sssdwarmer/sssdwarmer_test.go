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

package sssdwarmer

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"

	grouppb "github.com/cs3org/go-cs3apis/cs3/identity/group/v1beta1"
	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	registry "github.com/cs3org/go-cs3apis/cs3/storage/registry/v1beta1"
	"github.com/cs3org/reva/v3/pkg/appctx"
	"github.com/cs3org/reva/v3/pkg/auth/scope"
	"github.com/cs3org/reva/v3/pkg/rserverless"
	"github.com/cs3org/reva/v3/pkg/service"
	"github.com/cs3org/reva/v3/pkg/token/manager/jwt"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// The plugin loader casts New to this type.
var _ rserverless.NewService = New

// fakeProvider answers ListContainer and ListGrants by path. A path it does not
// know is answered with permission denied.
type fakeProvider struct {
	provider.ProviderAPIClient
	folders map[string][]*provider.ResourceInfo
	grants  map[string][]*provider.Grant
	// down fails every call, as when the provider cannot be reached.
	down bool
}

func (f *fakeProvider) check(ctx context.Context) error {
	return check(ctx, f.down)
}

func check(ctx context.Context, down bool) error {
	if down {
		return errors.New("connection refused")
	}
	if md, _ := metadata.FromOutgoingContext(ctx); len(md.Get(appctx.TokenHeader)) == 0 {
		return errors.New("no token")
	}
	return nil
}

// fakeRegistry gives the address of a mount id, and answers an unknown one with
// an internal error, like the dynamic storage registry.
type fakeRegistry struct {
	registry.RegistryAPIClient
	addresses map[string]string
	down      bool
}

func (f *fakeRegistry) GetStorageProviders(ctx context.Context, req *registry.GetStorageProvidersRequest, _ ...grpc.CallOption) (*registry.GetStorageProvidersResponse, error) {
	if err := check(ctx, f.down); err != nil {
		return nil, err
	}
	id := req.GetRef().GetResourceId().GetStorageId()
	addr, ok := f.addresses[id]
	if !ok {
		return &registry.GetStorageProvidersResponse{Status: &rpc.Status{Code: rpc.Code_CODE_INTERNAL, Message: "storage provider address not found for " + id}}, nil
	}
	return &registry.GetStorageProvidersResponse{
		Status:    &rpc.Status{Code: rpc.Code_CODE_OK},
		Providers: []*registry.ProviderInfo{{ProviderId: id, Address: addr}},
	}, nil
}

func (f *fakeProvider) ListContainer(ctx context.Context, req *provider.ListContainerRequest, _ ...grpc.CallOption) (*provider.ListContainerResponse, error) {
	if err := f.check(ctx); err != nil {
		return nil, err
	}
	infos, ok := f.folders[req.GetRef().GetPath()]
	if !ok {
		return &provider.ListContainerResponse{Status: &rpc.Status{Code: rpc.Code_CODE_PERMISSION_DENIED}}, nil
	}
	return &provider.ListContainerResponse{Status: &rpc.Status{Code: rpc.Code_CODE_OK}, Infos: infos}, nil
}

func (f *fakeProvider) ListGrants(ctx context.Context, req *provider.ListGrantsRequest, _ ...grpc.CallOption) (*provider.ListGrantsResponse, error) {
	if err := f.check(ctx); err != nil {
		return nil, err
	}
	grants, ok := f.grants[req.GetRef().GetPath()]
	if !ok {
		return &provider.ListGrantsResponse{Status: &rpc.Status{Code: rpc.Code_CODE_PERMISSION_DENIED}}, nil
	}
	return &provider.ListGrantsResponse{Status: &rpc.Status{Code: rpc.Code_CODE_OK}, Grants: grants}, nil
}

func folder(p string) *provider.ResourceInfo {
	return &provider.ResourceInfo{Path: p, Type: provider.ResourceType_RESOURCE_TYPE_CONTAINER}
}

func file(p string) *provider.ResourceInfo {
	return &provider.ResourceInfo{Path: p, Type: provider.ResourceType_RESOURCE_TYPE_FILE}
}

func groupGrant(name string) *provider.Grant {
	return &provider.Grant{Grantee: &provider.Grantee{
		Type: provider.GranteeType_GRANTEE_TYPE_GROUP,
		Id:   &provider.Grantee_GroupId{GroupId: &grouppb.GroupId{OpaqueId: name}},
	}}
}

func userGrant(name string) *provider.Grant {
	return &provider.Grant{Grantee: &provider.Grantee{
		Type: provider.GranteeType_GRANTEE_TYPE_USER,
		Id:   &provider.Grantee_UserId{UserId: &userpb.UserId{OpaqueId: name}},
	}}
}

// pin makes the service find p at addr.
func pin(t *testing.T, addr string, p *fakeProvider) {
	service.SetClientAt(addr, p)
	t.Cleanup(func() { service.SetClientAt(addr, nil) })
}

func newTestSvc(t *testing.T, reg *fakeRegistry, mountIDs ...string) *svc {
	tokens, err := jwt.New(map[string]any{"secret": "test"})
	if err != nil {
		t.Fatal(err)
	}
	scopes, err := scope.AddOwnerScope(nil)
	if err != nil {
		t.Fatal(err)
	}
	log := zerolog.Nop()
	return &svc{
		conf:        &config{MountIDs: mountIDs, MountPath: "/winspaces", SpaceDepth: 3},
		log:         &log,
		tokens:      tokens,
		serviceUser: &userpb.User{Id: &userpb.UserId{OpaqueId: "cbox"}, Username: "cbox"},
		scope:       scopes,
		storageRegistry: func(context.Context) (registry.RegistryAPIClient, error) {
			return reg, nil
		},
	}
}

func TestScan(t *testing.T) {
	hdd := &fakeProvider{
		folders: map[string][]*provider.ResourceInfo{
			"/winspaces":   {folder("/winspaces/c"), folder("/winspaces/d"), folder("/winspaces/.recycle"), file("/winspaces/README")},
			"/winspaces/c": {folder("/winspaces/c/cv-design-office"), file("/winspaces/c/notes.txt")},
			// /winspaces/d cannot be listed, so it is skipped
			// /winspaces/.recycle is hidden, so it is not listed
			"/winspaces/.recycle": {folder("/winspaces/.recycle/c")},
		},
		grants: map[string][]*provider.Grant{
			"/winspaces/c/cv-design-office": {groupGrant("CERNBOX-Project-CV-Design-Office-Admins"), userGrant("marchamb")},
			"/winspaces/.recycle/c":         {groupGrant("cernbox-project-deleted-admins")},
		},
	}
	gen := &fakeProvider{
		folders: map[string][]*provider.ResourceInfo{
			"/winspaces":   {folder("/winspaces/a")},
			"/winspaces/a": {folder("/winspaces/a/atlas-x"), folder("/winspaces/a/locked")},
		},
		grants: map[string][]*provider.Grant{
			"/winspaces/a/atlas-x": {groupGrant("cernbox-project-atlas-x-readers")},
			// the grants of /winspaces/a/locked cannot be read, so it is skipped
		},
	}
	pin(t, "hdd:21001", hdd)
	pin(t, "gen:21002", gen)

	// doyletest is not in the registry, so it is skipped
	reg := &fakeRegistry{addresses: map[string]string{"doylehdd01": "hdd:21001", "doylegen01": "gen:21002"}}
	s := newTestSvc(t, reg, "doylehdd01", "doylegen01", "doyletest")
	if err := s.scan(context.Background()); err != nil {
		t.Fatalf("scan() error = %v", err)
	}
	want := []string{"cernbox-project-atlas-x-readers", "cernbox-project-cv-design-office-admins"}
	if got := slices.Sorted(maps.Keys(*s.aclGroups.Load())); !slices.Equal(got, want) {
		t.Errorf("scan() groups = %v, want %v", got, want)
	}

	// A provider that cannot be reached fails the scan, and the groups of the
	// previous one stay.
	gen.down = true
	if err := s.scan(context.Background()); err == nil {
		t.Error("scan() with a provider down did not fail")
	}
	if got := slices.Sorted(maps.Keys(*s.aclGroups.Load())); !slices.Equal(got, want) {
		t.Errorf("a failed scan changed the groups to %v", got)
	}

	// So does a registry that cannot be reached.
	gen.down, reg.down = false, true
	if err := s.scan(context.Background()); err == nil {
		t.Error("scan() with the registry down did not fail")
	}
	if got := slices.Sorted(maps.Keys(*s.aclGroups.Load())); !slices.Equal(got, want) {
		t.Errorf("a failed scan changed the groups to %v", got)
	}
}

func TestScanNoGroups(t *testing.T) {
	pin(t, "empty:21004", &fakeProvider{folders: map[string][]*provider.ResourceInfo{"/winspaces": {}}})

	s := newTestSvc(t, &fakeRegistry{addresses: map[string]string{"doyletest": "empty:21004"}}, "doyletest")
	if err := s.scan(context.Background()); err == nil || !strings.Contains(err.Error(), "no groups") {
		t.Errorf("scan() error = %v, want no groups", err)
	}
	if s.aclGroups.Load() != nil {
		t.Error("a scan without groups stored some")
	}
}

func TestNewConfig(t *testing.T) {
	valid := map[string]any{
		"mount_ids":         []string{"doylehdd01"},
		"service_user_name": "cbox",
		"service_user_uid":  58679,
		"service_user_gid":  1028,
	}
	tests := []struct {
		name   string
		change map[string]any
		want   string
	}{
		{name: "no mount ids", change: map[string]any{"mount_ids": nil}, want: "mount_ids"},
		{name: "no service user", change: map[string]any{"service_user_name": ""}, want: "service_user_name"},
		{name: "space_depth at the mount path", change: map[string]any{"space_depth": 1}, want: "space_depth"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := maps.Clone(valid)
			maps.Copy(m, tt.change)
			if _, err := New(context.Background(), m); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("New() error = %v, want one about %s", err, tt.want)
			}
		})
	}
}

func TestLevels(t *testing.T) {
	for p, want := range map[string]int{"/winspaces": 1, "/winspaces/": 1, "/a/b": 2, "/": 0} {
		if got := levels(p); got != want {
			t.Errorf("levels(%q) = %d, want %d", p, got, want)
		}
	}
}

func TestInGroups(t *testing.T) {
	groups := map[string]struct{}{"cernbox-project-cv-design-office-admins": {}}
	if !inGroups([]string{"cern-staff", "CERNBOX-Project-CV-Design-Office-Admins"}, groups) {
		t.Error("a member of an ACL group is not found, case must not matter")
	}
	if inGroups([]string{"cern-staff"}, groups) {
		t.Error("a user in no ACL group is found")
	}
	if inGroups(nil, groups) {
		t.Error("a user without groups is found")
	}
}

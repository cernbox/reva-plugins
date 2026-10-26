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

package eoswrapper

import (
	"context"
	"testing"

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/v3/pkg/appctx"
	eosfs "github.com/cs3org/reva/v3/pkg/storage/fs/eos"
)

const projectQuotaNode = "/eos/project"

// quotaFS answers GetQuota as EOS does: a gid 99 quota on the root of a
// space, and a uid quota on the quota node.
type quotaFS struct {
	FSWithListRegexSupport
	project map[string]uint64
	user    map[int64]uint64
}

func (fs *quotaFS) GetQuota(ctx context.Context, ref *provider.Reference) (uint64, uint64, error) {
	u := appctx.ContextMustGetUser(ctx)
	if u.GidNumber == 99 {
		return fs.project[ref.Path], 0, nil
	}
	if ref.Path == projectQuotaNode {
		return fs.user[u.UidNumber], 0, nil
	}
	return 0, 0, nil
}

func TestGetQuota(t *testing.T) {
	user := &userpb.User{Id: &userpb.UserId{OpaqueId: "alice"}, UidNumber: 1000, GidNumber: 1000}
	fs := &quotaFS{
		project: map[string]uint64{"/c/cernbox": 100},
		user:    map[int64]uint64{1000: 5},
	}
	w := &wrapper{FSWithListRegexSupport: fs, conf: &eosfs.Config{QuotaNode: projectQuotaNode}}
	ctx := appctx.ContextSetUser(context.Background(), user)

	tests := []struct {
		name string
		path string
		want uint64
	}{
		{"project quota on the space root", "/c/cernbox/minutes", 100},
		{"user quota when the space has no project quota", "/c/other/minutes", 5},
		{"user quota above the space root", "/c", 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			total, _, err := w.GetQuota(ctx, &provider.Reference{Path: tt.path})
			if err != nil {
				t.Fatal(err)
			}
			if total != tt.want {
				t.Errorf("GetQuota(%s) = %d, want %d", tt.path, total, tt.want)
			}
		})
	}
}

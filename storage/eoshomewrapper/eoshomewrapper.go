// Copyright 2018-2023 CERN
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

package eoshomewrapper

import (
	"bytes"
	"context"
	"text/template"

	"github.com/Masterminds/sprig"
	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/v3"
	"github.com/cs3org/reva/v3/pkg/appctx"
	"github.com/cs3org/reva/v3/pkg/errtypes"
	"github.com/cs3org/reva/v3/pkg/storage"
	"github.com/cs3org/reva/v3/pkg/storage/utils/eosfs"
	"github.com/cs3org/reva/v3/pkg/utils/cfg"
)

func init() {
	reva.RegisterPlugin(wrapper{})
}

type FSWithListRegexSupport interface {
	storage.FS
	ListWithRegex(ctx context.Context, path, regex string, depth uint, user *userpb.User) ([]*provider.ResourceInfo, error)
}

type wrapper struct {
	FSWithListRegexSupport
	mountIDTemplate *template.Template
}

func (wrapper) RevaPlugin() reva.PluginInfo {
	return reva.PluginInfo{
		ID:  "grpc.services.storageprovider.drivers.eoshomewrapper",
		New: New,
	}
}

// New returns an implementation of the storage.FS interface that forms a wrapper
// around separate connections to EOS.
func New(ctx context.Context, m map[string]interface{}) (storage.FS, error) {
	var c eosfs.Config
	if err := cfg.Decode(m, &c); err != nil {
		return nil, err
	}
	// default to version invariance if not configured
	if _, ok := m["version_invariant"]; !ok {
		c.VersionInvariant = true
	}
	c.EnableHome = true

	t, ok := m["mount_id_template"].(string)
	if !ok || t == "" {
		t = "eoshome-{{substr 0 1 .Username}}"
	}

	eosFs, err := eosfs.NewEOSFS(ctx, &c)
	eos := eosFs.(*eosfs.Eosfs)
	if err != nil {
		return nil, err
	}

	mountIDTemplate, err := template.New("mountID").Funcs(sprig.TxtFuncMap()).Parse(t)
	if err != nil {
		return nil, err
	}

	return &wrapper{FSWithListRegexSupport: eos, mountIDTemplate: mountIDTemplate}, nil
}

// We need to override the two methods, GetMD and ListFolder to fill the
// StorageId in the ResourceInfo objects.

func (w *wrapper) GetMD(ctx context.Context, ref *provider.Reference, mdKeys []string) (*provider.ResourceInfo, error) {
	res, err := w.FSWithListRegexSupport.GetMD(ctx, ref, mdKeys)
	if err != nil {
		return nil, err
	}

	// We need to extract the mount ID based on the mapping template.
	//
	// Take the first letter of the username of the logged-in user, as the home
	// storage provider restricts requests only to the home namespace.
	res.Id.StorageId = w.getMountID(ctx, res)
	res.ParentId.StorageId = w.getMountID(ctx, res)

	return res, nil
}

func (w *wrapper) ListFolder(ctx context.Context, ref *provider.Reference, mdKeys []string) ([]*provider.ResourceInfo, error) {
	res, err := w.FSWithListRegexSupport.ListFolder(ctx, ref, mdKeys)
	if err != nil {
		return nil, err
	}
	for _, r := range res {
		r.Id.StorageId = w.getMountID(ctx, r)
		r.ParentId.StorageId = w.getMountID(ctx, r)
	}
	return res, nil
}

func (w *wrapper) DenyGrant(ctx context.Context, ref *provider.Reference, g *provider.Grantee) error {
	return errtypes.NotSupported("eos: deny grant is only enabled for project spaces")
}

func (w *wrapper) getMountID(ctx context.Context, r *provider.ResourceInfo) string {
	u := appctx.ContextMustGetUser(ctx)
	b := bytes.Buffer{}
	if err := w.mountIDTemplate.Execute(&b, u); err != nil {
		return ""
	}
	return b.String()
}

func (w *wrapper) ListWithRegex(ctx context.Context, path, regex string, depth uint, user *userpb.User) ([]*provider.ResourceInfo, error) {
	res, err := w.FSWithListRegexSupport.ListWithRegex(ctx, path, regex, depth, user)
	if err != nil {
		return nil, err
	}
	for _, r := range res {
		r.Id.StorageId = w.getMountID(ctx, r)
		r.ParentId.StorageId = w.getMountID(ctx, r)
	}
	return res, nil
}

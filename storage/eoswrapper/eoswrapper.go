// Copyright 2018-2025 CERN
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
	"bytes"
	"context"
	"errors"
	"os"
	"path"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig"
	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/v3"
	"github.com/cs3org/reva/v3/pkg/appctx"
	"github.com/cs3org/reva/v3/pkg/errtypes"
	"github.com/cs3org/reva/v3/pkg/spaces"
	"github.com/cs3org/reva/v3/pkg/storage"
	eosfs "github.com/cs3org/reva/v3/pkg/storage/fs/eos"
	"github.com/cs3org/reva/v3/pkg/storage/fs/registry"
	"github.com/cs3org/reva/v3/pkg/utils/cfg"
)

func init() {
	reva.RegisterPlugin(wrapper{})
	registry.Register("eoswrapper", New)
}

const (
	eosProjectsNamespace = "/eos/project"
	eosHomesNamespace    = "/eos/user"

	blockedDirectory = ".blocked"
)

type FSWithListRegexSupport interface {
	storage.FS
	ListWithRegex(ctx context.Context, path, regex string, depth uint, user *userpb.User) ([]*provider.ResourceInfo, error)
}

type wrapper struct {
	FSWithListRegexSupport
	conf            *eosfs.Config
	mountIDTemplate *template.Template
}

func (wrapper) RevaPlugin() reva.PluginInfo {
	return reva.PluginInfo{
		ID:  "grpc.services.storageprovider.drivers.eoswrapper",
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

	// allow recycle operations for project spaces
	if strings.HasPrefix(c.Namespace, eosProjectsNamespace) {
		c.AllowPathRecycleOperations = true
	}

	t, ok := m["mount_id_template"].(string)
	if !ok || t == "" {
		t = "eoshome-{{ trimAll \"/\" .Path | substr 0 1 }}"
	}

	eosFs, err := eosfs.NewEOSFS(ctx, &c)
	if err != nil {
		return nil, err
	}
	eos := eosFs.(*eosfs.Eosfs)

	mountIDTemplate, err := template.New("mountID").Funcs(sprig.TxtFuncMap()).Parse(t)
	if err != nil {
		return nil, err
	}

	return &wrapper{FSWithListRegexSupport: eos, conf: &c, mountIDTemplate: mountIDTemplate}, nil
}

// We need to override GetMD and ListFolder to fill the correct StorageId in the ResourceInfo objects.

func (w *wrapper) GetMD(ctx context.Context, ref *provider.Reference, mdKeys []string) (*provider.ResourceInfo, error) {
	res, err := w.FSWithListRegexSupport.GetMD(ctx, ref, mdKeys)
	if err != nil {
		return nil, err
	}

	// and that the space is not in a blocked state
	blockedPath := path.Join(w.conf.Namespace, blockedDirectory)
	if strings.HasPrefix(res.Path, blockedPath) {
		return nil, errtypes.NotFound("file is in a blocked space")
	}

	// We need to extract the mount ID based on the mapping template.
	//
	// Take the first letter of the resource path after the namespace has been removed.
	// If it's empty, leave it empty to be filled by storageprovider.
	res.Id.StorageId = w.getMountID(ctx, res)
	res.ParentId.StorageId = w.getMountID(ctx, res)

	if res.Id.SpaceId == "" {
		log := appctx.GetLogger(ctx)
		log.Error().Msgf("Space ID not set for stat to %s", res.Path)
	}

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

func (w *wrapper) GetQuota(ctx context.Context, ref *provider.Reference) (totalbytes, usedbytes uint64, err error) {
	// Let's frist try to fetch the new-style quota
	// This means, with GID 99 and on the project root (/eos/project/n/name) instead of the root node (/eos/project)

	// Get context with GID 99
	user, ok := appctx.ContextGetUser(ctx)
	if !ok {
		return 0, 0, errors.New("no user found in context")
	}
	gidctx := ctx
	appctx.ContextSetUser(gidctx, &userpb.User{
		Id:        user.Id,
		UidNumber: user.UidNumber,
		GidNumber: 99,
	})

	// Get project root
	// Length 3 becuase this is the path relative to the namespace, so
	// for /eos/project/c/cernbox, this would be /c/cernbox
	paths := strings.Split(ref.Path, string(os.PathSeparator))
	if len(paths) < 3 {
		return w.getOldStyleQuota(ctx, ref)
	}
	path := strings.Join(paths[:3], string(os.PathSeparator))

	total, used, err := w.FSWithListRegexSupport.GetQuota(gidctx, &provider.Reference{
		Path: path,
	})
	if err != nil || total == 0 {
		// Getting new-style quota failed, so we fall back to the old style way
		return w.getOldStyleQuota(ctx, ref)
	}

	return total, used, nil

}

// Old style quota means we take the quota set for the username in the context, on the quota node specified in the config
// Typically, this is /eos/project or /eos/user
func (w *wrapper) getOldStyleQuota(ctx context.Context, ref *provider.Reference) (totalbytes, usedbytes uint64, err error) {
	return w.FSWithListRegexSupport.GetQuota(ctx, &provider.Reference{
		Path: w.conf.QuotaNode,
	})
}

func (w *wrapper) DenyGrant(ctx context.Context, ref *provider.Reference, g *provider.Grantee) error {
	if w.isProjectInstance() {
		return w.FSWithListRegexSupport.DenyGrant(ctx, ref, g)
	}

	return errtypes.NotSupported("eosfs: deny grant is only enabled for project spaces")
}

func (w *wrapper) getMountID(ctx context.Context, r *provider.ResourceInfo) string {
	if r == nil {
		return ""
	}
	b := bytes.Buffer{}
	if err := w.mountIDTemplate.Execute(&b, r); err != nil {
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

func (w *wrapper) isProjectInstance() bool {
	return w.spaceType() == spaces.SpaceTypeProject
}

func (w *wrapper) isHomeInstance() bool {
	return w.spaceType() == spaces.SpaceTypeHome
}

func (w *wrapper) spaceType() spaces.SpaceType {
	if strings.HasPrefix(w.conf.Namespace, eosHomesNamespace) {
		return spaces.SpaceTypeHome
	} else if strings.HasPrefix(w.conf.Namespace, eosProjectsNamespace) {
		return spaces.SpaceTypeProject
	}
	return spaces.SpaceTypePublic
}

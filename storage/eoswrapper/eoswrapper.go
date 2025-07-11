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

package eoswrapper

import (
	"bytes"
	"context"
	"io"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig"
	"github.com/cernbox/reva-plugins/storage/eoshomewrapper"
	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/v3"
	"github.com/cs3org/reva/v3/pkg/appctx"
	"github.com/cs3org/reva/v3/pkg/errtypes"
	"github.com/cs3org/reva/v3/pkg/storage"
	"github.com/cs3org/reva/v3/pkg/storage/fs/registry"
	"github.com/cs3org/reva/v3/pkg/storage/utils/eosfs"
	"github.com/cs3org/reva/v3/pkg/utils"
	"github.com/cs3org/reva/v3/pkg/utils/cfg"
)

func init() {
	reva.RegisterPlugin(wrapper{})
	registry.Register("eoswrapper", New)
}

const (
	eosProjectsNamespace = "/eos/project"

	// We can use a regex for these, but that might have inferior performance.
	projectSpaceGroupsPrefix       = "cernbox-project-"
	projectSpaceAdminsGroupSuffix  = "-admins"
	projectSpaceWritersGroupSuffix = "-writers"
	projectSpaceReadersGroupSuffix = "-readers"

	requireAdmin  = 2
	requireWriter = 1
	requireReader = 0
)

type wrapper struct {
	eoshomewrapper.FSWithListRegexSupport
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
	if !c.EnableHome && strings.HasPrefix(c.Namespace, eosProjectsNamespace) {
		c.AllowPathRecycleOperations = true
	}

	t, ok := m["mount_id_template"].(string)
	if !ok || t == "" {
		t = "eoshome-{{ trimAll \"/\" .Path | substr 0 1 }}"
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

	return &wrapper{FSWithListRegexSupport: eos, conf: &c, mountIDTemplate: mountIDTemplate}, nil
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
	// Take the first letter of the resource path after the namespace has been removed.
	// If it's empty, leave it empty to be filled by storageprovider.
	res.Id.StorageId = w.getMountID(ctx, res)
	res.ParentId.StorageId = w.getMountID(ctx, res)

	if err = w.setProjectSharingPermissions(ctx, res); err != nil {
		return nil, err
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
		if err = w.setProjectSharingPermissions(ctx, r); err != nil {
			continue
		}
	}
	return res, nil
}

func (w *wrapper) ListRevisions(ctx context.Context, ref *provider.Reference) ([]*provider.FileVersion, error) {
	if err := w.userIsProjectMember(ctx, ref, requireReader); err != nil {
		return nil, errtypes.PermissionDenied("eosfs: files revisions can only be accessed by project memners")
	}

	return w.FSWithListRegexSupport.ListRevisions(ctx, ref)
}

func (w *wrapper) DownloadRevision(ctx context.Context, ref *provider.Reference, revisionKey string) (io.ReadCloser, error) {
	if err := w.userIsProjectMember(ctx, ref, requireReader); err != nil {
		return nil, errtypes.PermissionDenied("eosfs: files revisions can only be downloaded by project memners")
	}

	return w.FSWithListRegexSupport.DownloadRevision(ctx, ref, revisionKey)
}

func (w *wrapper) RestoreRevision(ctx context.Context, ref *provider.Reference, revisionKey string) error {
	if err := w.userIsProjectMember(ctx, ref, requireWriter); err != nil {
		return errtypes.PermissionDenied("eosfs: files revisions can only be restored by project writers or admins")
	}

	return w.FSWithListRegexSupport.RestoreRevision(ctx, ref, revisionKey)
}

func (w *wrapper) DenyGrant(ctx context.Context, ref *provider.Reference, g *provider.Grantee) error {
	// This is only allowed for project space admins
	if strings.HasPrefix(w.conf.Namespace, eosProjectsNamespace) {
		if err := w.userIsProjectMember(ctx, ref, requireAdmin); err != nil {
			return errtypes.PermissionDenied("eosfs: deny grant can only be set by project admins")
		}
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

func (w *wrapper) setProjectSharingPermissions(ctx context.Context, r *provider.ResourceInfo) error {
	// Check if this storage provider corresponds to a project spaces instance
	if strings.HasPrefix(w.conf.Namespace, eosProjectsNamespace) {
		// Extract project name from the path resembling /c/cernbox or /c/cernbox/minutes/..
		parts := strings.SplitN(r.Path, "/", 4)
		if len(parts) != 4 && len(parts) != 3 {
			// The request might be for / or /$letter
			// Nothing to do in that case
			return nil
		}
		adminGroup := projectSpaceGroupsPrefix + parts[2] + projectSpaceAdminsGroupSuffix
		user := appctx.ContextMustGetUser(ctx)

		_, isPublicShare := utils.HasPublicShareRole(user)

		for _, g := range user.Groups {
			if g == adminGroup {
				r.PermissionSet.AddGrant = true
				r.PermissionSet.RemoveGrant = true
				r.PermissionSet.UpdateGrant = true
				r.PermissionSet.ListGrants = true
				r.PermissionSet.GetQuota = true
				if !isPublicShare {
					r.PermissionSet.DenyGrant = true
				}
				return nil
			}
		}
	}
	return nil
}

func (w *wrapper) userIsProjectMember(ctx context.Context, ref *provider.Reference, requiredLevel int) error {
	// Check if this storage provider corresponds to a project spaces instance
	if !strings.HasPrefix(w.conf.Namespace, eosProjectsNamespace) {
		return nil
	}

	res, err := w.GetMD(ctx, ref, nil)
	if err != nil {
		return err
	}

	// Extract project name from the path resembling /c/cernbox or /c/cernbox/minutes/..
	parts := strings.SplitN(res.Path, "/", 4)
	if len(parts) != 4 && len(parts) != 3 {
		// The request might be for / or /$letter
		// Nothing to do in that case
		return nil
	}
	// build group names (currently hardcoded)
	adminsGroup := projectSpaceGroupsPrefix + parts[2] + projectSpaceAdminsGroupSuffix
	writersGroup := projectSpaceGroupsPrefix + parts[2] + projectSpaceWritersGroupSuffix
	readersGroup := projectSpaceGroupsPrefix + parts[2] + projectSpaceReadersGroupSuffix
	user := appctx.ContextMustGetUser(ctx)

	for _, g := range user.Groups {
		if (g == adminsGroup && requiredLevel <= requireAdmin) ||
			(g == writersGroup && requiredLevel <= requireWriter) ||
			(g == readersGroup && requiredLevel <= requireReader) {
			// User is a project member with sufficient permissions
			return nil
		}
	}
	return errtypes.PermissionDenied("")
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

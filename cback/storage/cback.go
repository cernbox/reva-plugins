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

package cbackfs

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/Masterminds/sprig"
	"github.com/bluele/gcache"
	"github.com/cernbox/reva-plugins/cback/utils"
	cback "github.com/cernbox/reva-plugins/cback/utils"
	user "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	types "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"
	"github.com/cs3org/reva/v3"
	"github.com/cs3org/reva/v3/pkg/appctx"
	"github.com/cs3org/reva/v3/pkg/errtypes"
	"github.com/cs3org/reva/v3/pkg/mime"
	"github.com/cs3org/reva/v3/pkg/storage"
	"github.com/cs3org/reva/v3/pkg/storage/fs/registry"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

type fs struct {
	conf       *Config
	client     *utils.Client
	cache      gcache.Cache
	tplStorage *template.Template
	tplCback   *template.Template
}

func init() {
	reva.RegisterPlugin(fs{})
}

var _ registry.NewFunc = New

// New returns an implementation to the storage.FS interface that expose
// the snapshots stored in cback.
func New(_ context.Context, m map[string]interface{}) (storage.FS, error) {
	c := &Config{}
	if err := mapstructure.Decode(m, c); err != nil {
		return nil, errors.Wrap(err, "cback: error decoding config")
	}
	c.init()

	tplStorage, err := template.New("tpl_storage").Funcs(sprig.TxtFuncMap()).Parse(c.TemplateToStorage)
	if err != nil {
		return nil, errors.Wrap(err, "cback: error creating template")
	}

	tplCback, err := template.New("tpl_cback").Funcs(sprig.TxtFuncMap()).Parse(c.TemplateToCback)
	if err != nil {
		return nil, errors.Wrap(err, "cback: error creating template")
	}

	client := utils.New(
		&utils.Config{
			URL:     c.APIURL,
			Token:   c.Token,
			Timeout: c.Timeout,
		},
	)

	return &fs{
		conf:       c,
		client:     client,
		cache:      gcache.New(c.Size).LRU().Build(),
		tplStorage: tplStorage,
		tplCback:   tplCback,
	}, nil
}

func split(path string, backups []*cback.Backup) (string, string, string, int, bool) {
	for _, b := range backups {
		if strings.HasPrefix(path, b.Source) {
			// the path could be in this form:
			// <b.Source>/<snap_id>/<path>
			// snap_id and path are optional
			rel, _ := filepath.Rel(b.Source, path)
			if rel == "." {
				// both snap_id and path were not provided
				return b.Source, "", "", b.ID, true
			}
			split := strings.SplitN(rel, "/", 2)

			var snap, p string
			snap = split[0]
			if len(split) == 2 {
				p = split[1]
			}
			return b.Source, snap, p, b.ID, true
		}
	}
	return "", "", "", 0, false
}

func (fs) RevaPlugin() reva.PluginInfo {
	return reva.PluginInfo{
		ID:  "grpc.services.storageprovider.drivers.cback",
		New: New,
	}
}

func (f *fs) convertToResourceInfo(r *utils.Resource, path string, resID, parentID *provider.ResourceId, owner *user.UserId) *provider.ResourceInfo {
	rtype := provider.ResourceType_RESOURCE_TYPE_FILE
	perms := permFile
	if r.IsDir() {
		rtype = provider.ResourceType_RESOURCE_TYPE_CONTAINER
		perms = permDir
	}

	return &provider.ResourceInfo{
		Type: rtype,
		Id:   resID,
		Checksum: &provider.ResourceChecksum{
			Type: provider.ResourceChecksumType_RESOURCE_CHECKSUM_TYPE_UNSET,
		},
		Etag:     strconv.FormatUint(uint64(r.CTime), 10),
		MimeType: mime.Detect(r.IsDir(), path),
		Mtime: &types.Timestamp{
			Seconds: uint64(r.CTime),
		},
		Path:          path,
		PermissionSet: perms,
		Size:          r.Size,
		Owner:         owner,
		ParentId:      parentID,
	}
}

func encodeBackupInResourceID(backupID int, snapshotID, source, path string) *provider.ResourceId {
	id := fmt.Sprintf("%d#%s#%s#%s", backupID, snapshotID, source, path)
	opaque := base64.StdEncoding.EncodeToString([]byte(id))
	return &provider.ResourceId{
		StorageId: "cback",
		OpaqueId:  opaque,
	}
}

// return b.Source, snap, p, b.ID, true.
func decodeResourceID(r *provider.ResourceId) (string, string, string, int, bool) {
	if r == nil {
		return "", "", "", 0, false
	}
	data, err := base64.StdEncoding.DecodeString(r.OpaqueId)
	if err != nil {
		return "", "", "", 0, false
	}
	split := strings.SplitN(string(data), "#", 4)
	if len(split) != 4 {
		return "", "", "", 0, false
	}
	backupID, err := strconv.ParseInt(split[0], 10, 64)
	if err != nil {
		return "", "", "", 0, false
	}
	return split[2], split[1], split[3], int(backupID), true
}

// GetBackupInfo returns a tuple path, snapshot, backup id from a resource id.
func GetBackupInfo(r *provider.ResourceId) (string, string, int, bool) {
	source, snap, path, id, ok := decodeResourceID(r)
	return filepath.Join(source, path), snap, id, ok
}

func (f *fs) placeholderResourceInfo(path string, owner *user.UserId, mtime *types.Timestamp, resID *provider.ResourceId) *provider.ResourceInfo {
	if mtime == nil {
		mtime = &types.Timestamp{
			Seconds: 0,
		}
	}
	if resID == nil {
		resID = &provider.ResourceId{
			StorageId: "cback",
			OpaqueId:  path,
		}
	}
	return &provider.ResourceInfo{
		Type: provider.ResourceType_RESOURCE_TYPE_CONTAINER,
		Id:   resID,
		Checksum: &provider.ResourceChecksum{
			Type: provider.ResourceChecksumType_RESOURCE_CHECKSUM_TYPE_UNSET,
		},
		Etag:          "",
		MimeType:      mime.Detect(true, path),
		Mtime:         mtime,
		Path:          path,
		PermissionSet: permDir,
		Size:          0,
		Owner:         owner,
	}
}

func hasPrefix(lst, prefix []string) bool {
	for i, p := range prefix {
		if lst[i] != p {
			return false
		}
	}
	return true
}

func (f *fs) isParentOfBackup(path string, backups []*utils.Backup) bool {
	pathSplit := []string{""}
	if path != "/" {
		pathSplit = strings.Split(path, "/")
	}
	for _, b := range backups {
		backupSplit := strings.Split(b.Source, "/")
		if hasPrefix(backupSplit, pathSplit) {
			return true
		}
	}
	return false
}

func (f *fs) GetMD(ctx context.Context, ref *provider.Reference, mdKeys []string) (*provider.ResourceInfo, error) {
	user, ok := appctx.ContextGetUser(ctx)
	if !ok {
		return nil, errtypes.UserRequired("cback: user not found in context")
	}

	var (
		source, snapshot, path string
		id                     int
	)

	backups, err := f.listBackups(ctx, user.Username)
	if err != nil {
		return nil, errors.Wrapf(err, "cback: error listing backups")
	}

	if ref.ResourceId != nil {
		source, snapshot, path, id, ok = decodeResourceID(ref.ResourceId)
		if ref.Path != "" {
			path = filepath.Join(path, ref.Path)
		}
	} else {
		source, snapshot, path, id, ok = split(ref.Path, backups)
		source = convertTemplate(source, f.tplCback)
	}

	if ok {
		if snapshot != "" && path != "" {
			// the path from the user is something like /eos/home-g/gdelmont/<snapshot_id>/rest/of/path
			// in this case the method has to return the stat of the file /eos/home-g/gdelmont/rest/of/path
			// in the snapshot <snapshot_id>
			res, err := f.stat(ctx, user.Username, id, snapshot, filepath.Join(source, path))
			if err != nil {
				return nil, err
			}
			return f.convertToResourceInfo(
				res,
				filepath.Join(source, snapshot, path),
				encodeBackupInResourceID(id, snapshot, source, path),
				encodeBackupInResourceID(id, snapshot, source, filepath.Dir(path)),
				user.Id,
			), nil
		} else if snapshot != "" && path == "" {
			// the path from the user is something like /eos/home-g/gdelmont/<snapshot_id>
			snap, err := f.getSnapshot(ctx, user.Username, id, snapshot)
			if err != nil {
				return nil, errors.Wrap(err, "cback: error getting snapshot")
			}
			return f.placeholderResourceInfo(filepath.Join(source, snapshot), user.Id, timeToTimestamp(snap.Time.Time), encodeBackupInResourceID(id, snapshot, source, "")), nil
		}
		// the path from the user is something like /eos/home-g/gdelmont
		return f.placeholderResourceInfo(source, user.Id, nil, nil), nil
	}

	// the path is not one of the backup. There is a situation in which
	// the user's path is a parent folder of some of the backups

	if f.isParentOfBackup(source, backups) {
		return f.placeholderResourceInfo(source, user.Id, nil, nil), nil
	}

	return nil, errtypes.NotFound(fmt.Sprintf("path %s does not exist", source))
}

func timeToTimestamp(t time.Time) *types.Timestamp {
	return &types.Timestamp{
		Seconds: uint64(t.Unix()),
	}
}

func (f *fs) getSnapshot(ctx context.Context, username string, backupID int, timestamp string) (*cback.Snapshot, error) {
	snapshots, err := f.listSnapshots(ctx, username, backupID)
	if err != nil {
		return nil, err
	}
	t, err := time.Parse(f.conf.TimestampFormat, timestamp)
	if err != nil {
		return nil, err
	}
	for _, snap := range snapshots {
		if snap.Time.Equal(t) {
			return snap, nil
		}
	}
	return nil, errtypes.NotFound(fmt.Sprintf("snapshot %s from backup %d not found", timestamp, backupID))
}

func (f *fs) ListFolder(ctx context.Context, ref *provider.Reference, mdKeys []string) ([]*provider.ResourceInfo, error) {
	user, ok := appctx.ContextGetUser(ctx)
	if !ok {
		return nil, errtypes.UserRequired("cback: user not found in context")
	}

	backups, err := f.listBackups(ctx, user.Username)
	if err != nil {
		return nil, errors.Wrapf(err, "cback: error listing backups")
	}

	source, snapshot, path, id, ok := split(ref.Path, backups)
	if ok {
		if snapshot != "" {
			// the path from the user is something like /eos/home-g/gdelmont/<snapshot_id>/(rest/of/path)
			// in this case the method has to return the content of the folder /eos/home-g/gdelmont/(rest/of/path)
			// in the snapshot <snapshot_id>
			content, err := f.listFolder(ctx, user.Username, id, snapshot, filepath.Join(source, path))
			if err != nil {
				return nil, err
			}
			res := make([]*provider.ResourceInfo, 0, len(content))
			parentID := encodeBackupInResourceID(id, snapshot, source, path)
			for _, info := range content {
				base := filepath.Base(info.Name)
				res = append(res, f.convertToResourceInfo(
					info,
					filepath.Join(source, snapshot, path, base),
					encodeBackupInResourceID(id, snapshot, source, filepath.Join(path, base)),
					parentID,
					user.Id,
				))
			}
			return res, nil
		}
		// the path from the user is something like /eos/home-g/gdelmont
		// the method needs to return the list of snapshots as folders
		snapshots, err := f.listSnapshots(ctx, user.Username, id)
		if err != nil {
			return nil, err
		}
		res := make([]*provider.ResourceInfo, 0, len(snapshots))
		for _, s := range snapshots {
			snapTime := s.Time.Format(f.conf.TimestampFormat)
			res = append(res, f.placeholderResourceInfo(filepath.Join(source, snapTime), user.Id, timeToTimestamp(s.Time.Time), encodeBackupInResourceID(id, snapTime, source, "")))
		}
		return res, nil
	}

	// the path is not one of the backup. Can happen that the
	// user's path is a parent folder of some of the backups
	resSet := make(map[string]struct{}) // used to discard duplicates
	var resources []*provider.ResourceInfo

	sourceSplit := []string{""}
	if ref.Path != "/" {
		sourceSplit = strings.Split(ref.Path, "/")
	}
	for _, b := range backups {
		backupSplit := strings.Split(b.Source, "/")
		if hasPrefix(backupSplit, sourceSplit) {
			base := backupSplit[len(sourceSplit)]
			path := filepath.Join(ref.Path, base)

			if _, ok := resSet[path]; !ok {
				resources = append(resources, f.placeholderResourceInfo(path, user.Id, nil, nil))
				resSet[path] = struct{}{}
			}
		}
	}

	if len(resources) != 0 {
		return resources, nil
	}

	return nil, errtypes.NotFound(fmt.Sprintf("path %s does not exist", ref.Path))
}

func (f *fs) Download(ctx context.Context, ref *provider.Reference) (io.ReadCloser, error) {
	user, ok := appctx.ContextGetUser(ctx)
	if !ok {
		return nil, errtypes.UserRequired("cback: user not found in context")
	}

	stat, err := f.GetMD(ctx, ref, nil)
	if err != nil {
		return nil, errors.Wrap(err, "cback: error statting resource")
	}

	if stat.Type != provider.ResourceType_RESOURCE_TYPE_FILE {
		return nil, errtypes.BadRequest("cback: can only download files")
	}

	source, snapshot, path, id, ok := decodeResourceID(stat.Id)
	if !ok {
		return nil, errtypes.BadRequest("cback: can only download files")
	}
	source = convertTemplate(source, f.tplCback)
	return f.client.Download(ctx, user.Username, id, snapshot, filepath.Join(source, path), true)
}

func convertTemplate(s string, t *template.Template) string {
	var b bytes.Buffer
	if err := t.Execute(&b, s); err != nil {
		panic(errors.Wrap(err, "error executing template"))
	}
	return b.String()
}

func (f *fs) GetHome(ctx context.Context) (string, error) {
	return "", errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) CreateHome(ctx context.Context) error {
	return errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) CreateDir(ctx context.Context, ref *provider.Reference) error {
	return errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) TouchFile(ctx context.Context, ref *provider.Reference) error {
	return errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) Delete(ctx context.Context, ref *provider.Reference) error {
	return errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) Move(ctx context.Context, oldRef, newRef *provider.Reference) error {
	return errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) ListRevisions(ctx context.Context, ref *provider.Reference) ([]*provider.FileVersion, error) {
	return nil, errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) DownloadRevision(ctx context.Context, ref *provider.Reference, key string) (io.ReadCloser, error) {
	return nil, errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) RestoreRevision(ctx context.Context, ref *provider.Reference, key string) error {
	return errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) GetPathByID(ctx context.Context, id *provider.ResourceId) (string, error) {
	return "", errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) AddGrant(ctx context.Context, ref *provider.Reference, g *provider.Grant) error {
	return errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) RemoveGrant(ctx context.Context, ref *provider.Reference, g *provider.Grant) error {
	return errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) UpdateGrant(ctx context.Context, ref *provider.Reference, g *provider.Grant) error {
	return errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) DenyGrant(ctx context.Context, ref *provider.Reference, g *provider.Grantee) error {
	return errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) ListGrants(ctx context.Context, ref *provider.Reference) ([]*provider.Grant, error) {
	return nil, errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) GetQuota(ctx context.Context, ref *provider.Reference) (uint64, uint64, error) {
	return 0, 0, errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) CreateReference(ctx context.Context, path string, targetURI *url.URL) error {
	return errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) Shutdown(ctx context.Context) error {
	return errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) SetArbitraryMetadata(ctx context.Context, ref *provider.Reference, md *provider.ArbitraryMetadata) error {
	return errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) UnsetArbitraryMetadata(ctx context.Context, ref *provider.Reference, keys []string) error {
	return errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) EmptyRecycle(ctx context.Context) error {
	return errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) CreateStorageSpace(ctx context.Context, req *provider.CreateStorageSpaceRequest) (*provider.CreateStorageSpaceResponse, error) {
	return nil, errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) ListRecycle(ctx context.Context, basePath, key, relativePath string, from, to *types.Timestamp) ([]*provider.RecycleItem, error) {
	return nil, errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) RestoreRecycleItem(ctx context.Context, basePath, key, relativePath string, restoreRef *provider.Reference) error {
	return errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) PurgeRecycleItem(ctx context.Context, basePath, key, relativePath string) error {
	return errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) ListStorageSpaces(ctx context.Context, filter []*provider.ListStorageSpacesRequest_Filter) ([]*provider.StorageSpace, error) {
	return nil, errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) UpdateStorageSpace(ctx context.Context, req *provider.UpdateStorageSpaceRequest) (*provider.UpdateStorageSpaceResponse, error) {
	return nil, errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) SetLock(ctx context.Context, ref *provider.Reference, lock *provider.Lock) error {
	return errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) GetLock(ctx context.Context, ref *provider.Reference) (*provider.Lock, error) {
	return nil, errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) RefreshLock(ctx context.Context, ref *provider.Reference, lock *provider.Lock, existingLockID string) error {
	return errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) Unlock(ctx context.Context, ref *provider.Reference, lock *provider.Lock) error {
	return errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) Upload(ctx context.Context, ref *provider.Reference, r io.ReadCloser, metadata map[string]string) error {
	return errtypes.NotSupported("Operation Not Permitted")
}

func (f *fs) InitiateUpload(ctx context.Context, ref *provider.Reference, uploadLength int64, metadata map[string]string) (map[string]string, error) {
	return nil, errtypes.NotSupported("Operation Not Permitted")
}

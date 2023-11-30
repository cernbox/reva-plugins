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
	"context"
	"fmt"
	"time"

	"github.com/cernbox/reva-plugins/cback/utils"
)

func (f *fs) listBackups(ctx context.Context, username string) ([]*utils.Backup, error) {
	key := "backups:" + username
	if d, err := f.cache.Get(key); err == nil {
		return d.([]*utils.Backup), nil
	}
	backups, err := f.client.ListBackups(ctx, username)
	if err != nil {
		return nil, err
	}
	for _, b := range backups {
		b.Source = convertTemplate(b.Source, f.tplStorage)
	}
	_ = f.cache.SetWithExpire(key, backups, time.Duration(f.conf.Expiration)*time.Second)
	return backups, nil
}

func (f *fs) stat(ctx context.Context, username string, id int, snapshot, path string) (*utils.Resource, error) {
	key := fmt.Sprintf("stat:%s:%d:%s:%s", username, id, snapshot, path)
	if s, err := f.cache.Get(key); err == nil {
		return s.(*utils.Resource), nil
	}
	s, err := f.client.Stat(ctx, username, id, snapshot, path, true)
	if err != nil {
		return nil, err
	}
	_ = f.cache.SetWithExpire(key, s, time.Duration(f.conf.Expiration)*time.Second)
	return s, nil
}

func (f *fs) listFolder(ctx context.Context, username string, id int, snapshot, path string) ([]*utils.Resource, error) {
	key := fmt.Sprintf("list:%s:%d:%s:%s", username, id, snapshot, path)
	if l, err := f.cache.Get(key); err == nil {
		return l.([]*utils.Resource), nil
	}
	path = convertTemplate(path, f.tplCback)
	l, err := f.client.ListFolder(ctx, username, id, snapshot, path, true)
	if err != nil {
		return nil, err
	}
	_ = f.cache.SetWithExpire(key, l, time.Duration(f.conf.Expiration)*time.Second)
	return l, nil
}

func (f *fs) listSnapshots(ctx context.Context, username string, id int) ([]*utils.Snapshot, error) {
	key := fmt.Sprintf("snapshots:%s:%d", username, id)
	if l, err := f.cache.Get(key); err == nil {
		return l.([]*utils.Snapshot), nil
	}
	l, err := f.client.ListSnapshots(ctx, username, id)
	if err != nil {
		return nil, err
	}
	for _, snap := range l {
		// truncate the time according to the given format
		t, _ := time.Parse(f.conf.TimestampFormat, snap.Time.Format(f.conf.TimestampFormat))
		snap.Time = utils.CBackTime{Time: t}
	}
	_ = f.cache.SetWithExpire(key, l, time.Duration(f.conf.Expiration)*time.Second)
	return l, nil
}

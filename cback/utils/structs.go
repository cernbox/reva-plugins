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

package utils

import (
	"encoding/json"
	"strings"
	"time"
)

// Group is the group in cback.
type Group struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// Backup represents the metadata information of a backuo job.
type Backup struct {
	ID         int    `json:"id"`
	Group      Group  `json:"group"`
	Repository string `json:"repository"`
	Username   string `json:"username"`
	Name       string `json:"name"`
	Source     string `json:"source"`
}

// Snapshot represents the metadata information of a snapshot in a backup.
type Snapshot struct {
	ID    string    `json:"id"`
	Time  CBackTime `json:"time"`
	Paths []string  `json:"paths"`
}

// Resource represents the metadata information of a file stored in cback.
type Resource struct {
	Name  string  `json:"name"`
	Type  string  `json:"type"`
	Mode  uint64  `json:"mode"`
	MTime float64 `json:"mtime"`
	ATime float64 `json:"atime"`
	CTime float64 `json:"ctime"`
	Inode uint64  `json:"inode"`
	Size  uint64  `json:"size"`
}

// Restore represents the metadata information of a restore job.
type Restore struct {
	ID           int       `json:"id"`
	BackupID     int       `json:"backup_id"`
	SnapshotID   string    `json:"snapshot"`
	Destionation string    `json:"destination"`
	Pattern      string    `json:"pattern"`
	Status       int       `json:"status"`
	Created      CBackTime `json:"created"`
}

type CBackTime struct{ time.Time }

func (c CBackTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.Time)
}

func (c *CBackTime) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), "\"")
	t, err := time.Parse("2006-01-02T15:04:05", s)
	if err != nil {
		// fall back to the default unmarshaler for date0time
		if err := json.Unmarshal(b, &t); err != nil {
			return err
		}

	}
	*c = CBackTime{t}
	return nil
}

// IsDir returns true if the resoure is a directory.
func (r *Resource) IsDir() bool {
	return r.Type == "dir"
}

// IsFile returns true if the resoure is a file.
func (r *Resource) IsFile() bool {
	return r.Type == "file"
}

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

package clipboard

import (
	"encoding/json"
	"fmt"
	"os"
	"os/user"
	"path"
	"strings"
	"time"
)

const (
	// Dir is where the clipboard lives in the home space. The leading dot
	// hides it from "cernbox ls".
	Dir = ".cernbox/clipboard"

	// DefaultSlot is the slot used when none is named.
	DefaultSlot = "default"

	// Version is the manifest format version. A reader refuses a higher one.
	Version = 1

	// DefaultTTL is how long a slot lives. Staged bytes use the quota of the user.
	DefaultTTL = 7 * 24 * time.Hour

	// MaxManifestSize is the most that a reader loads into memory.
	MaxManifestSize = 1 << 20

	manifestName = "manifest.json"
	payloadName  = "payload"
)

// ModeStream marks a live handover. A stored copy has an empty mode.
const ModeStream = "stream"

// Manifest is what one clipboard slot holds. Machines of different versions
// read the same file, so all fields are optional except the ones in Decode.
type Manifest struct {
	Version int    `json:"version"`
	Slot    string `json:"slot"`
	// Mode is ModeStream for a live handover. It is omitted for a stored copy.
	Mode string `json:"mode,omitempty"`
	// Stream is set only in ModeStream.
	Stream  *StreamInfo `json:"stream,omitempty"`
	Created time.Time   `json:"created"`
	// Expires is zero when the slot never expires (--ttl 0).
	Expires time.Time `json:"expires,omitzero"`
	Origin  Origin    `json:"origin"`
	// To is the user this was staged for, empty for an ordinary copy.
	To string `json:"to,omitempty"`
	// Entries are in the order of the arguments.
	Entries []Entry `json:"entries"`
}

// Entry is one copied file or directory.
type Entry struct {
	// Name is what paste writes the entry as.
	Name string `json:"name"`
	// Path is the absolute CERNBox path of the bytes.
	Path  string `json:"path"`
	IsDir bool   `json:"is_dir"`
	// Size is recursive for a directory.
	Size int64 `json:"size"`
	// Staged is true when the CLI uploaded the bytes. Clear deletes a staged
	// entry and leaves a referenced one alone: that is the real file of the user.
	Staged bool `json:"staged"`
	// Parts is the number of pieces of a piped entry, zero for a single object.
	// Then Path is a directory, and PartPath names the pieces to join in order.
	// A pipe has no length, and every upload to reva needs one, so a pipe is
	// cut in pieces whose length is known when each is sent.
	Parts int `json:"parts,omitempty"`
	// ETag is the version that was copied from a referenced path.
	ETag string `json:"etag,omitempty"`
}

// StreamInfo describes a live handover. The sender runs at most Window pieces
// ahead of the receiver, so the slot stays small whatever the size sent.
type StreamInfo struct {
	ChunkSize int64 `json:"chunk_size"`
	Window    int   `json:"window"`
}

// Done is the end marker of the sender. The receiver needs it to tell "no more
// pieces yet" from "no more pieces ever".
type Done struct {
	Parts int   `json:"parts"`
	Size  int64 `json:"size"`
}

// IsStream reports whether the slot is a live handover.
func (m *Manifest) IsStream() bool { return m.Mode == ModeStream }

// Origin is the machine a copy was made on.
type Origin struct {
	Host string `json:"host,omitempty"`
	User string `json:"user,omitempty"`
	// Dir is often the only thing that tells two files of one name apart.
	Dir string `json:"dir,omitempty"`
}

// LocalOrigin describes this machine. A field that cannot be read stays empty.
func LocalOrigin(cwd string) Origin {
	var o Origin
	if host, err := os.Hostname(); err == nil {
		o.Host = host
	}
	if u, err := user.Current(); err == nil {
		o.User = u.Username
	}
	o.Dir = cwd
	return o
}

// String renders an origin for a listing.
func (o Origin) String() string {
	switch {
	case o.Host != "" && o.User != "":
		return o.User + "@" + o.Host
	case o.Host != "":
		return o.Host
	case o.User != "":
		return o.User
	default:
		return "-"
	}
}

// NewManifest returns a manifest for slot. A ttl of zero or less means no expiry.
func NewManifest(slot string, now time.Time, ttl time.Duration, origin Origin) *Manifest {
	m := &Manifest{
		Version: Version,
		Slot:    slot,
		Created: now,
		Origin:  origin,
	}
	if ttl > 0 {
		m.Expires = now.Add(ttl)
	}
	return m
}

// Size is the total size of the entries.
func (m *Manifest) Size() int64 {
	var total int64
	for _, e := range m.Entries {
		total += e.Size
	}
	return total
}

// StagedSize is the size of the uploaded copies, which clearing the slot frees.
func (m *Manifest) StagedSize() int64 {
	var total int64
	for _, e := range m.Entries {
		if e.Staged {
			total += e.Size
		}
	}
	return total
}

// HasStaged reports whether any entry was uploaded.
func (m *Manifest) HasStaged() bool {
	for _, e := range m.Entries {
		if e.Staged {
			return true
		}
	}
	return false
}

// Expired reports whether the slot is past its expiry.
func (m *Manifest) Expired(now time.Time) bool {
	return !m.Expires.IsZero() && now.After(m.Expires)
}

// Encode renders the manifest for storage.
func (m *Manifest) Encode() ([]byte, error) {
	if m.Version == 0 {
		m.Version = Version
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encoding the clipboard manifest: %w", err)
	}
	return append(b, '\n'), nil
}

// Decode parses a stored manifest. It refuses a newer format: the fields that
// this version does not know could say where the bytes are, and pasting half
// of a copy is worse than refusing.
func Decode(b []byte) (*Manifest, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("the clipboard manifest is empty")
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("the clipboard manifest is not readable: %w", err)
	}
	if m.Version > Version {
		return nil, fmt.Errorf(
			"this clipboard was written by a newer cernbox (format %d, this one reads %d): upgrade to paste it",
			m.Version, Version)
	}
	for i, e := range m.Entries {
		if e.Name == "" || e.Path == "" {
			return nil, fmt.Errorf("the clipboard manifest is incomplete: entry %d has no name or no location", i+1)
		}
		if e.Parts < 0 {
			return nil, fmt.Errorf("the clipboard manifest is invalid: entry %q claims %d parts", e.Name, e.Parts)
		}
	}
	return &m, nil
}

// ValidateSlot checks a slot name. A slot is a directory name under the
// clipboard root, so a separator or a dot-dot would write outside it. This is
// the only check, so it only allows one plain segment.
func ValidateSlot(name string) error {
	const maxLen = 64
	switch {
	case name == "":
		return fmt.Errorf("the clipboard slot name is empty")
	case len(name) > maxLen:
		return fmt.Errorf("the clipboard slot name is longer than %d characters", maxLen)
	case name == "." || name == "..":
		return fmt.Errorf("%q is not a usable clipboard slot name", name)
	case strings.ContainsAny(name, `/\`):
		return fmt.Errorf("the clipboard slot name %q cannot contain a path separator", name)
	}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '-', r == '_', r == '.':
		default:
			return fmt.Errorf(
				"the clipboard slot name %q contains %q: use letters, digits, '-', '_' and '.'", name, r)
		}
	}
	return nil
}

// HandoverPrefix starts the name of a slot staged for somebody else. That slot
// is shared with them, so it is never the default slot: they would also read
// the next thing copied there.
const HandoverPrefix = "to-"

// HandoverSlot is the slot a handover to user is staged in.
func HandoverSlot(user string) string { return HandoverPrefix + user }

// HandoverRecipient reports who a slot was staged for, if it is a handover.
func HandoverRecipient(slot string) (string, bool) {
	rest, ok := strings.CutPrefix(slot, HandoverPrefix)
	if !ok || rest == "" {
		return "", false
	}
	return rest, true
}

// SlotDir is the directory holding one slot.
func SlotDir(root, slot string) string { return path.Join(root, slot) }

// ManifestPath is where a slot's manifest is stored.
func ManifestPath(root, slot string) string { return path.Join(root, slot, manifestName) }

// PayloadDir is where bytes staged for a slot are stored.
func PayloadDir(root, slot string) string { return path.Join(root, slot, payloadName) }

// PartPath returns where the n-th piece of an entry with parts is stored. The
// number is zero-padded, so a listing is in the order of the join. The pieces
// have a directory of their own, so one recursive delete releases them all.
func PartPath(base string, n int) string {
	return path.Join(base, fmt.Sprintf("%05d", n))
}

// The two machines of a live handover talk only through plain files: PUT, GET,
// PROPFIND and DELETE are all that reva gives two clients.
const (
	streamName = "stream"
	readerName = "reader"
	doneName   = "done"
)

// StreamDir is where the pieces of a live handover appear and are consumed.
func StreamDir(root, slot string) string { return path.Join(root, slot, streamName) }

// ChunkPath is the n-th piece in flight.
func ChunkPath(root, slot string, n int) string { return PartPath(StreamDir(root, slot), n) }

// ChunkIndex recovers a piece number from its name, and reports false for
// anything else in the directory.
func ChunkIndex(name string) (int, bool) {
	if len(name) != 5 {
		return 0, false
	}
	n := 0
	for _, r := range name {
		if r < '0' || r > '9' {
			return 0, false
		}
		n = n*10 + int(r-'0')
	}
	return n, true
}

// ReaderPath is the marker with which the receiver says that it has arrived.
// The sender waits for it. A handover shares the slot read-only and only the
// pieces directory is writable by the recipient, so their marker goes there.
func (m *Manifest) ReaderPath(root string) string {
	if m.To != "" {
		return path.Join(StreamDir(root, m.Slot), readerName)
	}
	return path.Join(root, m.Slot, readerName)
}

// DonePath is the end marker of the sender.
func DonePath(root, slot string) string { return path.Join(root, slot, doneName) }

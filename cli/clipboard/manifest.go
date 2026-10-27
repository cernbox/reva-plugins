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

// The manifest of the cross-machine clipboard: a small file, kept in the
// home space of the user, naming what was copied and where the bytes are.
//
// It is the one part of the feature that two machines have to agree on. A
// slot written by the CLI on lxplus is read by whatever version is installed
// on the laptop, which may be older or newer, so the encoding is versioned
// and every field is optional except the ones a reader cannot do without.

const (
	// Dir is where the clipboard lives inside the home space. The leading dot
	// keeps it out of an ordinary listing, since "cernbox ls" hides dot entries
	// unless asked: a user who never touches this feature never sees it.
	Dir = ".cernbox/clipboard"

	// DefaultSlot is used when none is named, so that the common case is
	// "cernbox copy" on one machine and "cernbox paste" on the other with
	// nothing to remember in between.
	DefaultSlot = "default"

	// Version is the manifest format version. A reader refuses anything higher
	// rather than guessing at fields it does not know.
	Version = 1

	// DefaultTTL is how long a slot lives before it is eligible for collection.
	// Staged bytes sit in the user's own quota, so they cannot live forever; a
	// week is long enough to cover "copy on Friday, paste on Monday".
	DefaultTTL = 7 * 24 * time.Hour

	// MaxManifestSize bounds what a reader will accept. The manifest is metadata
	// about a handful of entries, so anything larger is corrupt or hostile, and
	// reading it into memory unbounded would be the bug.
	MaxManifestSize = 1 << 20

	manifestName = "manifest.json"
	payloadName  = "payload"
)

// Mode distinguishes the two ways a slot can carry something.
const (
	// ModeStore is the default: the copy is on the server, and the machine that
	// made it can go away. Paste can happen a week later.
	ModeStore = ""
	// ModeStream is a live handover: the sender is still running and holding the
	// source open, and the bytes move only while the receiver is pulling them.
	// Nothing is stored: the server holds a small window in flight and the
	// receiver deletes each piece as it consumes it.
	ModeStream = "stream"
)

// Manifest is what one clipboard slot holds.
type Manifest struct {
	// Version is the format version this was written with.
	Version int `json:"version"`
	// Slot is the slot name, repeated inside the file so a manifest read on its
	// own still says where it belongs.
	Slot string `json:"slot"`
	// Mode is ModeStore or ModeStream. It is omitted for a stored copy so that
	// the ordinary manifest is unchanged.
	Mode string `json:"mode,omitempty"`
	// Stream describes a live handover, and is present only in ModeStream.
	Stream *StreamInfo `json:"stream,omitempty"`
	// Created is when the copy was made.
	Created time.Time `json:"created"`
	// Expires is when the slot becomes eligible for collection. Zero means it
	// never expires, which is what --ttl 0 asks for.
	Expires time.Time `json:"expires,omitzero"`
	// Origin records the machine that did the copying.
	Origin Origin `json:"origin"`
	// To is the user this was staged for, empty for an ordinary copy. It is
	// written so that a slot says who can read it without the reader having to
	// take the slot's name apart.
	To string `json:"to,omitempty"`
	// Entries are what was copied, in the order the arguments were given.
	Entries []Entry `json:"entries"`
}

// Entry is one copied file or directory.
type Entry struct {
	// Name is what the entry is called, and what it is pasted as.
	Name string `json:"name"`
	// Path is the absolute CERNBox path where the bytes are. For a staged entry
	// that is inside the slot's own payload directory; for a referenced one it is
	// wherever the file already lived.
	Path string `json:"path"`
	// IsDir reports whether this is a directory.
	IsDir bool `json:"is_dir"`
	// Size is the byte count, recursive for a directory.
	Size int64 `json:"size"`
	// Staged reports whether the CLI uploaded these bytes for the clipboard.
	//
	// This is the difference between the two kinds of copy, and it matters on
	// paste as much as on clear. A staged entry is a private duplicate that
	// clearing the slot should delete; a referenced one is the user's actual
	// file, which clearing must leave alone.
	Staged bool `json:"staged"`
	// Parts is how many pieces the entry was split into, and zero when it is a
	// single object at Path, which is every entry except a stream too large to
	// send in one request.
	//
	// When it is non-zero, Path is a directory and the pieces are the files
	// PartPath names, to be joined in order. Splitting is what lets a pipe be
	// copied at all: its length cannot be known in advance, and every way of
	// sending bytes to reva needs the length up front, so the stream is cut into
	// lengths that are known by the time each piece is sent.
	Parts int `json:"parts,omitempty"`
	// ETag identifies the version that was copied. On a referenced entry it is
	// how paste can tell that the source has changed since, which is worth
	// saying out loud rather than silently handing over different bytes.
	ETag string `json:"etag,omitempty"`
}

// StreamInfo describes a handover that is happening now rather than something
// sitting on the server.
//
// The sender writes it, blocks until a receiver claims the slot, and then feeds
// pieces through a window this size. The receiver deletes each piece as it reads
// it, so what the server holds at any moment is bounded by Window pieces rather
// than by the size of whatever is being sent: a hundred gigabytes can cross
// through a slot that never holds more than a few dozen megabytes.
type StreamInfo struct {
	// ChunkSize is how much of the stream travels in each piece.
	ChunkSize int64 `json:"chunk_size"`
	// Window is how many pieces the sender will run ahead of the receiver. It is
	// what stops a fast sender from turning the slot into storage after all.
	Window int `json:"window"`
}

// Done is what the sender writes when the source is exhausted, so the receiver
// can tell "no more pieces yet" from "no more pieces ever".
type Done struct {
	// Parts is how many pieces were sent in total.
	Parts int `json:"parts"`
	// Size is the total number of bytes, for the receiver to check against.
	Size int64 `json:"size"`
}

// IsStream reports whether the slot is a live handover rather than a stored copy.
func (m *Manifest) IsStream() bool { return m.Mode == ModeStream }

// Origin is the machine a copy was made on.
//
// It is recorded because a clipboard shared between every machine an account
// touches is otherwise ambiguous: "default holds report.pdf" is far less useful
// than knowing it was put there from lxplus812 twenty minutes ago.
type Origin struct {
	Host string `json:"host,omitempty"`
	User string `json:"user,omitempty"`
	// Dir is the working directory the copy was made from, which is often the
	// only thing that distinguishes two files of the same name.
	Dir string `json:"dir,omitempty"`
}

// LocalOrigin describes the machine this process is running on. Every field is
// best effort: none of them is load-bearing, and a hostname that cannot be read
// is not a reason to refuse to copy a file.
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

// NewManifest returns a manifest for slot, expiring after ttl. A ttl of zero or less
// means no expiry.
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

// Size is the total size of everything in the slot.
func (m *Manifest) Size() int64 {
	var total int64
	for _, e := range m.Entries {
		total += e.Size
	}
	return total
}

// StagedSize is how much of the slot is a duplicate the CLI made, and therefore
// how much quota clearing it would return.
func (m *Manifest) StagedSize() int64 {
	var total int64
	for _, e := range m.Entries {
		if e.Staged {
			total += e.Size
		}
	}
	return total
}

// HasStaged reports whether any bytes were uploaded for this slot.
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

// Find returns the entry with the given name.
func (m *Manifest) Find(name string) (Entry, bool) {
	for _, e := range m.Entries {
		if e.Name == name {
			return e, true
		}
	}
	return Entry{}, false
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

// Decode parses a stored manifest.
//
// A manifest from a newer CLI is refused rather than read as far as it parses:
// the fields this version does not know about could be the ones saying where
// the bytes are, and pasting half of a copy is worse than declining to.
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

// ValidateSlot checks a slot name.
//
// A slot becomes a directory name under the clipboard root, so a name with a
// separator or a dot-dot in it would write outside the clipboard entirely. This
// is the only place that is checked, so it is deliberately strict rather than
// clever: names are one plain segment.
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

// HandoverPrefix marks a slot staged for somebody else rather than for another
// of your own machines.
//
// A handover gets a slot of its own, named after the recipient, and never the
// default one. The slot directory is shared with them, so anything in it is
// theirs to read: putting a handover in the slot the rest of your copies go to
// would hand over the next thing you copied as well.
const HandoverPrefix = "to-"

// HandoverSlot is the slot a handover to user is staged in.
func HandoverSlot(user string) string { return HandoverPrefix + user }

// HandoverRecipient reports who a slot was staged for, and whether it is a
// handover at all.
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

// IsManifest reports whether name is the manifest file, so a listing of a slot
// can tell metadata from payload.
func IsManifest(name string) bool { return name == manifestName }

// PartPath returns where the n-th piece of a split entry is stored, given the
// entry's Path.
//
// The pieces sit inside a directory of their own rather than alongside each other
// under suffixed names, so that releasing them is one recursive delete instead of
// one request per piece, which for a large stream is the difference between a
// single request and thousands.
//
// The number is zero-padded so that a plain listing of the directory is in the
// order the pieces have to be joined, which is what makes a half-finished stream
// something a person can look at and understand.
func PartPath(base string, n int) string {
	return path.Join(base, fmt.Sprintf("%05d", n))
}

// Layout of a live handover. These are the only things the two machines say to
// each other, and every one of them is a plain file, because PUT, GET, PROPFIND
// and DELETE are the whole vocabulary reva gives two clients for talking.
const (
	streamName = "stream"
	readerName = "reader"
	doneName   = "done"
)

// StreamDir is where the pieces of a live handover appear and are consumed.
func StreamDir(root, slot string) string { return path.Join(root, slot, streamName) }

// ChunkPath is the n-th piece in flight.
func ChunkPath(root, slot string, n int) string {
	return path.Join(StreamDir(root, slot), fmt.Sprintf("%05d", n))
}

// ChunkIndex recovers a piece's number from its name, reporting false for
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

// ReaderPath is how the receiver says it has arrived. The sender blocks until
// this exists, which is what makes copy wait rather than upload.
func ReaderPath(root, slot string) string { return path.Join(root, slot, readerName) }

// ScratchReaderPath is the arrival marker for a handover, inside the one
// directory the recipient can write in.
func ScratchReaderPath(root, slot string) string {
	return path.Join(StreamDir(root, slot), readerName)
}

// ReaderPath is where this manifest's receiver announces itself, which depends
// on who the receiver is.
//
// Another of your own machines writes the marker at the top of the slot. Someone
// else cannot: a handover shares the slot read-only, and only the pieces
// directory is writable by them, precisely so that the manifest and everything
// else stays theirs to read and nothing more. So their marker goes in there,
// next to the pieces.
//
// Both sides read this from the same manifest, so there is no negotiation and
// nothing to get out of step.
func (m *Manifest) ReaderPath(root string) string {
	if m.To != "" {
		return ScratchReaderPath(root, m.Slot)
	}
	return ReaderPath(root, m.Slot)
}

// DonePath is how the sender says the source is exhausted. Without it the
// receiver could not tell a pause in a slow stream from the end of it.
func DonePath(root, slot string) string { return path.Join(root, slot, doneName) }

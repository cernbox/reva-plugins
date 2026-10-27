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
	"strings"
	"testing"
	"time"
)

func TestRoundTrip(t *testing.T) {
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	m := NewManifest("default", now, DefaultTTL, Origin{Host: "lxplus812", User: "gdelmont", Dir: "/afs/cern.ch/user/g/gdelmont"})
	m.Entries = []Entry{
		{Name: "report.pdf", Path: "/eos/user/g/gdelmont/report.pdf", Size: 2100, ETag: "abc"},
		{Name: "data", Path: "/eos/user/g/gdelmont/.cernbox/clipboard/default/payload/data", IsDir: true, Size: 4096, Staged: true},
	}

	b, err := m.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}

	if got.Slot != "default" || got.Version != Version {
		t.Errorf("slot/version did not survive: %+v", got)
	}
	if !got.Created.Equal(now) {
		t.Errorf("created = %v, want %v", got.Created, now)
	}
	if !got.Expires.Equal(now.Add(DefaultTTL)) {
		t.Errorf("expires = %v, want %v", got.Expires, now.Add(DefaultTTL))
	}
	if len(got.Entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(got.Entries))
	}
	if got.Entries[0].Staged || !got.Entries[1].Staged {
		t.Errorf("the staged flag did not survive: %+v", got.Entries)
	}
	if got.Origin.String() != "gdelmont@lxplus812" {
		t.Errorf("origin = %q", got.Origin.String())
	}
}

// TestNoExpiryIsOmitted: --ttl 0 means "keep it until I clear it", and a zero
// timestamp serialised as "0001-01-01T00:00:00Z" reads like a date in the past,
// which is exactly the thing that would make a collector delete it.
func TestNoExpiryIsOmitted(t *testing.T) {
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	m := NewManifest("default", now, 0, Origin{})

	b, err := m.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "expires") {
		t.Errorf("a slot with no expiry should not carry the field:\n%s", b)
	}
	got, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Expired(now.Add(10 * 365 * 24 * time.Hour)) {
		t.Error("a slot with no expiry must never expire")
	}
}

func TestExpired(t *testing.T) {
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	m := NewManifest("default", now, time.Hour, Origin{})

	if m.Expired(now.Add(59 * time.Minute)) {
		t.Error("should not be expired before the hour is up")
	}
	if !m.Expired(now.Add(61 * time.Minute)) {
		t.Error("should be expired after the hour is up")
	}
}

// TestDecodeRefusesANewerFormat: the fields a newer CLI added could be the ones
// saying where the bytes are, so a partial read is worse than an honest refusal.
func TestDecodeRefusesANewerFormat(t *testing.T) {
	_, err := Decode([]byte(`{"version":99,"slot":"default","entries":[]}`))
	if err == nil {
		t.Fatal("a newer format should be refused")
	}
	if !strings.Contains(err.Error(), "upgrade") {
		t.Errorf("the error should say what to do about it: %v", err)
	}
}

func TestDecodeRejectsIncompleteEntries(t *testing.T) {
	cases := map[string]string{
		"no name":     `{"version":1,"entries":[{"path":"/eos/x"}]}`,
		"no location": `{"version":1,"entries":[{"name":"x"}]}`,
		"empty":       ``,
		"not json":    `this is not json`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Decode([]byte(body)); err == nil {
				t.Error("should have been refused")
			}
		})
	}
}

func TestSizes(t *testing.T) {
	m := &Manifest{Entries: []Entry{
		{Name: "a", Path: "/a", Size: 100, Staged: true},
		{Name: "b", Path: "/b", Size: 200},
	}}

	if got := m.Size(); got != 300 {
		t.Errorf("Size = %d, want 300", got)
	}
	// Only the staged half is quota the CLI is responsible for.
	if got := m.StagedSize(); got != 100 {
		t.Errorf("StagedSize = %d, want 100", got)
	}
	if !m.HasStaged() {
		t.Error("HasStaged should be true")
	}

	referenced := &Manifest{Entries: []Entry{{Name: "b", Path: "/b", Size: 200}}}
	if referenced.HasStaged() {
		t.Error("a slot of references has nothing staged")
	}
	if got := referenced.StagedSize(); got != 0 {
		t.Errorf("StagedSize = %d, want 0", got)
	}
}

func TestFind(t *testing.T) {
	m := &Manifest{Entries: []Entry{{Name: "a", Path: "/a"}}}
	if _, ok := m.Find("a"); !ok {
		t.Error("should have found a")
	}
	if _, ok := m.Find("b"); ok {
		t.Error("should not have found b")
	}
}

// TestValidateSlot: a slot name becomes a directory under the clipboard root, so
// anything that could climb out of it has to be refused here: this is the only
// place it is checked.
func TestValidateSlot(t *testing.T) {
	good := []string{"default", "logs", "build-2026", "a.b_c", "X"}
	for _, name := range good {
		if err := ValidateSlot(name); err != nil {
			t.Errorf("ValidateSlot(%q) = %v, want nil", name, err)
		}
	}

	bad := []string{"", ".", "..", "a/b", `a\b`, "../../etc", "a b", "a:b", "a*", strings.Repeat("x", 65)}
	for _, name := range bad {
		if err := ValidateSlot(name); err == nil {
			t.Errorf("ValidateSlot(%q) = nil, want an error", name)
		}
	}
}

func TestLayout(t *testing.T) {
	const root = "/eos/user/g/gdelmont/.cernbox/clipboard"

	if got, want := SlotDir(root, "logs"), root+"/logs"; got != want {
		t.Errorf("SlotDir = %q, want %q", got, want)
	}
	if got, want := ManifestPath(root, "logs"), root+"/logs/manifest.json"; got != want {
		t.Errorf("ManifestPath = %q, want %q", got, want)
	}
	if got, want := PayloadDir(root, "logs"), root+"/logs/payload"; got != want {
		t.Errorf("PayloadDir = %q, want %q", got, want)
	}
	if !IsManifest("manifest.json") || IsManifest("payload") {
		t.Error("IsManifest does not distinguish metadata from payload")
	}
}

func TestLocalOrigin(t *testing.T) {
	// Every field is best effort, so the only contract is that it does not fail
	// and that the working directory is carried through.
	o := LocalOrigin("/tmp/work")
	if o.Dir != "/tmp/work" {
		t.Errorf("Dir = %q", o.Dir)
	}
	if o.String() == "" {
		t.Error("String should never be empty")
	}
}

func TestOriginString(t *testing.T) {
	cases := []struct {
		in   Origin
		want string
	}{
		{Origin{Host: "h", User: "u"}, "u@h"},
		{Origin{Host: "h"}, "h"},
		{Origin{User: "u"}, "u"},
		{Origin{}, "-"},
	}
	for _, c := range cases {
		if got := c.in.String(); got != c.want {
			t.Errorf("Origin%+v.String() = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestManifestReaderPathDependsOnWhoTheReceiverIs(t *testing.T) {
	const root = "/eos/user/e/einstein/.cernbox/clipboard"

	// Another of your own machines can write anywhere in the slot.
	own := NewManifest("default", time.Now(), 0, Origin{})
	own.Mode = ModeStream
	if got, want := own.ReaderPath(root), root+"/default/reader"; got != want {
		t.Errorf("own machine's marker at %q, want %q", got, want)
	}

	// Somebody else can only write in the pieces directory, because that is all a
	// handover shares with them.
	handover := NewManifest("to-marie", time.Now(), 0, Origin{})
	handover.Mode = ModeStream
	handover.To = "marie"
	if got, want := handover.ReaderPath(root), root+"/to-marie/stream/reader"; got != want {
		t.Errorf("recipient's marker at %q, want %q", got, want)
	}

	// The marker must not be mistaken for a piece, whichever directory it is in.
	if _, ok := ChunkIndex("reader"); ok {
		t.Error("the arrival marker parses as a piece number")
	}
}

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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cernbox/reva-plugins/cli/internal/plugintest"
	"github.com/cernbox/reva-plugins/cli/internal/userplane"
	collaboration "github.com/cs3org/go-cs3apis/cs3/sharing/collaboration/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/v3/pkg/cli"
	"github.com/spf13/cobra"
)

// The home of the fake is /, so the clipboard is at /.cernbox/clipboard.
const root = "/" + Dir

func newEnv(t *testing.T) *plugintest.Env {
	return plugintest.New(t, "alice", NewCopy, NewPaste, New)
}

func localFile(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func manifestOf(t *testing.T, e *plugintest.Env, slot string) *Manifest {
	t.Helper()
	m, err := Decode([]byte(e.Content(t, ManifestPath(root, slot))))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func readLocal(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestCopyStagesALocalFile(t *testing.T) {
	e := newEnv(t)
	src := localFile(t, "report.txt", "hello")
	r := e.Must(t, "copy", src)
	if !strings.Contains(r.Stderr, `to the "default" clipboard slot`) || !strings.Contains(r.Stderr, "cernbox paste") {
		t.Errorf("stderr %q, want the slot and the paste hint", r.Stderr)
	}
	m := manifestOf(t, e, DefaultSlot)
	if len(m.Entries) != 1 || !m.Entries[0].Staged || m.Entries[0].Size != 5 || m.Entries[0].Name != "report.txt" {
		t.Fatalf("entries %+v, want report.txt staged with 5 bytes", m.Entries)
	}
	if got := e.Content(t, m.Entries[0].Path); got != "hello" {
		t.Errorf("staged content %q", got)
	}
	if m.Expires.IsZero() {
		t.Error("no expiry with the default ttl")
	}
}

func TestCopyReferencesARemotePath(t *testing.T) {
	e := newEnv(t)
	e.B.AddFile("/docs/report.txt", []byte("remote"))
	e.Must(t, "copy", "cb:/docs/report.txt")
	m := manifestOf(t, e, DefaultSlot)
	if len(m.Entries) != 1 || m.Entries[0].Staged || m.Entries[0].Path != "/docs/report.txt" || m.Entries[0].ETag == "" {
		t.Fatalf("entries %+v, want a reference to /docs/report.txt with its etag", m.Entries)
	}
	if _, ok := e.B.Content(PayloadDir(root, DefaultSlot) + "/report.txt"); ok {
		t.Error("a reference was uploaded")
	}
}

func TestCopyAndPasteRoundTrip(t *testing.T) {
	e := newEnv(t)
	e.Must(t, "copy", localFile(t, "a.txt", "alpha"))
	dest := t.TempDir()
	r := e.Must(t, "paste", dest+"/")
	if got := readLocal(t, filepath.Join(dest, "a.txt")); got != "alpha" {
		t.Errorf("pasted %q", got)
	}
	// Paste leaves the clipboard alone, and says so.
	if !strings.Contains(r.Stderr, "Still on the clipboard") {
		t.Errorf("stderr %q, want the slot to survive", r.Stderr)
	}
	if _, ok := e.B.Content(ManifestPath(root, DefaultSlot)); !ok {
		t.Error("paste emptied the slot")
	}
}

func TestPasteRefusesAnExistingDestination(t *testing.T) {
	e := newEnv(t)
	e.Must(t, "copy", localFile(t, "a.txt", "new"))
	dest := t.TempDir()
	if err := os.WriteFile(filepath.Join(dest, "a.txt"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Run(t, nil, "paste", dest); err == nil || !strings.Contains(err.Error(), "-f") {
		t.Fatalf("paste over a file: %v, want a refusal that names -f", err)
	}
	if got := readLocal(t, filepath.Join(dest, "a.txt")); got != "old" {
		t.Errorf("the refused paste wrote %q", got)
	}
	e.Must(t, "paste", "-f", dest)
	if got := readLocal(t, filepath.Join(dest, "a.txt")); got != "new" {
		t.Errorf("paste -f wrote %q", got)
	}
}

func TestPasteToARemotePathMovesNoData(t *testing.T) {
	e := newEnv(t)
	e.B.AddFile("/docs/report.txt", []byte("remote"))
	e.B.AddFile("/out/.keep", nil)
	e.Must(t, "copy", "cb:/docs/report.txt")
	r := e.Must(t, "paste", "cb:/out/")
	if got := e.Content(t, "/out/report.txt"); got != "remote" {
		t.Errorf("pasted %q", got)
	}
	if !strings.Contains(r.Stderr, "nothing was transferred") {
		t.Errorf("stderr %q, want the server-side copy said", r.Stderr)
	}
}

func TestPasteReportsAReferenceThatIsGone(t *testing.T) {
	e := newEnv(t)
	e.B.AddFile("/docs/report.txt", []byte("remote"))
	e.Must(t, "copy", "cb:/docs/report.txt")
	e.Must(t, "copy", "--slot", "keep", "cb:/docs/report.txt")
	if err := e.B.Files().Remove(t.Context(), &provider.Reference{Path: "/docs/report.txt"}); err != nil {
		t.Fatal(err)
	}
	_, err := e.Run(t, nil, "paste", t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "moved or deleted") {
		t.Errorf("paste of a deleted reference: %v, want it explained", err)
	}
}

func TestCopyReplacesTheSlotAndReleasesThePayload(t *testing.T) {
	e := newEnv(t)
	e.Must(t, "copy", localFile(t, "first.txt", "1"))
	first := manifestOf(t, e, DefaultSlot).Entries[0].Path
	e.Must(t, "copy", localFile(t, "second.txt", "2"))
	m := manifestOf(t, e, DefaultSlot)
	if len(m.Entries) != 1 || m.Entries[0].Name != "second.txt" {
		t.Fatalf("entries %+v, want only second.txt", m.Entries)
	}
	if _, ok := e.B.Content(first); ok {
		t.Error("the payload of the replaced copy is still there")
	}
}

// Another machine replaced the manifest after this one read its etag: the
// write must fail and name the race, not orphan the other copy.
func TestCopyDetectsAConcurrentCopy(t *testing.T) {
	e := newEnv(t)
	e.Must(t, "copy", localFile(t, "a.txt", "1"))
	b := &board{Session: &userplane.Session{RT: cli.NewRuntime(nil), Cmd: &cobra.Command{}, B: e.B, D: e.B.Data()}}
	m := NewManifest(DefaultSlot, time.Now(), time.Hour, Origin{})
	err := b.writeSlot(t.Context(), root, m, "an-etag-of-before")
	if err == nil || !strings.Contains(err.Error(), "another computer copied to it") {
		t.Fatalf("write with a stale etag: %v", err)
	}
	if got := manifestOf(t, e, DefaultSlot); len(got.Entries) != 1 {
		t.Error("the write with a stale etag replaced the manifest")
	}
}

func TestCopyRefusals(t *testing.T) {
	e := newEnv(t)
	dir := t.TempDir()
	e.B.AddFile("/docs/x/a.txt", nil)
	a := localFile(t, "same.txt", "1")
	b := filepath.Join(t.TempDir(), "same.txt")
	if err := os.WriteFile(b, []byte("2"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, args := range map[string][]string{
		"local folder":    {"copy", dir},
		"remote folder":   {"copy", "cb:/docs/x"},
		"same name":       {"copy", a, b},
		"bad slot":        {"copy", "--slot", "../up", a},
		"stdin twice":     {"copy", "-", "-"},
		"to and slot":     {"copy", "--to", "bob", "--slot", "x", a},
		"stream two":      {"copy", "--stream", a, b},
		"stream remote":   {"copy", "--stream", "cb:/docs/x/a.txt"},
		"paste from+slot": {"paste", "--from", "bob", "--slot", "x"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := e.Run(t, nil, args...)
			if _, ok := errors.AsType[*cli.UsageError](err); !ok {
				t.Errorf("%v: %v, want a usage error", args, err)
			}
		})
	}
}

func TestCopyRecursive(t *testing.T) {
	e := newEnv(t)
	dir := filepath.Join(t.TempDir(), "data")
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sub", "x.txt"), []byte("xyz"), 0o600); err != nil {
		t.Fatal(err)
	}
	e.Must(t, "copy", "-r", dir)
	m := manifestOf(t, e, DefaultSlot)
	if !m.Entries[0].IsDir || m.Entries[0].Size != 3 {
		t.Fatalf("entry %+v, want the folder with 3 bytes", m.Entries[0])
	}
	dest := t.TempDir()
	e.Must(t, "paste", "--no-archive", dest)
	if got := readLocal(t, filepath.Join(dest, "data", "sub", "x.txt")); got != "xyz" {
		t.Errorf("pasted %q", got)
	}
}

func TestPasteFromAnEmptySlotSaysSo(t *testing.T) {
	e := newEnv(t)
	_, err := e.Run(t, nil, "paste", "--slot", "nothing", t.TempDir())
	if !errors.Is(err, errNoSlot) {
		t.Errorf("paste of a slot never written: %v, want errNoSlot", err)
	}
}

func TestPasteSeveralItemsNeedsADirectory(t *testing.T) {
	e := newEnv(t)
	e.Must(t, "copy", localFile(t, "a.txt", "1"), localFile(t, "b.txt", "2"))
	if _, err := e.Run(t, nil, "paste", filepath.Join(t.TempDir(), "missing")); err == nil || !strings.Contains(err.Error(), "existing directory") {
		t.Errorf("paste of two items to a new name: %v", err)
	}
	if _, err := e.Run(t, nil, "paste", "-"); err == nil || !strings.Contains(err.Error(), "one at a time") {
		t.Errorf("paste of two items to stdout: %v", err)
	}
}

func TestCopyStdinAndPasteStdout(t *testing.T) {
	e := newEnv(t)
	if _, err := e.Run(t, strings.NewReader("from a pipe"), "copy", "-", "--name", "pipe.txt"); err != nil {
		t.Fatal(err)
	}
	if m := manifestOf(t, e, DefaultSlot); m.Entries[0].Name != "pipe.txt" || m.Entries[0].Parts != 0 {
		t.Errorf("entry %+v, want one plain file named pipe.txt", m.Entries[0])
	}
	r := e.Must(t, "paste", "-")
	if r.Stdout != "from a pipe" {
		t.Errorf("stdout %q", r.Stdout)
	}
}

// A pipe longer than a chunk goes in pieces of known length, and paste
// joins them, to a file, to stdout and to CERNBox.
func TestCopyStreamsALargeStdinInPieces(t *testing.T) {
	e := newEnv(t)
	e.WriteConfig(t, "[transfer]\nchunk_size = \"4\"\n")
	if _, err := e.Run(t, strings.NewReader("0123456789"), "copy", "-"); err != nil {
		t.Fatal(err)
	}
	en := manifestOf(t, e, DefaultSlot).Entries[0]
	if en.Parts != 3 || en.Size != 10 || en.Name != "stdin" {
		t.Fatalf("entry %+v, want stdin in 3 parts of 10 bytes", en)
	}
	if got := e.Content(t, PartPath(en.Path, 2)); got != "89" {
		t.Errorf("last part %q", got)
	}
	if r := e.Must(t, "paste", "-"); r.Stdout != "0123456789" {
		t.Errorf("stdout %q", r.Stdout)
	}
	dest := t.TempDir()
	e.Must(t, "paste", dest)
	if got := readLocal(t, filepath.Join(dest, "stdin")); got != "0123456789" {
		t.Errorf("file %q", got)
	}
	e.Must(t, "paste", "cb:/joined.txt")
	if got := e.Content(t, "/joined.txt"); got != "0123456789" {
		t.Errorf("remote %q", got)
	}
	// One clear releases every piece.
	e.Must(t, "clipboard", "clear")
	if _, ok := e.B.Content(PartPath(en.Path, 0)); ok {
		t.Error("a piece survived the clear")
	}
}

func TestClipboardListAndClear(t *testing.T) {
	e := newEnv(t)
	if r := e.Must(t, "clipboard", "list"); !strings.Contains(r.Stderr, "The clipboard is empty") {
		t.Errorf("empty list: %q", r.Stderr)
	}
	e.B.AddFile("/docs/ref.txt", []byte("r"))
	e.Must(t, "copy", localFile(t, "a.txt", "12345"))
	e.Must(t, "copy", "--slot", "refs", "cb:/docs/ref.txt")
	r := e.Must(t, "clipboard", "list")
	for _, want := range []string{"SLOT", "default", "a.txt", "refs", "ref.txt"} {
		if !strings.Contains(r.Stdout, want) {
			t.Errorf("no %q in:\n%s", want, r.Stdout)
		}
	}
	r = e.Must(t, "clipboard", "list", "-o", "json")
	var views []map[string]any
	if err := json.Unmarshal([]byte(r.Stdout), &views); err != nil || len(views) != 2 || views[0]["slot"] != "default" {
		t.Fatalf("json %s: %v", r.Stdout, err)
	}

	r = e.Must(t, "clipboard", "clear", "refs")
	if strings.Contains(r.Stderr, "Freed") {
		t.Errorf("clearing a reference freed quota: %q", r.Stderr)
	}
	if got := e.Content(t, "/docs/ref.txt"); got != "r" {
		t.Error("clear touched the referenced file")
	}
	r = e.Must(t, "clipboard", "clear", "--all", "-o", "json")
	if !strings.Contains(r.Stdout, `"cleared": 1`) || !strings.Contains(r.Stdout, `"freed_bytes": 5`) {
		t.Errorf("clear --all json: %s", r.Stdout)
	}
	if r := e.Must(t, "clipboard", "clear"); !strings.Contains(r.Stderr, "already empty") {
		t.Errorf("a second clear: %q", r.Stderr)
	}
	if _, err := e.Run(t, nil, "clipboard", "clear", "--all", "x"); err == nil {
		t.Error("clear --all with a slot name: no error")
	}
}

func TestExpiredSlotIsCollected(t *testing.T) {
	e := newEnv(t)
	m := NewManifest("old", time.Now().Add(-48*time.Hour), time.Hour, Origin{})
	m.Entries = []Entry{{Name: "x", Path: PayloadDir(root, "old") + "/x", Staged: true, Size: 1}}
	data, _ := m.Encode()
	e.B.AddFile(ManifestPath(root, "old"), data)
	e.B.AddFile(m.Entries[0].Path, []byte("x"))
	r := e.Must(t, "clipboard", "list")
	if !strings.Contains(r.Stderr, `Removed the expired "old" clipboard slot`) {
		t.Errorf("stderr %q, want the expired slot collected", r.Stderr)
	}
	if _, ok := e.B.Content(m.Entries[0].Path); ok {
		t.Error("the expired payload is still there")
	}
}

func TestCopyJSONEmitsTheManifest(t *testing.T) {
	e := newEnv(t)
	r := e.Must(t, "copy", "-o", "json", "--ttl", "0", localFile(t, "a.txt", "1"))
	var m Manifest
	if err := json.Unmarshal([]byte(r.Stdout), &m); err != nil || m.Slot != DefaultSlot || !m.Expires.IsZero() {
		t.Errorf("json %s: %v, want the manifest without expiry", r.Stdout, err)
	}
	if r.Stderr != "" {
		t.Errorf("-o json printed messages: %q", r.Stderr)
	}
}

// The handover to oneself goes through the same share as to somebody else:
// the received share names the slot by its resource id.
func TestHandover(t *testing.T) {
	e := newEnv(t)
	e.B.AddFile("/docs/report.txt", []byte("for you"))
	r := e.Must(t, "copy", "--to", "alice", "cb:/docs/report.txt")
	if !strings.Contains(r.Stderr, "paste --from alice") {
		t.Errorf("stderr %q, want the paste hint", r.Stderr)
	}
	slot := HandoverSlot("alice")
	m := manifestOf(t, e, slot)
	// A handover stages also a CERNBox path: the recipient cannot read the
	// path of the sender.
	if !m.Entries[0].Staged || !strings.HasPrefix(m.Entries[0].Path, PayloadDir(root, slot)) || m.To != "alice" {
		t.Fatalf("manifest %+v, want the file staged in the slot", m)
	}
	info, err := e.B.Files().Stat(t.Context(), &provider.Reference{Path: SlotDir(root, slot)})
	if err != nil {
		t.Fatal(err)
	}
	shares, _ := e.B.Shares().List(t.Context(), info.Id)
	if len(shares) != 1 || !sharedWith(shares[0], "alice") {
		t.Fatalf("shares %v, want one with alice on the slot", shares)
	}
	// A second copy reuses the share.
	e.Must(t, "copy", "--to", "alice", "cb:/docs/report.txt")
	if shares, _ := e.B.Shares().List(t.Context(), info.Id); len(shares) != 1 {
		t.Errorf("%d shares after a second copy, want 1", len(shares))
	}

	if _, err := e.Run(t, nil, "paste", "--from", "alice", t.TempDir()); err == nil || !strings.Contains(err.Error(), "nothing has been handed over") {
		t.Fatalf("paste --from before the share arrived: %v", err)
	}
	rs := &collaboration.ReceivedShare{Share: shares[0], State: collaboration.ShareState_SHARE_STATE_ACCEPTED}
	e.B.AddReceived(rs)
	if r := e.Must(t, "clipboard", "list"); !strings.Contains(r.Stderr, "paste --from alice") {
		t.Errorf("list does not show the incoming handover: %q", r.Stderr)
	}
	dest := t.TempDir()
	r = e.Must(t, "paste", "--from", "alice", dest)
	if got := readLocal(t, filepath.Join(dest, "report.txt")); got != "for you" {
		t.Errorf("pasted %q", got)
	}
	if !rs.Hidden {
		t.Error("the collected handover is still in the share list")
	}
	if !strings.Contains(r.Stderr, "until they clear it") {
		t.Errorf("stderr %q", r.Stderr)
	}

	// Clear takes the grant back before the slot goes.
	e.Must(t, "clipboard", "clear", slot)
	if shares, _ := e.B.Shares().List(t.Context(), info.Id); len(shares) != 0 {
		t.Errorf("shares %v after clear, want none", shares)
	}
}

func TestStreamHandsOverLive(t *testing.T) {
	e := newEnv(t)
	e.WriteConfig(t, "[transfer]\nchunk_size = \"3\"\n")
	payload := strings.Repeat("abcdefghij", 5)
	send := e.Start(t, strings.NewReader(payload), "copy", "--stream", "-", "--name", "live.txt", "--wait", "20s")
	dest := t.TempDir()
	type result struct {
		r   plugintest.Result
		err error
	}
	sent := make(chan result, 1)
	go func() {
		r, err := send()
		sent <- result{r, err}
	}()
	// The receiver needs the manifest of the sender.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, ok := e.B.Content(ManifestPath(root, DefaultSlot)); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the sender wrote no manifest")
		}
		time.Sleep(20 * time.Millisecond)
	}
	r, err := e.Run(t, nil, "paste", dest, "--wait", "20s")
	if err != nil {
		t.Fatalf("paste: %v\n%s", err, r.Stderr)
	}
	s := <-sent
	if s.err != nil {
		t.Fatalf("copy --stream: %v\n%s", s.err, s.r.Stderr)
	}
	if got := readLocal(t, filepath.Join(dest, "live.txt")); got != payload {
		t.Errorf("received %q", got)
	}
	if !strings.Contains(s.r.Stderr, "Nothing was stored") {
		t.Errorf("sender stderr %q", s.r.Stderr)
	}
	// The sender tears the slot down.
	if _, ok := e.B.Content(ManifestPath(root, DefaultSlot)); ok {
		t.Error("the stream slot is still there")
	}
}

func TestStreamSenderGivesUpWithoutAReceiver(t *testing.T) {
	e := newEnv(t)
	_, err := e.Run(t, strings.NewReader("x"), "copy", "--stream", "-", "--wait", "300ms")
	if err == nil || !strings.Contains(err.Error(), "nobody pasted") {
		t.Fatalf("copy --stream without a paste: %v", err)
	}
	if _, ok := e.B.Content(ManifestPath(root, DefaultSlot)); ok {
		t.Error("the slot survived the timeout")
	}
}

func TestStreamCannotBePastedIntoCERNBox(t *testing.T) {
	e := newEnv(t)
	m := NewManifest(DefaultSlot, time.Now(), time.Hour, Origin{Host: "lxplus"})
	m.Mode = ModeStream
	m.Stream = &StreamInfo{ChunkSize: 4, Window: 4}
	m.Entries = []Entry{{Name: "x", Path: StreamDir(root, DefaultSlot), Size: -1, Staged: true}}
	data, _ := m.Encode()
	e.B.AddFile(ManifestPath(root, DefaultSlot), data)
	_, err := e.Run(t, nil, "paste", "cb:/x")
	if _, ok := errors.AsType[*cli.UsageError](err); !ok {
		t.Errorf("paste of a stream to CERNBox: %v, want a usage error", err)
	}
}

func TestPendingChunksCountsOnlyPieces(t *testing.T) {
	infos := []*provider.ResourceInfo{{Path: "/s/00000"}, {Path: "/s/00001"}, {Path: "/s/reader"}}
	if n := pendingChunks(infos); n != 2 {
		t.Errorf("pendingChunks = %d, want 2", n)
	}
}

func TestDisplayHelpers(t *testing.T) {
	now := time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)
	for d, want := range map[time.Duration]string{30 * time.Second: "just now", 5 * time.Minute: "5m ago", 3 * time.Hour: "3h ago", 50 * time.Hour: "2d ago"} {
		if got := agoColumn(now.Add(-d), now); got != want {
			t.Errorf("agoColumn(%s) = %q, want %q", d, got, want)
		}
	}
	m := &Manifest{Entries: []Entry{{Name: "a", IsDir: true}, {Name: "b"}, {Name: "c"}}}
	if got := contentsColumn(m); got != "a/ +2 more" {
		t.Errorf("contentsColumn = %q", got)
	}
	if slotArg(DefaultSlot) != "" || slotArg("x") != " x" || pasteHint("cernbox", "x") != "cernbox paste --slot x" {
		t.Error("the slot hints are wrong")
	}
}

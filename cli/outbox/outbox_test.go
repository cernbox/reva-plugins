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

package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cernbox/reva-plugins/cli/internal/plugintest"
	"github.com/cernbox/reva-plugins/cli/internal/userplane"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/v3/pkg/cli"
	"github.com/cs3org/reva/v3/pkg/cli/clitest"
)

const remote = "/Screenshots"

func setup(t *testing.T, files ...string) (*plugintest.Env, string) {
	t.Helper()
	e := plugintest.New(t, "alice", New)
	dir := t.TempDir()
	for i := 0; i+1 < len(files); i += 2 {
		write(t, filepath.Join(dir, files[i]), files[i+1])
	}
	return e, dir
}

func write(t *testing.T, p, content string) {
	t.Helper()
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func exists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

func TestPushUploadsWhatIsWaiting(t *testing.T) {
	e, dir := setup(t, "a.png", "alpha", "b.png", "beta")
	r := e.Must(t, "outbox", "push", dir, "--to", remote, "--settle", "0")
	if got := e.Content(t, remote+"/a.png"); got != "alpha" {
		t.Errorf("uploaded %q", got)
	}
	if !strings.Contains(r.Stderr, "Uploaded 2 files") {
		t.Errorf("stderr %q", r.Stderr)
	}
	if !exists(filepath.Join(dir, "a.png")) {
		t.Error("the local file is gone with the default policy")
	}
	if r := e.Must(t, "outbox", "push", dir, "--to", remote, "--settle", "0"); !strings.Contains(r.Stderr, "2 were already there") {
		t.Errorf("second pass: %q", r.Stderr)
	}
}

func TestLeavesAFileStillBeingWritten(t *testing.T) {
	e, dir := setup(t, "a.png", "alpha")
	r := e.Must(t, "outbox", "push", dir, "--to", remote, "--settle", "1h")
	if _, ok := e.B.Content(remote + "/a.png"); ok {
		t.Error("a file still being written went up")
	}
	if !strings.Contains(r.Stderr, "still being written") {
		t.Errorf("stderr %q", r.Stderr)
	}
}

func TestDateLayout(t *testing.T) {
	e, dir := setup(t, "a.png", "alpha")
	when := time.Date(2025, 3, 7, 10, 0, 0, 0, time.Local)
	if err := os.Chtimes(filepath.Join(dir, "a.png"), when, when); err != nil {
		t.Fatal(err)
	}
	e.Must(t, "outbox", "push", dir, "--to", remote, "--settle", "0", "--layout", "date")
	if got := e.Content(t, remote+"/2025/03/07/a.png"); got != "alpha" {
		t.Errorf("uploaded %q", got)
	}
}

func TestAfterDeleteAndMove(t *testing.T) {
	e, dir := setup(t, "a.png", "alpha")
	e.Must(t, "outbox", "push", dir, "--to", remote, "--settle", "0", "--after", "move")
	if exists(filepath.Join(dir, "a.png")) || !exists(filepath.Join(dir, uploadedDir, "a.png")) {
		t.Error("move did not put the file in .uploaded")
	}
	write(t, filepath.Join(dir, "b.png"), "beta")
	e.Must(t, "outbox", "push", dir, "--to", remote, "--settle", "0", "--after", "delete")
	if exists(filepath.Join(dir, "b.png")) {
		t.Error("delete left the file")
	}
	if got := e.Content(t, remote+"/b.png"); got != "beta" {
		t.Errorf("uploaded %q", got)
	}
}

func TestAfterDeleteKeepsTheFileWhenTheUploadFails(t *testing.T) {
	e, dir := setup(t, "a.png", "alpha")
	e.D = failingUpload{e.B.Data()}
	r, err := e.Run(t, nil, "outbox", "push", dir, "--to", remote, "--settle", "0", "--after", "delete")
	if err != nil {
		t.Fatalf("a failed file is a warning: %v", err)
	}
	if !exists(filepath.Join(dir, "a.png")) {
		t.Error("the local file was deleted although the upload failed")
	}
	if !strings.Contains(r.Stderr, "a.png: the storage is full") {
		t.Errorf("stderr %q, want the failure named", r.Stderr)
	}
}

type failingUpload struct{ *clitest.Data }

func (failingUpload) Upload(context.Context, *provider.Reference, func() (io.ReadCloser, error), int64, string) error {
	return errors.New("the storage is full")
}

func TestNeverOverwrites(t *testing.T) {
	e, dir := setup(t, "a.png", "new")
	e.B.AddFile(remote+"/a.png", []byte("already there, longer"))
	e.Must(t, "outbox", "push", dir, "--to", remote, "--settle", "0")
	if got := e.Content(t, remote+"/a.png"); got != "already there, longer" {
		t.Errorf("the remote file became %q", got)
	}
	if got := e.Content(t, remote+"/a (2).png"); got != "new" {
		t.Errorf("the upload is %q", got)
	}
}

func TestSkipsTempHiddenAndFolders(t *testing.T) {
	e, dir := setup(t, "good.png", "y", ".hidden", "n", "half.crdownload", "n")
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	r := e.Must(t, "outbox", "push", dir, "--to", remote, "--settle", "0")
	infos, err := e.B.Files().List(t.Context(), userplane.Ref(remote))
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || filepath.Base(infos[0].Path) != "good.png" {
		t.Errorf("uploaded %v, want only good.png", infos)
	}
	if !strings.Contains(r.Stderr, "1 folder skipped") {
		t.Errorf("stderr %q, want the folder said", r.Stderr)
	}
}

func TestStatusExplainsEachFile(t *testing.T) {
	e, dir := setup(t, "a.png", "alpha", "b.png", "beta")
	e.B.AddFile(remote+"/a.png", []byte("alpha"))
	r := e.Must(t, "outbox", "status", dir, "--to", remote, "--settle", "0", "-o", "json")
	var rows []map[string]any
	if err := json.Unmarshal([]byte(r.Stdout), &rows); err != nil {
		t.Fatal(err)
	}
	states := map[string]string{}
	for _, row := range rows {
		states[filepath.Base(row["local"].(string))] = row["state"].(string)
	}
	if states["a.png"] != "already up" || states["b.png"] != "waiting" {
		t.Errorf("states %v", states)
	}
}

func TestLinkPrintsTheURL(t *testing.T) {
	e, dir := setup(t, "a.png", "alpha")
	r := e.Must(t, "outbox", "push", dir, "--to", remote, "--settle", "0", "--link", "--endpoint", "https://box.example.org")
	if !strings.HasPrefix(r.Stdout, "https://box.example.org/s/") {
		t.Errorf("stdout %q, want the link", r.Stdout)
	}
}

func TestConfiguredFolders(t *testing.T) {
	e, dir := setup(t, "a.png", "alpha")
	if _, err := e.Run(t, nil, "outbox", "push"); err == nil || !strings.Contains(err.Error(), "[plugins.outbox]") {
		t.Errorf("push without config: %v", err)
	}
	old := configOf
	configOf = func(*cli.Runtime) (Config, error) {
		return Config{Folders: []Folder{{Local: dir, Remote: remote, After: "delete"}, {Local: filepath.Join(dir, "gone"), Remote: "/x"}}}, nil
	}
	t.Cleanup(func() { configOf = old })
	r := e.Must(t, "outbox", "push", "--settle", "0", "--after", "keep")
	if got := e.Content(t, remote+"/a.png"); got != "alpha" {
		t.Errorf("uploaded %q", got)
	}
	if !exists(filepath.Join(dir, "a.png")) {
		t.Error("--after keep did not override the config")
	}
	if !strings.Contains(r.Stderr, "skipping "+filepath.Join(dir, "gone")) {
		t.Errorf("stderr %q", r.Stderr)
	}
}

func TestRefusals(t *testing.T) {
	e, dir := setup(t)
	for name, args := range map[string][]string{
		"no destination": {"outbox", "push", dir},
		"bad layout":     {"outbox", "push", dir, "--to", remote, "--layout", "tree"},
		"bad after":      {"outbox", "push", dir, "--to", remote, "--after", "burn"},
		"to alone":       {"outbox", "push", "--to", remote},
		// No terminal and no credential that works without one.
		"watch unattended": {"outbox", "watch", dir, "--to", remote},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("KRB5CCNAME", filepath.Join(t.TempDir(), "none"))
			_, err := e.Run(t, nil, args...)
			if _, ok := errors.AsType[*cli.UsageError](err); !ok {
				t.Errorf("%v: %v, want a usage error", args, err)
			}
		})
	}
	if _, err := e.Run(t, nil, "outbox", "push", filepath.Join(dir, "missing"), "--to", remote); err == nil {
		t.Error("a folder of the command line that is not there: no error")
	}
}

func TestWatchUploadsOnArrival(t *testing.T) {
	e, dir := setup(t, "before.png", "1")
	run := e.Start(t, nil, "outbox", "watch", dir, "--to", remote, "--settle", "0", "--token", "t", "--timeout", "4s")
	done := make(chan error, 1)
	go func() {
		_, err := run()
		done <- err
	}()
	deadline := time.Now().Add(3 * time.Second)
	waitFor := func(name string) {
		t.Helper()
		for {
			if _, ok := e.B.Content(remote + "/" + name); ok {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s did not go up", name)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	waitFor("before.png")
	write(t, filepath.Join(dir, "after.png"), "2")
	waitFor("after.png")
	if err := <-done; err != nil {
		t.Errorf("watch: %v", err)
	}
}

func TestHookRewritesInPlaceAndRunsOnce(t *testing.T) {
	e, dir := setup(t, "a.png", "big original")
	log := filepath.Join(t.TempDir(), "calls.log")
	script := plugintest.Script(t, "echo \"$1|$CERNBOX_OUTBOX_REMOTE\" >> '"+log+"'\nprintf small > \"$1\"")
	for range 2 {
		e.Must(t, "outbox", "push", dir, "--to", remote, "--settle", "0", "--exec", script)
	}
	if got := e.Content(t, remote+"/a.png"); got != "small" {
		t.Errorf("uploaded %q, want what the hook left", got)
	}
	// The second pass finds the rewritten file up, before the hook.
	if calls := plugintest.Calls(t, log); len(calls) != 1 || calls[0] != filepath.Join(dir, "a.png")+"|"+remote+"/a.png" {
		t.Errorf("hook calls %q", calls)
	}
}

func TestHookFailureUploadsNothing(t *testing.T) {
	e, dir := setup(t, "a.png", "alpha")
	failing, _ := plugintest.RecordingHook(t, "1")
	r := e.Must(t, "outbox", "push", dir, "--to", remote, "--settle", "0", "--after", "delete", "--exec", failing)
	if _, ok := e.B.Content(remote + "/a.png"); ok {
		t.Error("uploaded although the hook failed")
	}
	if !exists(filepath.Join(dir, "a.png")) {
		t.Error("deleted although the hook failed")
	}
	if !strings.Contains(r.Stderr, "so it was not uploaded") {
		t.Errorf("stderr %q", r.Stderr)
	}
}

func TestHookRenameIsNotFollowed(t *testing.T) {
	e, dir := setup(t, "a.png", "alpha")
	script := plugintest.Script(t, "mv \"$1\" \"$1.done\"")
	r := e.Must(t, "outbox", "push", dir, "--to", remote, "--settle", "0", "--exec", script)
	if !strings.Contains(r.Stderr, "a rename is not followed") {
		t.Errorf("stderr %q", r.Stderr)
	}
	if _, ok := e.B.Content(remote + "/a.png"); ok {
		t.Error("something was uploaded")
	}
}

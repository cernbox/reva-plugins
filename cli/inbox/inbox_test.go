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

package inbox

import (
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cernbox/reva-plugins/cli/internal/plugintest"
	"github.com/cernbox/reva-plugins/cli/internal/userplane"
	"github.com/cs3org/reva/v3/pkg/cli"
)

const remote = "/incoming"

func setup(t *testing.T, files map[string]string) (*plugintest.Env, string) {
	t.Helper()
	e := plugintest.New(t, "alice", New)
	e.B.AddFile(remote+"/.keep", nil)
	for _, name := range slices.Sorted(maps.Keys(files)) {
		e.B.AddFile(remote+"/"+name, []byte(files[name]))
	}
	return e, t.TempDir()
}

func names(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

func readLocal(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestPullDownloadsWhatIsThere(t *testing.T) {
	e, local := setup(t, map[string]string{"a.txt": "alpha", "b.txt": "beta"})
	r := e.Must(t, "inbox", "pull", remote, "--to", local, "--settle", "0")
	if got := names(t, local); !slices.Equal(got, []string{"a.txt", "b.txt"}) {
		t.Errorf("collected %v", got)
	}
	if !strings.Contains(r.Stderr, "Downloaded 2 files") {
		t.Errorf("stderr %q", r.Stderr)
	}
	if got := e.Content(t, remote+"/a.txt"); got != "alpha" {
		t.Errorf("remote copy %q", got)
	}
	if r := e.Must(t, "inbox", "pull", remote, "--to", local, "--settle", "0"); !strings.Contains(r.Stderr, "2 files already here") {
		t.Errorf("second pass: %q", r.Stderr)
	}
}

// The settle test uses the clock of the server: the fake clock advances one
// second per change, so the last file is fresh and the first is not.
func TestPullWaitsForAFileStillArriving(t *testing.T) {
	e, local := setup(t, map[string]string{"a-old.txt": "1", "b-new.txt": "2"})
	r := e.Must(t, "inbox", "pull", remote, "--to", local, "--settle", "1s")
	if got := names(t, local); !slices.Equal(got, []string{"a-old.txt"}) {
		t.Errorf("collected %v, want only the settled file", got)
	}
	if !strings.Contains(r.Stderr, "1 file still arriving") {
		t.Errorf("stderr %q", r.Stderr)
	}
	r = e.Must(t, "inbox", "status", remote, "--to", local, "--settle", "1s", "-o", "json")
	var rows []map[string]any
	if err := json.Unmarshal([]byte(r.Stdout), &rows); err != nil {
		t.Fatal(err)
	}
	states := map[string]string{}
	for _, row := range rows {
		states[filepath.Base(row["remote"].(string))] = row["state"].(string)
	}
	if states["a-old.txt"] != "already here" || states["b-new.txt"] != "still arriving" {
		t.Errorf("states %v", states)
	}
}

func TestAfterMoveAndDelete(t *testing.T) {
	e, local := setup(t, map[string]string{"a.txt": "alpha"})
	e.Must(t, "inbox", "pull", remote, "--to", local, "--settle", "0", "--after", "move")
	if _, ok := e.B.Content(remote + "/a.txt"); ok {
		t.Error("move left the CERNBox copy")
	}
	if got := e.Content(t, remote+"/"+collectedDir+"/a.txt"); got != "alpha" {
		t.Errorf("moved copy %q", got)
	}

	e.B.AddFile(remote+"/b.txt", []byte("beta"))
	e.Must(t, "inbox", "pull", remote, "--to", local, "--settle", "0", "--after", "delete")
	if _, ok := e.B.Content(remote + "/b.txt"); ok {
		t.Error("delete left the CERNBox copy")
	}
	if got := readLocal(t, filepath.Join(local, "b.txt")); got != "beta" {
		t.Errorf("local %q", got)
	}
}

func TestDateLayoutUsesTheTimestampOfTheFile(t *testing.T) {
	e, local := setup(t, map[string]string{"a.txt": "alpha"})
	e.Must(t, "inbox", "pull", remote, "--to", local, "--settle", "0", "--layout", "date")
	info, err := e.B.Files().Stat(t.Context(), userplane.Ref(remote+"/a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	d := time.Unix(int64(info.Mtime.Seconds), 0)
	want := filepath.Join(local, d.Format("2006"), d.Format("01"), d.Format("02"), "a.txt")
	if got := readLocal(t, want); got != "alpha" {
		t.Errorf("%s: %q", want, got)
	}
}

func TestNeverWritesOverSomethingElse(t *testing.T) {
	e, local := setup(t, map[string]string{"a.txt": "remote"})
	if err := os.WriteFile(filepath.Join(local, "a.txt"), []byte("mine, longer"), 0o600); err != nil {
		t.Fatal(err)
	}
	e.Must(t, "inbox", "pull", remote, "--to", local, "--settle", "0")
	if got := readLocal(t, filepath.Join(local, "a.txt")); got != "mine, longer" {
		t.Errorf("the local file became %q", got)
	}
	if got := readLocal(t, filepath.Join(local, "a (2).txt")); got != "remote" {
		t.Errorf("the arrival is %q", got)
	}
}

func TestSkipsWhatIsNoFinishedFile(t *testing.T) {
	e, local := setup(t, map[string]string{"good.txt": "y", "half.part": "n", "backup~": "n"})
	e.B.AddFile(remote+"/sub/x.txt", []byte("n"))
	e.Must(t, "inbox", "pull", remote, "--to", local, "--settle", "0")
	if got := names(t, local); !slices.Equal(got, []string{"good.txt"}) {
		t.Errorf("collected %v", got)
	}
	if r := e.Must(t, "inbox", "status", remote, "--to", local, "--settle", "0"); !strings.Contains(r.Stderr, "1 subdirectory") {
		t.Errorf("status does not count the subfolder: %q", r.Stderr)
	}
}

func TestConfiguredFolders(t *testing.T) {
	e, local := setup(t, map[string]string{"a.txt": "alpha"})
	if _, err := e.Run(t, nil, "inbox", "pull"); err == nil || !strings.Contains(err.Error(), "[plugins.inbox]") {
		t.Errorf("pull without config: %v, want the config section named", err)
	}
	withConfig(t, Config{Folders: []Folder{
		{Remote: remote, Local: local, After: "move"},
		{Remote: "/gone", Local: filepath.Join(local, "gone")},
	}})
	r := e.Must(t, "inbox", "pull", "--settle", "0", "--after", "keep")
	if got := readLocal(t, filepath.Join(local, "a.txt")); got != "alpha" {
		t.Errorf("collected %q", got)
	}
	if _, ok := e.B.Content(remote + "/a.txt"); !ok {
		t.Error("--after keep did not override the config")
	}
	if !strings.Contains(r.Stderr, "skipping /gone") {
		t.Errorf("stderr %q", r.Stderr)
	}
	if _, err := e.Run(t, nil, "inbox", "pull", "--to", local); err == nil {
		t.Error("--to without a folder: no error")
	}
}

func withConfig(t *testing.T, c Config) {
	old := configOf
	configOf = func(*cli.Runtime) (Config, error) { return c, nil }
	t.Cleanup(func() { configOf = old })
}

func TestConfigFile(t *testing.T) {
	e, local := setup(t, map[string]string{"a.txt": "alpha"})
	e.WriteConfig(t, "[[plugins.inbox.folders]]\nremote = \""+remote+"\"\nlocal = \""+local+"\"\n")
	e.Must(t, "inbox", "pull", "--settle", "0")
	if got := readLocal(t, filepath.Join(local, "a.txt")); got != "alpha" {
		t.Errorf("collected %q", got)
	}
	e.WriteConfig(t, "[plugins.inbox]\nfolder = []\n")
	if _, err := e.Run(t, nil, "inbox", "pull"); err == nil || !strings.Contains(err.Error(), "folder") {
		t.Errorf("a typo in the section: %v", err)
	}
}

func TestRefusals(t *testing.T) {
	e, local := setup(t, nil)
	for name, args := range map[string][]string{
		"no destination": {"inbox", "pull", remote},
		"bad layout":     {"inbox", "pull", remote, "--to", local, "--layout", "tree"},
		"bad after":      {"inbox", "pull", remote, "--to", local, "--after", "burn"},
		"bad interval":   {"inbox", "watch", remote, "--to", local, "--interval", "0s"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := e.Run(t, nil, args...)
			if _, ok := errors.AsType[*cli.UsageError](err); !ok {
				t.Errorf("%v: %v, want a usage error", args, err)
			}
		})
	}
}

func TestHookRunsForEachFileCollected(t *testing.T) {
	e, local := setup(t, map[string]string{"m.csv": "1,2,3"})
	script, log := plugintest.RecordingHook(t, "0", "CERNBOX_INBOX_NAME", "CERNBOX_INBOX_SIZE", "CERNBOX_INBOX_REMOTE")
	for range 3 {
		e.Must(t, "inbox", "pull", remote, "--to", local, "--settle", "0", "--exec", script)
	}
	// With keep, nothing tells that the hook ran, so it runs only on the
	// download.
	calls := plugintest.Calls(t, log)
	want := filepath.Join(local, "m.csv") + "|m.csv|5|" + remote + "/m.csv"
	if len(calls) != 1 || calls[0] != want {
		t.Errorf("hook calls %q, want once %q", calls, want)
	}
}

func TestHookFailureLeavesTheFileInTheInbox(t *testing.T) {
	e, local := setup(t, map[string]string{"m.csv": "1,2,3"})
	failing, _ := plugintest.RecordingHook(t, "3")
	r := e.Must(t, "inbox", "pull", remote, "--to", local, "--settle", "0", "--after", "move", "--exec", failing)
	if !strings.Contains(r.Stderr, "exited with status 3, so it was left in the inbox") {
		t.Errorf("stderr %q", r.Stderr)
	}
	if _, ok := e.B.Content(remote + "/m.csv"); !ok {
		t.Fatal("the copy was moved although the hook failed")
	}
	working, log := plugintest.RecordingHook(t, "0")
	e.Must(t, "inbox", "pull", remote, "--to", local, "--settle", "0", "--after", "move", "--exec", working)
	if calls := plugintest.Calls(t, log); len(calls) != 1 {
		t.Errorf("the retry ran the hook %d times", len(calls))
	}
	if _, ok := e.B.Content(remote + "/" + collectedDir + "/m.csv"); !ok {
		t.Error("the retry did not move the copy")
	}
}

func TestHookGetsNoShell(t *testing.T) {
	const nasty = "safe.txt; touch pwned"
	e, local := setup(t, map[string]string{nasty: "x"})
	script, log := plugintest.RecordingHook(t, "0", "CERNBOX_INBOX_NAME")
	e.Must(t, "inbox", "pull", remote, "--to", local, "--settle", "0", "--exec", script)
	if calls := plugintest.Calls(t, log); len(calls) != 1 || calls[0] != filepath.Join(local, nasty)+"|"+nasty {
		t.Errorf("hook calls %q", calls)
	}
}

func TestHookIsKilledWhenItHangs(t *testing.T) {
	e, local := setup(t, map[string]string{"m.csv": "1"})
	script := plugintest.Script(t, "sleep 60")
	start := time.Now()
	r := e.Must(t, "inbox", "pull", remote, "--to", local, "--settle", "0", "--after", "move", "--exec", script, "--exec-timeout", "300ms")
	if time.Since(start) > 20*time.Second {
		t.Errorf("waited %s for a hook of 300ms", time.Since(start))
	}
	if !strings.Contains(r.Stderr, "did not finish") {
		t.Errorf("stderr %q", r.Stderr)
	}
	if _, ok := e.B.Content(remote + "/m.csv"); !ok {
		t.Error("the copy was moved although the hook never finished")
	}
}

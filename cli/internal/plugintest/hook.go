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

package plugintest

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// Script writes a shell script with body and returns its path. It skips
// the test where there is no /bin/sh.
func Script(t *testing.T, body string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("the hooks of the tests are shell scripts")
	}
	p := filepath.Join(t.TempDir(), "hook.sh")
	if err := os.WriteFile(p, []byte("#!/bin/sh\n"+body+"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	return p
}

// RecordingHook is a hook that appends a line per call to its log, with
// "$1|" followed by the env names in vars, and exits with code.
func RecordingHook(t *testing.T, code string, vars ...string) (script, log string) {
	t.Helper()
	log = filepath.Join(t.TempDir(), "calls.log")
	var line strings.Builder
	line.WriteString(`"$1`)
	for _, v := range vars {
		line.WriteString("|$" + v)
	}
	line.WriteString(`"`)
	return Script(t, "printf '%s\\n' "+line.String()+" >> '"+log+"'\nexit "+code), log
}

// Calls returns the lines of the log of a RecordingHook.
func Calls(t *testing.T, log string) []string {
	t.Helper()
	b, err := os.ReadFile(log)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(b)), "\n")
}

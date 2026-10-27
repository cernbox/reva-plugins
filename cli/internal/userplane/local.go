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

package userplane

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/cs3org/reva/v3/pkg/cli"
	"github.com/cs3org/reva/v3/pkg/cli/kerberos"
)

// ExpandHome expands a leading ~ to the home directory.
func ExpandHome(p string) string {
	if p != "~" && !strings.HasPrefix(p, "~/") && !strings.HasPrefix(p, `~\`) {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return p
	}
	return filepath.Join(home, p[1:])
}

// PathTaken reports whether something exists at the local path p.
func PathTaken(p string) bool {
	_, err := os.Lstat(p)
	return err == nil
}

// FreeLocalName returns a name next to p that nothing uses: "a (2).txt".
func FreeLocalName(p string) string {
	ext := filepath.Ext(p)
	stem := strings.TrimSuffix(p, ext)
	for n := 2; n < 1000; n++ {
		if c := fmt.Sprintf("%s (%d)%s", stem, n, ext); !PathTaken(c) {
			return c
		}
	}
	return p
}

// SkipName reports the names that are no finished file: hidden files,
// editor backups and the partial files of browsers and transfers.
func SkipName(name string) bool {
	if strings.HasPrefix(name, ".") || strings.HasSuffix(name, "~") {
		return true
	}
	lower := strings.ToLower(name)
	for _, ext := range []string{".part", ".crdownload", ".tmp", ".partial", ".download", ".swp"} {
		if strings.HasSuffix(lower, ext) {
			return true
		}
	}
	return false
}

// DefaultExecTimeout bounds a hook: one that hangs would stop an unattended
// run without a sign.
const DefaultExecTimeout = 5 * time.Minute

// hookWaitDelay is how long Wait waits for the output pipes after the kill:
// a grandchild keeps them open after its parent is gone.
const hookWaitDelay = time.Second

// RunHook runs command with path as its last argument, and env added to
// the environment. It never uses a shell: a stranger can choose the file
// names, with an upload link. The output goes to stderr, not into -o json.
func RunHook(ctx context.Context, stderr io.Writer, command []string, timeout time.Duration, path string, env []string) error {
	if len(command) == 0 {
		return nil
	}
	if timeout <= 0 {
		timeout = DefaultExecTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	c := exec.CommandContext(ctx, command[0], append(command[1:len(command):len(command)], path)...)
	c.Stdout, c.Stderr = stderr, stderr
	c.WaitDelay = hookWaitDelay
	c.Env = append(os.Environ(), env...)
	err := c.Run()
	switch {
	case err == nil:
		return nil
	case ctx.Err() != nil:
		return fmt.Errorf("%s did not finish within %s", command[0], timeout)
	}
	// A hook that could not start needs a fix. One that exited non-zero said no.
	if exit, ok := errors.AsType[*exec.ExitError](err); ok {
		return fmt.Errorf("%s exited with status %d", command[0], exit.ExitCode())
	}
	return fmt.Errorf("running %s: %w", command[0], err)
}

// CheckUnattended refuses to start a watch that cannot sign in without a
// person: the device flow waits for somebody to open a URL, which never
// happens in a service.
func CheckUnattended(rt *cli.Runtime) error {
	if isTerminal(os.Stdin) && isTerminal(os.Stderr) {
		return nil
	}
	for _, key := range []string{"token", "app_token", "app_token_file", "password"} {
		if v, _ := rt.Setting(key); v != "" {
			return nil
		}
	}
	if rt.AppTokenFile() != "" || rt.Password() != "" {
		return nil
	}
	if _, err := (&kerberos.Provider{}).Ticket(); err == nil {
		return nil
	}
	names := rt.Brand().Env.Names("app_token")
	where := "an app token"
	if len(names) > 0 {
		where += " in " + names[0]
	}
	return cli.Usagef("watching needs a credential that works without a terminal: a Kerberos ticket, or %s", where)
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

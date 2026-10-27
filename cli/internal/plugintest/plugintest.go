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

// Package plugintest runs the user-plane commands of the CLI plugins on the
// fake backend of pkg/cli/clitest. revacmd.NewRoot builds its own runtime,
// so a test cannot give it the fake: Env builds a root with the global
// flags and the hook of the core root, and the commands of the plugin.
package plugintest

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cs3org/reva/v3/pkg/cli"
	"github.com/cs3org/reva/v3/pkg/cli/brand"
	"github.com/cs3org/reva/v3/pkg/cli/clitest"
	"github.com/spf13/cobra"
)

// testBrand has the name of the cernbox build and nothing else of it.
var testBrand = brand.MustParse([]byte("[program]\nname = \"cernbox\"\n"))

// Env is a home and a config dir of their own, and a fake server.
type Env struct {
	B *clitest.Backend
	// Config is the user config file.
	Config string
	cmds   []func(*cli.Runtime) *cobra.Command
}

// New returns an env signed in as user, for the commands of cmds.
func New(t *testing.T, user string, cmds ...func(*cli.Runtime) *cobra.Command) *Env {
	t.Helper()
	cfg := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", cfg)
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	cobra.EnableTraverseRunHooks = true
	return &Env{B: clitest.New(user), Config: filepath.Join(cfg, "cernbox", "config.toml"), cmds: cmds}
}

// WriteConfig writes the user config.
func (e *Env) WriteConfig(t *testing.T, toml string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(e.Config), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(e.Config, []byte(toml), 0o600); err != nil {
		t.Fatal(err)
	}
}

// Result is what a run printed.
type Result struct {
	Stdout, Stderr string
}

// Run runs args with stdin in, and returns stdout and stderr.
func (e *Env) Run(t *testing.T, in io.Reader, args ...string) (Result, error) {
	t.Helper()
	return e.Start(t, in, args...)()
}

// Start builds the command line args, and returns the function that runs
// it, so that a test can run two at once: the runtime is built here, on the
// goroutine of the test.
func (e *Env) Start(t *testing.T, in io.Reader, args ...string) func() (Result, error) {
	t.Helper()
	rt := cli.NewRuntime(testBrand)
	rt.UseBackend(e.B, e.B.Data())
	root := &cobra.Command{
		Use:           "cernbox",
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			return rt.Load(cmd.Flags())
		},
	}
	rt.AddFlags(root.PersistentFlags())
	root.AddGroup(&cobra.Group{ID: "files", Title: "Files:"})
	for _, c := range e.cmds {
		root.AddCommand(c(rt))
	}
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	if in == nil {
		in = strings.NewReader("")
	}
	root.SetIn(in)
	root.SetArgs(args)
	return func() (Result, error) {
		err := root.Execute()
		return Result{out.String(), errOut.String()}, err
	}
}

// Must runs args and fails the test on an error.
func (e *Env) Must(t *testing.T, args ...string) Result {
	t.Helper()
	r, err := e.Run(t, nil, args...)
	if err != nil {
		t.Fatalf("%s: %v\nstdout:\n%s\nstderr:\n%s", strings.Join(args, " "), err, r.Stdout, r.Stderr)
	}
	return r
}

// Content returns the content of the remote file p, and fails the test
// when there is none.
func (e *Env) Content(t *testing.T, p string) string {
	t.Helper()
	b, ok := e.B.Content(p)
	if !ok {
		t.Fatalf("no remote file %s", p)
	}
	return string(b)
}

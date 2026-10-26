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

package cli_test

import (
	"bytes"
	"strings"
	"testing"

	_ "github.com/cernbox/reva-plugins/cli"
	"github.com/cs3org/reva/v3"
	revacmd "github.com/cs3org/reva/v3/cmd/reva/app"
)

// A defect of a plugin (a collision, a missing parent) only gives a warning
// when the CLI starts, so this test is what fails the build.
func TestCheckPlugins(t *testing.T) {
	if err := revacmd.CheckPlugins(revacmd.NewRoot()); err != nil {
		t.Fatal(err)
	}
}

func TestVersionListsThePlugins(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	root := revacmd.NewRoot()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs([]string{"version", "--plugins"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"cli.commands.otg", "github.com/cernbox/reva-plugins/cli/otg",
		"cli.commands.user.account", "github.com/cernbox/reva-plugins/cli/user",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("no %q in:\n%s", want, out.String())
		}
	}
}

// The CLI must not link the backend plugins.
func TestOnlyCLIPlugins(t *testing.T) {
	for _, p := range reva.GetPlugins("") {
		if !strings.HasPrefix(string(p.ID), "cli.") {
			t.Errorf("the CLI links the backend plugin %s", p.ID)
		}
	}
}

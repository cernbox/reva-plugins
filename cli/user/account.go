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

// Package user is the user account CLI plugin. It shows a CERN account from
// Grappa.
package user

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/cernbox/reva-plugins/user/rest/api"
	"github.com/cs3org/reva/v3/pkg/cli"
	"github.com/spf13/cobra"
)

func init() { cli.RegisterCommand("user account", New) }

// New returns the command `user account`.
func New(rt *cli.Runtime) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "account <user>",
		Short: "Show the CERN account of a user: type, owner and blocking state",
		Long: "Show the CERN account of a user, as Grappa knows it: the account type, the owner of a\n" +
			"secondary, service or application account, the department and the blocking state.\n" +
			"<user> is a username, or the email of a lightweight account.",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE: func(cmd *cobra.Command, args []string) error {
			var a api.Account
			if err := rt.Invoke(cmd.Context(), api.Target, api.Invocation, map[string]string{api.ArgUser: args[0]}, &a); err != nil {
				return err
			}
			return rt.Print(cmd.OutOrStdout(), cli.Table{Headers: []string{"Field", "Value"}, Rows: rows(&a), Items: a}, nil)
		},
	}
	return cli.AdminCommand(cmd)
}

// rows skips the fields that Grappa has no value for. The account type, the
// owner and the state are always there.
func rows(a *api.Account) [][]string {
	var r [][]string
	add := func(k, v string, always bool) {
		if v != "" || always {
			r = append(r, []string{k, cmp.Or(v, "-")})
		}
	}
	add("Username", a.Username, true)
	add("Name", a.DisplayName, false)
	add("Type", a.Type, true)
	add("Source", a.Source, false)
	add("Mail", a.Mail, false)
	if a.UIDNumber != 0 || a.GIDNumber != 0 {
		add("UID/GID", fmt.Sprintf("%d/%d", a.UIDNumber, a.GIDNumber), false)
	}
	add("Unit", join("-", a.Department, a.Group, a.Section), false)
	add("Phones", join(", ", a.Phone, a.Phone2, a.Mobile), false)
	add("Owner", owner(a.Owner), true)
	if a.Owner != nil {
		add("Owner state", state(a.Owner), true)
	}
	add("Active", yesNo(a.Active), true)
	add("Disabled", yesNo(a.Disabled), true)
	add("Blocked", yesNo(a.Blocked), true)
	add("Blocking reason", a.BlockingReason, false)
	add("Blocked since", date(a.BlockingTime), false)
	add("Blocking deadline", date(a.BlockingDeadline), false)
	return r
}

func owner(o *api.Account) string {
	if o == nil {
		return ""
	}
	if d := join(", ", o.DisplayName, o.Mail); d != "" {
		return fmt.Sprintf("%s (%s)", o.Username, d)
	}
	return o.Username
}

// state tells whether the owner can still answer for the account.
func state(o *api.Account) string {
	var s []string
	if !o.Active {
		s = append(s, "not active")
	}
	if o.Disabled {
		s = append(s, "disabled")
	}
	if o.Blocked {
		b := "blocked"
		if o.BlockingReason != "" {
			b += ": " + o.BlockingReason
		}
		s = append(s, b)
	}
	if len(s) == 0 {
		return "active"
	}
	return strings.Join(s, ", ")
}

func join(sep string, parts ...string) string {
	return strings.Join(slices.DeleteFunc(parts, func(p string) bool { return p == "" }), sep)
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func date(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Local().Format(time.DateTime + " MST")
}

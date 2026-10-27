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

// Package otg is the otg CLI plugin. It shows, sets and clears the OTG
// message of CERNBox.
package otg

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cernbox/reva-plugins/otg/api"
	"github.com/cs3org/reva/v3/pkg/cli"
	"github.com/spf13/cobra"
)

func init() { cli.RegisterCommand("otg", New) }

// New returns the command `otg`.
func New(rt *cli.Runtime) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "otg",
		Short:   "Show and set the outage (OTG) message that users see",
		GroupID: "fleet",
	}
	cli.AdminCommand(cmd)
	cmd.AddCommand(getCmd(rt), setCmd(rt), clearCmd(rt))
	return cmd
}

func getCmd(rt *cli.Runtime) *cobra.Command {
	return &cobra.Command{
		Use:               "get",
		Short:             "Show the OTG message, if there is one",
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return invoke(cmd, rt, api.Get, nil)
		},
	}
}

func setCmd(rt *cli.Runtime) *cobra.Command {
	var number, message, file string
	cmd := &cobra.Command{
		Use:   "set --number <OTG number> (--message <html> | --message-file <file>)",
		Short: "Replace the OTG message",
		Long: "Replace the OTG message that users see. The message can contain HTML.\n" +
			"Quote a message with spaces, or give it in a file (- for stdin).",
		Example:           `  cernbox otg set --number OTG0060602 --message 'EOS is <b>slow</b>, see the SSB'`,
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if file != "" {
				b, err := readFile(cmd, file)
				if err != nil {
					return err
				}
				message = string(b)
			}
			number, message = strings.TrimSpace(number), strings.TrimSpace(message)
			if number == "" || message == "" {
				return errors.New("the OTG number and the message must not be empty")
			}
			if err := rt.Confirm(fmt.Sprintf("show %s to all users: %s", number, preview(message))); err != nil {
				return err
			}
			return invoke(cmd, rt, api.Set, map[string]string{api.ArgNumber: number, api.ArgMessage: message})
		},
	}
	f := cmd.Flags()
	f.StringVar(&number, "number", "", "the OTG number from the CERN SSB, for example OTG0060602")
	f.StringVar(&message, "message", "", "the message, HTML allowed")
	f.StringVar(&file, "message-file", "", "read the message from this file, - for stdin")
	_ = cmd.MarkFlagRequired("number")
	_ = cmd.RegisterFlagCompletionFunc("number", cobra.NoFileCompletions)
	_ = cmd.RegisterFlagCompletionFunc("message", cobra.NoFileCompletions)
	cmd.MarkFlagsOneRequired("message", "message-file")
	cmd.MarkFlagsMutuallyExclusive("message", "message-file")
	return cli.Mutates(cmd)
}

func clearCmd(rt *cli.Runtime) *cobra.Command {
	cmd := &cobra.Command{
		Use:               "clear",
		Short:             "Remove the OTG message",
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := rt.Confirm("remove the OTG message for all users"); err != nil {
				return err
			}
			return invoke(cmd, rt, api.Clear, nil)
		},
	}
	return cli.Mutates(cmd)
}

func invoke(cmd *cobra.Command, rt *cli.Runtime, invocation string, args map[string]string) error {
	var st api.Status
	if err := rt.Invoke(cmd.Context(), api.Service, invocation, args, &st); err != nil {
		return err
	}
	return rt.Print(cmd.OutOrStdout(), st, func(w io.Writer) {
		if !st.Present {
			fmt.Fprintln(w, "No OTG message.")
			return
		}
		cli.Table{Headers: []string{"Number", "Message"}, Rows: [][]string{{st.Number, st.Message}}}.Write(w)
	})
}

func readFile(cmd *cobra.Command, name string) ([]byte, error) {
	if name == "-" {
		return io.ReadAll(cmd.InOrStdin())
	}
	return os.ReadFile(name)
}

// preview puts the message on one short line for the confirmation.
func preview(msg string) string {
	const limit = 80
	msg = strings.Join(strings.Fields(msg), " ")
	if r := []rune(msg); len(r) > limit {
		return string(r[:limit]) + "..."
	}
	return msg
}

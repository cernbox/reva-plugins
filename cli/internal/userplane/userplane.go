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

// Package userplane has what the user-plane commands of the cernbox CLI
// (clipboard, inbox, outbox) share.
package userplane

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path"
	"strings"
	"time"

	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/v3/pkg/cli"
	"github.com/cs3org/reva/v3/pkg/cli/backend"
	"github.com/cs3org/reva/v3/pkg/cli/data/transfer"
	"github.com/cs3org/reva/v3/pkg/cli/output"
	"github.com/spf13/cobra"
)

// Session is one run of a command on the user plane.
type Session struct {
	RT   *cli.Runtime
	Cmd  *cobra.Command
	B    backend.Backend
	D    backend.DataPlane
	home string
}

// Open connects the command cmd to the user plane of rt.
func Open(cmd *cobra.Command, rt *cli.Runtime) (*Session, error) {
	ctx := cmd.Context()
	b, err := rt.Backend(ctx)
	if err != nil {
		return nil, err
	}
	d, err := rt.Data(ctx)
	if err != nil {
		return nil, err
	}
	return &Session{RT: rt, Cmd: cmd, B: b, D: d}, nil
}

// Msg writes a message for a person to stderr. -q and -o json hide it.
func (s *Session) Msg(format string, args ...any) {
	s.RT.Msg(s.Cmd.ErrOrStderr(), format, args...)
}

// Warn writes a warning to stderr, which -q does not hide.
func (s *Session) Warn(format string, args ...any) {
	fmt.Fprintf(s.Cmd.ErrOrStderr(), s.RT.Name()+": "+format+"\n", args...)
}

// Line writes a line of the result to stdout.
func (s *Session) Line(format string, args ...any) {
	fmt.Fprintf(s.Cmd.OutOrStdout(), format+"\n", args...)
}

// JSON prints v with -o json, and nothing else otherwise: the messages
// already said it.
func (s *Session) JSON(v any) error {
	if s.RT.Output() != "json" {
		return nil
	}
	return s.RT.Print(s.Cmd.OutOrStdout(), v, func(io.Writer) {})
}

// Ref is the reference of the path p.
func Ref(p string) *provider.Reference { return &provider.Reference{Path: p} }

// Stat returns the resource at p.
func (s *Session) Stat(ctx context.Context, p string) (*provider.ResourceInfo, error) {
	return s.B.Files().Stat(ctx, Ref(p))
}

// List returns the contents of the folder p.
func (s *Session) List(ctx context.Context, p string) ([]*provider.ResourceInfo, error) {
	return s.B.Files().List(ctx, Ref(p))
}

// Mkdir creates the folder p and its parents. A folder that exists is fine.
func (s *Session) Mkdir(ctx context.Context, p string) error {
	return s.B.Files().Mkdir(ctx, Ref(p), true)
}

// RemoveIfPresent removes p, and reports whether there was something.
func (s *Session) RemoveIfPresent(ctx context.Context, p string) (bool, error) {
	if _, err := s.Stat(ctx, p); err != nil {
		if cli.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	err := s.B.Files().Remove(ctx, Ref(p))
	if cli.IsNotFound(err) {
		return false, nil
	}
	return err == nil, err
}

// FreeRemoteName returns a name next to p that nothing uses: "a (2).txt".
func (s *Session) FreeRemoteName(ctx context.Context, p string) (string, error) {
	ext := path.Ext(p)
	stem := strings.TrimSuffix(p, ext)
	for n := 2; n < 1000; n++ {
		candidate := fmt.Sprintf("%s (%d)%s", stem, n, ext)
		if _, err := s.Stat(ctx, candidate); err != nil {
			if cli.IsNotFound(err) {
				return candidate, nil
			}
			return "", err
		}
	}
	return "", fmt.Errorf("%s: a thousand files already have this name", p)
}

// PutBytes writes b to p. It can be retried, because b is still there.
func (s *Session) PutBytes(ctx context.Context, p string, b []byte) error {
	return s.D.Upload(ctx, Ref(p), func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(b)), nil
	}, int64(len(b)), "")
}

// Download returns the content of p, and its size or -1.
func (s *Session) Download(ctx context.Context, p string) (io.ReadCloser, int64, error) {
	return s.D.Download(ctx, Ref(p), 0)
}

// Home returns the path of the home space of the caller.
func (s *Session) Home(ctx context.Context) (string, error) {
	if s.home != "" {
		return s.home, nil
	}
	ref, err := s.B.Files().ResolveSpace(ctx, "home")
	if err != nil {
		return "", err
	}
	if ref.GetPath() == "" {
		return "", errors.New("the home space has no path")
	}
	s.home = ref.GetPath()
	return s.home, nil
}

// Username returns the user name of the caller, or "" if it cannot be read.
func (s *Session) Username(ctx context.Context) string {
	u, err := s.B.Users().Me(ctx)
	if err != nil {
		return ""
	}
	return u.GetUsername()
}

// Engine returns the transfer engine of the runtime.
func (s *Session) Engine(ctx context.Context, o cli.TransferOptions) (*transfer.Engine, error) {
	return s.RT.Engine(ctx, o)
}

// IsDir reports whether info is a folder.
func IsDir(info *provider.ResourceInfo) bool {
	return info.GetType() == provider.ResourceType_RESOURCE_TYPE_CONTAINER
}

// Name is the last element of the path of info.
func Name(info *provider.ResourceInfo) string { return path.Base(info.GetPath()) }

// Mtime is the modification time of info.
func Mtime(info *provider.ResourceInfo) time.Time {
	ts := info.GetMtime()
	return time.Unix(int64(ts.GetSeconds()), int64(ts.GetNanos()))
}

// IsConflict reports whether err says that the target changed or exists.
func IsConflict(err error) bool {
	se, ok := errors.AsType[*cli.StatusError](err)
	return ok && (se.Code == rpc.Code_CODE_FAILED_PRECONDITION || se.Code == rpc.Code_CODE_ALREADY_EXISTS ||
		se.Code == rpc.Code_CODE_ABORTED)
}

// Arg is a path argument of a command that moves data: remote when it has
// cb: or a space alias, else local, also /eos/..., which is a local mount
// on lxplus.
type Arg struct {
	Raw string
	// Path is the local path, or the absolute remote path.
	Path   string
	Remote bool
	// TrailingSlash is "into the folder", as for cp.
	TrailingSlash bool
}

// ParseArg classifies arg with the rules of cp. Runtime has no call for one
// argument, so it asks for a pair with the remote root on the other side,
// which needs no call to the server.
func ParseArg(ctx context.Context, rt *cli.Runtime, arg string) (Arg, error) {
	p, _, remote, _, _, err := rt.TransferPaths(ctx, arg, "cb:/")
	if err != nil {
		return Arg{}, err
	}
	trailing := len(arg) > 1 && (strings.HasSuffix(arg, "/") || strings.HasSuffix(arg, string(os.PathSeparator)))
	return Arg{Raw: arg, Path: p, Remote: remote, TrailingSlash: trailing}, nil
}

// Interruptible returns the context of cmd, which an interrupt cancels, so
// that a command that waits can clean up on Ctrl-C.
func Interruptible(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(cmd.Context(), os.Interrupt)
}

// ProseSize is a size in a sentence: output.HumanSize drops the unit below
// 1K, which is right in a column and not in "copied 1 item (21)".
func ProseSize(n int64) string {
	switch {
	case n == 1:
		return "1 byte"
	case n < 1024:
		return fmt.Sprintf("%d bytes", n)
	}
	return output.HumanSize(n)
}

// Plural is n with the word for one or for many.
func Plural(n int, one, many string) string {
	if n == 1 {
		return "1 " + one
	}
	return fmt.Sprintf("%d %s", n, many)
}

// ErrLine is the first line of err, for a warning on one line.
func ErrLine(err error) string {
	s, _, _ := strings.Cut(err.Error(), "\n")
	return s
}

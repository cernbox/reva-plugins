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

// Package inbox is the command inbox of the cernbox CLI: a CERNBox folder
// whose arrivals come down to a local one.
//
// It is the outbox the other way round, with two differences. It polls,
// because nothing tells a client that a remote folder changed. And it tests
// that a file has settled against the clock of the server, not of this
// machine, which can be wrong by the skew of the two clocks.
package inbox

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cernbox/reva-plugins/cli/internal/userplane"
	"github.com/cs3org/reva/v3/pkg/cli"
	"github.com/cs3org/reva/v3/pkg/cli/output"
	"github.com/spf13/cobra"
)

// Name is the name of the command and of its config section.
const Name = "inbox"

func init() { cli.RegisterCommand(Name, New) }

const (
	// defaultInterval is how often a watch looks: every pass is a request.
	defaultInterval = time.Minute
	defaultSettle   = 2 * time.Second
	collectedDir    = ".collected"
)

// Config is the section [plugins.inbox].
type Config struct {
	Folders []Folder `toml:"folders"`
}

// Folder is one CERNBox folder whose arrivals go to a local one.
type Folder struct {
	// Remote is the CERNBox folder to collect from.
	Remote string `toml:"remote"`
	// Local is where its files go. A leading ~ is expanded.
	Local string `toml:"local"`
	// Layout is flat, or date to file each arrival under YYYY/MM/DD of its
	// own timestamp.
	Layout string `toml:"layout"`
	// After is what happens to the CERNBox copy once it is down: keep (the
	// default, which cannot lose anything), move to .collected, or delete.
	After string `toml:"after"`
	// Exec is run for each file collected, with its local path as the last
	// argument. It is split on spaces and never given to a shell.
	Exec string `toml:"exec"`
}

// configOf is a variable so that tests can replace it.
var configOf = func(rt *cli.Runtime) (Config, error) {
	var c Config
	err := cli.PluginConfig(rt, Name, &c)
	return c, err
}

// New returns the command inbox.
func New(rt *cli.Runtime) *cobra.Command {
	cmd := &cobra.Command{
		Use:   Name,
		Short: "Download whatever appears in a CERNBox folder",
		Long: "Treat a CERNBox folder as an inbox: anything that turns up in it is\n" +
			"downloaded to a local folder.\n" +
			"\n" +
			"It is one way, always. Nothing local is changed except by adding to it, and\n" +
			"'cernbox sync' is still the command for keeping two sides matching.\n" +
			"\n" +
			"Folders are usually listed in the config file, under [plugins.inbox] folders,\n" +
			"each with its own destination, so that 'cernbox inbox pull' needs no\n" +
			"arguments. A folder can also be given on the command line with --to.",
		GroupID: "files",
	}
	cmd.AddCommand(pullCmd(rt), watchCmd(rt), statusCmd(rt))
	return cmd
}

type flags struct {
	to     string
	layout string
	after  string
	settle time.Duration
	exec   string
	execIn time.Duration
}

func (o *flags) register(cmd *cobra.Command) {
	f := cmd.Flags()
	f.StringVar(&o.to, "to", "", "local folder to download into")
	f.StringVar(&o.layout, "layout", "", "flat, or date to file each arrival under YYYY/MM/DD")
	f.StringVar(&o.after, "after", "", "what to do with the CERNBox copy once it is down: keep, move or delete")
	f.DurationVar(&o.settle, "settle", defaultSettle, "how long a file must sit unchanged before it counts as finished")
	f.StringVar(&o.exec, "exec", "", "command to run for each file collected, with its local path as the last argument")
	f.DurationVar(&o.execIn, "exec-timeout", userplane.DefaultExecTimeout, "how long --exec may take before it is killed")
}

type box struct{ *userplane.Session }

func open(cmd *cobra.Command, rt *cli.Runtime) (*box, error) {
	s, err := userplane.Open(cmd, rt)
	if err != nil {
		return nil, err
	}
	return &box{s}, nil
}

func (b *box) jobs(args []string, fl flags) ([]job, error) {
	if len(args) == 1 {
		if fl.to == "" {
			return nil, cli.Usagef("pass --to with the local folder to download %s into", args[0])
		}
		j, err := newJob(Folder{Remote: args[0], Local: fl.to, Layout: fl.layout, After: fl.after, Exec: fl.exec})
		if err != nil {
			return nil, err
		}
		j.execIn = fl.execIn
		return []job{j}, nil
	}
	if fl.to != "" {
		return nil, cli.Usagef("--to describes one folder, so name the folder it applies to")
	}
	c, err := configOf(b.RT)
	if err != nil {
		return nil, err
	}
	if len(c.Folders) == 0 {
		return nil, cli.Usagef("no inbox folders are configured. Name one with "+
			"'%s inbox pull REMOTE --to DIR', or list them under [plugins.inbox] folders in the config file", b.RT.Name())
	}
	out := make([]job, 0, len(c.Folders))
	for _, f := range c.Folders {
		// A flag overrides the config of every folder, so that --after keep is
		// a safety net over a config that says delete.
		f.Layout = cmp.Or(fl.layout, f.Layout)
		f.After = cmp.Or(fl.after, f.After)
		f.Exec = cmp.Or(fl.exec, f.Exec)
		j, err := newJob(f)
		if err != nil {
			return nil, err
		}
		j.configured = true
		j.execIn = fl.execIn
		out = append(out, j)
	}
	return out, nil
}

func pullCmd(rt *cli.Runtime) *cobra.Command {
	var fl flags
	cmd := &cobra.Command{
		Use:   "pull [REMOTE]",
		Short: "Download what is waiting, once",
		Long: "Download whatever is in the inbox folders and stop.\n\n" +
			"This is the form for a timer: it does the one pass and exits.",
		Example: "  cernbox inbox pull\n" +
			"  cernbox inbox pull /eos/project/c/cernbox/incoming --to ~/from-cernbox",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			b, err := open(cmd, rt)
			if err != nil {
				return err
			}
			jobs, err := b.jobs(args, fl)
			if err != nil {
				return err
			}
			var total result
			for _, j := range jobs {
				got, err := b.run(cmd.Context(), j, fl.settle)
				if err != nil {
					// A missing configured folder must not stop the others.
					if j.configured && cli.IsNotFound(err) {
						b.Warn("skipping %s: %v", j.remote, userplane.ErrLine(err))
						continue
					}
					return err
				}
				total.add(got)
			}
			total.report(b)
			return b.JSON(total.json())
		},
	}
	fl.register(cmd)
	return cli.Mutates(cmd)
}

func watchCmd(rt *cli.Runtime) *cobra.Command {
	var fl flags
	var interval time.Duration
	cmd := &cobra.Command{
		Use:   "watch [REMOTE]",
		Short: "Keep downloading as things appear",
		Long: "Watch the inbox folders and download what turns up.\n" +
			"\n" +
			"This asks on a timer, unlike 'outbox watch', which the file system tells\n" +
			"about a change. Nothing on the server announces one, so --interval is a\n" +
			"direct cost: one listing per folder per pass.\n" +
			"\n" +
			"For something that survives sleep, a lost network and a reboot, run\n" +
			"'inbox pull' from a timer instead, and give it an app token.",
		Example: "  cernbox inbox watch\n" +
			"  cernbox inbox watch --interval 5m",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if interval <= 0 {
				return cli.Usagef("--interval wants a positive duration")
			}
			b, err := open(cmd, rt)
			if err != nil {
				return err
			}
			jobs, err := b.jobs(args, fl)
			if err != nil {
				return err
			}
			if err := userplane.CheckUnattended(rt); err != nil {
				return err
			}
			for _, j := range jobs {
				b.Msg("Watching %s -> %s every %s", j.remote, j.local, interval)
			}
			ctx, cancel := userplane.Interruptible(cmd)
			defer cancel()
			return b.watch(ctx, jobs, fl.settle, interval)
		},
	}
	fl.register(cmd)
	cmd.Flags().DurationVar(&interval, "interval", defaultInterval, "how often to look in the folders")
	return cli.Mutates(cmd)
}

// watch ends without an error when the context ends: an interrupt or
// --timeout.
func (b *box) watch(ctx context.Context, jobs []job, settle, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		for _, j := range jobs {
			got, err := b.run(ctx, j, settle)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				// A watch that stops on one bad pass fails unseen overnight.
				b.Warn("%s: %v", j.remote, userplane.ErrLine(err))
				continue
			}
			got.report(b)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func statusCmd(rt *cli.Runtime) *cobra.Command {
	var fl flags
	cmd := &cobra.Command{
		Use:   "status [REMOTE]",
		Short: "Show what is waiting to be downloaded",
		Long: "List what the inbox would download, without downloading anything.\n\n" +
			"This is how you check that an inbox works, and what it waits on: a file\n" +
			"still being uploaded is listed as such rather than left unexplained.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			b, err := open(cmd, rt)
			if err != nil {
				return err
			}
			jobs, err := b.jobs(args, fl)
			if err != nil {
				return err
			}
			type row struct {
				Remote string `json:"remote"`
				Local  string `json:"local"`
				Size   int64  `json:"size"`
				State  string `json:"state"`
			}
			rows := []row{}
			t := cli.Table{Headers: []string{"Size", "State", "Remote", "Goes to"}}
			for _, j := range jobs {
				sc, err := b.scan(cmd.Context(), j, fl.settle)
				if err != nil {
					if j.configured && cli.IsNotFound(err) {
						b.Warn("skipping %s: %v", j.remote, userplane.ErrLine(err))
						continue
					}
					return err
				}
				for _, it := range sc.items {
					state := "ready"
					if !it.settled {
						state = "still arriving"
					} else if there, _ := alreadyLocal(j, it); there {
						state = "already here"
					}
					local := j.localFor(it)
					rows = append(rows, row{Remote: it.path, Local: local, Size: it.size, State: state})
					t.Rows = append(t.Rows, []string{output.HumanSize(it.size), state, it.path, local})
				}
				if sc.dirs > 0 {
					b.Msg("%s holds %s an inbox does not collect.", j.remote, userplane.Plural(sc.dirs, "subdirectory", "subdirectories"))
				}
			}
			t.Items = rows
			return rt.Print(cmd.OutOrStdout(), t, nil)
		},
	}
	fl.register(cmd)
	return cmd
}

type job struct {
	remote, local, layout, after string
	exec                         []string
	execIn                       time.Duration
	// configured is a folder of the config, whose absence is a warning; one
	// of the command line is a typo.
	configured bool
}

func newJob(f Folder) (job, error) {
	if f.Local == "" || f.Remote == "" {
		return job{}, cli.Usagef("an inbox folder needs both a CERNBox folder and a local path")
	}
	local, err := filepath.Abs(userplane.ExpandHome(f.Local))
	if err != nil {
		return job{}, cli.Usagef("%v", err)
	}
	j := job{remote: f.Remote, local: local, layout: cmp.Or(f.Layout, "flat"), after: cmp.Or(f.After, "keep"), exec: strings.Fields(f.Exec)}
	if j.layout != "flat" && j.layout != "date" {
		return job{}, cli.Usagef("unknown layout %q: want flat or date", j.layout)
	}
	if !slices.Contains([]string{"keep", "move", "delete"}, j.after) {
		return job{}, cli.Usagef("unknown --after %q: want keep, move or delete", j.after)
	}
	return j, nil
}

type scanResult struct {
	items []item
	// dirs counts the subfolders, which an inbox does not collect. Status says
	// so, because somebody who put a folder there would believe it collected.
	dirs int
}

type item struct {
	path, name string
	size       int64
	modified   time.Time
	settled    bool
}

// localFor is where a file goes. The date layout uses the date of the file,
// not of the collection.
func (j job) localFor(it item) string {
	if j.layout == "date" {
		d := it.modified
		return filepath.Join(j.local, fmt.Sprintf("%04d/%02d/%02d", d.Year(), int(d.Month()), d.Day()), it.name)
	}
	return filepath.Join(j.local, it.name)
}

// scan lists the folder one level deep: deciding that a tree finished
// arriving is much harder than for one file.
func (b *box) scan(ctx context.Context, j job, settle time.Duration) (scanResult, error) {
	remote, err := b.RT.RemotePath(ctx, j.remote)
	if err != nil {
		return scanResult{}, err
	}
	entries, err := b.List(ctx, remote)
	if err != nil {
		return scanResult{}, err
	}
	// The server wrote the timestamps, so use its clock.
	now, err := b.B.ServerTime(ctx)
	if err != nil {
		b.Warn("could not read the server clock (%v), so using this machine's", userplane.ErrLine(err))
		now = time.Now()
	}
	var sc scanResult
	for _, e := range entries {
		name := userplane.Name(e)
		switch {
		case userplane.IsDir(e):
			sc.dirs++
			continue
		case userplane.SkipName(name):
			continue
		}
		mod := userplane.Mtime(e)
		sc.items = append(sc.items, item{path: e.GetPath(), name: name, size: int64(e.GetSize()), modified: mod, settled: now.Sub(mod) >= settle})
	}
	slices.SortFunc(sc.items, func(x, y item) int { return strings.Compare(x.name, y.name) })
	return sc, nil
}

type result struct {
	downloaded, skipped, waiting int
	bytes                        int64
}

func (r *result) add(o result) {
	r.downloaded += o.downloaded
	r.skipped += o.skipped
	r.waiting += o.waiting
	r.bytes += o.bytes
}

func (r result) report(b *box) {
	if r.downloaded == 0 && r.skipped == 0 && r.waiting == 0 {
		b.Msg("Nothing to collect.")
		return
	}
	msg := fmt.Sprintf("Downloaded %s (%s)", userplane.Plural(r.downloaded, "file", "files"), output.HumanSize(r.bytes))
	if r.skipped > 0 {
		msg += fmt.Sprintf(", %s already here", userplane.Plural(r.skipped, "file", "files"))
	}
	if r.waiting > 0 {
		msg += fmt.Sprintf(", %s still arriving", userplane.Plural(r.waiting, "file", "files"))
	}
	b.Msg("%s.", msg)
}

func (r result) json() map[string]any {
	return map[string]any{"downloaded": r.downloaded, "skipped": r.skipped, "waiting": r.waiting, "bytes": r.bytes}
}

func (b *box) run(ctx context.Context, j job, settle time.Duration) (result, error) {
	sc, err := b.scan(ctx, j, settle)
	if err != nil {
		return result{}, err
	}
	var res result
	for _, it := range sc.items {
		if !it.settled {
			res.waiting++
			continue
		}
		got, size, err := b.collect(ctx, j, it)
		if err != nil {
			return res, err
		}
		if got {
			res.downloaded++
			res.bytes += size
		} else {
			res.skipped++
		}
	}
	return res, nil
}

// alreadyLocal reports whether the file is down. A local file of the same
// size counts as the same file.
func alreadyLocal(j job, it item) (bool, error) {
	info, err := os.Stat(j.localFor(it))
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return !info.IsDir() && info.Size() == it.size, nil
}

func (b *box) collect(ctx context.Context, j job, it item) (bool, int64, error) {
	local := j.localFor(it)
	switch there, err := alreadyLocal(j, it); {
	case err != nil:
		return false, 0, err
	case there:
		// Run the policy, so that a pass that failed to tidy up finishes the
		// job. Run the hook only with move or delete: a file still in the inbox
		// then means that its hook failed. With keep it would run on every pass.
		if len(j.exec) > 0 && j.after != "keep" {
			if err := b.hook(ctx, j, it, local); err != nil {
				return false, 0, nil
			}
		}
		return false, 0, b.after(ctx, j, it)
	}
	// An inbox only adds, so a different file with this name stays.
	if userplane.PathTaken(local) {
		local = userplane.FreeLocalName(local)
	}
	if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
		return false, 0, err
	}
	// Verify only when the CERNBox copy is about to go.
	e, err := b.Engine(ctx, cli.TransferOptions{Overwrite: true, Verify: j.after == "delete"})
	if err != nil {
		return false, 0, err
	}
	size, err := e.DownloadFile(ctx, it.path, local)
	if err != nil {
		return false, 0, err
	}
	// A failed hook stops the policy: the CERNBox copy is the only sign that
	// something is unfinished. The file is down, so it counts as collected.
	if len(j.exec) > 0 {
		if err := b.hook(ctx, j, it, local); err != nil {
			return true, size, nil
		}
	}
	return true, size, b.after(ctx, j, it)
}

func (b *box) hook(ctx context.Context, j job, it item, local string) error {
	err := userplane.RunHook(ctx, b.Cmd.ErrOrStderr(), j.exec, j.execIn, local, []string{
		"CERNBOX_INBOX_FILE=" + local,
		"CERNBOX_INBOX_REMOTE=" + it.path,
		"CERNBOX_INBOX_NAME=" + it.name,
		"CERNBOX_INBOX_SIZE=" + strconv.FormatInt(it.size, 10),
	})
	if err != nil {
		b.Warn("%s: %v, so it was left in the inbox", it.name, userplane.ErrLine(err))
	}
	return err
}

func (b *box) after(ctx context.Context, j job, it item) error {
	switch j.after {
	case "delete":
		return b.B.Files().Remove(ctx, userplane.Ref(it.path))
	case "move":
		dir := path.Join(path.Dir(it.path), collectedDir)
		if err := b.Mkdir(ctx, dir); err != nil && !userplane.IsConflict(err) {
			return err
		}
		dst := path.Join(dir, it.name)
		if _, err := b.Stat(ctx, dst); err == nil {
			if dst, err = b.FreeRemoteName(ctx, dst); err != nil {
				return err
			}
		}
		return b.B.Files().Move(ctx, userplane.Ref(it.path), userplane.Ref(dst), false)
	}
	return nil
}

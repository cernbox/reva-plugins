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

// Package outbox is the command outbox of the cernbox CLI: a local folder
// whose arrivals go up to CERNBox, such as the folder of the screenshots.
// It is one way: nothing in CERNBox changes except by adding to it.
package outbox

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
	link "github.com/cs3org/go-cs3apis/cs3/sharing/link/v1beta1"
	"github.com/cs3org/reva/v3/pkg/cli"
	"github.com/cs3org/reva/v3/pkg/cli/output"
	"github.com/cs3org/reva/v3/pkg/permissions"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/cobra"
)

// Name is the name of the command and of its config section.
const Name = "outbox"

func init() { cli.RegisterCommand(Name, New) }

const (
	// defaultSettle is how long a file must be unchanged to count as done: a
	// screenshot appears while it is written, and half an image goes up.
	defaultSettle = 2 * time.Second
	// defaultSweep is how often a watch looks through the folders anyway,
	// behind the notifications, for what they miss.
	defaultSweep = time.Minute
	settleCheck  = time.Second
	uploadedDir  = ".uploaded"
)

// Config is the section [plugins.outbox].
type Config struct {
	Folders []Folder `toml:"folders"`
}

// Folder is one local folder whose files go to CERNBox.
type Folder struct {
	// Local is the folder to watch. A leading ~ is expanded.
	Local string `toml:"local"`
	// Remote is the CERNBox folder its files go to.
	Remote string `toml:"remote"`
	// Layout is flat, or date to file each upload under YYYY/MM/DD of the
	// timestamp of the file.
	Layout string `toml:"layout"`
	// After is what happens to the local file once it is up: keep (the
	// default, which cannot lose anything), move to .uploaded, or delete.
	After string `toml:"after"`
	// Link creates a public link for each upload and prints it.
	Link bool `toml:"link"`
	// Exec is run over each file before it is uploaded, with its path as the
	// last argument. It may rewrite the file in place; a rename is not
	// followed. It is split on spaces and never given to a shell.
	Exec string `toml:"exec"`
}

// configOf is a variable so that tests can replace it.
var configOf = func(rt *cli.Runtime) (Config, error) {
	var c Config
	err := cli.PluginConfig(rt, Name, &c)
	return c, err
}

// New returns the command outbox.
func New(rt *cli.Runtime) *cobra.Command {
	cmd := &cobra.Command{
		Use:   Name,
		Short: "Upload whatever appears in a local folder",
		Long: "Treat a local folder as an outbox: anything that turns up in it is\n" +
			"uploaded to CERNBox.\n\n" +
			"It is one way, always. Nothing in CERNBox is changed except by adding to\n" +
			"it, and 'cernbox sync' is still the command for keeping two sides matching.\n\n" +
			"Folders are usually listed in the config file, under [plugins.outbox] folders,\n" +
			"each with its own destination, so that 'cernbox outbox push' needs no\n" +
			"arguments. A folder can also be given on the command line with --to.",
		GroupID: "files",
	}
	cmd.AddCommand(pushCmd(rt), watchCmd(rt), statusCmd(rt))
	return cmd
}

type flags struct {
	to     string
	layout string
	after  string
	link   bool
	settle time.Duration
	exec   string
	execIn time.Duration
}

func (o *flags) register(cmd *cobra.Command) {
	f := cmd.Flags()
	f.StringVar(&o.to, "to", "", "CERNBox folder to upload into")
	f.StringVar(&o.layout, "layout", "", "where each file lands: flat, or date for YYYY/MM/DD folders")
	f.StringVar(&o.after, "after", "", "what to do with the local file: keep, move or delete")
	f.BoolVar(&o.link, "link", false, "create a public link for each upload and print it")
	f.DurationVar(&o.settle, "settle", defaultSettle, "how long a file must be unchanged before it counts as finished")
	f.StringVar(&o.exec, "exec", "", "command to run over each file before it is uploaded, with its path as the last argument")
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
			return nil, cli.Usagef("pass --to with the CERNBox folder to upload %s into", args[0])
		}
		j, err := newJob(Folder{Local: args[0], Remote: fl.to, Layout: fl.layout, After: fl.after, Link: fl.link, Exec: fl.exec})
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
		return nil, cli.Usagef("no outbox folders are configured. Name one with "+
			"'%s outbox push DIR --to REMOTE', or list them under [plugins.outbox] folders in the config file", b.RT.Name())
	}
	out := make([]job, 0, len(c.Folders))
	for _, f := range c.Folders {
		// A flag overrides the config of every folder, so that --after keep is
		// a safety net over a config that says delete.
		f.Layout = cmp.Or(fl.layout, f.Layout)
		f.After = cmp.Or(fl.after, f.After)
		f.Exec = cmp.Or(fl.exec, f.Exec)
		f.Link = f.Link || fl.link
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

func pushCmd(rt *cli.Runtime) *cobra.Command {
	var fl flags
	cmd := &cobra.Command{
		Use:   "push [DIR]",
		Short: "Upload what is waiting, then stop",
		Long: "Upload everything waiting in the outbox folders and exit, which is the form\n" +
			"to run from a timer.\n" +
			"\n" +
			"A file that has changed within --settle is still being written, so it is left\n" +
			"for next time rather than uploaded half finished.",
		Example: "  cernbox outbox push\n" +
			"  cernbox outbox push ~/Pictures/Screenshots --to Screenshots --layout date",
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
				total.add(got)
				if err != nil {
					return err
				}
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
	var sweep time.Duration
	cmd := &cobra.Command{
		Use:   "watch [DIR]",
		Short: "Keep uploading as files appear",
		Long: "Watch the outbox folders and upload what appears, until interrupted.\n" +
			"\n" +
			"Needs a credential that works with nobody present: a Kerberos ticket, or an\n" +
			"app token in CERNBOX_APP_TOKEN.\n" +
			"\n" +
			"Good for a session at a desk. To survive sleep, a lost network and a reboot,\n" +
			"run 'outbox push' from a timer instead.",
		Example: "  cernbox outbox watch\n  cernbox outbox watch ~/scratch --to Scratch",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
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
				b.Msg("Watching %s -> %s", j.local, j.remote)
			}
			ctx, cancel := userplane.Interruptible(cmd)
			defer cancel()
			return b.watch(ctx, jobs, fl.settle, sweep)
		},
	}
	fl.register(cmd)
	cmd.Flags().DurationVar(&sweep, "sweep", defaultSweep, "how often to look through the folders anyway, behind the notifications")
	return cli.Mutates(cmd)
}

// watch uploads as files appear. The file system says when, so a big folder
// is not read every few seconds. The notifications are not enough alone: the
// kernel drops them when its queue overflows, network file systems often
// send none, and files from before the watch were never announced. The sweep
// catches those. The settle wait is separate: a notification says that a
// file exists, not that its writer finished.
func (b *box) watch(ctx context.Context, jobs []job, settle, sweep time.Duration) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		b.Warn("cannot watch for changes (%v), so looking every %s instead", userplane.ErrLine(err), sweep)
	} else {
		defer w.Close()
		for _, j := range jobs {
			if err := w.Add(j.local); err != nil {
				b.Warn("cannot watch %s (%v), so it will only be looked at every %s", j.local, userplane.ErrLine(err), sweep)
			}
		}
	}
	// Nil channels are never ready, so a failed watcher leaves the work to the
	// timers.
	var events chan fsnotify.Event
	var errs chan error
	if w != nil {
		events, errs = w.Events, w.Errors
	}
	b.sweep(ctx, jobs, settle)

	pending := map[string]bool{}
	settleTicker := time.NewTicker(settleCheck)
	defer settleTicker.Stop()
	sweepTicker := time.NewTicker(sweep)
	defer sweepTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev := <-events:
			// A rename into the folder is a Create. A file being written is a Write.
			if ev.Op&(fsnotify.Create|fsnotify.Write) != 0 && !userplane.SkipName(filepath.Base(ev.Name)) {
				pending[ev.Name] = true
			}
		case err := <-errs:
			if err != nil {
				b.Warn("watching: %v", userplane.ErrLine(err))
			}
		case <-settleTicker.C:
			for p := range pending {
				done, err := b.tryPending(ctx, jobs, p, settle)
				if err != nil {
					b.Warn("%s: %v", filepath.Base(p), userplane.ErrLine(err))
				}
				if done || err != nil {
					delete(pending, p)
				}
			}
		case <-sweepTicker.C:
			b.sweep(ctx, jobs, settle)
		}
	}
}

// sweep warns about failures instead of returning them, so that one bad file
// does not stop the watch.
func (b *box) sweep(ctx context.Context, jobs []job, settle time.Duration) {
	for _, j := range jobs {
		if got, err := b.run(ctx, j, settle); err != nil {
			b.Warn("%s: %v", j.local, userplane.ErrLine(err))
		} else if got.uploaded > 0 {
			got.report(b)
		}
	}
}

// tryPending uploads a file that had an event, once it stopped changing. It
// reports whether the file is done with: sent or gone.
func (b *box) tryPending(ctx context.Context, jobs []job, p string, settle time.Duration) (bool, error) {
	i := slices.IndexFunc(jobs, func(j job) bool { return j.local == filepath.Dir(p) })
	if i < 0 {
		// Outside the folders, as the .uploaded of --after move.
		return true, nil
	}
	info, err := os.Lstat(p)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return true, nil
	case err != nil:
		return true, err
	case info.IsDir() || info.Mode()&os.ModeSymlink != 0:
		return true, nil
	case time.Since(info.ModTime()) < settle:
		return false, nil
	}
	it := item{path: p, name: filepath.Base(p), size: info.Size(), modified: info.ModTime(), settled: true}
	_, _, err = b.send(ctx, jobs[i], it)
	return err == nil, err
}

func statusCmd(rt *cli.Runtime) *cobra.Command {
	var fl flags
	cmd := &cobra.Command{
		Use:   "status [DIR]",
		Short: "Show what is waiting to be uploaded",
		Long: "List what the outbox would upload, without uploading anything.\n\n" +
			"This is how you check that an outbox works, and what it waits on: a file\n" +
			"still being written is listed as such rather than left unexplained.",
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
				Local  string `json:"local"`
				Remote string `json:"remote"`
				Size   int64  `json:"size"`
				State  string `json:"state"`
			}
			rows := []row{}
			dirs := 0
			for _, j := range jobs {
				sc, err := scan(j, fl.settle, time.Now())
				if err != nil {
					if j.configured && errors.Is(err, fs.ErrNotExist) {
						b.Warn("skipping %s: %v", j.local, userplane.ErrLine(err))
						continue
					}
					return err
				}
				dirs += sc.dirs
				for _, it := range sc.items {
					state := "waiting"
					if !it.settled {
						state = "still being written"
					} else if up, err := b.alreadyThere(cmd.Context(), j, it); err == nil && up {
						state = "already up"
					}
					rows = append(rows, row{Local: it.path, Remote: j.remoteFor(it), Size: it.size, State: state})
				}
			}
			t := cli.Table{Headers: []string{"State", "Size", "Local", "Goes to"}, Items: rows}
			for _, r := range rows {
				t.Rows = append(t.Rows, []string{r.State, output.HumanSize(r.Size), r.Local, r.Remote})
			}
			if err := rt.Print(cmd.OutOrStdout(), t, nil); err != nil {
				return err
			}
			if len(rows) == 0 {
				b.Msg("Nothing is waiting.")
			}
			if dirs > 0 {
				b.Warn("%s skipped: an outbox uploads files, not folders.", userplane.Plural(dirs, "folder", "folders"))
			}
			return nil
		},
	}
	fl.register(cmd)
	return cmd
}

type job struct {
	local, remote, layout, after string
	link                         bool
	exec                         []string
	execIn                       time.Duration
	// configured is a folder of the config, whose absence is a warning; one
	// of the command line is a typo.
	configured bool
}

func newJob(f Folder) (job, error) {
	if f.Local == "" || f.Remote == "" {
		return job{}, cli.Usagef("an outbox folder needs both a local path and a CERNBox folder")
	}
	local, err := filepath.Abs(userplane.ExpandHome(f.Local))
	if err != nil {
		return job{}, cli.Usagef("%v", err)
	}
	j := job{local: local, remote: f.Remote, layout: cmp.Or(f.Layout, "flat"), after: cmp.Or(f.After, "keep"), link: f.Link, exec: strings.Fields(f.Exec)}
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
	// dirs counts the subfolders, which an outbox does not upload. The user is
	// told, because somebody who dropped a folder in would believe it sent.
	dirs int
}

type item struct {
	path, name string
	size       int64
	modified   time.Time
	settled    bool
}

// remoteFor is where a file goes. The date layout uses the date of the file,
// not of the upload.
func (j job) remoteFor(it item) string {
	if j.layout == "date" {
		d := it.modified
		return path.Join(j.remote, fmt.Sprintf("%04d/%02d/%02d", d.Year(), int(d.Month()), d.Day()), it.name)
	}
	return path.Join(j.remote, it.name)
}

// scan lists the folder one level deep. A file being written keeps its
// modification time current, so one stat tells if it settled.
func scan(j job, settle time.Duration, now time.Time) (scanResult, error) {
	entries, err := os.ReadDir(j.local)
	if err != nil {
		return scanResult{}, err
	}
	var sc scanResult
	for _, e := range entries {
		if userplane.SkipName(e.Name()) {
			continue
		}
		if e.IsDir() {
			sc.dirs++
			continue
		}
		info, err := e.Info()
		// A link is not followed: one to / would upload a home.
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		sc.items = append(sc.items, item{
			path: filepath.Join(j.local, e.Name()), name: e.Name(), size: info.Size(),
			modified: info.ModTime(), settled: now.Sub(info.ModTime()) >= settle,
		})
	}
	return sc, nil
}

type result struct {
	uploaded, skipped, pending, failed, dirs int
	bytes                                    int64
}

func (r *result) add(o result) {
	r.uploaded += o.uploaded
	r.skipped += o.skipped
	r.pending += o.pending
	r.failed += o.failed
	r.dirs += o.dirs
	r.bytes += o.bytes
}

func (r result) report(b *box) {
	switch {
	case r.uploaded > 0:
		b.Msg("Uploaded %s (%s).", userplane.Plural(r.uploaded, "file", "files"), output.HumanSize(r.bytes))
	case r.pending == 0 && r.skipped == 0 && r.failed == 0:
		b.Msg("Nothing to upload.")
	}
	switch r.skipped {
	case 0:
	case 1:
		b.Msg("1 was already there.")
	default:
		b.Msg("%d were already there.", r.skipped)
	}
	if r.pending > 0 {
		b.Msg("%d are still being written and were left for next time.", r.pending)
	}
	if r.dirs > 0 {
		b.Warn("%s skipped: an outbox uploads files, not folders. '%s cp -r' or '%s sync' will send a directory.",
			userplane.Plural(r.dirs, "folder", "folders"), b.RT.Name(), b.RT.Name())
	}
}

func (r result) json() map[string]any {
	return map[string]any{"uploaded": r.uploaded, "skipped": r.skipped, "pending": r.pending, "failed": r.failed, "bytes": r.bytes}
}

func (b *box) run(ctx context.Context, j job, settle time.Duration) (result, error) {
	var res result
	sc, err := scan(j, settle, time.Now())
	if err != nil {
		// A missing configured folder (a drive not mounted) must not stop the
		// others. One of the command line is a typo, so that is an error.
		if j.configured && errors.Is(err, fs.ErrNotExist) {
			b.Warn("skipping %s: %v", j.local, userplane.ErrLine(err))
			return res, nil
		}
		return res, err
	}
	res.dirs = sc.dirs
	for _, it := range sc.items {
		if !it.settled {
			res.pending++
			continue
		}
		up, size, err := b.send(ctx, j, it)
		if err != nil {
			res.failed++
			b.Warn("%s: %v", it.name, userplane.ErrLine(err))
			continue
		}
		if up {
			res.uploaded++
			res.bytes += size
		} else {
			res.skipped++
		}
	}
	return res, nil
}

// alreadyThere reports whether the destination holds the file. It compares
// sizes, not content, as sync does.
func (b *box) alreadyThere(ctx context.Context, j job, it item) (bool, error) {
	remote, err := b.RT.RemotePath(ctx, j.remoteFor(it))
	if err != nil {
		return false, err
	}
	info, err := b.Stat(ctx, remote)
	if cli.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return !userplane.IsDir(info) && int64(info.GetSize()) == it.size, nil
}

// send uploads one file and applies the policy to the local copy. It reports
// whether something was sent.
func (b *box) send(ctx context.Context, j job, it item) (bool, int64, error) {
	remote, err := b.RT.RemotePath(ctx, j.remoteFor(it))
	if err != nil {
		return false, 0, err
	}
	switch info, err := b.Stat(ctx, remote); {
	case err != nil && !cli.IsNotFound(err):
		return false, 0, err
	case err == nil && !userplane.IsDir(info) && int64(info.GetSize()) == it.size:
		// Run the policy, so that a run that failed to tidy up finishes the job.
		return false, 0, b.after(j, it)
	case err == nil:
		// An outbox only adds, so a different file with this name stays.
		if remote, err = b.FreeRemoteName(ctx, remote); err != nil {
			return false, 0, err
		}
	}
	// The hook runs only once the file is going up, so that a hook that
	// rewrites in place does not run on every pass: the next pass finds the
	// size of the upload and skips the file before this point.
	if len(j.exec) > 0 {
		if err := b.hook(ctx, j, it, remote); err != nil {
			return false, 0, nil
		}
		if _, err := os.Stat(it.path); errors.Is(err, fs.ErrNotExist) {
			b.Warn("%s: %s left nothing at that path, so there was nothing to upload. "+
				"A hook may rewrite a file where it is, but a rename is not followed.", it.name, j.exec[0])
			return false, 0, nil
		} else if err != nil {
			return false, 0, err
		}
	}
	if err := b.Mkdir(ctx, path.Dir(remote)); err != nil && !userplane.IsConflict(err) {
		return false, 0, err
	}
	// Verify only when the local copy is about to go.
	e, err := b.Engine(ctx, cli.TransferOptions{Overwrite: true, Verify: j.after == "delete"})
	if err != nil {
		return false, 0, err
	}
	size, err := e.UploadFile(ctx, it.path, remote)
	if err != nil {
		return false, 0, err
	}
	if j.link {
		if url, err := b.linkFor(ctx, remote); err != nil {
			b.Warn("uploaded %s but could not make a link: %v", it.name, userplane.ErrLine(err))
		} else {
			// On stdout, so that it can go into a pipe.
			b.Line("%s", url)
		}
	}
	return true, size, b.after(j, it)
}

func (b *box) hook(ctx context.Context, j job, it item, remote string) error {
	err := userplane.RunHook(ctx, b.Cmd.ErrOrStderr(), j.exec, j.execIn, it.path, []string{
		"CERNBOX_OUTBOX_FILE=" + it.path,
		"CERNBOX_OUTBOX_REMOTE=" + remote,
		"CERNBOX_OUTBOX_NAME=" + it.name,
		"CERNBOX_OUTBOX_SIZE=" + strconv.FormatInt(it.size, 10),
	})
	if err != nil {
		b.Warn("%s: %v, so it was not uploaded", it.name, userplane.ErrLine(err))
	}
	return err
}

// linkFor makes a read-only public link. Without an endpoint, the token is
// all there is.
func (b *box) linkFor(ctx context.Context, remote string) (string, error) {
	info, err := b.Stat(ctx, remote)
	if err != nil {
		return "", err
	}
	s, err := b.B.Links().Create(ctx, &link.CreatePublicShareRequest{
		ResourceInfo: info,
		Grant:        &link.Grant{Permissions: &link.PublicSharePermissions{Permissions: permissions.NewViewerRole().CS3ResourcePermissions()}},
	})
	if err != nil {
		return "", err
	}
	if s.GetToken() == "" {
		return "", errors.New("the server returned no link")
	}
	if ep := b.RT.HTTPEndpoint(); ep != "" {
		return strings.TrimSuffix(ep, "/") + "/s/" + s.GetToken(), nil
	}
	return s.GetToken(), nil
}

func (b *box) after(j job, it item) error {
	switch j.after {
	case "delete":
		return os.Remove(it.path)
	case "move":
		dir := filepath.Join(j.local, uploadedDir)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		dst := filepath.Join(dir, it.name)
		if userplane.PathTaken(dst) {
			dst = userplane.FreeLocalName(dst)
		}
		return os.Rename(it.path, dst)
	}
	return nil
}

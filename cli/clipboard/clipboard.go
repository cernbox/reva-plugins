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

// Package clipboard is the clipboard of the cernbox CLI, which crosses
// machines: `cernbox copy` on one, `cernbox paste` on another, with CERNBox
// in between, and `cernbox clipboard list|clear` for the slots. It is the
// one state that both machines can see, so it lives in the home space, in
// the same place and format as in cernbox-cli.
//
// A path already in CERNBox is only pointed at: paste to another CERNBox
// path is a server-side copy, with no data through the client. A local path
// is staged: uploaded on copy, downloaded on paste. Which one applies
// follows the rule of cp: cb: or a space alias is CERNBox, anything else is
// local, also /eos/..., which is a local mount on lxplus.
//
// The commands use Runtime.Backend and Runtime.Data, so they work over
// both transports.
package clipboard

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/cernbox/reva-plugins/cli/internal/userplane"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/v3/pkg/cli"
	"github.com/cs3org/reva/v3/pkg/cli/output"
	"github.com/spf13/cobra"
)

func init() {
	cli.RegisterCommand("copy", NewCopy)
	cli.RegisterCommand("paste", NewPaste)
	cli.RegisterCommand("clipboard", New)
}

const expiryLayout = "2006-01-02"

// board is one run of a clipboard command.
type board struct {
	*userplane.Session
	in io.Reader
}

func open(cmd *cobra.Command, rt *cli.Runtime) (*board, error) {
	s, err := userplane.Open(cmd, rt)
	if err != nil {
		return nil, err
	}
	return &board{Session: s, in: cmd.InOrStdin()}, nil
}

// NewCopy returns the command copy.
func NewCopy(rt *cli.Runtime) *cobra.Command {
	var opts copyOptions
	cmd := &cobra.Command{
		Use:   "copy PATH...",
		Short: "Copy files, to paste on another computer",
		Long: "Put files on a clipboard stored in CERNBox, to paste on another computer.\n" +
			"\n" +
			"A CERNBox path (cb:/eos/...) is only pointed at, so nothing is transferred; a\n" +
			"file on your computer is uploaded. Copying replaces what the clipboard held,\n" +
			"and pasting does not empty it.\n" +
			"\n" +
			"--stream stores nothing and waits for the paste. --to hands it to somebody\n" +
			"else, which makes that slot readable by them. The two combine.",
		Example: "  cernbox copy ./report.pdf\n" +
			"  cernbox copy cb:/eos/user/g/gdelmont/report.pdf\n" +
			"  cernbox copy -r ./data --slot build\n" +
			"  cernbox copy ./report.pdf --to marie\n" +
			"  tar cz dir | cernbox copy - --name dir.tgz",
		GroupID: "files",
		Args:    cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := userplane.Interruptible(cmd)
			defer cancel()
			b, err := open(cmd, rt)
			if err != nil {
				return err
			}
			opts.slotGiven = cmd.Flags().Changed("slot")
			return b.copy(ctx, args, opts)
		},
	}
	f := cmd.Flags()
	f.StringVar(&opts.slot, "slot", DefaultSlot, "clipboard slot to copy into")
	f.BoolVarP(&opts.recursive, "recursive", "r", false, "copy directories and their contents")
	f.BoolVarP(&opts.recursive, "recursive-upper", "R", false, "same as --recursive")
	_ = f.MarkHidden("recursive-upper")
	f.DurationVar(&opts.ttl, "ttl", DefaultTTL, "how long the copy lives; 0 keeps it until you clear it")
	f.StringVar(&opts.name, "name", "", `name to store standard input under (default "stdin")`)
	f.BoolVar(&opts.verify, "verify", false, "compute and check checksums when staging")
	f.IntVarP(&opts.jobs, "jobs", "j", 0, "number of files to transfer at once")
	f.BoolVar(&opts.stream, "stream", false, "wait for a paste on another computer and send the file straight to it")
	f.DurationVar(&opts.wait, "wait", 10*time.Minute, "how long to wait for the other computer, with --stream")
	f.StringVar(&opts.to, "to", "", "hand this to another person, who collects it with 'paste --from'")
	return cli.Mutates(cmd)
}

type copyOptions struct {
	slot      string
	recursive bool
	ttl       time.Duration
	name      string
	verify    bool
	jobs      int
	// stream hands the source over live instead of storing a copy.
	stream bool
	// wait bounds every step that depends on the other machine.
	wait time.Duration
	// to is the person the copy is for, empty for your own clipboard.
	to string
	// slotGiven is whether --slot was typed, which --to refuses.
	slotGiven bool
}

// NewPaste returns the command paste.
func NewPaste(rt *cli.Runtime) *cobra.Command {
	var opts pasteOptions
	cmd := &cobra.Command{
		Use:   "paste [DEST]",
		Short: "Paste what was copied on another computer",
		Long: "Paste what was copied.\n" +
			"\n" +
			"With no destination the files land in the current directory under their own\n" +
			"names. A CERNBox path (cb:/eos/...) is copied by the server without sending\n" +
			"data, and - writes a single file to standard output.\n" +
			"\n" +
			"Pasting leaves the clipboard alone, so you can paste elsewhere too. --from\n" +
			"collects what somebody copied for you; 'cernbox clipboard list' shows who has.",
		Example: "  cernbox paste\n" +
			"  cernbox paste ./incoming/\n" +
			"  cernbox paste cb:/eos/project/c/cernbox/data/\n" +
			"  cernbox paste --from einstein ./incoming/\n" +
			"  cernbox paste --slot build - | tar xz",
		GroupID: "files",
		Args:    cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := userplane.Interruptible(cmd)
			defer cancel()
			b, err := open(cmd, rt)
			if err != nil {
				return err
			}
			opts.slotGiven = cmd.Flags().Changed("slot")
			return b.paste(ctx, args, opts)
		},
	}
	f := cmd.Flags()
	f.StringVar(&opts.slot, "slot", DefaultSlot, "clipboard slot to paste from")
	f.BoolVarP(&opts.force, "force", "f", false, "overwrite existing destinations")
	f.BoolVar(&opts.verify, "verify", false, "compute and check checksums")
	f.BoolVar(&opts.noArchive, "no-archive", false, "download a directory file by file instead of as one archive")
	f.IntVarP(&opts.jobs, "jobs", "j", 0, "number of files to transfer at once")
	f.DurationVar(&opts.wait, "wait", 10*time.Minute, "how long to wait for a computer that is sending with --stream")
	f.StringVar(&opts.from, "from", "", "collect what this person copied for you")
	return cli.Mutates(cmd)
}

type pasteOptions struct {
	slot      string
	force     bool
	verify    bool
	noArchive bool
	jobs      int
	// wait bounds how long a live handover waits for the far end.
	wait time.Duration
	// from is whose clipboard to read, empty for your own.
	from string
	// slotGiven is whether --slot was typed, which --from refuses.
	slotGiven bool
}

// New returns the command clipboard, which manages the slots that copy and
// paste use.
func New(rt *cli.Runtime) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "clipboard",
		Short: "See and clear the clipboard",
		Long: "The clipboard that 'cernbox copy' and 'cernbox paste' share.\n\n" +
			"It is stored in CERNBox, so every computer you are signed in on sees the\n" +
			"same clipboard. Uploaded files use your quota until you clear them.\n\n" +
			"Use --slot to keep more than one copy at a time.",
		GroupID: "files",
	}
	list := &cobra.Command{
		Use:   "list",
		Short: "Show what is on the clipboard",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			b, err := open(cmd, rt)
			if err != nil {
				return err
			}
			return b.list(cmd.Context())
		},
	}
	var all bool
	clear := &cobra.Command{
		Use:   "clear [SLOT...]",
		Short: "Empty the clipboard and free the space",
		Long: "Empty the clipboard. With no name, the default slot.\n\n" +
			"Files that were only pointed at are left alone. Only the uploaded copies\n" +
			"are deleted, and they go to the trash, so 'cernbox trash purge' frees the\n" +
			"quota.",
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			b, err := open(cmd, rt)
			if err != nil {
				return err
			}
			return b.clear(cmd.Context(), args, all)
		},
	}
	clear.Flags().BoolVar(&all, "all", false, "clear every slot")
	cmd.AddCommand(list, cli.Mutates(clear))
	return cmd
}

// errNoSlot is a slot with no manifest: nothing was copied there.
var errNoSlot = errors.New("nothing has been copied here yet")

// -- copy ---------------------------------------------------------------------

// source is one argument of copy, classified before anything moves.
type source struct {
	stdin bool
	arg   userplane.Arg
	// name is what the entry is called on the clipboard, and what paste
	// writes it as.
	name string
}

func (b *board) copy(ctx context.Context, args []string, opts copyOptions) error {
	if opts.to != "" {
		slot, err := handoverSlotFor(opts.to, opts.slotGiven)
		if err != nil {
			return err
		}
		opts.slot = slot
	}
	if err := ValidateSlot(opts.slot); err != nil {
		return cli.Usagef("%v", err)
	}
	// All is parsed and named before anything moves: a typo in the last
	// argument must not leave a slot with half a copy.
	sources, err := b.classify(ctx, args, opts)
	if err != nil {
		return err
	}
	if opts.stream {
		// A handover is a live pipe between two machines: there is nothing for
		// a second source to do while the first is read.
		if len(sources) != 1 {
			return cli.Usagef("--stream hands over one thing at a time, and %d were given", len(sources))
		}
		return b.streamCopy(ctx, sources[0], opts)
	}

	root, err := b.root(ctx)
	if err != nil {
		return err
	}
	// The manifest is replaced only while it has this etag, so that two
	// machines copying to one slot at once give an error, not a broken slot.
	prior, priorETag, err := b.priorSlot(ctx, root, opts.slot)
	if err != nil {
		return err
	}
	// An upload into a folder that is not there fails. A handover stages
	// everything, also the CERNBox paths that a copy only points at.
	if anyStaged(sources) || opts.to != "" {
		if err := b.Mkdir(ctx, PayloadDir(root, opts.slot)); err != nil {
			return err
		}
	}

	m := NewManifest(opts.slot, time.Now(), opts.ttl, LocalOrigin(workingDir()))
	m.To = opts.to
	for _, src := range sources {
		e, err := b.copyOne(ctx, root, src, opts)
		if err != nil {
			return err
		}
		m.Entries = append(m.Entries, e)
	}

	// The staging is done before the manifest is replaced, so until here the
	// slot still holds the previous copy in full: a failure above leaves it
	// pastable, never a manifest that points at deleted bytes.
	if err := b.writeSlot(ctx, root, m, priorETag); err != nil {
		return err
	}
	// Only now is the payload of the previous copy unreferenced.
	b.discardUnreferenced(ctx, prior, m)
	// Copy makes the quota pressure, so it gives some back: nothing runs on
	// a schedule to do it.
	b.collectExpired(ctx, root)

	name := b.RT.Name()
	if opts.to != "" {
		// Shared last, so that a failed copy leaves nothing readable behind.
		if err := b.shareHandover(ctx, SlotDir(root, opts.slot), opts.to, roleViewer); err != nil {
			return err
		}
		b.Msg("Copied %s (%s) for %s.", itemCount(len(m.Entries)), userplane.ProseSize(m.Size()), opts.to)
		b.Msg("They collect it with '%s paste --from %s'. It stays on your quota until '%s clipboard clear %s'.",
			name, b.Username(ctx), name, opts.slot)
		return b.JSON(m)
	}
	b.Msg("Copied %s (%s) to the %q clipboard slot", itemCount(len(m.Entries)), userplane.ProseSize(m.Size()), opts.slot)
	b.Msg("Paste with '%s'", pasteHint(name, opts.slot))
	return b.JSON(m)
}

// classify parses the arguments and names each entry, refusing what would
// silently lose one.
func (b *board) classify(ctx context.Context, args []string, opts copyOptions) ([]source, error) {
	out := make([]source, 0, len(args))
	seenStdin := false
	for _, arg := range args {
		if arg == "-" {
			if seenStdin {
				return nil, cli.Usagef("standard input can only be copied once")
			}
			seenStdin = true
			out = append(out, source{stdin: true, name: cmp.Or(opts.name, "stdin")})
			continue
		}
		a, err := userplane.ParseArg(ctx, b.RT, arg)
		if err != nil {
			return nil, err
		}
		name := path.Base(a.Path)
		if !a.Remote {
			name = filepath.Base(strings.TrimRight(a.Path, string(os.PathSeparator)))
		}
		if name == "" || name == "." || name == "/" || name == string(os.PathSeparator) {
			return nil, cli.Usagef("%q has no name to copy it under: name a file or a directory rather than a root", arg)
		}
		out = append(out, source{arg: a, name: name})
	}
	// Two arguments with the same base name would land on each other, in the
	// slot and wherever they are pasted.
	seen := map[string]bool{}
	for _, s := range out {
		if seen[s.name] {
			return nil, cli.Usagef("two files are both called %q: copy them one at a time, or use --slot", s.name)
		}
		seen[s.name] = true
	}
	return out, nil
}

func anyStaged(sources []source) bool {
	return slices.ContainsFunc(sources, func(s source) bool { return s.stdin || !s.arg.Remote })
}

// copyOne puts one source on the clipboard, and uploads it only when it is
// not in CERNBox already.
func (b *board) copyOne(ctx context.Context, root string, src source, opts copyOptions) (Entry, error) {
	staged := path.Join(PayloadDir(root, opts.slot), src.name)
	switch {
	case src.stdin:
		size, parts, err := b.stageStdin(ctx, staged)
		if err != nil {
			return Entry{}, err
		}
		return Entry{Name: src.name, Path: staged, Size: size, Staged: true, Parts: parts}, nil
	case src.arg.Remote && opts.to != "":
		return b.copyRemoteIntoSlot(ctx, src, staged)
	case src.arg.Remote:
		return b.referenceRemote(ctx, src, opts)
	}
	return b.stageLocal(ctx, src, staged, opts)
}

// copyRemoteIntoSlot copies a CERNBox path into the slot, on the server.
// The recipient of a handover cannot read the path of the sender, and a
// share of it would give far more than the one thing handed over.
func (b *board) copyRemoteIntoSlot(ctx context.Context, src source, staged string) (Entry, error) {
	info, err := b.Stat(ctx, src.arg.Path)
	if err != nil {
		return Entry{}, err
	}
	if err := b.B.Files().Copy(ctx, userplane.Ref(src.arg.Path), userplane.Ref(staged), true); err != nil {
		return Entry{}, err
	}
	return Entry{Name: src.name, Path: staged, IsDir: userplane.IsDir(info), Size: int64(info.GetSize()), Staged: true, ETag: info.GetEtag()}, nil
}

// referenceRemote records a path that is in CERNBox, without moving a byte.
func (b *board) referenceRemote(ctx context.Context, src source, opts copyOptions) (Entry, error) {
	info, err := b.Stat(ctx, src.arg.Path)
	if err != nil {
		return Entry{}, err
	}
	if userplane.IsDir(info) && !opts.recursive {
		return Entry{}, cli.Usagef("%s is a directory: pass -r to copy it", src.arg.Path)
	}
	return Entry{Name: src.name, Path: src.arg.Path, IsDir: userplane.IsDir(info), Size: int64(info.GetSize()), ETag: info.GetEtag()}, nil
}

// stageLocal uploads a local path into the slot.
func (b *board) stageLocal(ctx context.Context, src source, staged string, opts copyOptions) (Entry, error) {
	info, err := os.Stat(src.arg.Path)
	if err != nil {
		return Entry{}, err
	}
	if info.IsDir() && !opts.recursive {
		return Entry{}, cli.Usagef("%s is a directory: pass -r to copy it", src.arg.Path)
	}
	// On lxplus this may be the FUSE mount of the space that the clipboard is
	// in, and then the upload is a waste of the whole file.
	if strings.HasPrefix(src.arg.Path, "/eos/") {
		b.Msg("Tip: cb:%s would point at the file instead of uploading a copy.", src.arg.Path)
	}
	bar := b.NewMeter(src.name, -1)
	defer bar.Stop()
	e, err := b.Engine(ctx, cli.TransferOptions{Overwrite: true, Verify: opts.verify, Jobs: opts.jobs, Progress: bar.Progress()})
	if err != nil {
		return Entry{}, err
	}
	stats, err := e.UploadTree(ctx, src.arg.Path, staged)
	if err != nil {
		return Entry{}, err
	}
	return Entry{Name: src.name, Path: staged, IsDir: info.IsDir(), Size: stats.Bytes, Staged: true}, nil
}

// stageStdin sends standard input to CERNBox without writing it to local
// disk, and returns its size and its number of pieces.
//
// A pipe has no length, and every upload of reva needs one up front: PUT
// wants Content-Length, and TUS has no creation-defer-length. So the stream
// is read a chunk at a time and each chunk is its own file, whose length is
// known when it goes out. Memory stays at one chunk, and paste joins the
// pieces. A temporary file would need as much free local disk as the
// stream, which is what lxplus does not have. A stream of one chunk is one
// plain file.
func (b *board) stageStdin(ctx context.Context, base string) (int64, int, error) {
	buf := make([]byte, b.RT.Transfer().ChunkSize)
	n, err := io.ReadFull(b.in, buf)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		if err := b.PutBytes(ctx, base, buf[:n]); err != nil {
			return 0, 0, err
		}
		return int64(n), 0, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("reading standard input: %w", err)
	}
	// The pieces go in a folder of their own, which one remove releases.
	if err := b.Mkdir(ctx, base); err != nil {
		return 0, 0, err
	}
	b.Msg("Sending in %s pieces...", output.HumanSize(int64(len(buf))))
	var total int64
	parts := 0
	for {
		if err := b.PutBytes(ctx, PartPath(base, parts), buf[:n]); err != nil {
			return 0, 0, err
		}
		total += int64(n)
		parts++
		n, err = io.ReadFull(b.in, buf)
		switch err {
		case nil, io.ErrUnexpectedEOF:
		case io.EOF:
			return total, parts, nil
		default:
			return 0, 0, fmt.Errorf("reading standard input: %w", err)
		}
	}
}

// streamEntry writes the content of e to w, and joins its pieces. Nothing is
// held beyond the buffer of io.Copy.
func (b *board) streamEntry(ctx context.Context, e Entry, w io.Writer) error {
	if e.Parts == 0 {
		return b.streamOne(ctx, e, e.Path, w)
	}
	for i := range e.Parts {
		if err := b.streamOne(ctx, e, PartPath(e.Path, i), w); err != nil {
			return err
		}
	}
	return nil
}

func (b *board) streamOne(ctx context.Context, e Entry, p string, w io.Writer) error {
	body, _, err := b.Download(ctx, p)
	if err != nil {
		return pasteError(err, e)
	}
	defer body.Close()
	if _, err := io.Copy(w, body); err != nil {
		return fmt.Errorf("pasting %s: %w", e.Name, err)
	}
	return nil
}

// -- paste --------------------------------------------------------------------

func (b *board) paste(ctx context.Context, args []string, opts pasteOptions) error {
	if opts.from != "" && opts.slotGiven {
		return cli.Usagef("--from reads the slot the sender staged for you, which is named after you, " +
			"so it cannot be combined with --slot")
	}
	if err := ValidateSlot(opts.slot); err != nil {
		return cli.Usagef("%v", err)
	}
	in, err := b.pasteSource(ctx, &opts)
	if err != nil {
		return err
	}
	m, err := b.readSlot(ctx, in.root, opts.slot)
	if err != nil {
		return err
	}
	if len(m.Entries) == 0 {
		return fmt.Errorf("the %q clipboard slot is empty", opts.slot)
	}
	if m.Expired(time.Now()) {
		// The bytes are still here, since the manifest was: say that they
		// are stale rather than refuse them.
		b.Warn("this copy expired on %s and may be removed at any time", m.Expires.Local().Format(expiryLayout))
	}
	dest := "."
	if len(args) == 1 {
		dest = args[0]
	}
	// A live handover has a process on the other side, which waits to be
	// told that somebody arrived, and nothing to download until then.
	if m.IsStream() {
		if dest != "-" {
			if a, err := userplane.ParseArg(ctx, b.RT, dest); err == nil && a.Remote {
				return cli.Usagef("%s is sending this live, so it can only be pasted to your computer "+
					"or to standard output. Nothing is stored in CERNBox to copy from.", m.Origin)
			}
		}
		return b.streamPaste(ctx, in.root, m, dest, opts)
	}
	if err := b.pasteStored(ctx, m, dest, opts); err != nil {
		return err
	}
	// A collected handover is plumbing of the CLI: it goes out of the share
	// list of the recipient, which grants and revokes nothing.
	if opts.from != "" {
		b.hideHandover(ctx, in)
	}
	return nil
}

func (b *board) pasteStored(ctx context.Context, m *Manifest, dest string, opts pasteOptions) error {
	if dest == "-" {
		return b.pasteToStdout(ctx, m)
	}
	a, err := userplane.ParseArg(ctx, b.RT, dest)
	if err != nil {
		return err
	}
	if a.Remote {
		return b.pasteRemote(ctx, m, a, opts)
	}
	return b.pasteLocal(ctx, m, a, opts)
}

// pasteLocal downloads the slot to the local file system.
func (b *board) pasteLocal(ctx context.Context, m *Manifest, a userplane.Arg, opts pasteOptions) error {
	dest := a.Path
	// A trailing slash says "into a folder", so make one. Without it the rule
	// is that of cp: a folder that exists means into it.
	if a.TrailingSlash {
		if err := os.MkdirAll(dest, 0o755); err != nil {
			return err
		}
	}
	st, statErr := os.Stat(dest)
	targets, err := pasteTargets(m, dest, statErr == nil && st.IsDir(), filepath.Join)
	if err != nil {
		return err
	}
	// Refused before anything moves, so that a paste of several items
	// happens or does not, and never stops half way.
	if !opts.force {
		if clashes := slices.DeleteFunc(slices.Clone(targets), func(t string) bool { return !userplane.PathTaken(t) }); len(clashes) > 0 {
			return fmt.Errorf("%s already exists: pass -f to overwrite", strings.Join(clashes, ", "))
		}
	}

	bar := b.NewMeter(pasteLabel(m), manifestTotal(m))
	defer bar.Stop()
	e, err := b.Engine(ctx, cli.TransferOptions{Overwrite: true, Verify: opts.verify, NoArchive: opts.noArchive, Jobs: opts.jobs, Progress: bar.Progress()})
	if err != nil {
		return err
	}
	var total int64
	for i, en := range m.Entries {
		bar.SetLabel(en.Name)
		// The pieces of a stream are joined on the way to disk: the engine
		// cannot fetch them as one file.
		if en.Parts > 0 {
			n, err := b.pasteStreamedToFile(ctx, en, targets[i], bar)
			if err != nil {
				return err
			}
			total += n
			continue
		}
		stats, err := e.DownloadTree(ctx, en.Path, targets[i])
		if err != nil {
			return pasteError(err, en)
		}
		total += stats.Bytes
	}
	bar.Stop()
	b.Msg("Pasted %s (%s) into %s", itemCount(len(m.Entries)), userplane.ProseSize(total), dest)
	b.reportSlotSurvives(m, opts.from)
	return b.JSON(m)
}

// pasteRemote copies the slot to another CERNBox path, on the server.
func (b *board) pasteRemote(ctx context.Context, m *Manifest, a userplane.Arg, opts pasteOptions) error {
	dest := a.Path
	if a.TrailingSlash {
		if err := b.Mkdir(ctx, dest); err != nil {
			return err
		}
	}
	info, statErr := b.Stat(ctx, dest)
	targets, err := pasteTargets(m, dest, statErr == nil && userplane.IsDir(info), path.Join)
	if err != nil {
		return err
	}
	if !opts.force {
		var clashes []string
		for _, t := range targets {
			if _, err := b.Stat(ctx, t); err == nil {
				clashes = append(clashes, t)
			}
		}
		if len(clashes) > 0 {
			return fmt.Errorf("%s already exists: pass -f to overwrite", strings.Join(clashes, ", "))
		}
	}

	bar := b.NewMeter(pasteLabel(m), manifestTotal(m))
	defer bar.Stop()
	streamed := false
	for i, e := range m.Entries {
		bar.SetLabel(e.Name)
		// A server-side copy cannot join pieces, so a stream is pulled and
		// pushed back: the one paste inside CERNBox that moves data.
		if e.Parts > 0 {
			streamed = true
			if err := b.pasteStreamedToRemote(ctx, e, targets[i], bar); err != nil {
				return err
			}
			continue
		}
		if err := b.B.Files().Copy(ctx, userplane.Ref(e.Path), userplane.Ref(targets[i]), opts.force); err != nil {
			return pasteError(err, e)
		}
	}
	bar.Stop()
	if streamed {
		b.Msg("Pasted %s (%s) into %s", itemCount(len(m.Entries)), userplane.ProseSize(m.Size()), dest)
	} else {
		b.Msg("Pasted %s (%s) into %s. The server made the copy, so nothing was transferred.",
			itemCount(len(m.Entries)), userplane.ProseSize(m.Size()), dest)
	}
	b.reportSlotSurvives(m, opts.from)
	return b.JSON(m)
}

// pasteToStdout writes one item to stdout: the other half of
// "tar cz dir | cernbox copy -".
func (b *board) pasteToStdout(ctx context.Context, m *Manifest) error {
	if len(m.Entries) != 1 {
		return cli.Usagef("the clipboard holds %s: standard output takes one at a time", itemCount(len(m.Entries)))
	}
	e := m.Entries[0]
	if e.IsDir {
		return cli.Usagef("%s is a directory: paste it to a path rather than to standard output", e.Name)
	}
	bar := b.NewMeter(e.Name, e.Size)
	defer bar.Stop()
	return b.streamEntry(ctx, e, bar.Writer(b.Cmd.OutOrStdout()))
}

// pasteStreamedToFile joins the pieces of e into a local file, as they
// arrive: it needs room for the result and nothing more.
func (b *board) pasteStreamedToFile(ctx context.Context, e Entry, target string, bar *userplane.Meter) (int64, error) {
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return 0, err
	}
	// Written beside the target and renamed, as the transfer engine does: a
	// reader of the target never sees a half-joined file.
	part := target + ".part"
	f, err := os.Create(part)
	if err != nil {
		return 0, err
	}
	err = b.streamEntry(ctx, e, bar.Writer(f))
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(part)
		return 0, err
	}
	if err := os.Rename(part, target); err != nil {
		return 0, err
	}
	return e.Size, nil
}

// pasteStreamedToRemote rebuilds the pieces of e at target through a pipe,
// so the file is never whole anywhere but in flight. The size in the
// manifest is the length that the upload needs.
func (b *board) pasteStreamedToRemote(ctx context.Context, e Entry, target string, bar *userplane.Meter) error {
	pr, pw := io.Pipe()
	go func() {
		// A failed download is a failed upload, not a truncated file.
		pw.CloseWithError(b.streamEntry(ctx, e, bar.Writer(pw)))
	}()
	defer pr.Close()
	return b.D.UploadStream(ctx, userplane.Ref(target), pr, e.Size)
}

// pasteTargets is where each entry lands, with the rule of cp: a folder
// that exists means into it, anything else names the one item.
func pasteTargets(m *Manifest, dest string, destIsDir bool, join func(...string) string) ([]string, error) {
	if destIsDir {
		out := make([]string, 0, len(m.Entries))
		for _, e := range m.Entries {
			out = append(out, join(dest, e.Name))
		}
		return out, nil
	}
	if len(m.Entries) != 1 {
		return nil, cli.Usagef("the clipboard holds %s, so %s has to be an existing directory", itemCount(len(m.Entries)), dest)
	}
	return []string{dest}, nil
}

// pasteError explains what the clipboard points at and is gone: a
// referenced entry is the file of the user, which may have moved since.
func pasteError(err error, e Entry) error {
	if !cli.IsNotFound(err) {
		return err
	}
	if e.Staged {
		return fmt.Errorf("pasting %s: the copy is gone. It may have expired, or been cleared from another computer", e.Name)
	}
	return fmt.Errorf("pasting %s: the clipboard points at %s, which has been moved or deleted since it was copied", e.Name, e.Path)
}

// reportSlotSurvives says that paste did not empty the clipboard, which is
// what a user is most likely to assume wrongly.
func (b *board) reportSlotSurvives(m *Manifest, from string) {
	switch {
	case from != "":
		// The slot of somebody else, on their quota: clear does nothing here.
		b.Msg("It stays on %s's clipboard until they clear it. Paste it again if you need to.", from)
	case !m.HasStaged():
		b.Msg("Still on the clipboard. Paste it again anywhere, or '%s clipboard clear' to forget it.", b.RT.Name())
	default:
		b.Msg("Still on the clipboard, using %s. Free it with '%s clipboard clear%s'.",
			userplane.ProseSize(m.StagedSize()), b.RT.Name(), slotArg(m.Slot))
	}
}

// -- clipboard list and clear -------------------------------------------------

// slotView is a slot in the listing: the manifest, and who handed it over
// when it was not the caller. The manifest is embedded, so that -o json
// keeps its shape with one more key.
type slotView struct {
	*Manifest
	From string `json:"from,omitempty"`
}

func (b *board) list(ctx context.Context) error {
	root, err := b.root(ctx)
	if err != nil {
		return err
	}
	b.collectExpired(ctx, root)
	manifests, err := b.readSlots(ctx, root)
	if err != nil {
		return err
	}
	slots := make([]slotView, 0, len(manifests))
	for _, m := range manifests {
		slots = append(slots, slotView{Manifest: m})
	}
	// What others staged for you is on their quota and pasted with --from,
	// but belongs here: else only they can tell you that they sent it.
	slots = append(slots, b.incomingHandovers(ctx)...)
	if len(slots) == 0 {
		b.Msg("The clipboard is empty. Use '%s copy PATH' to put something on it.", b.RT.Name())
	}
	now := time.Now()
	t := cli.Table{Headers: []string{"Slot", "From", "Contents", "Size", "Origin", "Copied", "Expires"}, Items: slots}
	for _, s := range slots {
		t.Rows = append(t.Rows, []string{
			s.Slot, orDash(s.From), contentsColumn(s.Manifest), output.HumanSize(s.Size()),
			s.Origin.String(), agoColumn(s.Created, now), manifestExpiry(s.Manifest),
		})
	}
	if err := b.RT.Print(b.Cmd.OutOrStdout(), t, nil); err != nil {
		return err
	}
	for _, s := range slots {
		if s.From != "" {
			b.Msg("Collect what %s sent with '%s paste --from %s'.", s.From, b.RT.Name(), s.From)
		}
	}
	return nil
}

func (b *board) clear(ctx context.Context, slots []string, all bool) error {
	root, err := b.root(ctx)
	if err != nil {
		return err
	}
	switch {
	case all && len(slots) > 0:
		return cli.Usagef("--all clears every slot, so it cannot be combined with a slot name")
	case all:
		manifests, err := b.readSlots(ctx, root)
		if err != nil {
			return err
		}
		for _, m := range manifests {
			slots = append(slots, m.Slot)
		}
		if len(slots) == 0 {
			b.Msg("The clipboard is already empty.")
			return b.JSON(map[string]any{"cleared": 0, "freed_bytes": 0})
		}
	case len(slots) == 0:
		slots = []string{DefaultSlot}
	}
	var freed int64
	cleared := 0
	for _, slot := range slots {
		if err := ValidateSlot(slot); err != nil {
			return cli.Usagef("%v", err)
		}
		// Read first, to tell how much quota comes back.
		if m, err := b.readSlot(ctx, root, slot); err == nil {
			freed += m.StagedSize()
		} else if !errors.Is(err, errNoSlot) {
			return err
		}
		// The grants of a handover go before their folder, or they outlive
		// it in the share list, naming a path in the trash.
		b.unshareHandover(ctx, root, slot)
		removed, err := b.RemoveIfPresent(ctx, SlotDir(root, slot))
		if err != nil {
			return err
		}
		if !removed {
			b.Msg("The %q clipboard slot is already empty.", slot)
			continue
		}
		cleared++
		b.Msg("Cleared the %q clipboard slot.", slot)
	}
	if freed > 0 {
		b.Msg("Freed %s. The files are in your trash; '%s trash purge' frees the quota.", userplane.ProseSize(freed), b.RT.Name())
	}
	return b.JSON(map[string]any{"cleared": cleared, "freed_bytes": freed})
}

// -- storage ------------------------------------------------------------------

// pasteSource is the clipboard that paste reads, and sets the slot when it
// is that of somebody else.
func (b *board) pasteSource(ctx context.Context, opts *pasteOptions) (incoming, error) {
	if opts.from == "" {
		root, err := b.root(ctx)
		return incoming{root: root}, err
	}
	me := b.Username(ctx)
	if me == "" {
		return incoming{}, fmt.Errorf("paste --from %s: cannot tell who you are, so cannot tell which slot was staged for you", opts.from)
	}
	opts.slot = HandoverSlot(me)
	return b.senderClipboard(ctx, opts.from, opts.slot)
}

// root is where the clipboard lives: the home space, which every account
// has, and which both machines find without being told.
func (b *board) root(ctx context.Context) (string, error) {
	home, err := b.Home(ctx)
	if err != nil {
		return "", err
	}
	return path.Join(home, Dir), nil
}

func (b *board) readSlot(ctx context.Context, root, slot string) (*Manifest, error) {
	body, _, err := b.Download(ctx, ManifestPath(root, slot))
	if err != nil {
		if cli.IsNotFound(err) {
			return nil, fmt.Errorf("the %q clipboard slot: %w", slot, errNoSlot)
		}
		return nil, err
	}
	defer body.Close()
	data, err := io.ReadAll(io.LimitReader(body, MaxManifestSize))
	if err != nil {
		return nil, fmt.Errorf("reading the %q clipboard slot: %w", slot, err)
	}
	m, err := Decode(data)
	if err != nil {
		return nil, fmt.Errorf("reading the %q clipboard slot: %w", slot, err)
	}
	if m.Slot == "" {
		m.Slot = slot
	}
	return m, nil
}

// readSlots reads every slot, in name order. A slot that cannot be read is
// reported and skipped: one broken slot must not hide the others.
func (b *board) readSlots(ctx context.Context, root string) ([]*Manifest, error) {
	entries, err := b.List(ctx, root)
	if err != nil {
		if cli.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	slices.SortFunc(entries, func(x, y *provider.ResourceInfo) int { return strings.Compare(userplane.Name(x), userplane.Name(y)) })
	var out []*Manifest
	for _, e := range entries {
		if !userplane.IsDir(e) {
			continue
		}
		m, err := b.readSlot(ctx, root, userplane.Name(e))
		if err != nil {
			b.Warn("skipping the %q clipboard slot: %v", userplane.Name(e), err)
			continue
		}
		out = append(out, m)
	}
	return out, nil
}

// priorSlot is what a slot holds before a copy replaces it, and the etag of
// its manifest. A manifest that does not decode is an empty slot: the copy
// replaces it, and refusing would leave a slot that cannot be overwritten.
func (b *board) priorSlot(ctx context.Context, root, slot string) (*Manifest, string, error) {
	info, err := b.Stat(ctx, ManifestPath(root, slot))
	if err != nil {
		if cli.IsNotFound(err) {
			return nil, "", nil
		}
		return nil, "", err
	}
	m, err := b.readSlot(ctx, root, slot)
	if err != nil {
		return nil, info.GetEtag(), nil
	}
	return m, info.GetEtag(), nil
}

// writeSlot stores the manifest. ifMatch is the etag that it must still
// have: another one means that another machine copied to the slot
// meanwhile, and overwriting would orphan its staged bytes in the quota.
func (b *board) writeSlot(ctx context.Context, root string, m *Manifest, ifMatch string) error {
	data, err := m.Encode()
	if err != nil {
		return err
	}
	p := ManifestPath(root, m.Slot)
	if err := b.Mkdir(ctx, path.Dir(p)); err != nil {
		return err
	}
	if ifMatch == "" {
		// reva ignores If-None-Match, so two first writes of one slot at once
		// cannot be told apart; the origin in the manifest shows which won.
		return b.PutBytes(ctx, p, data)
	}
	err = b.D.UploadIfUnchanged(ctx, userplane.Ref(p), func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(string(data))), nil
	}, int64(len(data)), ifMatch)
	if userplane.IsConflict(err) {
		return fmt.Errorf("copy to the %q slot: another computer copied to it at the same time. "+
			"Run '%s clipboard list' to see what it holds, then copy again%s", m.Slot, b.RT.Name(), slotHint(m.Slot))
	}
	return err
}

// discardUnreferenced deletes what the previous copy staged and the new one
// does not, so that a slot replaced over and over does not grow. It runs
// after the manifest is written, and is best effort: the copy succeeded.
func (b *board) discardUnreferenced(ctx context.Context, prior, current *Manifest) {
	if prior == nil {
		return
	}
	kept := map[string]bool{}
	for _, e := range current.Entries {
		kept[e.Path] = true
	}
	for _, e := range prior.Entries {
		// Only the duplicates of the clipboard: a referenced entry is the real
		// file of the user.
		if !e.Staged || kept[e.Path] {
			continue
		}
		if _, err := b.RemoveIfPresent(ctx, e.Path); err != nil {
			b.Warn("could not release the previous copy of %s: %v", e.Name, err)
		}
	}
}

// collectExpired deletes the slots past their expiry, on the commands that
// look at the clipboard anyway. Best effort: it must not fail the command
// that the user ran.
func (b *board) collectExpired(ctx context.Context, root string) {
	entries, err := b.List(ctx, root)
	if err != nil {
		return
	}
	now := time.Now()
	for _, e := range entries {
		if !userplane.IsDir(e) {
			continue
		}
		m, err := b.readSlot(ctx, root, userplane.Name(e))
		if err != nil || !m.Expired(now) {
			continue
		}
		b.unshareHandover(ctx, root, userplane.Name(e))
		if _, err := b.RemoveIfPresent(ctx, SlotDir(root, userplane.Name(e))); err != nil {
			continue
		}
		b.Msg("Removed the expired %q clipboard slot.", userplane.Name(e))
	}
}

// -- display ------------------------------------------------------------------

func contentsColumn(m *Manifest) string {
	switch len(m.Entries) {
	case 0:
		return "-"
	case 1:
		return entryLabel(m.Entries[0])
	}
	return fmt.Sprintf("%s +%d more", entryLabel(m.Entries[0]), len(m.Entries)-1)
}

func entryLabel(e Entry) string {
	if e.IsDir {
		return e.Name + "/"
	}
	return e.Name
}

func agoColumn(t, now time.Time) string {
	if t.IsZero() {
		return "-"
	}
	switch d := now.Sub(t); {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}

func manifestExpiry(m *Manifest) string {
	switch {
	case m.Expires.IsZero():
		return "never"
	case m.Expired(time.Now()):
		return "expired"
	}
	return m.Expires.Local().Format(expiryLayout)
}

func manifestTotal(m *Manifest) int64 {
	var total int64
	for _, e := range m.Entries {
		if e.Size < 0 {
			return -1
		}
		total += e.Size
	}
	return total
}

func pasteLabel(m *Manifest) string {
	if len(m.Entries) == 1 {
		return m.Entries[0].Name
	}
	return itemCount(len(m.Entries))
}

func itemCount(n int) string { return userplane.Plural(n, "item", "items") }

// slotArg is the argument that names a slot in a hint, nothing for the
// default one, so that the hints are the commands to type.
func slotArg(slot string) string {
	if slot == "" || slot == DefaultSlot {
		return ""
	}
	return " " + slot
}

func slotHint(slot string) string {
	if slot == "" || slot == DefaultSlot {
		return ""
	}
	return " with --slot " + slot
}

func pasteHint(name, slot string) string {
	if slot == DefaultSlot {
		return name + " paste"
	}
	return name + " paste --slot " + slot
}

func orDash(s string) string { return cmp.Or(s, "-") }

// workingDir is recorded on a copy, to tell apart two files of one name.
func workingDir() string {
	dir, _ := os.Getwd()
	return dir
}

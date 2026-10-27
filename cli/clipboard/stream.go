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

package clipboard

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/cernbox/reva-plugins/cli/internal/userplane"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/v3/pkg/cli"
)

// A live handover still passes through CERNBox, because a laptop is behind NAT
// and lxplus takes no inbound connections. It saves the storage: the receiver
// deletes each piece as it reads it. Nothing notifies, so both sides poll.

const (
	// streamWindow is how many pieces the sender may run ahead of the receiver.
	streamWindow = 4
	streamPoll   = 250 * time.Millisecond
)

func (b *board) streamCopy(ctx context.Context, src source, opts copyOptions) error {
	root, err := b.root(ctx)
	if err != nil {
		return err
	}
	body, size, err := b.openStreamSource(src)
	if err != nil {
		return err
	}
	defer body.Close()
	chunk := b.RT.Transfer().ChunkSize

	// The old payload would be left with no manifest.
	prior, priorETag, err := b.priorSlot(ctx, root, opts.slot)
	if err != nil {
		return err
	}
	m := NewManifest(opts.slot, time.Now(), opts.ttl, LocalOrigin(workingDir()))
	m.Mode = ModeStream
	m.To = opts.to
	m.Stream = &StreamInfo{ChunkSize: chunk, Window: streamWindow}
	m.Entries = []Entry{{Name: src.name, Path: StreamDir(root, opts.slot), Size: size, Staged: true}}
	if err := b.Mkdir(ctx, StreamDir(root, opts.slot)); err != nil {
		return err
	}
	if err := b.writeSlot(ctx, root, m, priorETag); err != nil {
		return err
	}
	b.discardUnreferenced(ctx, prior, m)

	defer b.teardownStream(root, opts.slot)

	// The recipient must see the slot, and write in the pieces folder to arrive.
	if opts.to != "" {
		if err := b.shareHandover(ctx, SlotDir(root, opts.slot), opts.to, roleViewer); err != nil {
			return err
		}
		if err := b.shareHandover(ctx, StreamDir(root, opts.slot), opts.to, roleEditor); err != nil {
			return err
		}
	}

	b.Msg("Waiting for %s...", b.streamPeer(ctx, opts))
	if err := b.waitForReader(ctx, root, m, opts.wait); err != nil {
		return err
	}
	b.Msg("Connected. Sending %s...", src.name)
	sent, parts, err := b.feedStream(ctx, root, opts.slot, body, chunk, opts.wait)
	if err != nil {
		return err
	}
	if err := b.putJSON(ctx, DonePath(root, opts.slot), Done{Parts: parts, Size: sent}); err != nil {
		return err
	}
	// The teardown would truncate the pieces still in flight.
	if err := b.waitForDrain(ctx, root, opts.slot, opts.wait); err != nil {
		return err
	}
	b.Msg("Sent %s. Nothing was stored in CERNBox.", userplane.ProseSize(sent))
	return b.JSON(m)
}

func (b *board) openStreamSource(src source) (io.ReadCloser, int64, error) {
	if src.stdin {
		return io.NopCloser(b.in), -1, nil
	}
	if src.arg.Remote {
		return nil, 0, cli.Usagef("%s is already in CERNBox, so there is nothing to send. Copy it without "+
			"--stream: pasting it then costs no transfer at all.", src.arg.Raw)
	}
	info, err := os.Stat(src.arg.Path)
	if err != nil {
		return nil, 0, err
	}
	if info.IsDir() {
		return nil, 0, cli.Usagef("%s is a directory: stream a single file, or tar it: 'tar cz %s | %s copy --stream -'",
			src.arg.Path, src.arg.Path, b.RT.Name())
	}
	f, err := os.Open(src.arg.Path)
	if err != nil {
		return nil, 0, err
	}
	return f, info.Size(), nil
}

func (b *board) waitForReader(ctx context.Context, root string, m *Manifest, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	for {
		if _, err := b.Stat(ctx, m.ReaderPath(root)); err == nil {
			return nil
		} else if !cli.IsNotFound(err) {
			return err
		}
		if time.Now().After(deadline) {
			hint := pasteHint(b.RT.Name(), m.Slot)
			if m.To != "" {
				hint = b.RT.Name() + " paste --from " + b.Username(ctx)
			}
			return fmt.Errorf("stream %s: nobody pasted within %s. Run '%s' on the other computer while "+
				"this one waits, or use --wait to give it longer", m.Slot, wait, hint)
		}
		if err := sleepCtx(ctx, streamPoll); err != nil {
			return err
		}
	}
}

func (b *board) feedStream(ctx context.Context, root, slot string, body io.Reader, chunk int64, wait time.Duration) (int64, int, error) {
	buf := make([]byte, chunk)
	var sent int64
	parts := 0
	for {
		n, err := io.ReadFull(body, buf)
		if n > 0 {
			if err := b.awaitWindow(ctx, root, slot, wait); err != nil {
				return 0, 0, err
			}
			if err := b.PutBytes(ctx, ChunkPath(root, slot, parts), buf[:n]); err != nil {
				return 0, 0, err
			}
			sent += int64(n)
			parts++
		}
		switch err {
		case nil:
		case io.EOF, io.ErrUnexpectedEOF:
			return sent, parts, nil
		default:
			return 0, 0, fmt.Errorf("reading the source: %w", err)
		}
	}
}

func (b *board) awaitWindow(ctx context.Context, root, slot string, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	for {
		pending, err := b.List(ctx, StreamDir(root, slot))
		if err != nil {
			return err
		}
		if pendingChunks(pending) < streamWindow {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("stream %s: the other computer stopped reading for %s. It may have been interrupted", slot, wait)
		}
		if err := sleepCtx(ctx, streamPoll); err != nil {
			return err
		}
	}
}

// pendingChunks counts the pieces in flight. It skips the reader marker of a
// handover, which is in the same folder, or the drain would never end.
func pendingChunks(entries []*provider.ResourceInfo) int {
	n := 0
	for _, e := range entries {
		if _, ok := ChunkIndex(userplane.Name(e)); ok {
			n++
		}
	}
	return n
}

func (b *board) waitForDrain(ctx context.Context, root, slot string, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	for {
		pending, err := b.List(ctx, StreamDir(root, slot))
		if err != nil {
			if cli.IsNotFound(err) {
				return nil
			}
			return err
		}
		if pendingChunks(pending) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("stream %s: the other computer did not finish reading within %s", slot, wait)
		}
		if err := sleepCtx(ctx, streamPoll); err != nil {
			return err
		}
	}
}

// teardownStream has a context of its own: it usually runs after an interrupt,
// and a cancelled context would stop the cleanup.
func (b *board) teardownStream(root, slot string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	b.unshareHandover(ctx, root, slot)
	if _, err := b.RemoveIfPresent(ctx, SlotDir(root, slot)); err != nil {
		b.Warn("could not clean up the %q stream slot: %v", slot, err)
	}
}

func (b *board) streamPaste(ctx context.Context, root string, m *Manifest, dest string, opts pasteOptions) error {
	if len(m.Entries) != 1 {
		return fmt.Errorf("the %q clipboard slot is damaged: it does not name a single file", m.Slot)
	}
	e := m.Entries[0]
	out, finish, err := b.openStreamSink(e, dest, opts)
	if err != nil {
		return err
	}
	// This releases the sender.
	if err := b.PutBytes(ctx, m.ReaderPath(root), []byte(LocalOrigin(workingDir()).String()+"\n")); err != nil {
		_ = finish(err)
		return err
	}
	b.Msg("Receiving %s from %s...", e.Name, m.Origin)
	bar := b.NewMeter(e.Name, e.Size)
	defer bar.Stop()
	received, err := b.drainStream(ctx, root, m, bar.Writer(out), opts.wait)
	bar.Stop()
	if ferr := finish(err); err == nil {
		err = ferr
	}
	if err != nil {
		return err
	}
	b.Msg("Received %s", userplane.ProseSize(received))
	return b.JSON(m)
}

// openStreamSink returns the writer and a function that closes it: rename on
// success, remove on failure.
func (b *board) openStreamSink(e Entry, dest string, opts pasteOptions) (io.Writer, func(error) error, error) {
	if dest == "-" {
		return b.Cmd.OutOrStdout(), func(error) error { return nil }, nil
	}
	target := dest
	if st, err := os.Stat(dest); err == nil && st.IsDir() {
		target = filepath.Join(dest, e.Name)
	}
	if !opts.force && userplane.PathTaken(target) {
		return nil, nil, fmt.Errorf("%s already exists: pass -f to overwrite", target)
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return nil, nil, err
	}
	// An interrupted handover must not leave a partial file that looks whole.
	part := target + ".part"
	f, err := os.Create(part)
	if err != nil {
		return nil, nil, err
	}
	finish := func(streamErr error) error {
		closeErr := f.Close()
		if streamErr != nil || closeErr != nil {
			os.Remove(part)
			return closeErr
		}
		return os.Rename(part, target)
	}
	return f, finish, nil
}

// drainStream reads the pieces in order and deletes each one, to make room for
// the next.
//
// A piece can be visible before it is fully written, and reading it would lose
// the rest without an error. So each piece is weighed first: all but the last
// are one chunk, and the last is what the done marker leaves. A piece of the
// wrong size is looked at again later.
func (b *board) drainStream(ctx context.Context, root string, m *Manifest, out io.Writer, wait time.Duration) (int64, error) {
	slot := m.Slot
	var chunk int64
	if m.Stream != nil {
		chunk = m.Stream.ChunkSize
	}
	var received int64
	var done *Done
	deadline := time.Now().Add(wait)
	for next := 0; ; {
		expect := chunk
		if done != nil && next == done.Parts-1 {
			expect = done.Size - received
		}
		body, size, err := b.Download(ctx, ChunkPath(root, slot, next))
		switch {
		case err == nil:
			// A size of -1 cannot be weighed before: the count after must do.
			if expect > 0 && size >= 0 && size != expect {
				body.Close()
				// A short piece is still being written, or is the last one.
				if done == nil {
					fresh, err := b.readDone(ctx, root, slot)
					if err != nil {
						return received, err
					}
					if fresh != nil {
						done = fresh
						continue
					}
				}
				if time.Now().After(deadline) {
					return received, fmt.Errorf("receive %s: piece %d of the stream stopped at %d of %d bytes for %s. "+
						"The other computer may have been interrupted", slot, next, size, expect, wait)
				}
				if err := sleepCtx(ctx, streamPoll); err != nil {
					return received, err
				}
				continue
			}
			n, err := io.Copy(out, body)
			body.Close()
			if err != nil {
				return received, fmt.Errorf("receive %s: %w", slot, err)
			}
			if expect > 0 && n != expect {
				return received, fmt.Errorf("receive %s: piece %d of the stream was %d bytes but %d arrived", slot, next, expect, n)
			}
			received += n
			if _, err := b.RemoveIfPresent(ctx, ChunkPath(root, slot, next)); err != nil {
				return received, err
			}
			next++
			deadline = time.Now().Add(wait)
			continue
		case !cli.IsNotFound(err):
			return received, err
		}
		// No piece yet: the sender is on it, or it is done.
		if done, err = b.readDone(ctx, root, slot); err != nil {
			return received, err
		}
		if done != nil && next >= done.Parts {
			if done.Size != received {
				return received, fmt.Errorf("receive %s: %d bytes were sent but %d arrived", slot, done.Size, received)
			}
			return received, nil
		}
		if time.Now().After(deadline) {
			return received, fmt.Errorf("receive %s: nothing arrived for %s. The other computer may have been interrupted", slot, wait)
		}
		if err := sleepCtx(ctx, streamPoll); err != nil {
			return received, err
		}
	}
}

// readDone returns nil while there is no end marker.
func (b *board) readDone(ctx context.Context, root, slot string) (*Done, error) {
	body, _, err := b.Download(ctx, DonePath(root, slot))
	if err != nil {
		if cli.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	defer body.Close()
	data, err := io.ReadAll(io.LimitReader(body, MaxManifestSize))
	if err != nil {
		return nil, err
	}
	var d Done
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("receive %s: the end marker: %w", slot, err)
	}
	return &d, nil
}

func (b *board) putJSON(ctx context.Context, p string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return b.PutBytes(ctx, p, append(data, '\n'))
}

// sleepCtx waits d, or less when the command is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

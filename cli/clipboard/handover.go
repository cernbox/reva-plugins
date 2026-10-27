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
	"fmt"
	"path"
	"strings"

	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	collaboration "github.com/cs3org/go-cs3apis/cs3/sharing/collaboration/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/v3/pkg/cli"
	"github.com/cs3org/reva/v3/pkg/permissions"
)

type role int

const (
	roleViewer role = iota
	roleEditor
)

func (r role) permissions() *provider.ResourcePermissions {
	if r == roleEditor {
		return permissions.NewEditorRole().CS3ResourcePermissions()
	}
	return permissions.NewViewerRole().CS3ResourcePermissions()
}

// handoverSlotFor refuses --slot, which would share more than the caller meant.
func handoverSlotFor(user string, slotGiven bool) (string, error) {
	if strings.TrimSpace(user) == "" {
		return "", cli.Usagef("--to needs a username")
	}
	if slotGiven {
		return "", cli.Usagef("--to uses a slot of its own, named after the recipient, so it cannot be " +
			"combined with --slot. Everything in that slot is readable by them.")
	}
	slot := HandoverSlot(user)
	if err := ValidateSlot(slot); err != nil {
		return "", cli.Usagef("%q is not a username this can hand over to: %v", user, err)
	}
	return slot, nil
}

func sharedWith(s *collaboration.Share, user string) bool {
	g := s.GetGrantee()
	return g.GetType() == provider.GranteeType_GRANTEE_TYPE_USER && strings.EqualFold(g.GetUserId().GetOpaqueId(), user)
}

// shareHandover gives user the folder dir with the role r, unless they have it.
func (b *board) shareHandover(ctx context.Context, dir, user string, r role) error {
	info, err := b.Stat(ctx, dir)
	if err != nil {
		return err
	}
	if info.GetId() == nil {
		return fmt.Errorf("sharing %s: the server reported no resource id for it", dir)
	}
	// Sharing again is an error on some servers and a duplicate on others.
	if shares, err := b.B.Shares().List(ctx, info.Id); err == nil {
		for _, s := range shares {
			if sharedWith(s, user) {
				return nil
			}
		}
	}
	_, err = b.B.Shares().Create(ctx, &collaboration.CreateShareRequest{
		ResourceInfo: info,
		Grant: &collaboration.ShareGrant{
			Permissions: &collaboration.SharePermissions{Permissions: r.permissions()},
			Grantee: &provider.Grantee{
				Type: provider.GranteeType_GRANTEE_TYPE_USER,
				Id:   &provider.Grantee_UserId{UserId: &userpb.UserId{OpaqueId: user, Type: userpb.UserType_USER_TYPE_PRIMARY}},
			},
		},
	})
	return err
}

// unshareHandover takes back the grants of a handover before its slot is
// removed, or the share would follow the folder to the trash. Failures are only
// warnings, so that the payload is not left behind.
func (b *board) unshareHandover(ctx context.Context, root, slot string) {
	user, ok := HandoverRecipient(slot)
	if !ok {
		return
	}
	// A live handover also shares the pieces folder.
	for _, dir := range []string{StreamDir(root, slot), SlotDir(root, slot)} {
		b.revokeAccess(ctx, dir, user)
	}
}

func (b *board) revokeAccess(ctx context.Context, dir, user string) {
	info, err := b.Stat(ctx, dir)
	if err != nil || info.GetId() == nil {
		return
	}
	shares, err := b.B.Shares().List(ctx, info.Id)
	if err != nil {
		return
	}
	for _, s := range shares {
		if !sharedWith(s, user) {
			continue
		}
		if err := b.B.Shares().Remove(ctx, s.GetId().GetOpaqueId()); err != nil {
			b.Warn("could not take back %s's access to %s: %v", user, dir, err)
		}
	}
}

type incoming struct {
	// root is the clipboard root of the sender.
	root    string
	shareID string
	hidden  bool
}

// handovers returns the slots named slot that were shared with the caller by
// sender, or by anybody when sender is empty. It uses the received shares and
// not a path built from the name of the sender: the share proves the handover.
func (b *board) handovers(ctx context.Context, sender, slot string) ([]incoming, []string, error) {
	recvd, err := b.B.Shares().Received(ctx)
	if err != nil {
		return nil, nil, err
	}
	var out []incoming
	var from []string
	for _, rs := range recvd {
		s := rs.GetShare()
		by := s.GetOwner().GetOpaqueId()
		if sender != "" && !strings.EqualFold(by, sender) && !strings.EqualFold(s.GetCreator().GetOpaqueId(), sender) {
			continue
		}
		info, err := b.B.Files().Stat(ctx, &provider.Reference{ResourceId: s.GetResourceId()})
		if err != nil || path.Base(info.GetPath()) != slot || !strings.HasPrefix(info.GetPath(), "/") {
			continue
		}
		out = append(out, incoming{root: path.Dir(info.GetPath()), shareID: s.GetId().GetOpaqueId(), hidden: rs.GetHidden()})
		from = append(from, by)
	}
	return out, from, nil
}

func (b *board) senderClipboard(ctx context.Context, sender, slot string) (incoming, error) {
	found, _, err := b.handovers(ctx, sender, slot)
	if err != nil {
		return incoming{}, err
	}
	if len(found) == 0 {
		return incoming{}, fmt.Errorf("paste --from %s: nothing has been handed over to you by this person. "+
			"They copy it with '%s copy --to %s PATH'", sender, b.RT.Name(), b.Username(ctx))
	}
	return found[0], nil
}

// hideHandover takes a collected handover out of the share list of the
// recipient. It cannot be created hidden: the flag is on the copy of the
// recipient.
func (b *board) hideHandover(ctx context.Context, in incoming) {
	if in.shareID == "" || in.hidden {
		return
	}
	if err := b.B.Shares().HideReceived(ctx, in.shareID); err != nil {
		b.Warn("could not take this handover out of your share list: %v", err)
	}
}

// incomingHandovers skips a share whose slot has no manifest: the share
// outlives the copy.
func (b *board) incomingHandovers(ctx context.Context) []slotView {
	me := b.Username(ctx)
	if me == "" {
		return nil
	}
	slot := HandoverSlot(me)
	found, from, err := b.handovers(ctx, "", slot)
	if err != nil {
		return nil
	}
	var out []slotView
	for i, in := range found {
		m, err := b.readSlot(ctx, in.root, slot)
		if err != nil {
			continue
		}
		out = append(out, slotView{Manifest: m, From: from[i]})
	}
	return out
}

func (b *board) streamPeer(ctx context.Context, opts copyOptions) string {
	if opts.to != "" {
		return fmt.Sprintf("%s to run '%s paste --from %s'", opts.to, b.RT.Name(), b.Username(ctx))
	}
	return fmt.Sprintf("'%s' on another computer", pasteHint(b.RT.Name(), opts.slot))
}

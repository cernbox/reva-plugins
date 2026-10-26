package otg

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"github.com/cernbox/reva-plugins/otg/api"
	"github.com/cs3org/reva/v3/pkg/admin"
	"github.com/cs3org/reva/v3/pkg/appctx"
	"github.com/cs3org/reva/v3/pkg/invoke"
)

// All the invocations are Shared: the instances read the same table, so one
// instance is sufficient to read or write it.
func (s *Otg) registerInvocations() {
	s.Set = invoke.NewSet()
	s.Set.Add(api.Get, "Show the OTG message that users see, if there is one.").
		Shared().
		Handle(invoke.Typed(s.invokeGet))
	s.Set.Add(api.Set, "Replace the OTG message that users see.").
		Shared().
		Mutating().
		Handle(invoke.Typed(s.invokeSet))
	s.Set.Add(api.Clear, "Remove the OTG message.").
		Shared().
		Mutating().
		Handle(invoke.Typed(s.invokeClear))
}

func (s *Otg) invokeGet(ctx context.Context, _ struct{}) (invoke.Result, error) {
	num, msg, err := s.getOTG(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return result(api.Status{})
	}
	if err != nil {
		return nil, err
	}
	return result(api.Status{Present: true, Number: num, Message: msg})
}

func (s *Otg) invokeSet(ctx context.Context, a api.SetArgs) (invoke.Result, error) {
	number, message := strings.TrimSpace(a.Number), strings.TrimSpace(a.Message)
	// A required argument can be present and empty, and the web interface
	// would show an empty banner.
	if number == "" || message == "" {
		return nil, errors.New("otg: number and message must not be empty")
	}
	err := s.replaceOTG(ctx, number, message)
	audit(ctx, api.Set, number, err)
	if err != nil {
		return nil, err
	}
	return result(api.Status{Present: true, Number: number, Message: message})
}

func (s *Otg) invokeClear(ctx context.Context, _ struct{}) (invoke.Result, error) {
	err := s.replaceOTG(ctx, "", "")
	audit(ctx, api.Clear, "", err)
	if err != nil {
		return nil, err
	}
	return result(api.Status{})
}

// replaceOTG makes the table hold only the given message, or no message if
// number is empty. It uses one transaction, so a reader never sees two
// messages or a cleared table between the two statements.
func (s *Otg) replaceOTG(ctx context.Context, number, message string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // after Commit, Rollback does nothing
	if _, err := tx.ExecContext(ctx, "DELETE FROM cbox_otg_ocis"); err != nil {
		return err
	}
	if number != "" {
		if _, err := tx.ExecContext(ctx, "INSERT INTO cbox_otg_ocis (otg_number, message) VALUES (?, ?)", number, message); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// result encodes st with its JSON tags, so that the keys are the ones a client
// decodes into api.Status.
func result(st api.Status) (invoke.Result, error) {
	b, err := json.Marshal(st)
	if err != nil {
		return nil, err
	}
	var r invoke.Result
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	return r, nil
}

func audit(ctx context.Context, invocation, target string, err error) {
	admin.Audit(ctx, admin.AuditEvent{
		Action:  "otg." + invocation,
		Actor:   actor(ctx),
		Target:  target,
		Kind:    string(invoke.KindMutating),
		Granted: err == nil,
		Err:     err,
	})
}

// actor is the user of the admin token, or the local root user of the Unix
// socket. Reva has no exported helper for this yet.
func actor(ctx context.Context) string {
	if u, ok := appctx.ContextGetUser(ctx); ok && u.GetUsername() != "" {
		return u.GetUsername()
	}
	return "unknown"
}

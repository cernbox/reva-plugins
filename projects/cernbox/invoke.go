package cernbox

import (
	"cmp"
	"context"
	"strconv"
	"time"

	"github.com/cs3org/reva/v3/pkg/admin"
	"github.com/cs3org/reva/v3/pkg/admin/adminpb"
	"github.com/cs3org/reva/v3/pkg/appctx"
	"github.com/cs3org/reva/v3/pkg/errtypes"
	"github.com/cs3org/reva/v3/pkg/invoke"
	"github.com/cs3org/reva/v3/pkg/projects"
)

type setInfoArgs struct {
	ID                      string  `arg:"id" help:"the space id; give id or name"`
	Name                    string  `arg:"name" help:"the project name; give id or name"`
	ServiceAccount          *string `arg:"service_account" help:"the service account of the project"`
	SnowTicket              *string `arg:"snow_ticket" help:"the ServiceNow ticket that asked for the project"`
	BackupName              *string `arg:"backup_name" help:"the name of the project in the backup system"`
	InitialCapacityBytes    *uint64 `arg:"initial_capacity_bytes" help:"the capacity that the ticket asked for, in bytes"`
	UserProvidedDescription *string `arg:"user_provided_description" help:"the use case given in the ticket"`
	Comments                *string `arg:"comments" help:"notes for the second and third level support"`
	ResourceAdmins          *string `arg:"resource_admins" help:"the resource admins group"`
}

// The invocation is shared: all the instances use the same table. It needs no
// flush_cache, because the cached project list does not show these columns.
func (c *Catalogue) registerInvocations() {
	c.invocations = invoke.NewSet()
	c.invocations.Add("set_info", "Set the CERN columns of a project: service account, ticket, backup name, capacity, use case, comments, resource admins.").
		Mutating().Shared().
		Handle(invoke.Typed(c.setInfo))
}

// Invocations implements invoke.Invokable.
func (c *Catalogue) Invocations() []invoke.InvocationSpec {
	return c.invocations.Invocations()
}

// Invoke implements invoke.Invokable.
func (c *Catalogue) Invoke(ctx context.Context, name string, args map[string]any) (invoke.Result, error) {
	return c.invocations.Invoke(ctx, name, args)
}

func (c *Catalogue) setInfo(ctx context.Context, a setInfoArgs) (invoke.Result, error) {
	updates := map[string]any{}
	logged := map[string]string{}
	for col, v := range map[string]*string{
		"service_account":           a.ServiceAccount,
		"snow_ticket":               a.SnowTicket,
		"backup_name":               a.BackupName,
		"user_provided_description": a.UserProvidedDescription,
		"comments":                  a.Comments,
		"resource_admins":           a.ResourceAdmins,
	} {
		if v != nil {
			updates[col], logged[col] = *v, *v
		}
	}
	if a.InitialCapacityBytes != nil {
		updates["initial_capacity_bytes"] = *a.InitialCapacityBytes
		logged["initial_capacity_bytes"] = strconv.FormatUint(*a.InitialCapacityBytes, 10)
	}
	if len(updates) == 0 {
		return nil, errtypes.BadRequest("set_info needs at least one column to set")
	}
	ref := projects.Ref{SpaceID: a.ID, Name: a.Name}
	r, err := c.ProjectsManager.ProjectRecord(ctx, ref)
	if err == nil {
		updates["updated_at"] = time.Now()
		err = c.db.WithContext(ctx).Model(&columns{}).
			Where("space_id = ?", spaceID(r.Space.GetId().GetOpaqueId())).
			Updates(updates).Error
	}
	audit(ctx, cmp.Or(a.Name, a.ID), err, logged)
	if err != nil {
		return nil, err
	}
	if r, err = c.ProjectRecord(ctx, ref); err != nil {
		return nil, err
	}
	return invoke.ProtoResult(&adminpb.AdminSpace{Space: r.Space, Status: string(r.Status), Extra: r.Extra})
}

func audit(ctx context.Context, target string, err error, fields map[string]string) {
	u, _ := appctx.ContextGetUser(ctx)
	admin.Audit(ctx, admin.AuditEvent{
		Action:  "spacesregistry." + invoke.DriverPrefix + "set_info",
		Actor:   u.GetUsername(),
		Target:  "project:" + target,
		Kind:    string(invoke.KindMutating),
		Granted: err == nil,
		Err:     err,
		Fields:  fields,
	})
}

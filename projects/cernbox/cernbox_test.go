package cernbox

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/cs3org/reva/v3/pkg/errtypes"
	"github.com/cs3org/reva/v3/pkg/projects"
)

func newCatalogue(t *testing.T, dbPath string) *Catalogue {
	t.Helper()
	c, err := New(context.Background(), map[string]any{
		"db_engine":              "sqlite",
		"db_name":                dbPath,
		"path_template":          "/eos/project/{{ .Initial }}/{{ .Name }}",
		"storage_id_template":    "newproject-{{ .Initial }}",
		"readers_group_template": "cernbox-project-{{ .Name }}-readers",
		"writers_group_template": "cernbox-project-{{ .Name }}-writers",
		"admins_group_template":  "cernbox-project-{{ .Name }}-admins",
	})
	if err != nil {
		t.Fatal(err)
	}
	return c.(*Catalogue)
}

func register(t *testing.T, c *Catalogue, name string) {
	t.Helper()
	if _, err := c.RegisterProject(context.Background(), projects.Registration{Name: name, Owner: "cboxsvc"}); err != nil {
		t.Fatal(err)
	}
}

func setInfo(t *testing.T, c *Catalogue, args map[string]any) map[string]any {
	t.Helper()
	res, err := c.Invoke(context.Background(), "set_info", args)
	if err != nil {
		t.Fatal(err)
	}
	extra, _ := res["extra"].(map[string]any)
	return extra
}

var cernExtra = map[string]string{
	"service_account":           "svc-alpha",
	"snow_ticket":               "RQF0000001",
	"backup_name":               "alpha-x1y2",
	"initial_capacity_bytes":    "1000000000000",
	"user_provided_description": "data of the alpha experiment",
	"comments":                  "moved from AFS",
	"resource_admins":           "alpha-admins",
}

var cernArgs = func() map[string]any {
	args := map[string]any{"name": "alpha"}
	for k, v := range cernExtra {
		args[k] = v
	}
	return args
}()

func checkExtra(t *testing.T, what string, extra map[string]string) {
	t.Helper()
	for k, v := range cernExtra {
		if extra[k] != v {
			t.Errorf("%s: extra[%s] = %q, want %q", what, k, extra[k], v)
		}
	}
	if extra["created_at"] == "" {
		t.Errorf("%s: the extra of the sql catalogue is missing: %v", what, extra)
	}
}

func TestColumnsReachTheRecords(t *testing.T) {
	ctx := context.Background()
	c := newCatalogue(t, filepath.Join(t.TempDir(), "projects.db"))
	register(t, c, "alpha")
	register(t, c, "beta")

	extra := setInfo(t, c, cernArgs)
	if extra["snow_ticket"] != "RQF0000001" || extra["initial_capacity_bytes"] != "1000000000000" {
		t.Fatalf("set_info result: %v", extra)
	}

	r, err := c.ProjectRecord(ctx, projects.Ref{Name: "alpha"})
	if err != nil {
		t.Fatal(err)
	}
	checkExtra(t, "get", r.Extra)

	recs, err := c.ListProjectRecords(ctx, projects.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 {
		t.Fatalf("list: %d records", len(recs))
	}
	for _, r := range recs {
		switch r.Space.GetName() {
		case "alpha":
			checkExtra(t, "list", r.Extra)
		case "beta":
			if _, ok := r.Extra["snow_ticket"]; ok {
				t.Errorf("list: beta has the columns of alpha: %v", r.Extra)
			}
		}
	}

	r, err = c.SetProjectStatus(ctx, projects.Ref{Name: "alpha"}, projects.ProjectStatusArchived)
	if err != nil {
		t.Fatal(err)
	}
	checkExtra(t, "set_status", r.Extra)

	r, err = c.UnregisterProject(ctx, projects.Ref{Name: "alpha"}, false)
	if err != nil {
		t.Fatal(err)
	}
	checkExtra(t, "soft delete", r.Extra)
}

func TestSetInfoChangesOnlyTheGivenColumns(t *testing.T) {
	ctx := context.Background()
	c := newCatalogue(t, filepath.Join(t.TempDir(), "projects.db"))
	register(t, c, "alpha")
	setInfo(t, c, cernArgs)

	r, err := c.ProjectRecord(ctx, projects.Ref{Name: "alpha"})
	if err != nil {
		t.Fatal(err)
	}
	extra := setInfo(t, c, map[string]any{"id": r.Space.GetId().GetOpaqueId(), "comments": "back on EOS"})
	if extra["comments"] != "back on EOS" || extra["snow_ticket"] != "RQF0000001" {
		t.Fatalf("set_info result: %v", extra)
	}
}

func TestSetInfoErrors(t *testing.T) {
	ctx := context.Background()
	c := newCatalogue(t, filepath.Join(t.TempDir(), "projects.db"))
	register(t, c, "alpha")

	var bad errtypes.BadRequest
	if _, err := c.Invoke(ctx, "set_info", map[string]any{"name": "alpha"}); !errors.As(err, &bad) {
		t.Errorf("no column: %v, want a bad request", err)
	}
	var nf errtypes.NotFound
	if _, err := c.Invoke(ctx, "set_info", map[string]any{"name": "nope", "comments": "x"}); !errors.As(err, &nf) {
		t.Errorf("unknown project: %v, want not found", err)
	}
}

func TestHardDeleteReturnsTheColumns(t *testing.T) {
	ctx := context.Background()
	c := newCatalogue(t, filepath.Join(t.TempDir(), "projects.db"))
	register(t, c, "alpha")
	setInfo(t, c, cernArgs)

	r, err := c.UnregisterProject(ctx, projects.Ref{Name: "alpha"}, true)
	if err != nil {
		t.Fatal(err)
	}
	checkExtra(t, "hard delete", r.Extra)
	if _, err := c.ProjectRecord(ctx, projects.Ref{Name: "alpha"}); err == nil {
		t.Fatal("the project is still there")
	}
}

func TestNullColumns(t *testing.T) {
	ctx := context.Background()
	c := newCatalogue(t, filepath.Join(t.TempDir(), "projects.db"))
	register(t, c, "alpha")
	err := c.db.Exec(`UPDATE projects SET resource_admins = NULL, user_provided_description = NULL, service_account = NULL,
		comments = NULL, snow_ticket = NULL, backup_name = NULL, initial_capacity_bytes = NULL`).Error
	if err != nil {
		t.Fatal(err)
	}
	r, err := c.ProjectRecord(ctx, projects.Ref{Name: "alpha"})
	if err != nil {
		t.Fatal(err)
	}
	for k := range cernExtra {
		if _, ok := r.Extra[k]; ok {
			t.Errorf("extra has %s for a NULL column: %v", k, r.Extra)
		}
	}
}

func TestNewAddsTheMissingColumns(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "projects.db")
	c := newCatalogue(t, dbPath)
	register(t, c, "alpha")
	if err := c.db.Migrator().DropColumn(&columns{}, "SnowTicket"); err != nil {
		t.Fatal(err)
	}

	c = newCatalogue(t, dbPath)
	if !c.db.Migrator().HasColumn(&columns{}, "SnowTicket") {
		t.Fatal("snow_ticket was not added")
	}
	extra := setInfo(t, c, map[string]any{"name": "alpha", "snow_ticket": "RQF0000002"})
	if extra["snow_ticket"] != "RQF0000002" {
		t.Fatalf("set_info result: %v", extra)
	}
}

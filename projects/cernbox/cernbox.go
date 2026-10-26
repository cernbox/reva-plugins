// Package cernbox is the projects catalogue of CERNBox: the sql catalogue of
// reva, with the columns of the projects table that only CERN uses.
package cernbox

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/cs3org/reva/v3"
	"github.com/cs3org/reva/v3/pkg/invoke"
	"github.com/cs3org/reva/v3/pkg/projects"
	"github.com/cs3org/reva/v3/pkg/projects/manager/sql"
	"gorm.io/gorm"
)

func init() {
	reva.RegisterPlugin(plugin{})
}

type plugin struct{}

func (plugin) RevaPlugin() reva.PluginInfo {
	return reva.PluginInfo{
		ID:  "grpc.services.spacesregistry.drivers.cernbox",
		New: New,
	}
}

// Catalogue adds the CERN columns to the extra of each record of the Admin
// API. The users of the CS3 calls do not see them.
type Catalogue struct {
	*sql.ProjectsManager
	db          *gorm.DB
	invocations *invoke.Set
}

// columns are the CERN columns of the projects table. The types are those
// of the reva model that had them, so that both describe the same table.
type columns struct {
	SpaceID        string `gorm:"size:255"`
	ResourceAdmins string
	// The use case given in the ticket that asked for the project.
	UserProvidedDescription string
	ServiceAccount          string
	// Notes for the second and third level support.
	Comments   string
	SnowTicket string
	// The name in the backup system: the project name and a salt.
	BackupName           string `gorm:"size:255"`
	InitialCapacityBytes uint64
}

func (columns) TableName() string { return "projects" }

// fields are the columns, by the field name that gorm migrates.
var fields = []string{"ResourceAdmins", "UserProvidedDescription", "ServiceAccount", "Comments", "SnowTicket", "BackupName", "InitialCapacityBytes"}

// lookupChunk keeps the IN lists of a long list below the variable limit of
// sqlite.
const lookupChunk = 500

// New takes the configuration of the sql catalogue.
func New(ctx context.Context, m map[string]any) (projects.Catalogue, error) {
	cat, err := sql.New(ctx, m)
	if err != nil {
		return nil, err
	}
	pm, ok := cat.(*sql.ProjectsManager)
	if !ok {
		return nil, fmt.Errorf("cernbox: the sql catalogue is a %T", cat)
	}
	db := pm.DB()
	// Only a missing column is added: an AutoMigrate could alter the
	// columns that the production table has already.
	mig := db.Migrator()
	for _, f := range fields {
		if mig.HasColumn(&columns{}, f) {
			continue
		}
		if err := mig.AddColumn(&columns{}, f); err != nil {
			return nil, fmt.Errorf("cernbox: adding the column %s: %w", f, err)
		}
	}
	c := &Catalogue{ProjectsManager: pm, db: db}
	c.registerInvocations()
	return c, nil
}

var _ projects.Admin = (*Catalogue)(nil)

// ListProjectRecords implements projects.Admin.
func (c *Catalogue) ListProjectRecords(ctx context.Context, f projects.Filter) ([]*projects.Record, error) {
	recs, err := c.ProjectsManager.ListProjectRecords(ctx, f)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(recs))
	for _, r := range recs {
		ids = append(ids, spaceID(r.Space.GetId().GetOpaqueId()))
	}
	cols := map[string]columns{}
	for chunk := range slices.Chunk(ids, lookupChunk) {
		got, err := c.load(ctx, "space_id IN ?", chunk)
		if err != nil {
			return nil, err
		}
		maps.Copy(cols, got)
	}
	for _, r := range recs {
		addExtra(r, cols)
	}
	return recs, nil
}

// ProjectRecord implements projects.Admin.
func (c *Catalogue) ProjectRecord(ctx context.Context, ref projects.Ref) (*projects.Record, error) {
	r, err := c.ProjectsManager.ProjectRecord(ctx, ref)
	return c.withExtra(ctx, r, err)
}

// SetProjectStatus implements projects.Admin.
func (c *Catalogue) SetProjectStatus(ctx context.Context, ref projects.Ref, status projects.ProjectStatus) (*projects.Record, error) {
	r, err := c.ProjectsManager.SetProjectStatus(ctx, ref, status)
	return c.withExtra(ctx, r, err)
}

// SetProjectOwner implements projects.Admin.
func (c *Catalogue) SetProjectOwner(ctx context.Context, ref projects.Ref, owner string) (*projects.Record, error) {
	r, err := c.ProjectsManager.SetProjectOwner(ctx, ref, owner)
	return c.withExtra(ctx, r, err)
}

// RegisterProject implements projects.Admin. The CERN columns of a new
// project are empty: driver.set_info sets them.
func (c *Catalogue) RegisterProject(ctx context.Context, r projects.Registration) (*projects.Record, error) {
	rec, err := c.ProjectsManager.RegisterProject(ctx, r)
	return c.withExtra(ctx, rec, err)
}

// UnregisterProject implements projects.Admin.
func (c *Catalogue) UnregisterProject(ctx context.Context, ref projects.Ref, hard bool) (*projects.Record, error) {
	if !hard {
		r, err := c.ProjectsManager.UnregisterProject(ctx, ref, hard)
		return c.withExtra(ctx, r, err)
	}
	// A hard delete removes the row, so the columns are read before. A name
	// can select one of several rows, thus all of them are read.
	var cols map[string]columns
	var err error
	if ref.SpaceID != "" {
		cols, err = c.load(ctx, "space_id = ?", spaceID(ref.SpaceID))
	} else {
		cols, err = c.load(ctx, "name = ?", ref.Name)
	}
	if err != nil {
		return nil, err
	}
	r, err := c.ProjectsManager.UnregisterProject(ctx, ref, hard)
	if err != nil {
		return nil, err
	}
	addExtra(r, cols)
	return r, nil
}

// withExtra takes the results of a call of the sql catalogue.
func (c *Catalogue) withExtra(ctx context.Context, r *projects.Record, err error) (*projects.Record, error) {
	if err != nil {
		return nil, err
	}
	cols, err := c.load(ctx, "space_id = ?", spaceID(r.Space.GetId().GetOpaqueId()))
	if err != nil {
		return nil, err
	}
	addExtra(r, cols)
	return r, nil
}

// load returns the columns of the rows that match, by space id. The model
// has no DeletedAt, so the soft-deleted rows match too.
func (c *Catalogue) load(ctx context.Context, query string, args ...any) (map[string]columns, error) {
	var rows []columns
	if err := c.db.WithContext(ctx).Where(query, args...).Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make(map[string]columns, len(rows))
	for _, r := range rows {
		out[r.SpaceID] = r
	}
	return out, nil
}

// addExtra uses the keys of the reva model that had the columns, so that
// the results do not change when the columns leave it.
func addExtra(r *projects.Record, cols map[string]columns) {
	col, ok := cols[spaceID(r.Space.GetId().GetOpaqueId())]
	if !ok {
		return
	}
	if r.Extra == nil {
		r.Extra = map[string]string{}
	}
	for k, v := range map[string]string{
		"resource_admins":           col.ResourceAdmins,
		"user_provided_description": col.UserProvidedDescription,
		"service_account":           col.ServiceAccount,
		"comments":                  col.Comments,
		"snow_ticket":               col.SnowTicket,
		"backup_name":               col.BackupName,
	} {
		if v != "" {
			r.Extra[k] = v
		}
	}
	if col.InitialCapacityBytes > 0 {
		r.Extra["initial_capacity_bytes"] = strconv.FormatUint(col.InitialCapacityBytes, 10)
	}
}

// spaceID returns the space id of the catalogue from a CS3 space id, which
// is <storage id>$<space id>.
func spaceID(id string) string {
	if _, s, ok := strings.Cut(id, "$"); ok {
		return s
	}
	return id
}

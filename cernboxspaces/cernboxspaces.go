package cernboxspaces

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"

	group "github.com/cs3org/go-cs3apis/cs3/identity/group/v1beta1"
	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	"github.com/cs3org/reva/v3"
	"github.com/cs3org/reva/v3/pkg/appctx"
	"github.com/cs3org/reva/v3/pkg/errtypes"
	"github.com/cs3org/reva/v3/pkg/rgrpc/todo/pool"
	"github.com/cs3org/reva/v3/pkg/rhttp/global"
	"github.com/cs3org/reva/v3/pkg/sharedconf"
	"github.com/cs3org/reva/v3/pkg/utils/cfg"
	"github.com/go-chi/chi/v5"
	"github.com/juliangruber/go-intersect"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

func init() {
	reva.RegisterPlugin(cboxProj{})
}

type SpaceType int

const (
	SpaceType_INVALID SpaceType = iota
	SpaceType_ALL
	SpaceType_EOSPROJECT
	SpaceType_WINSPACE
)

type cboxProj struct {
	log    *zerolog.Logger
	c      *config
	db     *sql.DB
	router *chi.Mux
}

func (cboxProj) RevaPlugin() reva.PluginInfo {
	return reva.PluginInfo{
		ID:  "http.services.cernboxspaces",
		New: New,
	}
}

type config struct {
	Username              string `mapstructure:"username"`
	Password              string `mapstructure:"password"`
	Host                  string `mapstructure:"host"`
	Port                  int    `mapstructure:"port"`
	Name                  string `mapstructure:"name"`
	Table                 string `mapstructure:"table"`
	Prefix                string `mapstructure:"prefix"`
	GatewaySvc            string `mapstructure:"gatewaysvc"`
	SkipUserGroupsInToken bool   `mapstructure:"skip_user_groups_in_token"`
}

type project struct {
	Name        string `json:"name,omitempty"`
	Path        string `json:"path,omitempty"`
	Permissions string `json:"permissions,omitempty"`
}

var projectRegex = regexp.MustCompile(`^cernbox-project-(?P<Name>.+)-(?P<Permissions>admins|writers|readers)\z`)

func (c *config) ApplyDefaults() {
	if c.Prefix == "" {
		c.Prefix = "cernboxspaces"
	}

	c.GatewaySvc = sharedconf.GetGatewaySVC(c.GatewaySvc)

	c.SkipUserGroupsInToken = c.SkipUserGroupsInToken || sharedconf.SkipUserGroupsInToken()
}

func New(ctx context.Context, m map[string]interface{}) (global.Service, error) {
	var c config
	if err := cfg.Decode(m, &c); err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", c.Username, c.Password, c.Host, c.Port, c.Name))
	if err != nil {
		return nil, errors.Wrap(err, "error creating sql connection")
	}

	r := chi.NewRouter()

	log := appctx.GetLogger(ctx)
	p := &cboxProj{
		log:    log,
		c:      &c,
		db:     db,
		router: r,
	}

	p.initRouter()

	return p, nil
}

func (p *cboxProj) initRouter() {
	p.router.Get("/{project}/admins", p.GetProjectAdmins)
	p.router.Get("/", p.GetProjectsHandler)
}

func (p *cboxProj) Handler() http.Handler {
	return p.router
}

func encodeProjectsInJSON(p []*project) ([]byte, error) {
	out := struct {
		Projects []*project `json:"projects,omitempty"`
	}{
		Projects: p,
	}
	return json.Marshal(out)
}

func (p *cboxProj) Prefix() string {
	return p.c.Prefix
}

func (p *cboxProj) Close() error {
	return p.db.Close()
}

func (p *cboxProj) Unprotected() []string {
	return nil
}

func (p *cboxProj) GetProjectsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var sType SpaceType
	switch chi.URLParam(r, "type") {
	case "eosprojects":
		sType = SpaceType_EOSPROJECT
	case "winspaces":
		sType = SpaceType_WINSPACE
	default:
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	spaces, err := p.getSpaces(ctx, sType)
	if err != nil {
		if errors.Is(err, errtypes.UserRequired("")) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}

	data, err := encodeProjectsInJSON(spaces)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(data)
}

func (p *cboxProj) GetProjectAdmins(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user, ok := appctx.ContextGetUser(ctx)
	if !ok {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	project := chi.URLParam(r, "project")
	if !p.userHasAccessToProject(ctx, user, project) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	admins, err := p.getProjectAdmins(ctx, project)
	if err != nil {
		// TODO: better error handling
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	d, err := json.Marshal(admins)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(d)
}

type user struct {
	Username    string `json:"username"`
	Mail        string `json:"mail"`
	DisplayName string `json:"display_name"`
}

func (p *cboxProj) userHasAccessToProject(ctx context.Context, user *userpb.User, spaceName string) bool {
	spaces, err := p.getSpaces(ctx, SpaceType_ALL)
	if err != nil {
		return false
	}

	for _, s := range spaces {
		if s.Name == spaceName {
			return true
		}
	}
	return false
}

func (p *cboxProj) getProjectAdmins(ctx context.Context, project string) ([]user, error) {
	client, err := pool.GetGatewayServiceClient(pool.Endpoint(p.c.GatewaySvc))
	if err != nil {
		return nil, err
	}

	g := fmt.Sprintf("cernbox-project-%s-admins", project)

	res, err := client.GetMembers(ctx, &group.GetMembersRequest{
		GroupId: &group.GroupId{
			OpaqueId: g,
		},
	})

	switch {
	case err != nil:
		return nil, err
	case res.Status.Code == rpc.Code_CODE_NOT_FOUND:
		return nil, errtypes.NotFound(fmt.Sprintf("group %s not found", g))
	case res.Status.Code != rpc.Code_CODE_OK:
		return nil, errtypes.InternalError(res.Status.Message)
	}

	users := make([]user, 0, len(res.Members))
	for _, m := range res.Members {
		resUser, err := client.GetUser(ctx, &userpb.GetUserRequest{
			UserId: m,
		})

		switch {
		case err != nil:
			return nil, err
		case res.Status.Code == rpc.Code_CODE_NOT_FOUND:
			return nil, errtypes.NotFound(fmt.Sprintf("user %s not found", m.OpaqueId))
		case res.Status.Code != rpc.Code_CODE_OK:
			return nil, errtypes.InternalError(res.Status.Message)
		}

		if u := resUser.GetUser(); u != nil {
			users = append(users, user{
				Username:    u.Username,
				Mail:        u.Mail,
				DisplayName: u.DisplayName,
			})
		}
	}

	return users, nil
}

func (p *cboxProj) getSpaces(ctx context.Context, sType SpaceType) ([]*project, error) {
	user, ok := appctx.ContextGetUser(ctx)
	if !ok {
		return nil, errtypes.UserRequired("")
	}

	groups := user.Groups
	if p.c.SkipUserGroupsInToken {
		var err error
		groups, err = p.getUserGroups(ctx, user)
		if err != nil {
			return nil, errors.Wrap(err, "error getting user groups")
		}
	}

	userProjects := make(map[string]string)
	var userProjectsKeys []string

	for _, group := range groups {
		match := projectRegex.FindStringSubmatch(group)
		if match != nil {
			if userProjects[match[1]] == "" {
				userProjectsKeys = append(userProjectsKeys, match[1])
			}
			userProjects[match[1]] = getHigherPermission(userProjects[match[1]], match[2])
		}
	}

	if len(userProjectsKeys) == 0 {
		// User has no projects... lets bail
		return []*project{}, nil
	}

	var dbProjects []string
	dbProjectsPaths := make(map[string]string)
	dbProjectsStorages := make(map[string]string)
	query := fmt.Sprintf("SELECT project_name, eos_relative_path, storage FROM %s", p.c.Table)
	switch {
	case sType == SpaceType_EOSPROJECT:
		query = query + " WHERE storage = 'eos'"
	case sType == SpaceType_WINSPACE:
		query = query + " WHERE storage = 'cephfs'"
	case sType == SpaceType_ALL:
	default:
		return nil, errtypes.BadRequest("Invalid space type")
	}
	results, err := p.db.Query(query)
	if err != nil {
		return nil, errors.Wrap(err, "error getting projects from db")
	}

	for results.Next() {
		var name string
		var path string
		var storage string
		err = results.Scan(&name, &path, &storage)
		if err != nil {
			return nil, errors.Wrap(err, "error scanning rows from db")
		}
		dbProjects = append(dbProjects, name)
		dbProjectsPaths[name] = path
		dbProjectsStorages[name] = storage
	}

	validProjects := intersect.Simple(dbProjects, userProjectsKeys)

	var projects []*project
	for _, p := range validProjects {
		name := p.(string)
		permissions := userProjects[name]
		switch storage := dbProjectsStorages[name]; storage {
		case "eos":
			projects = append(projects, &project{
				Name:        name,
				Path:        fmt.Sprintf("/eos/project/%s", dbProjectsPaths[name]),
				Permissions: permissions[:len(permissions)-1],
			})
		case "cephfs":
			projects = append(projects, &project{
				Name:        name,
				Path:        fmt.Sprintf("/winspaces/%s", dbProjectsPaths[name]),
				Permissions: permissions[:len(permissions)-1],
			})
		default:
			continue
		}
	}

	return projects, nil
}

func (p *cboxProj) getUserGroups(ctx context.Context, user *userpb.User) ([]string, error) {
	client, err := pool.GetGatewayServiceClient(pool.Endpoint(p.c.GatewaySvc))
	if err != nil {
		return nil, err
	}

	res, err := client.GetUserGroups(context.Background(), &userpb.GetUserGroupsRequest{UserId: user.Id})
	if err != nil {
		return nil, err
	}

	return res.Groups, nil
}

var permissionsLevel = map[string]int{
	"admins":  1,
	"writers": 2,
	"readers": 3,
}

func getHigherPermission(perm1, perm2 string) string {
	if perm1 == "" {
		return perm2
	}
	if permissionsLevel[perm1] < permissionsLevel[perm2] {
		return perm1
	}
	return perm2
}

// Copyright 2018-2023 CERN
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

package cback

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"text/template"
	"time"

	"github.com/Masterminds/sprig"
	cbackfs "github.com/cernbox/reva-plugins/cback/storage"
	cback "github.com/cernbox/reva-plugins/cback/utils"
	gateway "github.com/cs3org/go-cs3apis/cs3/gateway/v1beta1"
	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	storage "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/cs3org/reva/v3"
	"github.com/cs3org/reva/v3/pkg/appctx"
	"github.com/cs3org/reva/v3/pkg/rgrpc/todo/pool"
	"github.com/cs3org/reva/v3/pkg/rhttp/global"
	"github.com/cs3org/reva/v3/pkg/sharedconf"
	"github.com/go-chi/chi/v5"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
)

func init() {
	reva.RegisterPlugin(svc{})
}

type config struct {
	Prefix            string `mapstructure:"prefix"`
	Token             string `mapstructure:"token"`
	URL               string `mapstructure:"url"`
	Insecure          bool   `mapstructure:"insecure"`
	Timeout           int    `mapstructure:"timeout"`
	GatewaySvc        string `mapstructure:"gatewaysvc"`
	StorageID         string `mapstructure:"storage_id"`
	TemplateToStorage string `mapstructure:"template_to_storage"`
	TemplateToCback   string `mapstructure:"template_to_cback"`
}

type svc struct {
	config     *config
	router     *chi.Mux
	client     *cback.Client
	gw         gateway.GatewayAPIClient
	tplStorage *template.Template
	tplCback   *template.Template
}

func (svc) RevaPlugin() reva.PluginInfo {
	return reva.PluginInfo{
		ID:  "http.services.cback",
		New: New,
	}
}

var _ global.NewService = New

// New returns a new cback http service.
func New(ctx context.Context, m map[string]interface{}) (global.Service, error) {
	c := &config{}
	if err := mapstructure.Decode(m, c); err != nil {
		return nil, errors.Wrap(err, "cback: error decodinf config")
	}

	c.init()

	gw, err := pool.GetGatewayServiceClient(pool.Endpoint(c.GatewaySvc))
	if err != nil {
		return nil, errors.Wrap(err, "cback: error getting gateway client")
	}

	tplStorage, err := template.New("tpl_storage").Funcs(sprig.TxtFuncMap()).Parse(c.TemplateToStorage)
	if err != nil {
		return nil, errors.Wrap(err, "cback: error creating template")
	}

	tplCback, err := template.New("tpl_cback").Funcs(sprig.TxtFuncMap()).Parse(c.TemplateToCback)
	if err != nil {
		return nil, errors.Wrap(err, "cback: error creating template")
	}

	r := chi.NewRouter()
	s := &svc{
		config: c,
		gw:     gw,
		router: r,
		client: cback.New(&cback.Config{
			URL:     c.URL,
			Token:   c.Token,
			Timeout: c.Timeout,
		}),
		tplStorage: tplStorage,
		tplCback:   tplCback,
	}

	s.initRouter()

	return s, nil
}

// Close cleanup the cback http service.
func (s *svc) Close() error {
	return nil
}

func (c *config) init() {
	if c.Prefix == "" {
		c.Prefix = "cback"
	}
	if c.TemplateToStorage == "" {
		c.TemplateToStorage = "{{.}}"
	}
	if c.TemplateToCback == "" {
		c.TemplateToCback = "{{.}}"
	}
	c.GatewaySvc = sharedconf.GetGatewaySVC(c.GatewaySvc)
}

func (s *svc) Prefix() string {
	return s.config.Prefix
}

func (s *svc) Unprotected() []string {
	return nil
}

func (s *svc) initRouter() {
	s.router.Get("/restores", s.getRestores)
	s.router.Get("/restores/{id}", s.getRestoreByID)
	s.router.Post("/restores", s.createRestore)

	s.router.Get("/backups", s.getBackups)
}

type restoreOut struct {
	ID          int       `json:"id"`
	Path        string    `json:"path"`
	Destination string    `json:"destination"`
	Status      int       `json:"status"`
	Created     time.Time `json:"created"`
}

func (s *svc) convertToRestoureOut(r *cback.Restore) *restoreOut {
	dest, _ := getPath(r.Destionation, s.tplStorage)
	return &restoreOut{
		ID:          r.ID,
		Path:        r.Pattern,
		Destination: dest,
		Status:      r.Status,
		Created:     r.Created.Time,
	}
}

func (s *svc) createRestore(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	user, ok := appctx.ContextGetUser(ctx)
	if !ok {
		http.Error(w, "user not authenticated", http.StatusUnauthorized)
		return
	}

	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "missing path", http.StatusBadRequest)
		return
	}

	stat, err := s.gw.Stat(ctx, &storage.StatRequest{
		Ref: &storage.Reference{
			Path: path,
		},
	})

	switch {
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	case stat.Status.Code == rpc.Code_CODE_NOT_FOUND:
		http.Error(w, stat.Status.Message, http.StatusNotFound)
		return
	case stat.Status.Code != rpc.Code_CODE_OK:
		http.Error(w, stat.Status.Message, http.StatusInternalServerError)
		return
	}

	if stat.Info.Id == nil || stat.Info.Id.StorageId != s.config.StorageID {
		http.Error(w, fmt.Sprintf("path not belonging to %s storage driver", s.config.StorageID), http.StatusBadRequest)
		return
	}

	path, snapshotID, backupID, ok := cbackfs.GetBackupInfo(stat.Info.Id)
	if !ok {
		http.Error(w, "cannot restore the given path", http.StatusBadRequest)
		return
	}

	restore, err := s.client.NewRestore(ctx, user.Username, backupID, s.cbackPath(path), snapshotID, true)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.writeJSON(w, s.convertToRestoureOut(restore))
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func (s *svc) cbackPath(p string) string {
	return must(getPath(p, s.tplCback))
}

func (s *svc) getRestores(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	user, ok := appctx.ContextGetUser(ctx)
	if !ok {
		http.Error(w, "user not authenticated", http.StatusUnauthorized)
		return
	}

	list, err := s.client.ListRestores(ctx, user.Username)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := make([]*restoreOut, 0, len(list))
	for _, r := range list {
		res = append(res, s.convertToRestoureOut(r))
	}

	s.writeJSON(w, res)
}

func (s *svc) writeJSON(w http.ResponseWriter, r any) {
	w.WriteHeader(http.StatusOK)
	w.Header().Add("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(r)
}

func (s *svc) getRestoreByID(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	user, ok := appctx.ContextGetUser(ctx)
	if !ok {
		http.Error(w, "user not authenticated", http.StatusUnauthorized)
		return
	}

	id := chi.URLParam(r, "id")
	restoreID, err := strconv.ParseInt(id, 10, 32)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	restore, err := s.client.GetRestore(ctx, user.Username, int(restoreID))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.writeJSON(w, s.convertToRestoureOut(restore))
}

func getPath(p string, tpl *template.Template) (string, error) {
	var b bytes.Buffer
	if err := tpl.Execute(&b, p); err != nil {
		return "", err
	}
	return b.String(), nil
}

func (s *svc) getBackups(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	user, ok := appctx.ContextGetUser(ctx)
	if !ok {
		http.Error(w, "user not authenticated", http.StatusUnauthorized)
		return
	}

	list, err := s.client.ListBackups(ctx, user.Username)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	paths := make([]string, 0, len(list))
	for _, b := range list {
		d, err := getPath(b.Source, s.tplStorage)
		if err != nil {
			continue
		}
		paths = append(paths, d)
	}

	s.writeJSON(w, paths)
}

func (s *svc) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.router.ServeHTTP(w, r)
	})
}

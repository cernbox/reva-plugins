package web

import (
	"context"
	"net/http"

	"github.com/cs3org/reva"
	"github.com/cs3org/reva/pkg/appctx"
	"github.com/cs3org/reva/pkg/rhttp/global"
	"github.com/cs3org/reva/pkg/utils/cfg"
	"github.com/rs/zerolog"
)

func init() {
	reva.RegisterPlugin(web{})
}

type web struct {
	log *zerolog.Logger
	c   *config
}

func (web) RevaPlugin() reva.PluginInfo {
	return reva.PluginInfo{
		ID:  "http.services.web",
		New: New,
	}
}

type config struct {
	Prefix string `mapstructure:"prefix"`
}

func (c *config) ApplyDefaults() {
	if c.Prefix == "" {
		c.Prefix = "web"
	}
}

func New(ctx context.Context, m map[string]interface{}) (global.Service, error) {
	var c config
	if err := cfg.Decode(m, &c); err != nil {
		return nil, err
	}

	log := appctx.GetLogger(ctx)

	e := &web{log: log, c: &c}
	return e, nil
}

func (e *web) Handler() http.Handler {
	httpfsWeb := http.FS(webFS)
	httpfsWebExt := http.FS(webExtFS)

	webHandler := http.FileServer(httpfsWeb)
	webExtHandler := http.FileServer(httpfsWebExt)

	mux := http.NewServeMux()
	mux.Handle("/cernbox", webHandler)
	mux.Handle("/", webExtHandler)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	})
}

func (e *web) Prefix() string {
	return e.c.Prefix
}

func (e *web) Close() error {
	return nil
}

func (e *web) Unprotected() []string {
	return []string{"/"}
}

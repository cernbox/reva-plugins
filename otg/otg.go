package otg

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/cs3org/reva/v3"
	"github.com/cs3org/reva/v3/pkg/rhttp/global"
	"github.com/cs3org/reva/v3/pkg/utils/cfg"
)

func init() {
	reva.RegisterPlugin(Otg{})
}

type config struct {
	Prefix     string `mapstructure:"prefix"`
	DbUsername string `mapstructure:"db_username"`
	DbPassword string `mapstructure:"db_password"`
	DbHost     string `mapstructure:"db_host"`
	DbPort     int    `mapstructure:"db_port"`
	DbName     string `mapstructure:"db_name"`
}

// New returns a new otg service
func New(ctx context.Context, m map[string]interface{}) (global.Service, error) {
	var c config
	if err := cfg.Decode(m, &c); err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", c.DbUsername, c.DbPassword, c.DbHost, c.DbPort, c.DbName))
	if err != nil {
		return nil, err
	}

	return &Otg{conf: &c, db: db}, nil
}

// Close performs cleanup.
func (s *Otg) Close() error {
	return s.db.Close()
}

func (c *config) ApplyDefaults() {
	if c.Prefix == "" {
		c.Prefix = "otg"
	}
}

// Otg is an HTTP service that
// expose an otg to the user.
type Otg struct {
	conf *config
	db   *sql.DB
}

func (Otg) RevaPlugin() reva.PluginInfo {
	return reva.PluginInfo{
		ID:  "http.services.otg",
		New: New,
	}
}

func (s *Otg) Prefix() string {
	return s.conf.Prefix
}

func (s *Otg) Unprotected() []string {
	return nil
}

func (s *Otg) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			code := http.StatusMethodNotAllowed
			http.Error(w, http.StatusText(code), code)
			return
		}

		num, msg, err := s.getOTG(r.Context())
		if err != nil {
			var code int
			if errors.Is(err, sql.ErrNoRows) {
				code = http.StatusNoContent
			} else {
				code = http.StatusInternalServerError
			}
			http.Error(w, http.StatusText(code), code)
			return
		}

		encodeMessageAndSend(w, num, msg)
	})
}

func encodeMessageAndSend(w http.ResponseWriter, num string, msg string) {
	res := struct {
		Number  string `json:"number"`
		Message string `json:"message"`
	}{
		Number:  num,
		Message: msg,
	}
	data, err := json.Marshal(&res)
	if err != nil {
		code := http.StatusInternalServerError
		http.Error(w, http.StatusText(code), code)
		return
	}
	w.Write(data)
}

func (s *Otg) getOTG(ctx context.Context) (string, string, error) {
	row := s.db.QueryRowContext(ctx, "SELECT otg_number, message FROM cbox_otg_ocis")
	if row.Err() != nil {
		return "", "", row.Err()
	}

	var num string
	var msg string
	if err := row.Scan(&num, &msg); err != nil {
		return "", "", err
	}

	return num, msg, nil
}

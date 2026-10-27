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

package cernsso

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/cs3org/reva/v3/pkg/cli"
	"github.com/cs3org/reva/v3/pkg/cli/auth"
	"github.com/cs3org/reva/v3/pkg/cli/brand"
	"github.com/cs3org/reva/v3/pkg/cli/kerberos"
)

type fakeIDP struct {
	ts *httptest.Server

	rejectNegotiate bool
	noRedirect      bool
	otherState      bool
	authError       string

	mu           sync.Mutex
	gotNegotiate string
	gotChallenge string
	gotVerifier  string
	gotScope     string
	gotGrant     string
	gotRefresh   string
	issued       int
}

func newFakeIDP(t *testing.T) *fakeIDP {
	t.Helper()
	idp := &fakeIDP{}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer":                 idp.ts.URL,
			"authorization_endpoint": idp.ts.URL + "/authorize",
			"token_endpoint":         idp.ts.URL + "/token",
		})
	})
	mux.HandleFunc("/authorize", func(w http.ResponseWriter, r *http.Request) {
		idp.mu.Lock()
		defer idp.mu.Unlock()
		q := r.URL.Query()
		idp.gotNegotiate = r.Header.Get("Authorization")
		idp.gotChallenge = q.Get("code_challenge")
		idp.gotScope = q.Get("scope")
		switch {
		case idp.rejectNegotiate || !strings.HasPrefix(idp.gotNegotiate, "Negotiate "):
			w.WriteHeader(http.StatusUnauthorized)
			return
		case idp.noRedirect:
			w.WriteHeader(http.StatusOK)
			return
		}
		redirect, _ := url.Parse(q.Get("redirect_uri"))
		params := url.Values{}
		if idp.authError != "" {
			params.Set("error", idp.authError)
			params.Set("error_description", "the client may not do that")
		} else {
			params.Set("code", "auth-code-123")
			params.Set("state", q.Get("state"))
			if idp.otherState {
				params.Set("state", "another-state")
			}
		}
		redirect.RawQuery = params.Encode()
		w.Header().Set("Location", redirect.String())
		w.WriteHeader(http.StatusFound)
	})
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		idp.mu.Lock()
		defer idp.mu.Unlock()
		_ = r.ParseForm()
		idp.gotGrant = r.Form.Get("grant_type")
		oauthError := func(code, desc string) {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": code, "error_description": desc})
		}
		switch idp.gotGrant {
		case "authorization_code":
			idp.gotVerifier = r.Form.Get("code_verifier")
			if idp.gotVerifier == "" {
				oauthError("invalid_request", "PKCE verifier missing")
				return
			}
			if r.Form.Get("code") != "auth-code-123" {
				oauthError("invalid_grant", "unknown code")
				return
			}
		case "refresh_token":
			idp.gotRefresh = r.Form.Get("refresh_token")
			if idp.gotRefresh != "refresh-me" {
				oauthError("invalid_grant", "refresh token expired")
				return
			}
		default:
			oauthError("unsupported_grant_type", idp.gotGrant)
			return
		}
		idp.issued++
		resp := map[string]any{
			"access_token": fmt.Sprintf("access-token-%d", idp.issued),
			"token_type":   "Bearer",
			"expires_in":   1200,
		}
		if idp.gotGrant == "authorization_code" {
			resp["refresh_token"] = "refresh-me"
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	idp.ts = httptest.NewServer(mux)
	t.Cleanup(idp.ts.Close)
	return idp
}

type stubTicket struct {
	err    error
	negErr error
	spn    string
}

func (s *stubTicket) Ticket() (*kerberos.Ticket, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &kerberos.Ticket{Principal: "einstein@CERN.CH", Realm: "CERN.CH"}, nil
}

func (s *stubTicket) Negotiate(spn string) (string, error) {
	s.spn = spn
	if s.negErr != nil {
		return "", s.negErr
	}
	return "spnego-for-" + spn, nil
}

func provider(idp *fakeIDP, tk *stubTicket) *Provider {
	return &Provider{
		OIDC:       auth.OIDCConfig{Issuer: idp.ts.URL, ClientID: "cernbox-cli"},
		HTTPClient: idp.ts.Client(),
		Ticket:     tk,
	}
}

func TestToken(t *testing.T) {
	idp := newFakeIDP(t)
	tk := &stubTicket{}
	tok, err := provider(idp, tk).Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tok.Kind != auth.KindBearer || tok.Value != "access-token-1" || tok.Provider != Name {
		t.Errorf("token %+v, want the bearer token of the IdP from %s", tok, Name)
	}
	if tok.RefreshToken != "refresh-me" {
		t.Errorf("refresh token %q, want one, so that a shell loop renews it cheaply", tok.RefreshToken)
	}
	if tok.Expiry.IsZero() {
		t.Error("no expiry from expires_in")
	}
	if tok.Subject != "einstein" {
		t.Errorf("subject %q, want the principal without its realm", tok.Subject)
	}
	host := strings.TrimPrefix(idp.ts.URL, "http://")
	host, _, _ = strings.Cut(host, ":")
	if tk.spn != "HTTP/"+host {
		t.Errorf("SPN %q, want HTTP/<issuer host>", tk.spn)
	}
	if idp.gotNegotiate != "Negotiate spnego-for-"+tk.spn {
		t.Errorf("the authorization request carried %q, want the SPNEGO token", idp.gotNegotiate)
	}
	if !strings.Contains(idp.gotScope, "offline_access") {
		t.Errorf("scope %q, want offline_access for a refresh token", idp.gotScope)
	}
	if idp.gotVerifier == "" || challengeOf(idp.gotVerifier) != idp.gotChallenge {
		t.Errorf("challenge %q is not S256 of the verifier %q", idp.gotChallenge, idp.gotVerifier)
	}
}

func TestTokenSPNOfConfig(t *testing.T) {
	idp := newFakeIDP(t)
	tk := &stubTicket{}
	p := provider(idp, tk)
	p.SPN = "HTTP/auth.cern.ch"
	if _, err := p.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	if tk.spn != "HTTP/auth.cern.ch" {
		t.Errorf("SPN %q, want the one of the config", tk.spn)
	}
}

func TestTokenErrors(t *testing.T) {
	for name, c := range map[string]struct {
		idp  func(*fakeIDP)
		tk   stubTicket
		want string
	}{
		"ticket refused":  {idp: func(i *fakeIDP) { i.rejectNegotiate = true }, want: "kinit"},
		"no redirect":     {idp: func(i *fakeIDP) { i.noRedirect = true }, want: "Kerberos may not be enabled"},
		"another state":   {idp: func(i *fakeIDP) { i.otherState = true }, want: "another state"},
		"oauth error":     {idp: func(i *fakeIDP) { i.authError = "unauthorized_client" }, want: "unauthorized_client"},
		"negotiate fails": {tk: stubTicket{negErr: errors.New("the KDC does not know the service principal HTTP/auth.cern.ch")}, want: "does not know"},
	} {
		t.Run(name, func(t *testing.T) {
			idp := newFakeIDP(t)
			if c.idp != nil {
				c.idp(idp)
			}
			_, err := provider(idp, &c.tk).Token(context.Background())
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Errorf("Token: %v, want %q", err, c.want)
			}
		})
	}
}

func TestAvailable(t *testing.T) {
	idp := newFakeIDP(t)
	if !provider(idp, &stubTicket{}).Available(context.Background()) {
		t.Error("not available with a ticket")
	}
	if provider(idp, &stubTicket{err: errors.New("no ccache")}).Available(context.Background()) {
		t.Error("available without a ticket")
	}
	p := provider(idp, &stubTicket{})
	p.OIDC.Issuer = ""
	if p.Available(context.Background()) {
		t.Error("available without an issuer")
	}
	if h := provider(idp, &stubTicket{}).IdentityHint(); h != "einstein@CERN.CH" {
		t.Errorf("identity hint %q, want the principal", h)
	}
}

func TestRefresh(t *testing.T) {
	idp := newFakeIDP(t)
	tok, err := provider(idp, &stubTicket{}).Refresh(context.Background(), &auth.Token{RefreshToken: "refresh-me", Subject: "einstein"})
	if err != nil {
		t.Fatal(err)
	}
	if idp.gotGrant != "refresh_token" || idp.gotRefresh != "refresh-me" {
		t.Errorf("grant %q with %q, want refresh_token with refresh-me", idp.gotGrant, idp.gotRefresh)
	}
	if tok.Value == "" || tok.Subject != "einstein" || tok.Provider != Name {
		t.Errorf("token %+v, want a new one for einstein", tok)
	}
	// The IdP does not rotate it: the old one must stay.
	if tok.RefreshToken != "refresh-me" {
		t.Errorf("refresh token %q, want the old one kept", tok.RefreshToken)
	}
	if _, err := provider(idp, &stubTicket{}).Refresh(context.Background(), &auth.Token{RefreshToken: "stale"}); err == nil || !strings.Contains(err.Error(), "invalid_grant") {
		t.Errorf("Refresh with a stale token: %v, want invalid_grant", err)
	}
}

func writeUserConfig(t *testing.T, conf string) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	t.Setenv("HOME", t.TempDir())
	if err := os.MkdirAll(filepath.Join(dir, "cbtest"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cbtest", "config.toml"), []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestNew(t *testing.T) {
	writeUserConfig(t, "[plugins.cern-sso]\nspn = \"HTTP/sso.example.org\"\nccache = \"/nonexistent/cc\"\n")
	b := brand.MustParse([]byte("[program]\nname = \"cbtest\"\n[auth]\nchain = [\"cern-sso\"]\n" +
		"[auth.oidc]\nissuer = \"https://auth.example.org/realms/x\"\nclient_id = \"cli\"\n"))
	if !slices.Contains(cli.AuthProviders(), Name) {
		t.Fatalf("%s is not a sign-in method of this build: %v", Name, cli.AuthProviders())
	}
	p, ok := New(cli.NewRuntime(b)).(*Provider)
	if !ok {
		t.Fatalf("New returned %T", p)
	}
	if p.err != nil {
		t.Fatal(p.err)
	}
	if p.OIDC.Issuer != "https://auth.example.org/realms/x" || p.OIDC.ClientID != "cli" || p.SPN != "HTTP/sso.example.org" {
		t.Errorf("provider %+v, want the brand and the config", p)
	}
	if kp, ok := p.Ticket.(*kerberos.Provider); !ok || kp.CCache != "/nonexistent/cc" {
		t.Errorf("ticket %#v, want the ccache of the config", p.Ticket)
	}
	if p.Available(context.Background()) {
		t.Error("available without a credential cache")
	}
}

func TestNewBadConfig(t *testing.T) {
	writeUserConfig(t, "[plugins.cern-sso]\nspm = \"x\"\n")
	b := brand.MustParse([]byte("[program]\nname = \"cbtest\"\n"))
	p := New(cli.NewRuntime(b))
	// A typo must not make the method vanish from the chain without a word.
	if !p.Available(context.Background()) {
		t.Error("a broken config section hides the method")
	}
	if _, err := p.Token(context.Background()); err == nil || !strings.Contains(err.Error(), "spm") {
		t.Errorf("Token: %v, want the unknown key", err)
	}
}

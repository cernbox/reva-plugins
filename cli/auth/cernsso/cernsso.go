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

// Package cernsso is the cern-sso sign-in method of the cernbox CLI. The
// Kerberos ticket of kinit signs in to CERN SSO, which issues the OIDC token
// that CERNBox takes.
//
// It comes after the kerberos method in the chain. It is for when the KDC
// does not know the SPN of CERNBox, or when the server has no Kerberos auth
// provider.
package cernsso

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/cs3org/reva/v3/pkg/cli"
	"github.com/cs3org/reva/v3/pkg/cli/auth"
	"github.com/cs3org/reva/v3/pkg/cli/kerberos"
)

// Name is the name of the method.
const Name = "cern-sso"

func init() { cli.RegisterAuth(Name, New) }

// Config is the section [plugins.cern-sso] of the config files.
type Config struct {
	// SPN is the service principal of CERN SSO. Empty means HTTP/<issuer host>.
	SPN string `toml:"spn"`
	// RedirectURI is a redirect URI of the client. Empty means the
	// out-of-band one.
	RedirectURI string `toml:"redirect_uri"`
	// CCache is the Kerberos credential cache. Empty means $KRB5CCNAME, then
	// /tmp/krb5cc_<uid>.
	CCache string `toml:"ccache"`
}

// Ticket reads the Kerberos ticket and makes SPNEGO tokens from it.
// Tests replace it, because a token needs a KDC.
type Ticket interface {
	Ticket() (*kerberos.Ticket, error)
	Negotiate(spn string) (string, error)
}

// Provider is the cern-sso auth provider.
type Provider struct {
	OIDC             auth.OIDCConfig
	HTTPClient       *http.Client
	Ticket           Ticket
	SPN, RedirectURI string

	// err is the error of the config section, which New cannot return.
	err error

	once   sync.Once
	ticket *kerberos.Ticket
	tkErr  error
}

var (
	_ auth.Provider       = (*Provider)(nil)
	_ auth.Refresher      = (*Provider)(nil)
	_ auth.IdentityHinter = (*Provider)(nil)
)

// New returns the provider of the runtime rt.
func New(rt *cli.Runtime) cli.AuthProvider {
	o := rt.Brand().Auth.OIDC
	var c Config
	err := cli.PluginConfig(rt, Name, &c)
	return &Provider{
		OIDC:        auth.OIDCConfig{Issuer: o.Issuer, ClientID: o.ClientID, Scopes: o.Scopes},
		HTTPClient:  rt.HTTPClient().GetNativeHTTP(),
		Ticket:      &kerberos.Provider{CCache: c.CCache},
		SPN:         c.SPN,
		RedirectURI: c.RedirectURI,
		err:         err,
	}
}

// Name implements auth.Provider.
func (p *Provider) Name() string { return Name }

// Available implements auth.Provider. It only reads the local credential
// cache. A broken config section counts as available, so that Token reports
// it instead of the chain skipping the method without a word.
func (p *Provider) Available(context.Context) bool {
	if p.err != nil {
		return true
	}
	if p.OIDC.Issuer == "" || p.OIDC.ClientID == "" {
		return false
	}
	_, err := p.load()
	return err == nil
}

// IdentityHint implements auth.IdentityHinter.
func (p *Provider) IdentityHint() string {
	t, err := p.load()
	if err != nil {
		return ""
	}
	return t.Principal
}

func (p *Provider) load() (*kerberos.Ticket, error) {
	p.once.Do(func() { p.ticket, p.tkErr = p.Ticket.Ticket() })
	return p.ticket, p.tkErr
}

// Token implements auth.Provider with the authorization code flow and PKCE.
func (p *Provider) Token(ctx context.Context) (*auth.Token, error) {
	if p.err != nil {
		return nil, p.err
	}
	t, err := p.load()
	if err != nil {
		return nil, err
	}
	meta, err := auth.Discover(ctx, p.HTTPClient, p.OIDC.Issuer)
	if err != nil {
		return nil, err
	}
	if meta.AuthorizationEndpoint == "" {
		return nil, fmt.Errorf("the identity provider at %s advertises no authorization endpoint", p.OIDC.Issuer)
	}
	verifier, err := random(32)
	if err != nil {
		return nil, err
	}
	state, err := random(16)
	if err != nil {
		return nil, err
	}
	code, err := p.authorizationCode(ctx, meta.AuthorizationEndpoint, challengeOf(verifier), state)
	if err != nil {
		return nil, err
	}
	tr, err := auth.PostForm(ctx, p.HTTPClient, meta.TokenEndpoint, url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"client_id":     {p.OIDC.ClientID},
		"redirect_uri":  {p.redirectURI()},
		"code_verifier": {verifier},
	})
	if err != nil {
		return nil, fmt.Errorf("exchanging the authorization code: %w", err)
	}
	tok := tr.Token(Name)
	tok.Subject = t.Username()
	return tok, nil
}

func (p *Provider) authorizationCode(ctx context.Context, endpoint, challenge, state string) (string, error) {
	q := url.Values{
		"response_type":         {"code"},
		"client_id":             {p.OIDC.ClientID},
		"redirect_uri":          {p.redirectURI()},
		"scope":                 {strings.Join(scopes(p.OIDC), " ")},
		"state":                 {state},
		"code_challenge":        {challenge},
		"code_challenge_method": {"S256"},
	}
	sep := "?"
	if strings.Contains(endpoint, "?") {
		sep = "&"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+sep+q.Encode(), nil)
	if err != nil {
		return "", err
	}
	spn, err := p.spn()
	if err != nil {
		return "", err
	}
	neg, err := p.Ticket.Negotiate(spn)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Negotiate "+neg)

	// The redirect carries the code. Following it would send the code to the
	// redirect URI, or fail on the out-of-band one.
	client := *p.HTTPClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	res, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("contacting the identity provider: %w", err)
	}
	res.Body.Close()

	if res.StatusCode == http.StatusUnauthorized {
		return "", errors.New("the identity provider refused the Kerberos ticket (HTTP 401): it may have expired, run kinit")
	}
	location := res.Header.Get("Location")
	if location == "" {
		return "", fmt.Errorf("the identity provider answered %s without a redirect: "+
			"Kerberos may not be enabled for the client %s", res.Status, p.OIDC.ClientID)
	}
	loc, err := url.Parse(location)
	if err != nil {
		return "", fmt.Errorf("the identity provider sent a redirect that is not a URL: %w", err)
	}
	params := loc.Query()
	if e := params.Get("error"); e != "" {
		if d := params.Get("error_description"); d != "" {
			e += ": " + d
		}
		return "", fmt.Errorf("the identity provider refused the request: %s", e)
	}
	// A redirect with another state is not the answer to this request.
	if got := params.Get("state"); got != "" && got != state {
		return "", errors.New("the identity provider sent back another state")
	}
	code := params.Get("code")
	if code == "" {
		return "", errors.New("the redirect of the identity provider has no authorization code")
	}
	return code, nil
}

// Refresh implements auth.Refresher.
func (p *Provider) Refresh(ctx context.Context, tok *auth.Token) (*auth.Token, error) {
	if tok.RefreshToken == "" {
		return nil, errors.New("no refresh token")
	}
	meta, err := auth.Discover(ctx, p.HTTPClient, p.OIDC.Issuer)
	if err != nil {
		return nil, err
	}
	tr, err := auth.PostForm(ctx, p.HTTPClient, meta.TokenEndpoint, url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {tok.RefreshToken},
		"client_id":     {p.OIDC.ClientID},
	})
	if err != nil {
		return nil, err
	}
	out := tr.Token(Name)
	// Not every issuer rotates the refresh token.
	out.RefreshToken = cmp.Or(out.RefreshToken, tok.RefreshToken)
	out.Subject = tok.Subject
	return out, nil
}

func (p *Provider) redirectURI() string {
	return cmp.Or(p.RedirectURI, "urn:ietf:wg:oauth:2.0:oob")
}

func (p *Provider) spn() (string, error) {
	if p.SPN != "" {
		return p.SPN, nil
	}
	u, err := url.Parse(p.OIDC.Issuer)
	if err != nil || u.Hostname() == "" {
		return "", fmt.Errorf("no service principal for the issuer %q: set spn in [plugins.%s]", p.OIDC.Issuer, Name)
	}
	return "HTTP/" + u.Hostname(), nil
}

// scopes returns the scopes of the device flow. offline_access gives the
// refresh token.
func scopes(c auth.OIDCConfig) []string {
	if len(c.Scopes) > 0 {
		return c.Scopes
	}
	return []string{"openid", "profile", "offline_access"}
}

func challengeOf(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func random(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

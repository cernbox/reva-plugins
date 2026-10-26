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

package rest

import (
	"context"
	"fmt"
	neturl "net/url"
	"strings"
	"time"

	"github.com/cernbox/reva-plugins/user/rest/api"
	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	"github.com/cs3org/reva/v3/pkg/errtypes"
	"github.com/cs3org/reva/v3/pkg/invoke"
	"github.com/cs3org/reva/v3/pkg/utils"
)

// accountFields are the Identity fields that driver.account reads. Without
// field=, Grappa returns also personal data that operators do not need.
var accountFields = []string{
	"id", "upn", "type", "source", "displayName", "firstName", "lastName", "primaryAccountEmail",
	"uid", "gid", "cernDepartment", "cernGroup", "cernSection", "telephone1", "telephone2",
	"portablePhone", "activeUser", "disabled", "blocked", "blockingReason", "blockingTime",
	"blockingDeadline", "ownerId",
}

// accountIdentity is a Grappa Identity with the fields of accountFields.
type accountIdentity struct {
	Identity
	ID               string `json:"id"`
	FirstName        string `json:"firstName"`
	LastName         string `json:"lastName"`
	CernDepartment   string `json:"cernDepartment"`
	CernGroup        string `json:"cernGroup"`
	CernSection      string `json:"cernSection"`
	Telephone1       string `json:"telephone1"`
	Telephone2       string `json:"telephone2"`
	PortablePhone    string `json:"portablePhone"`
	Disabled         bool   `json:"disabled"`
	Blocked          bool   `json:"blocked"`
	BlockingReason   string `json:"blockingReason"`
	BlockingTime     string `json:"blockingTime"`
	BlockingDeadline string `json:"blockingDeadline"`
	OwnerID          string `json:"ownerId"`
}

type accountArgs struct {
	User string `arg:"user,required" help:"the username, or the email of a lightweight account"`
}

// The invocation is Shared because all the instances read the same Grappa.
func (m *manager) registerInvocations() {
	m.invocations = invoke.NewSet()
	m.invocations.Add(api.Name, "Show a CERN account from Grappa: type, owner, department and blocking state.").
		Shared().
		Handle(invoke.Typed(m.account))
}

// Invocations implements invoke.Invokable: the userprovider publishes them as driver.*.
func (m *manager) Invocations() []invoke.InvocationSpec {
	return m.invocations.Invocations()
}

// Invoke implements invoke.Invokable.
func (m *manager) Invoke(ctx context.Context, name string, args map[string]any) (invoke.Result, error) {
	return m.invocations.Invoke(ctx, name, args)
}

func (m *manager) account(ctx context.Context, a accountArgs) (invoke.Result, error) {
	id, err := m.lookupIdentity(ctx, a.User)
	if err != nil {
		return nil, err
	}
	acc, err := accountOf(id)
	if err != nil {
		return nil, err
	}
	if id.OwnerID != "" && id.OwnerID != id.ID {
		owner, err := m.fetchIdentity(ctx, id.OwnerID)
		if err != nil {
			return nil, fmt.Errorf("rest: owner of %s: %w", a.User, err)
		}
		if acc.Owner, err = accountOf(owner); err != nil {
			return nil, err
		}
	}
	return acc.Result()
}

func (m *manager) lookupIdentity(ctx context.Context, user string) (*accountIdentity, error) {
	id := &userpb.UserId{OpaqueId: user}
	if strings.Contains(user, "@") {
		id.Type = userpb.UserType_USER_TYPE_LIGHTWEIGHT
	}
	// A cache miss is not an error: the cache can be cold, and Grappa has
	// also the accounts that the bulk fetch does not keep.
	if u, err := m.cache.GetByUsername(ctx, user); err == nil {
		id = u.GetId()
	}
	if id.Type != userpb.UserType_USER_TYPE_LIGHTWEIGHT {
		return m.fetchIdentity(ctx, id.OpaqueId)
	}

	url := fmt.Sprintf("%s/api/v1.0/Identity/by_email/%s?filter=unconfirmed%%3Afalse&%s", m.conf.APIBaseURL, neturl.PathEscape(id.OpaqueId), fieldQuery())
	var r struct {
		Data []*accountIdentity `json:"data"`
	}
	if err := m.apiTokenManager.SendAPIGetRequest(ctx, url, false, &r); err != nil {
		return nil, notFoundOr(err, user)
	}
	for _, i := range r.Data {
		if i != nil && i.UserType() == userpb.UserType_USER_TYPE_LIGHTWEIGHT {
			return i, nil
		}
	}
	return nil, errtypes.NotFound(user)
}

// fetchIdentity takes the Identity id or its upn.
func (m *manager) fetchIdentity(ctx context.Context, id string) (*accountIdentity, error) {
	url := fmt.Sprintf("%s/api/v1.0/Identity/%s?%s", m.conf.APIBaseURL, neturl.PathEscape(id), fieldQuery())
	var r struct {
		Data *accountIdentity `json:"data"`
	}
	if err := m.apiTokenManager.SendAPIGetRequest(ctx, url, false, &r); err != nil {
		return nil, notFoundOr(err, id)
	}
	if r.Data == nil {
		return nil, errtypes.NotFound(id)
	}
	return r.Data, nil
}

func fieldQuery() string {
	q := neturl.Values{"field": accountFields}
	return q.Encode()
}

// notFoundOr maps a 404 to errtypes.NotFound. SendAPIGetRequest gives only
// the HTTP status in the error text.
func notFoundOr(err error, what string) error {
	if strings.Contains(err.Error(), "returned 404") {
		return errtypes.NotFound(what)
	}
	return err
}

func accountOf(i *accountIdentity) (*api.Account, error) {
	blockingTime, err := grappaTime(i.BlockingTime)
	if err != nil {
		return nil, err
	}
	blockingDeadline, err := grappaTime(i.BlockingDeadline)
	if err != nil {
		return nil, err
	}
	username := i.Upn
	if i.UserType() == userpb.UserType_USER_TYPE_LIGHTWEIGHT {
		// The same id as parseAndCacheUser gives.
		username = i.PrimaryAccountEmail
	}
	return &api.Account{
		Username:         username,
		Type:             utils.UserTypeToString(i.UserType()),
		Source:           i.Source,
		DisplayName:      i.DisplayName,
		FirstName:        i.FirstName,
		LastName:         i.LastName,
		Mail:             i.PrimaryAccountEmail,
		UIDNumber:        int64(i.UID),
		GIDNumber:        int64(i.GID),
		Department:       i.CernDepartment,
		Group:            i.CernGroup,
		Section:          i.CernSection,
		Phone:            i.Telephone1,
		Phone2:           i.Telephone2,
		Mobile:           i.PortablePhone,
		Active:           i.ActiveUser,
		Disabled:         i.Disabled,
		Blocked:          i.Blocked,
		BlockingReason:   i.BlockingReason,
		BlockingTime:     blockingTime,
		BlockingDeadline: blockingDeadline,
	}, nil
}

// grappaTime parses a Grappa date-time. Grappa can omit the offset (see
// cernboxcop user list-inactive); then the time is taken as UTC.
func grappaTime(s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999"} {
		if t, err := time.Parse(layout, s); err == nil {
			return &t, nil
		}
	}
	return nil, fmt.Errorf("rest: cannot parse the Grappa time %q", s)
}

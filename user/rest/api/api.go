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

// Package api has the result of the driver.account invocation of the
// user/rest driver. It has no init(), so that a CLI plugin can import it
// without the driver.
package api

import (
	"encoding/json"
	"time"
)

const (
	// Target is the service that publishes the invocation.
	Target = "userprovider"
	// Name is the name that the driver gives to the invocation.
	Name = "account"
	// Invocation is the name to call: the userprovider adds the "driver."
	// prefix to the invocations of its driver.
	Invocation = "driver." + Name
	// ArgUser is the argument with the username.
	ArgUser = "user"
)

// Account is a CERN account as Grappa knows it. All the keys are always in
// the result, with an empty value or null if Grappa has no value.
type Account struct {
	Username string `json:"username"`
	// Type is the CS3 type of the account: primary, secondary, service,
	// application or lightweight.
	Type string `json:"type"`
	// Source is the identity provider of the account, "cern" for CERN accounts.
	Source      string `json:"source"`
	DisplayName string `json:"display_name"`
	FirstName   string `json:"first_name"`
	LastName    string `json:"last_name"`
	Mail        string `json:"mail"`
	UIDNumber   int64  `json:"uid_number"`
	GIDNumber   int64  `json:"gid_number"`
	Department  string `json:"department"`
	Group       string `json:"group"`
	Section     string `json:"section"`
	Phone       string `json:"phone"`
	Phone2      string `json:"phone2"`
	Mobile      string `json:"mobile"`
	// Active is false for a person that has left CERN.
	Active           bool       `json:"active"`
	Disabled         bool       `json:"disabled"`
	Blocked          bool       `json:"blocked"`
	BlockingReason   string     `json:"blocking_reason"`
	BlockingTime     *time.Time `json:"blocking_time"`
	BlockingDeadline *time.Time `json:"blocking_deadline"`
	// Owner is the account of the person that owns a secondary, service or
	// application account. It is nil for an account that has no other owner.
	Owner *Account `json:"owner"`
}

// Result gives the account as an invocation result. The values are JSON
// values, so that the result is the same before and after the transport.
func (a *Account) Result() (map[string]any, error) {
	b, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}
	var r map[string]any
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	return r, nil
}

// FromResult reads an account from the result of the invocation.
func FromResult(r map[string]any) (*Account, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	var a Account
	if err := json.Unmarshal(b, &a); err != nil {
		return nil, err
	}
	return &a, nil
}

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

// Package cli links the CLI plugins of CERNBox into the `cernbox` build of
// the reva CLI (gaia --binary reva --with github.com/cernbox/reva-plugins/cli).
// It imports only CLI plugins, and the root package only backend plugins, so
// that revad and the CLI each link only their own plugins.
package cli

import (
	// Add here all the CLI plugins of cernbox.
	_ "github.com/cernbox/reva-plugins/cli/auth/cernsso"
	_ "github.com/cernbox/reva-plugins/cli/clipboard"
	_ "github.com/cernbox/reva-plugins/cli/inbox"
	_ "github.com/cernbox/reva-plugins/cli/otg"
	_ "github.com/cernbox/reva-plugins/cli/outbox"
	_ "github.com/cernbox/reva-plugins/cli/user"
)

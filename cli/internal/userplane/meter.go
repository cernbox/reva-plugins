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

package userplane

import (
	"io"
	"os"
	"sync"

	"github.com/cs3org/reva/v3/pkg/cli/data/transfer"
	"github.com/cs3org/reva/v3/pkg/cli/output"
)

// Meter is a progress bar on stderr, or nothing when Runtime.ShowProgress
// says no.
type Meter struct {
	m *output.Meter

	mu   sync.Mutex
	base int64
	cur  map[string]int64
}

// NewMeter starts a bar for total bytes, -1 for unknown.
func (s *Session) NewMeter(label string, total int64) *Meter {
	if !s.RT.ShowProgress() {
		return &Meter{}
	}
	return &Meter{m: output.NewMeter(os.Stderr, label, total), cur: map[string]int64{}}
}

// Writer counts what goes through w.
func (m *Meter) Writer(w io.Writer) io.Writer {
	if m.m == nil {
		return w
	}
	return m.m.Writer(w)
}

// SetLabel names what moves now.
func (m *Meter) SetLabel(label string) {
	if m.m != nil {
		m.m.SetLabel(label)
	}
}

// Progress feeds the bar from the events of the transfer engine, which give
// a running total per file.
func (m *Meter) Progress() transfer.ProgressFunc {
	if m.m == nil {
		return nil
	}
	return func(ev transfer.Event) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if ev.Done {
			m.base += ev.Transferred
			delete(m.cur, ev.Path)
		} else {
			m.cur[ev.Path] = ev.Transferred
		}
		done := m.base
		for _, n := range m.cur {
			done += n
		}
		m.m.Set(done, -1)
	}
}

// Stop erases the bar.
func (m *Meter) Stop() {
	if m.m != nil {
		m.m.Stop()
	}
}

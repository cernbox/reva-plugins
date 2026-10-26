package otg

import (
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/cernbox/reva-plugins/otg/api"
	"github.com/cs3org/reva/v3/pkg/control/controlpb"
	"github.com/cs3org/reva/v3/pkg/invoke"
	"github.com/cs3org/reva/v3/pkg/invoke/client"
	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/grpc"
)

func newOtg(t *testing.T) *Otg {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "otg.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec("CREATE TABLE cbox_otg_ocis (otg_number TEXT, message TEXT)"); err != nil {
		t.Fatal(err)
	}
	s := &Otg{conf: &config{}, db: db}
	s.registerInvocations()
	return s
}

func rows(t *testing.T, s *Otg) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM cbox_otg_ocis").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestCatalogue(t *testing.T) {
	s := newOtg(t)
	want := map[string]invoke.InvocationKind{api.Get: invoke.KindReadonly, api.Set: invoke.KindMutating, api.Clear: invoke.KindMutating}
	specs := s.Invocations()
	if len(specs) != len(want) {
		t.Fatalf("invocations: %+v", specs)
	}
	for _, spec := range specs {
		if spec.Kind != want[spec.Name] || spec.Scope != invoke.ScopeShared {
			t.Errorf("%s: kind %q scope %q", spec.Name, spec.Kind, spec.Scope)
		}
		if spec.Name == api.Set {
			if len(spec.Args) != 2 || !spec.Args[0].Required || !spec.Args[1].Required {
				t.Errorf("set args: %+v", spec.Args)
			}
		}
	}
}

func TestInvocations(t *testing.T) {
	s := newOtg(t)
	ctx := context.Background()

	res, err := s.Invoke(ctx, api.Get, nil)
	if err != nil || res["present"] != false {
		t.Fatalf("get on an empty table: %v %v", res, err)
	}
	if _, err := s.Invoke(ctx, api.Set, map[string]any{"number": "OTG1"}); err == nil {
		t.Fatal("set without a message must fail")
	}
	if _, err := s.Invoke(ctx, api.Set, map[string]any{"number": "OTG1", "message": " "}); err == nil {
		t.Fatal("set with an empty message must fail")
	}
	if n := rows(t, s); n != 0 {
		t.Fatalf("a refused set wrote %d rows", n)
	}

	res, err = s.Invoke(ctx, api.Set, map[string]any{"number": "OTG1", "message": "Down at <b>noon</b>"})
	if err != nil || res["number"] != "OTG1" || res["present"] != true {
		t.Fatalf("set: %v %v", res, err)
	}
	if _, err := s.Invoke(ctx, api.Set, map[string]any{"number": "OTG2", "message": "Later"}); err != nil {
		t.Fatal(err)
	}
	if n := rows(t, s); n != 1 {
		t.Fatalf("set must replace the message, the table has %d rows", n)
	}
	res, err = s.Invoke(ctx, api.Get, nil)
	if err != nil || res["number"] != "OTG2" || res["message"] != "Later" {
		t.Fatalf("get: %v %v", res, err)
	}

	res, err = s.Invoke(ctx, api.Clear, nil)
	if err != nil || res["present"] != false {
		t.Fatalf("clear: %v %v", res, err)
	}
	if n := rows(t, s); n != 0 {
		t.Fatalf("after clear the table has %d rows", n)
	}
	// clear is idempotent: an operator can run it without a get first
	if _, err := s.Invoke(ctx, api.Clear, nil); err != nil {
		t.Fatal(err)
	}
}

// controlServer does what reva's control channel does on a node. That server
// is internal to reva, so the test cannot use it.
type controlServer struct {
	controlpb.UnimplementedControlServer
}

func (controlServer) Invoke(ctx context.Context, req *controlpb.InvokeRequest) (*controlpb.InvokeResponse, error) {
	args := make(map[string]any, len(req.Args))
	for k, v := range req.Args {
		args[k] = v
	}
	res, err := invoke.Invoke(ctx, req.Target, req.Invocation, args)
	if err != nil {
		return &controlpb.InvokeResponse{Error: err.Error()}, nil
	}
	b, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	return &controlpb.InvokeResponse{ResultJson: string(b)}, nil
}

// TestOverGRPC sends an HTML message with spaces, quotes and "=" through the
// control channel, as an operator does with
// `reva invoke otg set number=OTG0012345 message="<b>Planned</b> intervention"`,
// and checks that it arrives and comes back unchanged.
func TestOverGRPC(t *testing.T) {
	s := newOtg(t)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	controlpb.RegisterControlServer(srv, controlServer{})
	go srv.Serve(lis) //nolint:errcheck // Serve returns when the test stops the server
	t.Cleanup(srv.Stop)

	node := lis.Addr().String() + "/" + api.Service
	invoke.RegisterInstance(node, api.Service, nil, s, nil)
	if shared := strings.Split(invoke.Metadata(node)[invoke.MetaSharedInvocations], ","); !slices.Contains(shared, api.Set) || !slices.Contains(shared, api.Clear) {
		t.Fatalf("the registry metadata must advertise set and clear as shared: %v", shared)
	}

	ep := client.Endpoint{Node: node, Service: api.Service, Addr: lis.Addr().String(), Target: node}
	message := `<b>Planned</b> intervention, see <a href="https://cern.service-now.com/x?id=1">the 'SSB' entry</a>`
	r := client.InvokeOne(context.Background(), ep, api.Set, map[string]string{"number": "OTG0012345", "message": message})
	if r.Error != "" {
		t.Fatalf("set: %s", r.Error)
	}
	var got api.Status
	if err := json.Unmarshal([]byte(r.ResultJSON), &got); err != nil {
		t.Fatal(err)
	}
	if want := (api.Status{Present: true, Number: "OTG0012345", Message: message}); got != want {
		t.Fatalf("set returned %+v, want %+v", got, want)
	}
	if num, msg, err := s.getOTG(context.Background()); err != nil || num != "OTG0012345" || msg != message {
		t.Fatalf("stored %q %q %v", num, msg, err)
	}

	r = client.InvokeOne(context.Background(), ep, api.Set, map[string]string{"number": "OTG0012345"})
	if !strings.Contains(r.Error, `missing argument "message"`) {
		t.Fatalf("set without a message: %+v", r)
	}
}

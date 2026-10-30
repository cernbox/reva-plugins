package sssdwarmer

import (
	"context"
	"encoding/binary"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/cs3org/reva/v3/pkg/rserverless"
	"github.com/rs/zerolog"
)

// The plugin loader casts New to this type.
var _ rserverless.NewService = New

func aclValue(entries ...[3]uint32) []byte {
	v := binary.LittleEndian.AppendUint32(nil, aclVersion)
	for _, e := range entries {
		v = binary.LittleEndian.AppendUint16(v, uint16(e[0]))
		v = binary.LittleEndian.AppendUint16(v, uint16(e[1]))
		v = binary.LittleEndian.AppendUint32(v, e[2])
	}
	return v
}

func TestACLGroupIDs(t *testing.T) {
	const undefinedID = 0xffffffff
	tests := []struct {
		name  string
		value []byte
		want  []uint32
	}{
		{
			name: "named groups only",
			// user::rwx, user:1001:rwx, group::r-x, group:11044827:rwx, group:2766:r-x, mask::rwx, other::---
			value: aclValue(
				[3]uint32{0x01, 7, undefinedID},
				[3]uint32{0x02, 7, 1001},
				[3]uint32{0x04, 5, undefinedID},
				[3]uint32{0x08, 7, 11044827},
				[3]uint32{0x08, 5, 2766},
				[3]uint32{0x10, 7, undefinedID},
				[3]uint32{0x20, 0, undefinedID},
			),
			want: []uint32{11044827, 2766},
		},
		{
			name:  "no named groups",
			value: aclValue([3]uint32{0x01, 7, undefinedID}, [3]uint32{0x04, 5, undefinedID}, [3]uint32{0x20, 0, undefinedID}),
		},
		{
			name:  "unknown version",
			value: append([]byte{1, 0, 0, 0}, aclValue([3]uint32{0x08, 7, 1})[4:]...),
		},
		{
			name:  "truncated entry is ignored",
			value: aclValue([3]uint32{0x08, 7, 42})[:10],
		},
		{
			name: "empty",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := aclGroupIDs(tt.value); !slices.Equal(got, tt.want) {
				t.Errorf("aclGroupIDs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestScan(t *testing.T) {
	setfacl, err := exec.LookPath("setfacl")
	if err != nil {
		t.Skip("setfacl is not installed")
	}
	g, err := user.LookupGroupId(strconv.Itoa(os.Getgid()))
	if err != nil {
		t.Skipf("cannot resolve the primary group: %v", err)
	}

	root := t.TempDir()
	project := filepath.Join(root, "project", "c", "cv-design-office")
	if err := os.MkdirAll(project, 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(setfacl, "-m", "g:"+g.Gid+":rwx", "-m", "d:g:"+g.Gid+":rwx", project).CombinedOutput(); err != nil {
		t.Skipf("the file system does not take ACLs: %v: %s", err, out)
	}

	log := zerolog.Nop()
	s := &svc{conf: &config{ProjectGlob: filepath.Join(root, "project", "*", "*")}, log: &log}
	if err := s.scan(context.Background()); err != nil {
		t.Fatalf("scan() error = %v", err)
	}
	if _, ok := (*s.aclGroups.Load())[strings.ToLower(g.Name)]; !ok {
		t.Errorf("scan() groups = %v, want %s in them", *s.aclGroups.Load(), g.Name)
	}

	// A failed scan keeps the groups of the previous one.
	s.conf.ProjectGlob = filepath.Join(root, "missing", "*")
	if err := s.scan(context.Background()); err == nil {
		t.Error("scan() of no folders did not fail")
	}
	if _, ok := (*s.aclGroups.Load())[strings.ToLower(g.Name)]; !ok {
		t.Error("a failed scan dropped the groups of the previous one")
	}
}

func TestInGroups(t *testing.T) {
	groups := map[string]struct{}{"cernbox-project-cv-design-office-admins": {}}
	if !inGroups([]string{"cern-staff", "CERNBOX-Project-CV-Design-Office-Admins"}, groups) {
		t.Error("a member of an ACL group is not found, case must not matter")
	}
	if inGroups([]string{"cern-staff"}, groups) {
		t.Error("a user in no ACL group is found")
	}
	if inGroups(nil, groups) {
		t.Error("a user without groups is found")
	}
}

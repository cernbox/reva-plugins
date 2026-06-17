package cache

import (
    "context"
    "encoding/json"
    "fmt"
    "strings"

    redispools "github.com/cernbox/reva-plugins/redispools"
    grouppb "github.com/cs3org/go-cs3apis/cs3/identity/group/v1beta1"
    userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
    "github.com/cs3org/reva/v3/pkg/appctx"
    "github.com/cs3org/reva/v3/pkg/errtypes"
)

// Key schema:
//   group:id:<opaqueID>                          → JSON grouppb.Group
//   group:name:<opaqueID>_<display_lower_snake>  → bare UUID string
//   group:members:<opaqueID>                     → JSON []*userpb.UserId

const (
    groupIDPrefix      = "group:id:"
    groupNamePrefix    = "group:name:"
    groupMembersPrefix = "group:members:"
)

// GroupCache is a Redis-backed cache for CS3 group objects and their member lists.
type GroupCache struct {
    pools          *redispools.RedisPools
    groupTTLSecs   int // 5 × fetch_interval
    membersTTLSecs int // member list TTL
}

// NewGroupCache creates a GroupCache.
//   fetchInterval        – seconds between full syncs
//   membersCacheMinutes  – TTL for per-group member lists
func NewGroupCache(pools *redispools.RedisPools, fetchInterval, membersCacheMinutes int) *GroupCache {
    return &GroupCache{
        pools:          pools,
        groupTTLSecs:   5 * fetchInterval,
        membersTTLSecs: membersCacheMinutes * 60,
    }
}

// StoreGroup writes a group under all applicable index keys.
func (c *GroupCache) StoreGroup(g *grouppb.Group) error {
    if err := store(c.pools, groupIDPrefix+strings.ToLower(g.Id.OpaqueId), g, c.groupTTLSecs); err != nil {
        return err
    }
    if g.DisplayName != "" {
        nameKey := groupNamePrefix +
            strings.ToLower(g.Id.OpaqueId) + "_" +
            strings.ReplaceAll(strings.ToLower(g.DisplayName), " ", "_")
        // Store only the UUID; callers resolve it through GetByID.
        if err := store(c.pools, nameKey, g.Id.OpaqueId, c.groupTTLSecs); err != nil {
            return err
        }
    }
    return nil
}

// GetByID looks up a group by its OpaqueId.
func (c *GroupCache) GetByID(ctx context.Context, opaqueID string) (*grouppb.Group, error) {
    var g grouppb.Group
    if err := fetch(ctx, c.pools, groupIDPrefix+strings.ToLower(opaqueID), &g); err != nil {
        return nil, errtypes.NotFound(opaqueID)
    }
    return &g, nil
}

// GetByName looks up a group by display name. It scans the name index to find
// the UUID, then resolves the full object via GetByID.
func (c *GroupCache) GetByName(ctx context.Context, name string) (*grouppb.Group, error) {
    pattern := fmt.Sprintf(
        "%s*_%s*",
        groupNamePrefix,
        strings.ReplaceAll(strings.ToLower(name), " ", "_"),
    )
    values, err := scanAndFetch(ctx, c.pools, pattern)
    if err != nil {
        return nil, err
    }
    if len(values) == 0 {
        return nil, errtypes.NotFound(name)
    }
    // values[0] is the bare UUID stored by StoreGroup.
    return c.GetByID(ctx, values[0])
}

// Find scans all group index keys matching query and returns the deduplicated
// set of groups. Entries may be either a JSON-encoded Group (from the primary
// key) or a bare UUID string (from the name index); both are handled.
func (c *GroupCache) Find(ctx context.Context, query string) ([]*grouppb.Group, error) {
    pattern := fmt.Sprintf(
        "group:*%s*",
        strings.ReplaceAll(strings.ToLower(query), " ", "_"),
    )
    values, err := scanAndFetch(ctx, c.pools, pattern)
    if err != nil {
        return nil, err
    }

    seen := make(map[string]*grouppb.Group)
    for _, s := range values {
        // Try to decode as a full Group first.
        var g grouppb.Group
        if err := json.Unmarshal([]byte(s), &g); err == nil && g.Id != nil {
            seen[g.Id.OpaqueId] = &g
            continue
        }
        // Otherwise it's a bare UUID from the name index — dereference it.
        if full, err := c.GetByID(ctx, s); err == nil {
            seen[full.Id.OpaqueId] = full
        }
    }

    result := make([]*grouppb.Group, 0, len(seen))
    for _, g := range seen {
        result = append(result, g)
    }
    appctx.GetLogger(ctx).Debug().Str("pattern", pattern).Int("results", len(result)).Msg("cache: Find groups")
    return result, nil
}

// StoreMembers writes the member list for a group.
func (c *GroupCache) StoreMembers(gid *grouppb.GroupId, members []*userpb.UserId) error {
    return store(c.pools, groupMembersPrefix+strings.ToLower(gid.OpaqueId), members, c.membersTTLSecs)
}

// GetMembers returns the cached member list for a group, or an error if the
// entry is absent (so the caller can fall back to a live API fetch).
func (c *GroupCache) GetMembers(ctx context.Context, opaqueID string) ([]*userpb.UserId, error) {
    var members []*userpb.UserId
    if err := fetch(ctx, c.pools, groupMembersPrefix+strings.ToLower(opaqueID), &members); err != nil {
        return nil, err
    }
    return members, nil
}

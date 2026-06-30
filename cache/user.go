package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	redispools "github.com/cernbox/reva-plugins/redispools"
	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	"github.com/cs3org/reva/v3/pkg/appctx"
	"github.com/cs3org/reva/v3/pkg/errtypes"
)

// Key schema:
//   user:id:<opaqueID>                          → JSON userpb.User
//   user:username:<login>                       → JSON userpb.User
//   user:mail:<email>                           → JSON userpb.User
//   user:name:<opaqueID>_<display_lower_snake>  → JSON userpb.User
//   user:groups:<opaqueID>                      → JSON []string

const (
	userIDPrefix          = "user:id:"
	userUsernamePrefix    = "user:username:"
	userMailPrefix        = "user:mail:"
	userNamePrefix        = "user:name:"
	userGroupsPrefix      = "user:groups:"
	userIAMUUIDPrefix     = "user:iamuuid:"   // opaqueID -> IAM account UUID
	userOpaqueIDByIAMUUID = "user:byiamuuid:" // IAM account UUID -> opaqueID
)

// UserCache is a Redis-backed cache for CS3 user objects and their group
// membership lists.
type UserCache struct {
	pools         *redispools.RedisPools
	userTTLSecs   int // 5 × fetch_interval
	groupsTTLSecs int // group membership TTL
}

// NewUserCache creates a UserCache.
//
//	fetchInterval       – seconds between full IAM/GRAPPA syncs (used to
//	                      derive user record TTL as 5× this value)
//	groupsCacheMinutes  – TTL for per-user group membership lists
func NewUserCache(pools *redispools.RedisPools, fetchInterval, groupsCacheMinutes int) *UserCache {
	return &UserCache{
		pools:         pools,
		userTTLSecs:   5 * fetchInterval,
		groupsTTLSecs: groupsCacheMinutes * 60,
	}
}

// StoreUser writes a user under all applicable index keys.
func (c *UserCache) StoreUser(u *userpb.User) error {
	if err := store(c.pools, userIDPrefix+strings.ToLower(u.Id.OpaqueId), u, c.userTTLSecs); err != nil {
		return err
	}
	if u.Username != "" {
		if err := store(c.pools, userUsernamePrefix+strings.ToLower(u.Username), u, c.userTTLSecs); err != nil {
			return err
		}
	}
	if u.Mail != "" {
		if err := store(c.pools, userMailPrefix+strings.ToLower(u.Mail), u, c.userTTLSecs); err != nil {
			return err
		}
	}
	if u.DisplayName != "" {
		nameKey := userNamePrefix +
			strings.ToLower(u.Id.OpaqueId) + "_" +
			strings.ReplaceAll(strings.ToLower(u.DisplayName), " ", "_")
		if err := store(c.pools, nameKey, u, c.userTTLSecs); err != nil {
			return err
		}
	}
	return nil
}

// GetByID looks up a user by their OpaqueId.
func (c *UserCache) GetByID(ctx context.Context, opaqueID string) (*userpb.User, error) {
	var u userpb.User
	if err := fetch(ctx, c.pools, userIDPrefix+strings.ToLower(opaqueID), &u); err != nil {
		return nil, errtypes.NotFound(opaqueID)
	}
	return &u, nil
}

// GetByUsername looks up a user by their login name.
func (c *UserCache) GetByUsername(ctx context.Context, username string) (*userpb.User, error) {
	var u userpb.User
	if err := fetch(ctx, c.pools, userUsernamePrefix+strings.ToLower(username), &u); err != nil {
		return nil, errtypes.NotFound(username)
	}
	return &u, nil
}

// GetByMail looks up a user by their primary email address.
func (c *UserCache) GetByMail(ctx context.Context, mail string) (*userpb.User, error) {
	var u userpb.User
	if err := fetch(ctx, c.pools, userMailPrefix+strings.ToLower(mail), &u); err != nil {
		return nil, errtypes.NotFound(mail)
	}
	return &u, nil
}

// Find scans all user index keys matching query and returns the deduplicated
// set of users. An empty query returns all cached users.
func (c *UserCache) Find(ctx context.Context, query string) ([]*userpb.User, error) {
	pattern := fmt.Sprintf(
		"user:*%s*",
		strings.ReplaceAll(strings.ToLower(query), " ", "_"),
	)
	values, err := scanAndFetch(ctx, c.pools, pattern)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]*userpb.User)
	for _, s := range values {
		var u userpb.User
		// Only keep users that have no "inactive" status set (cf. parseAndCacheUser)
		if err := json.Unmarshal([]byte(s), &u); err == nil && u.Id != nil && u.Id.OpaqueId != "" && u.Status != userpb.UserStatus_USER_STATUS_EXPIRING {
			seen[u.Id.OpaqueId] = &u
		}
	}

	result := make([]*userpb.User, 0, len(seen))
	for _, u := range seen {
		result = append(result, u)
	}
	appctx.GetLogger(ctx).Debug().Str("pattern", pattern).Int("results", len(result)).Msg("cache: Find users")
	return result, nil
}

// StoreGroups writes the group membership list for a user.
func (c *UserCache) StoreGroups(uid *userpb.UserId, groups []string) error {
	return store(c.pools, userGroupsPrefix+strings.ToLower(uid.OpaqueId), groups, c.groupsTTLSecs)
}

// GetGroups returns the cached group membership list for a user, or an error
// if the entry is absent (so the caller can fall back to a live API fetch).
func (c *UserCache) GetGroups(ctx context.Context, opaqueID string) ([]string, error) {
	var groups []string
	if err := fetch(ctx, c.pools, userGroupsPrefix+strings.ToLower(opaqueID), &groups); err != nil {
		return nil, err
	}
	return groups, nil
}

// StoreIAMUUID records the two-way mapping between a remapped OpaqueId and
// the original IAM account UUID, so callers can resolve in either direction:
//   - GetIAMUUID:   opaqueID -> iamUUID  (used by the user driver for
//     group-membership lookups against the IAM API)
//   - GetOpaqueIDByIAMUUID: iamUUID -> opaqueID  (used by the group driver
//     to report the same OpaqueId for a member that GetUser/FindUsers
//     would report)
//
// Only called for accounts that were actually remapped via primary_users;
// the common case (OpaqueId == IAM UUID) needs no entry in either direction.
func (c *UserCache) StoreIAMUUID(opaqueID, iamUUID string) error {
	if err := store(c.pools, userIAMUUIDPrefix+strings.ToLower(opaqueID), iamUUID, c.userTTLSecs); err != nil {
		return err
	}
	return store(c.pools, userOpaqueIDByIAMUUID+strings.ToLower(iamUUID), opaqueID, c.userTTLSecs)
}

// GetIAMUUID resolves the IAM account UUID for a given OpaqueId. If no
// mapping is present, callers should fall back to treating opaqueID itself
// as the IAM UUID.
func (c *UserCache) GetIAMUUID(ctx context.Context, opaqueID string) (string, error) {
	var uuid string
	if err := fetch(ctx, c.pools, userIAMUUIDPrefix+strings.ToLower(opaqueID), &uuid); err != nil {
		return "", err
	}
	return uuid, nil
}

// GetOpaqueIDByIAMUUID resolves the public OpaqueId for a given IAM account
// UUID. If no mapping is present, callers should fall back to treating the
// IAM UUID itself as the OpaqueId (the common, non-remapped case).
func (c *UserCache) GetOpaqueIDByIAMUUID(ctx context.Context, iamUUID string) (string, error) {
	var opaqueID string
	if err := fetch(ctx, c.pools, userOpaqueIDByIAMUUID+strings.ToLower(iamUUID), &opaqueID); err != nil {
		return "", err
	}
	return opaqueID, nil
}

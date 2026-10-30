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

// Package sssdwarmer warms the SSSD cache of a cephmount node. Reva stores the
// groups of a user in Redis when it fetches them, for example at login. The
// service then looks the user up, as the cephmount driver does for the first
// request of a user, so that this request does not wait for LDAP. It does this
// only for users in a group with a grant on a project: warming every user filled
// the SSSD cache with users who never open a project.
package sssdwarmer

import (
	"context"
	"errors"
	"fmt"
	"os/user"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cernbox/reva-plugins/cache"
	redispools "github.com/cernbox/reva-plugins/redispools"
	authpb "github.com/cs3org/go-cs3apis/cs3/auth/provider/v1beta1"
	userpb "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	rpc "github.com/cs3org/go-cs3apis/cs3/rpc/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	registry "github.com/cs3org/go-cs3apis/cs3/storage/registry/v1beta1"
	"github.com/cs3org/reva/v3"
	"github.com/cs3org/reva/v3/pkg/appctx"
	"github.com/cs3org/reva/v3/pkg/auth/scope"
	"github.com/cs3org/reva/v3/pkg/rjobs"
	"github.com/cs3org/reva/v3/pkg/rserverless"
	"github.com/cs3org/reva/v3/pkg/service"
	"github.com/cs3org/reva/v3/pkg/sharedconf"
	"github.com/cs3org/reva/v3/pkg/token"
	"github.com/cs3org/reva/v3/pkg/token/manager/jwt"
	"github.com/cs3org/reva/v3/pkg/utils/cfg"
	"github.com/gomodule/redigo/redis"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/metadata"
)

const (
	// The keyspace notifications of the SET commands on database 0. They
	// need notify-keyspace-events to have at least the K and $ classes.
	setEventsChannel = "__keyevent@0__:set"
	scanJobName      = "sssdwarmer.acl_groups"
	pingInterval     = 30 * time.Second
	// receiveTimeout is longer than pingInterval, so a connection that
	// stops answering the pings fails the receive and is redialed.
	receiveTimeout = 60 * time.Second
	maxBackoff     = 30 * time.Second
)

func init() {
	reva.RegisterPlugin(SSSDWarmer{})
}

// SSSDWarmer is the serverless service that warms the SSSD cache.
type SSSDWarmer struct{}

// RevaPlugin returns the plugin info.
func (SSSDWarmer) RevaPlugin() reva.PluginInfo {
	return reva.PluginInfo{
		ID:  "serverless.services.sssdwarmer",
		New: New,
	}
}

type config struct {
	// The address at which the redis server is running
	RedisAddress string `mapstructure:"redis_address" docs:"localhost:6379"`
	// The address at which the redis sentinel is running (only used when redis_sentinel_mode is enabled)
	RedisSentinelAddress string `mapstructure:"redis_sentinel_address" docs:""`
	// The username for connecting to the redis server
	RedisUsername string `mapstructure:"redis_username" docs:""`
	// The password for connecting to the redis server
	RedisPassword string `mapstructure:"redis_password" docs:""`
	// The name of the master node in case Redis Sentinel mode is enabled
	RedisMasterName string `mapstructure:"redis_master_name" docs:""`
	// Whether to use Redis Sentinel mode
	RedisSentinelMode bool `mapstructure:"redis_sentinel_mode" docs:""`
	// The mount ids of the storage providers whose projects are read. The storage
	// registry gives their addresses.
	MountIDs []string `mapstructure:"mount_ids" docs:"" validate:"required"`
	// The mount_path of these storage providers
	MountPath string `mapstructure:"mount_path" docs:"/winspaces"`
	// The space_depth of these storage providers, so the project folders are this deep
	SpaceDepth int `mapstructure:"space_depth" docs:"3"`
	// The secret that signs the token of the service user, falls back to [shared]
	JWTSecret string `mapstructure:"jwt_secret" docs:""`
	// The account that lists the projects and their grants. The cephmount
	// driver lists a folder as the caller, so the account must exist on the
	// node and be allowed to list the folders above the projects.
	ServiceUserName string `mapstructure:"service_user_name" docs:"" validate:"required"`
	ServiceUserUID  int64  `mapstructure:"service_user_uid" docs:"" validate:"required"`
	ServiceUserGID  int64  `mapstructure:"service_user_gid" docs:"" validate:"required"`
	// How often the grants of the projects are read again
	ScanIntervalSeconds int `mapstructure:"scan_interval_seconds" docs:"900"`
}

func (c *config) ApplyDefaults() {
	if c.RedisAddress == "" {
		c.RedisAddress = ":6379"
	}
	if c.RedisSentinelAddress == "" {
		c.RedisSentinelAddress = c.RedisAddress
	}
	if c.MountPath == "" {
		c.MountPath = "/winspaces"
	}
	if c.SpaceDepth == 0 {
		c.SpaceDepth = 3
	}
	c.JWTSecret = sharedconf.GetJWTSecret(c.JWTSecret)
	if c.ScanIntervalSeconds == 0 {
		c.ScanIntervalSeconds = 900
	}
}

type svc struct {
	conf   *config
	log    *zerolog.Logger
	pools  *redispools.RedisPools
	users  *cache.UserCache
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	// The identity of the calls to the storage registry and providers.
	tokens      token.Manager
	serviceUser *userpb.User
	scope       map[string]*authpb.Scope
	// storageRegistry is service.StorageRegistry, replaced in the tests.
	storageRegistry func(context.Context) (registry.RegistryAPIClient, error)

	// aclGroups holds the lowercase names of the groups with a grant on a
	// project. It is nil until a scan succeeds, and until then every user is
	// warmed.
	aclGroups atomic.Pointer[map[string]struct{}]
	// Counted since the last scan, which logs them.
	warmed, skipped, failed atomic.Int64
}

// New returns a new sssdwarmer service. The scan of the project grants is a
// periodic job, so the jobs service must be enabled in the same process.
func New(ctx context.Context, m map[string]any) (rserverless.Service, error) {
	var c config
	if err := cfg.Decode(m, &c); err != nil {
		return nil, err
	}
	if c.SpaceDepth <= levels(c.MountPath) {
		return nil, fmt.Errorf("sssdwarmer: space_depth %d puts no folder below mount_path %s", c.SpaceDepth, c.MountPath)
	}

	tokens, err := jwt.New(map[string]any{"secret": c.JWTSecret})
	if err != nil {
		return nil, err
	}
	// The scan reads every project, so the token gets the owner scope, as for a
	// login.
	scopes, err := scope.AddOwnerScope(nil)
	if err != nil {
		return nil, err
	}

	pools, err := redispools.NewRedisPoolsWithSentinelAddress(ctx, c.RedisAddress, c.RedisSentinelAddress, c.RedisUsername, c.RedisPassword, c.RedisSentinelMode, c.RedisMasterName)
	if err != nil {
		return nil, err
	}

	runCtx, cancel := context.WithCancel(ctx)
	s := &svc{
		conf:   &c,
		log:    appctx.GetLogger(ctx),
		pools:  pools,
		users:  cache.NewUserCache(pools, 0, 0), // only read, so the TTLs are not used
		ctx:    runCtx,
		cancel: cancel,
		done:   make(chan struct{}),
		tokens: tokens,
		serviceUser: &userpb.User{
			Id: &userpb.UserId{
				OpaqueId: c.ServiceUserName,
				Type:     userpb.UserType_USER_TYPE_PRIMARY,
			},
			Username:  c.ServiceUserName,
			UidNumber: c.ServiceUserUID,
			GidNumber: c.ServiceUserGID,
		},
		scope:           scopes,
		storageRegistry: service.StorageRegistry,
	}

	// Every node has its own SSSD, so every node scans.
	if err := rjobs.RegisterPeriodic(rjobs.Periodic{
		Name:       scanJobName,
		Schedule:   "@every " + (time.Duration(c.ScanIntervalSeconds) * time.Second).String(),
		Scope:      rjobs.ScopeAllNodes,
		RunOnStart: true,
		Run:        s.scan,
	}); err != nil {
		cancel()
		return nil, err
	}
	return s, nil
}

// Start listens to the Redis events until the service is closed.
func (s *svc) Start() {
	defer close(s.done)

	backoff := time.Second
	for {
		started := time.Now()
		err := s.listen()
		if s.ctx.Err() != nil {
			return
		}
		// A connection that worked for a while starts the backoff again.
		if time.Since(started) > maxBackoff {
			backoff = time.Second
		}
		s.log.Error().Err(err).Dur("retry_in", backoff).Msg("sssdwarmer: listening to redis failed")
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, maxBackoff)
	}
}

// listen subscribes to the SET events and handles them until the connection
// fails or the service is closed.
func (s *svc) listen() error {
	// The events are sent by the instance that runs the SET, so subscribe on
	// the master.
	conn := s.pools.Write.Get()
	defer conn.Close()

	psc := redis.PubSubConn{Conn: conn}
	if err := psc.Subscribe(setEventsChannel); err != nil {
		return err
	}

	// The pinger must be gone before the connection goes back to the pool.
	stop, pingerDone := make(chan struct{}), make(chan struct{})
	defer func() {
		close(stop)
		<-pingerDone
	}()
	go func() {
		defer close(pingerDone)
		t := time.NewTicker(pingInterval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-s.ctx.Done():
				// Ends the receive loop below.
				_ = psc.Unsubscribe()
				return
			case <-t.C:
				if err := psc.Ping(""); err != nil {
					return
				}
			}
		}
	}()

	for {
		switch v := psc.ReceiveWithTimeout(receiveTimeout).(type) {
		case redis.Message:
			s.handle(string(v.Data))
		case redis.Subscription:
			if v.Count == 0 {
				return errors.New("unsubscribed")
			}
			s.log.Info().Str("channel", v.Channel).Msg("sssdwarmer: subscribed")
		case redis.Pong:
		case error:
			return v
		}
	}
}

func (s *svc) handle(key string) {
	opaqueID, ok := cache.GroupsKeyOwner(key)
	if !ok {
		return
	}

	if groups := s.aclGroups.Load(); groups != nil {
		names, err := s.users.GetGroups(s.ctx, opaqueID)
		if err != nil {
			s.log.Warn().Err(err).Str("user", opaqueID).Msg("sssdwarmer: cannot read the groups, warming anyway")
		} else if !inGroups(names, *groups) {
			s.skipped.Add(1)
			return
		}
	}

	// The same lookups as a new cephmount user thread.
	start := time.Now()
	u, err := user.Lookup(opaqueID)
	if err == nil {
		_, err = u.GroupIds()
	}
	if err != nil {
		s.failed.Add(1)
		s.log.Debug().Err(err).Str("user", opaqueID).Msg("sssdwarmer: lookup failed")
		return
	}
	s.warmed.Add(1)
	s.log.Debug().Str("user", opaqueID).Dur("took", time.Since(start)).Msg("sssdwarmer: warmed")
}

func inGroups(names []string, groups map[string]struct{}) bool {
	for _, name := range names {
		if _, ok := groups[strings.ToLower(name)]; ok {
			return true
		}
	}
	return false
}

// scan reads the grants of the projects of the storage providers and keeps the
// names of their groups. When it fails, the groups of the previous scan stay.
//
// A call that fails, fails the scan: the registry or a provider is gone, and
// projects would be missing. Something that is answered with an error is
// skipped: a mount the registry does not know, which the gateway does not
// serve, or a folder the service user cannot read.
func (s *svc) scan(ctx context.Context) error {
	tkn, err := s.tokens.MintToken(ctx, s.serviceUser, s.scope)
	if err != nil {
		return fmt.Errorf("sssdwarmer: minting the service token: %w", err)
	}
	ctx = metadata.AppendToOutgoingContext(ctx, appctx.TokenHeader, tkn)

	reg, err := s.storageRegistry(ctx)
	if err != nil {
		return fmt.Errorf("sssdwarmer: getting the storage registry: %w", err)
	}

	names := map[string]struct{}{}
	projects := 0
	for _, mountID := range s.conf.MountIDs {
		res, err := reg.GetStorageProviders(ctx, &registry.GetStorageProvidersRequest{
			Ref: &provider.Reference{ResourceId: &provider.ResourceId{StorageId: mountID}},
		})
		if err != nil {
			return fmt.Errorf("sssdwarmer: %s: looking up the storage provider: %w", mountID, err)
		}
		if code := res.GetStatus().GetCode(); code != rpc.Code_CODE_OK || len(res.GetProviders()) == 0 {
			s.log.Warn().Str("mount_id", mountID).Str("code", code.String()).Str("error", res.GetStatus().GetMessage()).Msg("sssdwarmer: the storage registry has no provider, skipping the mount")
			continue
		}
		addr := res.GetProviders()[0].GetAddress()

		client, err := service.StorageProviderAt(addr)
		if err != nil {
			return fmt.Errorf("sssdwarmer: %s at %s: %w", mountID, addr, err)
		}
		paths, err := s.listProjects(ctx, client)
		if err != nil {
			return fmt.Errorf("sssdwarmer: %s at %s: %w", mountID, addr, err)
		}
		projects += len(paths)

		for _, p := range paths {
			res, err := client.ListGrants(ctx, &provider.ListGrantsRequest{Ref: &provider.Reference{Path: p}})
			if err != nil {
				return fmt.Errorf("sssdwarmer: %s at %s: reading the grants of %s: %w", mountID, addr, p, err)
			}
			if code := res.GetStatus().GetCode(); code != rpc.Code_CODE_OK {
				s.log.Warn().Str("mount_id", mountID).Str("project", p).Str("code", code.String()).Str("error", res.GetStatus().GetMessage()).Msg("sssdwarmer: cannot read the grants, skipping the project")
				continue
			}
			for _, g := range res.GetGrants() {
				if id := g.GetGrantee().GetGroupId(); id != nil {
					names[strings.ToLower(id.GetOpaqueId())] = struct{}{}
				}
			}
		}
	}
	if len(names) == 0 {
		return errors.New("sssdwarmer: no groups in the project grants")
	}

	s.aclGroups.Store(&names)
	s.log.Info().
		Int("projects", projects).
		Int("groups", len(names)).
		Int64("warmed", s.warmed.Swap(0)).
		Int64("skipped", s.skipped.Swap(0)).
		Int64("failed", s.failed.Swap(0)).
		Msg("sssdwarmer: scanned the project grants, counts are since the last scan")
	return nil
}

// listProjects returns the paths of the project folders of a storage provider,
// the folders space_depth deep.
func (s *svc) listProjects(ctx context.Context, client provider.ProviderAPIClient) ([]string, error) {
	folders := []string{s.conf.MountPath}
	for range s.conf.SpaceDepth - levels(s.conf.MountPath) {
		var below []string
		for _, f := range folders {
			res, err := client.ListContainer(ctx, &provider.ListContainerRequest{Ref: &provider.Reference{Path: f}})
			if err != nil {
				return nil, fmt.Errorf("listing %s: %w", f, err)
			}
			if code := res.GetStatus().GetCode(); code != rpc.Code_CODE_OK {
				s.log.Warn().Str("folder", f).Str("code", code.String()).Str("error", res.GetStatus().GetMessage()).Msg("sssdwarmer: cannot list the folder, skipping it")
				continue
			}
			for _, ri := range res.GetInfos() {
				// Hidden folders, like the .recycle of cephmount, hold no projects.
				if ri.GetType() == provider.ResourceType_RESOURCE_TYPE_CONTAINER && !strings.HasPrefix(path.Base(ri.GetPath()), ".") {
					below = append(below, ri.GetPath())
				}
			}
		}
		folders = below
	}
	return folders, nil
}

// levels returns how many names p has: 1 for /winspaces.
func levels(p string) int {
	p = strings.Trim(path.Clean(p), "/")
	if p == "" {
		return 0
	}
	return strings.Count(p, "/") + 1
}

// Close stops listening to Redis.
func (s *svc) Close(ctx context.Context) error {
	s.cancel()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

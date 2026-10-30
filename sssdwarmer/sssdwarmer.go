// Package sssdwarmer warms the SSSD cache of a cephmount node. Reva stores the
// groups of a user in Redis when it fetches them, for example at login. The
// service then looks the user up, as the cephmount driver does for the first
// request of a user, so that this request does not wait for LDAP. It does this
// only for users in a group of the project ACLs: warming every user filled the
// SSSD cache with users who never open a project.
package sssdwarmer

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cernbox/reva-plugins/cache"
	redispools "github.com/cernbox/reva-plugins/redispools"
	"github.com/cs3org/reva/v3"
	"github.com/cs3org/reva/v3/pkg/appctx"
	"github.com/cs3org/reva/v3/pkg/rjobs"
	"github.com/cs3org/reva/v3/pkg/rserverless"
	"github.com/cs3org/reva/v3/pkg/utils/cfg"
	"github.com/gomodule/redigo/redis"
	"github.com/rs/zerolog"
	"golang.org/x/sys/unix"
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
	// The project folders whose ACLs name the groups that give access to the projects
	ProjectGlob string `mapstructure:"project_glob" docs:"/mnt/*/project/*/*"`
	// How often the ACLs of the project folders are read again
	ScanIntervalSeconds int `mapstructure:"scan_interval_seconds" docs:"900"`
}

func (c *config) ApplyDefaults() {
	if c.RedisAddress == "" {
		c.RedisAddress = ":6379"
	}
	if c.RedisSentinelAddress == "" {
		c.RedisSentinelAddress = c.RedisAddress
	}
	if c.ProjectGlob == "" {
		c.ProjectGlob = "/mnt/*/project/*/*"
	}
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

	// aclGroups holds the lowercase names of the groups in the project ACLs.
	// It is nil until a scan succeeds, and until then every user is warmed.
	aclGroups atomic.Pointer[map[string]struct{}]
	// Counted since the last scan, which logs them.
	warmed, skipped, failed atomic.Int64
}

// New returns a new sssdwarmer service. The scan of the project ACLs is a
// periodic job, so the jobs service must be enabled in the same process.
func New(ctx context.Context, m map[string]any) (rserverless.Service, error) {
	var c config
	if err := cfg.Decode(m, &c); err != nil {
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

// scan reads the ACLs of the project folders and keeps the names of their
// groups. When it fails, the groups of the previous scan stay.
func (s *svc) scan(ctx context.Context) error {
	paths, err := filepath.Glob(s.conf.ProjectGlob)
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return fmt.Errorf("sssdwarmer: no folders match %s", s.conf.ProjectGlob)
	}

	gids := map[uint32]struct{}{}
	buf := make([]byte, 64*1024) // the largest xattr value Linux allows
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return err
		}
		for _, attr := range []string{"system.posix_acl_access", "system.posix_acl_default"} {
			// Fails with ENODATA when the folder has no such ACL.
			n, err := unix.Getxattr(path, attr, buf)
			if err != nil {
				continue
			}
			for _, gid := range aclGroupIDs(buf[:n]) {
				gids[gid] = struct{}{}
			}
		}
	}
	if len(gids) == 0 {
		return errors.New("sssdwarmer: no groups in the project ACLs")
	}

	names := make(map[string]struct{}, len(gids))
	for gid := range gids {
		// Groups that are not e-groups do not resolve.
		g, err := user.LookupGroupId(strconv.FormatUint(uint64(gid), 10))
		if err != nil {
			continue
		}
		names[strings.ToLower(g.Name)] = struct{}{}
	}
	if len(names) == 0 {
		return errors.New("sssdwarmer: no names for the groups in the project ACLs")
	}

	s.aclGroups.Store(&names)
	s.log.Info().
		Int("groups", len(names)).
		Int64("warmed", s.warmed.Swap(0)).
		Int64("skipped", s.skipped.Swap(0)).
		Int64("failed", s.failed.Swap(0)).
		Msg("sssdwarmer: scanned the project ACLs, counts are since the last scan")
	return nil
}

// The format of a POSIX ACL xattr value: a version, then one entry of 8 bytes
// (tag, permissions, id) for each item, all little endian.
const (
	aclVersion    = 2
	aclHeaderSize = 4
	aclEntrySize  = 8
	aclTagGroup   = 0x08
)

// aclGroupIDs returns the ids of the named groups in a POSIX ACL xattr value.
func aclGroupIDs(value []byte) []uint32 {
	if len(value) < aclHeaderSize || binary.LittleEndian.Uint32(value) != aclVersion {
		return nil
	}
	var gids []uint32
	for e := value[aclHeaderSize:]; len(e) >= aclEntrySize; e = e[aclEntrySize:] {
		if binary.LittleEndian.Uint16(e) == aclTagGroup {
			gids = append(gids, binary.LittleEndian.Uint32(e[4:]))
		}
	}
	return gids
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

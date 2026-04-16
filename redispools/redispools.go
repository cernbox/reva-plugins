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

package redispools

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cs3org/reva/v3/pkg/appctx"
	"github.com/gomodule/redigo/redis"
)

// RedisPools provides separate pools for read-local and write-master.
//
//   - Read pool always dials the configured address (local deployed instance).
//   - Write pool dials the configured address if sentinel is disabled, otherwise
//     discovers the current master via Sentinel and dials the master.
//
// Authentication:
//   - dialRedis uses the provided username/password.
//   - Sentinel is dialed using the same username/password, assuming Sentinel auth
//     matches Redis auth in this deployment.
type RedisPools struct {
	Read  *redis.Pool
	Write *redis.Pool
}

// SetVal sets a value in the redis pool
func (p *RedisPools) SetVal(key, val string, expiration int) error {
	conn := p.Write.Get()
	defer conn.Close()
	if conn != nil {
		args := []interface{}{key, val}
		if expiration != -1 {
			args = append(args, "EX", expiration)
		}
		if _, err := conn.Do("SET", args...); err != nil {
			return err
		}
		return nil
	}
	return errors.New("redispools: unable to get connection from redis pool")
}

// DoWithReadFallback runs fn on a connection from the read pool, falling back
// to the write pool if the read pool is unavailable or fn returns an error.
// redis.ErrNil is treated as a successful "not found" result and does not
// trigger a fallback. The connection is released before fn is retried on the
// write pool, so fn must not retain it.
func (p *RedisPools) DoWithReadFallback(ctx context.Context, fn func(redis.Conn) (interface{}, error)) (interface{}, error) {
	log := appctx.GetLogger(ctx)

	conn := p.Read.Get()
	if conn != nil && conn.Err() == nil {
		val, err := fn(conn)
		conn.Close()
		if err == nil || err == redis.ErrNil {
			return val, err
		}
		log.Debug().Err(err).Msg("redispools: read pool op failed, falling back to write pool")
	} else if conn != nil {
		log.Debug().Err(conn.Err()).Msg("redispools: read pool connection failed, falling back to write pool")
		conn.Close()
	} else {
		log.Debug().Msg("redispools: read pool provided nil connection, falling back to write pool")
	}

	conn = p.Write.Get()
	if conn == nil {
		return nil, errors.New("redispools: unable to get connection from redis pool")
	}
	defer conn.Close()

	return fn(conn)
}

// GetVal gets a value from the redis pool
func (p *RedisPools) GetVal(ctx context.Context, key string) (string, error) {
	val, err := p.DoWithReadFallback(ctx, func(c redis.Conn) (interface{}, error) {
		return redis.String(c.Do("GET", key))
	})
	if err != nil {
		return "", err
	}
	return val.(string), nil
}

func dialRedis(address, username, password string) (redis.Conn, error) {
	var opts []redis.DialOption
	if password != "" {
		if username != "" {
			opts = append(opts, redis.DialUsername(username))
		}
		opts = append(opts, redis.DialPassword(password))
	}
	return redis.Dial("tcp", address, opts...)
}

// NewRedisPoolsWithSentinelAddress allows specifying a separate Sentinel endpoint.
//
// - address: Redis data node address (used for reads, and for writes when sentinelMode=false)
// - sentinelAddress: Sentinel address (used only for master discovery when sentinelMode=true)
func NewRedisPoolsWithSentinelAddress(ctx context.Context, address, sentinelAddress, username, password string, sentinelMode bool, masterName string) (*RedisPools, error) {
	log := appctx.GetLogger(ctx)

	readPool := createRedisPool(func() (redis.Conn, error) {
		// Prefer local instance.
		c, err := dialRedis(address, username, password)
		if err == nil {
			return c, nil
		}

		// If local is down and sentinel mode is enabled, fall back to master.
		if sentinelMode {
			masterAddr, rerr := resolveSentinelMaster(sentinelAddress, username, password, masterName)
			if rerr == nil && masterAddr != "" {
				log.Debug().Any("masterAddr", masterAddr).Msg("read pool falling back to Sentinel-discovered master")
				return dialRedis(masterAddr, username, password)
			}
		}

		return nil, err
	}, false, sentinelMode)

	writePool := createRedisPool(func() (redis.Conn, error) {
		var target string

		if !sentinelMode {
			target = address
		} else {
			masterAddr, err := resolveSentinelMaster(sentinelAddress, username, password, masterName)
			log.Info().Any("masterAddr", masterAddr).Msg("rest: MasterAddresses resolved by Sentinel")
			if err != nil {
				// Do not fall back to `address` because it's typically a read-only replica,
				// which would cause "READONLY" errors. Return the error directly to surface the root cause.
				return nil, fmt.Errorf("resolveMaster failed: %w", err)
			}
			target = masterAddr
		}

		c, err := dialRedis(target, username, password)
		if err != nil {
			return nil, fmt.Errorf("dialRedis to %s failed: %w", target, err)
		}

		if sentinelMode {
			// Specifically verify that the newly dialed node answers as "master".
			// This prevents silent fallback errors or misconfigured Sentinels pointing us to a replica.
			reply, err := redis.Values(c.Do("ROLE"))
			if err == nil && len(reply) > 0 {
				if role, err := redis.String(reply[0], nil); err == nil && role != "master" {
					c.Close()
					return nil, fmt.Errorf("write-pool dialed %s, but its role is %s (expected master). Check Redis/Sentinel configuration", target, role)
				}
			}
		}

		// Add a successful connection log for your debug
		log.Info().Any("target", target).Msg("Successfully connected to write-pool target")

		return c, nil
	}, true, sentinelMode)

	return &RedisPools{Read: readPool, Write: writePool}, nil
}

// createRedisPool creates a redis.Pool with the provided dial function and test on borrow logic.
// including the optional check for master role if sentinel mode is enabled.
func createRedisPool(dial func() (redis.Conn, error), isWritePool bool, sentinelMode bool) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     50,
		MaxActive:   1000,
		IdleTimeout: 240 * time.Second,
		Dial:        dial,
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			if err != nil {
				return err
			}
			if isWritePool && sentinelMode {
				reply, err := redis.Values(c.Do("ROLE"))
				if err == nil && len(reply) > 0 {
					if role, err := redis.String(reply[0], nil); err == nil && role != "master" {
						return errors.New("redis connection is not a master")
					}
				}
			}
			return err
		},
	}
}

// resolveSentinelMaster connects to the Sentinel and retrieves the current master address for the given master name.
func resolveSentinelMaster(sentinelAddress, username, password, masterName string) (string, error) {
	if masterName == "" {
		return "", errors.New("utils: redis_sentinel_mode enabled but redis_master_name is empty")
	}

	sentinelConn, err := dialRedis(sentinelAddress, username, password)
	if err != nil {
		return "", err
	}
	defer sentinelConn.Close()

	reply, err := redis.Values(sentinelConn.Do("SENTINEL", "get-master-addr-by-name", masterName))
	if err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "unknown command") && strings.Contains(msg, "sentinel") {
			return "", errors.New("utils: sentinelAddress is not a Sentinel endpoint")
		}
		return "", err
	}
	if len(reply) != 2 {
		return "", errors.New("utils: invalid sentinel reply for get-master-addr-by-name")
	}

	host, err := redis.String(reply[0], nil)
	if err != nil {
		return "", err
	}
	port, err := redis.String(reply[1], nil)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%s", host, port), nil
}

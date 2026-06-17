package cache

import (
    "context"
    "encoding/json"

    redispools "github.com/cernbox/reva-plugins/redispools"
    "github.com/gomodule/redigo/redis"
)

// store JSON-marshals v and writes it to Redis under key with a TTL in seconds.
// Pass ttlSecs = -1 for no expiry.
func store(pools *redispools.RedisPools, key string, v any, ttlSecs int) error {
    encoded, err := json.Marshal(v)
    if err != nil {
        return err
    }
    return pools.SetVal(key, string(encoded), ttlSecs)
}

// fetch retrieves key from Redis and JSON-unmarshals the value into v.
// Returns an error if the key is absent or the value cannot be decoded.
func fetch(ctx context.Context, pools *redispools.RedisPools, key string, v any) error {
    raw, err := pools.GetVal(ctx, key)
    if err != nil {
        return err
    }
    return json.Unmarshal([]byte(raw), v)
}

// scanAndFetch runs KEYS <pattern> followed by MGET and returns the raw
// string values. Duplicate keys are collapsed by Redis itself. An empty
// result set is not an error — callers get back an empty slice.
func scanAndFetch(ctx context.Context, pools *redispools.RedisPools, pattern string) ([]string, error) {
    raw, err := pools.DoWithReadFallback(ctx, func(conn redis.Conn) (any, error) {
        keys, err := redis.Strings(conn.Do("KEYS", pattern))
        if err != nil {
            return nil, err
        }
        if len(keys) == 0 {
            return []string{}, nil
        }
        args := make([]any, len(keys))
        for i, k := range keys {
            args[i] = k
        }
        return redis.Strings(conn.Do("MGET", args...))
    })
    if err != nil {
        return nil, err
    }
    return raw.([]string), nil
}

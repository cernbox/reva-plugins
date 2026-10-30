# SSSD warmer

A serverless service for the revad of a cephmount node. The cephmount driver
looks up each user and their groups through SSSD. When SSSD has to ask LDAP,
this takes about one second, which is almost the whole time the spaces registry
gives a project, so the project can be left out of the user's spaces.

The service looks the user up before that first request. It listens to the
Redis events of the `user:groups:<user>` keys, which the `user/rest` plugin
writes when it fetches the groups of a user, for example at login. It warms
only users who are in a group of the project ACLs, because warming every user
filled the SSSD cache with users who never open a project.

It replaces the `cboxusergroupd` daemon.

## How it works

- A periodic job, `sssdwarmer.acl_groups`, reads the ACLs of the project
  folders at start and then every `scan_interval_seconds`, and keeps the names
  of their groups. It runs in every process that has the service, so the jobs
  service must be enabled there too. It needs no NATS.
- For each `user:groups:<user>` event, the service reads the groups of the
  user from Redis. If one of them is in a project ACL, it looks the user up and
  gets their group ids, like a new cephmount user thread.
- When it cannot read the groups, or no scan has worked yet, it warms the user
  anyway.
- Each scan logs the number of groups, and the users warmed, skipped and
  failed since the previous scan. The single users are logged at debug level.

Redis must send the keyspace events of the string commands:
`notify-keyspace-events` must contain at least `K` and `$`.

Enable it in one revad per node: every revad that has it listens to Redis and
warms the same users.

## Configuration

```toml
[serverless.services.jobs]

[serverless.services.sssdwarmer]
redis_address = "redis.example.org:6379"
redis_sentinel_address = "redis.example.org:26379"
redis_sentinel_mode = true
redis_master_name = "master"
redis_password = "secret"
# defaults
project_glob = "/mnt/*/project/*/*"
scan_interval_seconds = 900
```

# SSSD warmer

A serverless service for the revad of a cephmount node. The cephmount driver
looks up each user and their groups through SSSD. When SSSD has to ask LDAP,
this takes about one second, which is almost the whole time the spaces registry
gives a project, so the project can be left out of the user's spaces.

The service looks the user up before that first request. It listens to the
Redis events of the `user:groups:<user>` keys, which the `user/rest` plugin
writes when it fetches the groups of a user, for example at login. It warms
only users who are in a group with a grant on a project, because warming every
user filled the SSSD cache with users who never open a project.

It replaces the `cboxusergroupd` daemon.

## How it works

- A periodic job, `sssdwarmer.acl_groups`, reads the grants of the projects
  at start and then every `scan_interval_seconds`, and keeps the names of their
  groups. For each mount in `mount_ids`, it asks the storage registry for the
  address of the storage provider, lists the project folders (`space_depth`
  deep, from `mount_path`) with `ListContainer`, and reads their grants with
  `ListGrants`. Hidden folders, like `.recycle`, are left out. It runs in
  every process that has the service, so the jobs service must be enabled
  there too. The jobs need no NATS, but the storage registry is found through
  the service registry, like the gateway.
- The scan calls the storage registry and providers as the service user, with
  a token signed with `jwt_secret`. The cephmount driver lists a folder as the
  caller, so the service user must exist on the node and be allowed to list the
  folders above the projects.
- A mount the registry does not know, a folder the service user cannot list, or
  a project whose grants it cannot read, is skipped with a warning. A registry
  or provider that cannot be reached fails the scan, and the groups of the
  previous scan stay.
- For each `user:groups:<user>` event, the service reads the groups of the
  user from Redis. If one of them has a grant on a project, it looks the user
  up and gets their group ids, like a new cephmount user thread.
- When it cannot read the groups, or no scan has worked yet, it warms the user
  anyway.
- Each scan logs the number of projects and groups, and the users warmed,
  skipped and failed since the previous scan. The single users are logged at debug level.

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
# the cephmount mounts on the node
mount_ids = ["mount_1", "mount_2"]
service_user_name = "account_name"
service_user_uid = 12345
service_user_gid = 12345
# defaults
mount_path = "/winspaces"   # the mount_path of the storage providers
space_depth = 3             # their space_depth
jwt_secret = ""             # falls back to [shared]
scan_interval_seconds = 900
```

# Reva plugins

This repository hosts a collection of plugins designed for CERNBox

## Available Plugins

Here is a list of plugins currently available in this repository:

* thumbnails: An HTTP service generating thumbnails.
* otg: An HTTP service exposing an OTG message (deprecated).
* cback: An HTTP service and a storage provider to access and restore CERNBox backups.
* rest: A REDIS-based cache for users and groups.

For more information about each plugin, please refer to the respective plugin's README file in the `<plugin>/` directory.

## CLI plugins

The `cernbox` build of the reva CLI adds commands from the packages in `cli/`. They call the invocations of the
backend plugins through the Admin API:

* `cernbox otg get|set|clear`: the `otg` invocations.
* `cernbox user account <user>`: the `driver.account` invocation of the userprovider (`user/rest`).

The package `github.com/cernbox/reva-plugins/cli` imports only the CLI plugins, and the root package only the
backend plugins, so that revad and the CLI each link only their own plugins. A plugin that the CLI refuses (for
example, a name that a core command has) only gives a warning when the CLI starts. The test of `cli` runs
`revacmd.CheckPlugins`, so that such a defect fails CI.

## Development

`go.mod` requires a released version of reva. To build against a local reva checkout, use a Go workspace. Do not
commit it: `.gitignore` excludes `go.work` and `go.work.sum`.

```sh
go work init . ../reva
go build ./...
go test ./...
```

To build against the version in `go.mod` again, remove `go.work` or set `GOWORK=off`.

CI builds and tests against the version in `go.mod` and against reva master, so that a change in reva that breaks the
plugins shows early.

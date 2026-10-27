# Reva plugins

This repository hosts a collection of plugins designed for CERNBox

## Available Plugins

Here is a list of plugins currently available in this repository:

* thumbnails: An HTTP service generating thumbnails.
* otg: An HTTP service exposing an OTG message (deprecated).
* cback: An HTTP service and a storage provider to access and restore CERNBox backups.
* rest: A REDIS-based cache for users and groups.
* projects/cernbox: The projects catalogue of the spacesregistry, with the CERN columns.

For more information about each plugin, please refer to the respective plugin's README file in the `<plugin>/` directory.

## CLI plugins

The `cernbox` build of the reva CLI adds commands from the packages in `cli/`. They call the invocations of the
backend plugins through the Admin API:

* `cernbox otg get|set|clear`: the `otg` invocations.
* `cernbox user account <user>`: the `driver.account` invocation of the userprovider (`user/rest`).

The user-plane commands of cernbox-cli use the backend interface and the data plane of the CLI, over gRPC or
HTTPS:

* `cernbox copy|paste` and `cernbox clipboard list|clear` (`cli/clipboard`): the clipboard across machines and
  people, kept in the home space.
* `cernbox inbox pull|watch|status` (`cli/inbox`) and `cernbox outbox push|watch|status` (`cli/outbox`): a
  CERNBox folder whose arrivals come down, and a local folder whose arrivals go up. The folders are
  `[plugins.inbox] folders` and `[plugins.outbox] folders` of the config files.

`cli/auth/cernsso` adds the sign-in method `cern-sso`: the Kerberos ticket of kinit signs in to CERN SSO, which
issues an OIDC token. It comes after `kerberos` in the chain of the brand.

`cli/cernbox.toml` is the brand of the build: the name, the CERNBox endpoint, the sign-in chain, the env names of
cernbox-cli and the texts. gaia embeds it with `--brand`, and the test of `cli` checks it with `revacmd.CheckBrand`
against the plugins of this module.

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

# CERNBoxSpaces plugin

The CERNBoxSpaces service is an HTTP plugin for reva that keeps an index of which EOS projects and Windows spaces a user is part of.

It supports a `type` query parameter to either return the EOS projects or the WinSpaces.

## Configuration

```
[http.services.cernboxspaces]
username = "dbuser"
password = "dbpassword"
host = "dbhost.example.org"
port = 3306
name = "dbname"
table = "cbox_projects"
prefix = "cernboxspaces"
```

# EOSProjects plugin

The EOSProjects service is an HTTP plugin for reva that keeps an index of which projects a user is part of.

## Configuration

```
[http.services.eosprojects]
username = "root"
password = "password"
host = "host.example.org"
port = 3306
name = "dbname"
table = "cbox_projects"
prefix = "projects"
```

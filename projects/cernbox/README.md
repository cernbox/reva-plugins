# projects/cernbox catalogue

The projects catalogue of CERNBox for the spacesregistry. It is the `sql` catalogue of reva, and it also keeps the
columns of the `projects` table that only CERN uses: `service_account`, `snow_ticket`, `backup_name`,
`initial_capacity_bytes`, `user_provided_description`, `comments` and `resource_admins`.

The Admin API shows these columns in the `extra` of each project (`list`, `get`, `set_status`, `set_owner`, `create`,
`delete`, `update`). The users of the CS3 calls do not see them. `driver.set_info` sets them.

## Configuration

The keys are those of the `sql` catalogue.

```
[grpc.services.spacesregistry]
driver = "cernbox"

[grpc.services.spacesregistry.drivers.cernbox]
path_template = "/eos/project/{{ .Initial }}/{{ .Name }}"
storage_id_template = "newproject-{{ .Initial }}"
readers_group_template = "cernbox-project-{{ .Name }}-readers"
writers_group_template = "cernbox-project-{{ .Name }}-writers"
admins_group_template = "cernbox-project-{{ .Name }}-admins"
```

At start, the driver adds the columns that the table does not have. It does not change the columns that exist.

## Example

```
reva invoke spacesregistry driver.set_info name=cernbox snow_ticket=RQF0000001 backup_name=cernbox-x1y2
```

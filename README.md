# sqldb-sqlx capability provider

This capability provider allows wasmCloud actors to connect to Postgres, MySQL,
or MS SQL databases, and implements the `wasmcloud:sqldb` capability contract.

### Capabilities:

- execute statements (create table, insert, update, etc.)
- select statements with parameters
- configurable connection pool with sensible defaults

### JSON Configuration settings

| Setting                  | Description                                                                                                                                                                                                                                                                                             |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `uri`                    | database connection string. Must begin with scheme `postgres://` or `postgresql://`. Example: `postgresql://user:password@host:5678?dbname=customers`. See [uri reference](https://docs.rs/tokio-postgres/0.7.2/tokio_postgres/config/struct.Config.html) for complete documentation on all the options |
| `pool.max_connections`   | max size of connection pool. Default is 8                                                                                                                                                                                                                                                               |
| `pool.min_idle`          | minimum number of idle connections in pool. Default is 0. With this default, the provider does not consume resources until needed. If you need fast application startup time, you may wish to set this to 1 or more, and increase max_lifetime_secs to 86400.                                           |
| `pool.max_lifetime_secs` | when a connection has reached this age, after it has finished processing its current workload, it is closed instead of being returned to the pool. Default is 7200 (2 hours).                                                                                                                           |
| `pool.idle_timeout_secs` | the amount of time a connection will remain idle in the pool before it is closed. This setting can be useful to reduce billing costs if your database is billed by connection-time. Default is 600 (10 minutes).                                                                                        |

### Link

- Edit `linkdata.json` to adjust your settings. To make these active when
  linking an actor to this provider, set a link value with key `config_b64` with
  value from the output of the command

```shell
base64 -w0 linkdata.json
```

### Limitations:

The following features are not currently supported:

- transactions
- batch operations
- streaming results
- prepared statements
- query results contain any Array type, Custom data type, or other column type
  not listed in the table below.

### Supported Postgres data types

Conversion from Postgres data type to CBOR data types

| Supported Data Types | CBOR type | Notes                    |
| -------------------- | --------- | ------------------------ |
| BOOL                 | boolean   |                          |
| CHAR                 | i8        |                          |
| INT2                 | i16       |                          |
| INT4                 | i32       |                          |
| INT8                 | i64       |                          |
| OID                  | u32       |                          |
| FLOAT4               | f32       |                          |
| FLOAT8               | f64       |                          |
| CHAR_ARRAY           | string    |                          |
| VARCHAR              | string    |                          |
| TEXT                 | string    |                          |
| NAME                 | string    |                          |
| JSON                 | string    |                          |
| BYTEA                | bytes     |                          |
| UUID                 | string    | UUID converted to string |
| TIMESTAMP            | string    | RFC3339 format, in UTC   |
| DATE                 | string    |                          |
| TIME                 | string    |                          |

Build with 'make'. Test with 'make test'.

### Using the included Github Actions

If you store your source code on Github, we've gone ahead and included two
actions: `build.yml` and `release.yml` under `.github/workflows`. The build
action will automatically build, lint, and check formatting for your actor. The
release action will automatically release a new version of your actor whenever
code is pushed to `main`, or when you push a tag with the form `vX.Y.Z`.

These actions require 3 secrets

1. `WASH_ISSUER_KEY`, which can be generated with `wash keys gen issuer`, then
   look for the 58 character `Seed` value

2. `WASH_SUBJECT_KEY`, which can be generated with `wash keys gen module`, then
   look for the 58 character `Seed` value

3. `WASMCLOUD_PAT`, which can be created by following the [Github PAT
   instructions](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)
   and ensuring the `write:packages` permission is enabled

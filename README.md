# Databricks input plugin for Embulk

Databricks input plugin for Embulk loads records from Databricks.

## Overview

* **Plugin type**: input
* **Resume supported**: yes

## Configuration

- **driver_path**: path to the jar file of the JDBC driver. If not set, [the bundled JDBC driver](https://docs.databricks.com/en/integrations/jdbc/index.html) will be used. (string, optional)
- **options**: extra JDBC properties (hash, default: {})
- **server_hostname**: The Databricks compute resource’s Server Hostname value, see [Compute settings for the Databricks JDBC Driver](https://docs.databricks.com/en/integrations/jdbc/compute.html). (string, required)
- **http_path**: The Databricks compute resource’s HTTP Path value, see [Compute settings for the Databricks JDBC Driver](https://docs.databricks.com/en/integrations/jdbc/compute.html). (string, required)
- **auth_type**: The Databricks authentication type, personal access token (PAT)-based or machine-to-machine (M2M) authentication. (`pat`, `oauth-m2m`, default: `pat`)
- If **auth_type** is `pat`,
  - **personal_access_token**: The Databaricks personal_access_token, see [Authentication settings for the Databricks JDBC Driver](https://docs.databricks.com/en/integrations/jdbc/authentication.html#authentication-pat). (string, required)
- If **auth_type** is `m2m-auth`,
  - **oauth2_client_id**: The Databaricks oauth2_client_id, see [Use a service principal to authenticate with Databricks](https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html). (string, required)
  - **oauth2_client_secret**: The Databaricks oauth2_client_secret, see [Use a service principal to authenticate with Databricks](https://docs.databricks.com/en/dev-tools/auth/oauth-m2m.html). (string, required)
- **catalog_name**: destination catalog name (string, optional)
- **schema_name**: destination schema name (string, optional)
- **where**: WHERE condition to filter the rows (string, default: no-condition)
- **fetch_rows**: number of rows to fetch one time (used for java.sql.Statement#setFetchSize) (integer, default: 10000)
- **connect_timeout**: timeout for establishment of a database connection. (integer (seconds), default: 300)
- **socket_timeout**: timeout for socket read operations. 0 means no timeout. (integer (seconds), default: 1800)
- If you write SQL directly,
  - **query**: SQL to run (string)
- If **query** is not set,
  - **table**: destination table name (string, required)
  - **select**: expression of select (e.g. `id, created_at`) (string, default: "*")
  - **where**: WHERE condition to filter the rows (string, default: no-condition)
  - **order_by**: expression of ORDER BY to sort rows (e.g. `created_at DESC, id ASC`) (string, default: not sorted)
- **incremental**: if true, enables incremental loading. See next section for details (boolean, default: false)
- **incremental_columns**: column names for incremental loading (array of strings, default: use primary keys). Columns of integer types, string types, `timestamp` are supported.
- **last_record**: values of the last record for incremental loading (array of objects, default: load all records)
- **default_timezone**: If the sql type of a column is `date`/`time`/`datetime` and the embulk type is `string`, column values are formatted int this default_timezone. You can overwrite timezone for each columns using column_options option. (string, default: `UTC`)
- **default_column_options**: advanced: column_options for each JDBC type as default. key-value pairs where key is a JDBC type (e.g. 'DATE', 'BIGINT') and value is same as column_options's value.
- **column_options**: advanced: key-value pairs where key is a column name and value is options for the column.
  - **value_type**: embulk get values from database as this value_type. Typically, the value_type determines `getXXX` method of `java.sql.PreparedStatement`.
  (string, default: depends on the sql type of the column. Available values options are: `long`, `double`, `float`, `decimal`, `boolean`, `string`, `json`, `date`, `time`, `timestamp`)
  - **type**: Column values are converted to this embulk type.
  Available values options are: `boolean`, `long`, `double`, `string`, `json`, `timestamp`).
  By default, the embulk type is determined according to the sql type of the column (or value_type if specified).
  - **timestamp_format**: If the sql type of the column is `date`/`time`/`datetime` and the embulk type is `string`, column values are formatted by this timestamp_format. And if the embulk type is `timestamp`, this timestamp_format may be used in the output plugin. For example, stdout plugin use the timestamp_format, but *csv formatter plugin doesn't use*. (string, default : `%Y-%m-%d` for `date`, `%H:%M:%S` for `time`, `%Y-%m-%d %H:%M:%S` for `timestamp`)
  - **timezone**: If the sql type of the column is `date`/`time`/`datetime` and the embulk type is `string`, column values are formatted in this timezone.
(string, value of default_timezone option is used by default)
- **before_setup**: if set, this SQL will be executed before setup. You can prepare table for input by this option.
- **before_select**: if set, this SQL will be executed before the SELECT query. (Other plugins execute query in the same transaction, but Databricks does not support transaction in multi statement, so this plugin does not support it.)
- **after_select**: if set, this SQL will be executed after the SELECT query. (Other plugins execute query in the same transaction, but Databricks does not support transaction in multi statement, so this plugin does not support it.)


### Incremental loading

Incremental loading uses monotonically increasing unique columns (such as auto-increment (IDENTITY) column) to load records inserted (or updated) after last execution.

First, if `incremental: true` is set, this plugin loads all records with additional ORDER BY. For example, if `incremental_columns: [updated_at, id]` option is set, query will be as following:

```
SELECT * FROM (
  ...original query is here...
)
ORDER BY updated_at, id
```

When bulk data loading finishes successfully, it outputs `last_record: ` paramater as config-diff so that next execution uses it.

At the next execution, when `last_record: ` is also set, this plugin generates additional WHERE conditions to load records larger than the last record. For example, if `last_record: ["2017-01-01T00:32:12.487659", 5291]` is set,

```
SELECT * FROM (
  ...original query is here...
)
WHERE updated_at > '2017-01-01 00:32:12.487659' OR (updated_at = '2017-01-01 00:32:12.487659' AND id > 5291)
ORDER BY updated_at, id
```

Then, it updates `last_record: ` so that next execution uses the updated last_record.

**IMPORTANT**: If you set `incremental_columns: ` option, make sure that there is an index on the columns to avoid full table scan. For this example, following index should be created:

```
CREATE INDEX embulk_incremental_loading_index ON table (updated_at, id);
```

Recommended usage is to leave `incremental_columns` unset and let this plugin automatically finds an auto-increment (IDENTITY) primary key. Currently, only strings, integers, TIMESTAMP and TIMESTAMPTZ are supported as incremental_columns.

## Example

```yaml
in:
  type: databricks
  server_hostname: dbc-xxxx.cloud.databricks.com
  http_path: /sql/1.0/warehouses/xxxxx
  personal_access_token: dapixxxxxx
  catalog_name: test_catalog
  schema_name: test_schema
  table: test_date
  select: "col1, col2, col3"
  where: "col4 != 'a'"
  order_by: "col1 DESC"
```

This configuration will generate following SQL:

```
SELECT col1, col2, col3
FROM "my_table"
WHERE col4 != 'a'
ORDER BY col1 DESC
```

If you need a complex SQL,

```yaml
in:
  type: databricks
  server_hostname: dbc-xxxx.cloud.databricks.com
  http_path: /sql/1.0/warehouses/xxxxx
  personal_access_token: dapixxxxxx
  catalog_name: test_catalog
  query: |
    SELECT t1.id, t1.name, t2.id AS t2_id, t2.name AS t2_name
    FROM table1 AS t1
    LEFT JOIN table2 AS t2
      ON t1.id = t2.t1_id
```

Advanced configuration:

```yaml
in:
  type: databricks
  server_hostname: dbc-xxxx.cloud.databricks.com
  http_path: /sql/1.0/warehouses/xxxxx
  personal_access_token: dapixxxxxx
  catalog_name: test_catalog
  schema_name: test_schema
  table: test_date
  select: "col1, col2, col3"
  where: "col4 != 'a'"
  default_column_options:
    TIMESTAMP: { type: string, timestamp_format: "%Y/%m/%d %H:%M:%S", timezone: "+0900"}
    BIGINT: { type: string }
  column_options:
    col1: {type: long}
    col3: {type: string, timestamp_format: "%Y/%m/%d", timezone: "+0900"}
  after_select: "update my_table set col5 = '1' where col4 != 'a'"

```

## NOTE

### Correspondence table for databrick types and JDBC Types

| databrick types | JDBC Types  | 
|--------------- |----------- |
| BIGINT          | BIGINT      | 
| BINARY          | unsupported | 
| BOOLEAN         | BOOLEAN     | 
| DATE            | DATE        | 
| DECIMAL         | DECIMAL     | 
| DOUBLE          | DOUBLE      | 
| FLOAT           | REAL        | 
| INT             | INTEGER     | 
| INTERVAL        | VARCHAR     | 
| SMALLINT        | SMALLINT    | 
| STRING          | VARCHAR     | 
| TIMETAMP        | TIMESTAMP   | 
| TIMETAMP\_NTZ   | unsupported | 
| TINYINT         | TINYINT     | 
| ARRAY           | VARCHAR     | 
| MAP             | VARCHAR     | 
| STRUCT          | VARCHAR     | 

### TIMESTAMP_NTZ

[The official Databricks JDBC driver does not support TIMESTAMP_NTZ](https://docs.databricks.com/en/sql/language-manual/data-types/timestamp-ntz-type.html#notes), so this plugin officially does not support TIMESTAMP_NTZ.


## Build

```
$ ./gradlew gem
```

Running tests:

```
$ EMBULK_INPUT_DATABRICKS_TEST_CONFIG="example/test.yml" ./gradlew test # Create example/test.yml based on example/test.yml.example
```

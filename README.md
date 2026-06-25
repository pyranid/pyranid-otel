# pyranid-otel

OpenTelemetry metrics integration for [Pyranid](https://www.pyranid.com).

## Installation

```xml
<dependency>
  <groupId>com.pyranid</groupId>
  <artifactId>pyranid-otel</artifactId>
  <version>1.1.0</version>
</dependency>
```

## Configuration

```java
OpenTelemetryMetricsCollector metricsCollector = OpenTelemetryMetricsCollector
  .withOpenTelemetry(openTelemetry)
  .poolName("primary")
  .namespace("orders")
  .recordCollectionName(false)
  .build();

Database database = Database.withDataSource(dataSource)
  .databaseType(DatabaseType.POSTGRESQL)
  .metricsCollector(metricsCollector)
  .build();
```

`poolName(null)` or leaving it unset skips pool-name-gated connection metrics. `namespace(null)` omits `db.namespace`; no JDBC metadata lookup is performed by the builder. `recordCollectionName(false)` is the default and avoids table-name cardinality unless you opt in.

When `recordCollectionName(true)` is enabled, `db.collection.name` is extracted with a lightweight SQL heuristic, not a full SQL parser. It works best for simple single-table `SELECT`, `INSERT`, `UPDATE`, and `DELETE` statements. Multi-table joins, CTEs, subqueries, aliases, schema-qualified names, and generated SQL may produce an incomplete or surprising collection name. Leading block and line comments are skipped before detecting `db.operation.name`.

## Metrics

Stable OpenTelemetry semantic-convention metric:

- `db.client.operation.duration`

Development-status OpenTelemetry semantic-convention metrics:

- `db.client.response.returned_rows`
- `db.client.connection.wait_time` when `poolName` is configured
- `db.client.connection.use_time` when `poolName` is configured

Pyranid-specific metrics:

- `pyranid.statement.preparation.duration`
- `pyranid.statement.execution.duration`
- `pyranid.statement.mapping.duration`
- `pyranid.statement.errors`
- `pyranid.statement.batch.size`
- `pyranid.statement.rows_affected`
- `pyranid.transaction.closure.duration`
- `pyranid.transaction.commit.duration`
- `pyranid.transaction.rollback.duration`
- `pyranid.transaction.physical.begin_failures`
- `pyranid.transaction.count`
- `pyranid.transaction.active`
- `pyranid.savepoint.operations`
- `pyranid.fetchstream.duration`
- `pyranid.fetchstream.rows_consumed`
- `pyranid.post_transaction.operations`

Pyranid does not synthesize pool-internal metrics such as idle counts, max connections, pending requests, timeouts, or create time. Those belong to the connection pool or proxy.

## Attribute Notes

`db.system.name` is mapped from Pyranid's configured or detected database type:

| Pyranid `DatabaseType` | `db.system.name` |
| --- | --- |
| `POSTGRESQL` | `postgresql` |
| `MYSQL` | `mysql` |
| `MARIA_DB` | `mariadb` |
| `SQLITE` | `sqlite` |
| `SQL_SERVER` | `microsoft.sql_server` |
| `ORACLE` | `oracle.db` |
| `GENERIC` | `other_sql` |

`db.operation.name` is the first SQL verb after leading SQL comments, uppercased. This adapter does not parse or normalize full SQL grammar.

## Development Verification

From the `pyranid-otel/` project directory, run:

```bash
mvn -q verify
mvn -q javadoc:javadoc
```

Artifact signing and Maven Central publishing are isolated in the `release` profile. Use `mvn -P release deploy` only when publishing a release or snapshot; normal local and CI `verify` runs do not require GPG credentials.

## Compatibility

This release depends on Pyranid `4.4.0` and pins OpenTelemetry Java BOM `1.61.0`. Future minor releases may bump the OTel BOM when database semantic conventions or instrument behavior changes.

The jar declares the automatic JPMS module name `com.pyranid.otel`.

## Notes

`OpenTelemetryMetricsCollector.snapshot()` returns `Optional.empty()` because metrics are read through the OTel SDK/exporter pipeline.

This adapter does not emit `Statement.id`, full SQL text, `db.query.text`, trace/span data, or arbitrary query parameters. Those values are user-controlled or high-cardinality and belong in tracing or custom instrumentation when needed. Custom collectors can still read the full `StatementContext`.

`db.client.connection.use_time` measures connection hold time, not exact physical transaction lifetime. It includes transaction setup and cleanup around the commit/rollback operation.

`pyranid.transaction.active` increments when a physical JDBC transaction begins and decrements once at the terminal physical transaction outcome. Commit failure is not terminal by itself because Pyranid attempts rollback afterward; the rollback callback owns the single decrement. Physical-begin failures do not decrement unless the transaction was first recorded as active.

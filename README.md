# pyranid-otel

OpenTelemetry metrics integration for [Pyranid](https://www.pyranid.com).

## Installation

```xml
<dependency>
  <groupId>com.pyranid</groupId>
  <artifactId>pyranid-otel</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

`pyranid-otel` is optional. The core `com.pyranid:pyranid` artifact remains dependency-free.

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

## Compatibility

This module pins OpenTelemetry Java BOM `1.61.0`, chosen at implementation time for Pyranid `4.2.0-SNAPSHOT`. Future minor releases may bump the OTel BOM when database semantic conventions or instrument behavior changes.

No JPMS module metadata is declared in `1.0.0-SNAPSHOT`.

## Notes

`OpenTelemetryMetricsCollector.snapshot()` returns `Optional.empty()` because metrics are read through the OTel SDK/exporter pipeline.

`Statement.id` from `Query.id(...)` is not emitted as a default metric attribute. It is user-controlled and can be high-cardinality; custom collectors can still read it from `StatementContext`.

`db.client.connection.use_time` measures connection hold time, not exact physical transaction lifetime. It includes transaction setup and cleanup around the commit/rollback operation.

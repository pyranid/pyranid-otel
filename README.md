# pyranid-otel

OpenTelemetry metrics integration for [Pyranid](https://www.pyranid.com), implemented by
[`OpenTelemetryMetricsCollector`](https://otel.javadoc.pyranid.com/com/pyranid/otel/OpenTelemetryMetricsCollector.html),
an implementation of Pyranid's
[`MetricsCollector`](https://javadoc.pyranid.com/com/pyranid/MetricsCollector.html) interface.

## Installation

```xml
<dependency>
  <groupId>com.pyranid</groupId>
  <artifactId>pyranid-otel</artifactId>
  <version>1.3.0-SNAPSHOT</version>
</dependency>
```

## Configuration

Create a collector with
[`OpenTelemetryMetricsCollector::withOpenTelemetry(...)`](https://otel.javadoc.pyranid.com/com/pyranid/otel/OpenTelemetryMetricsCollector.html#withOpenTelemetry(io.opentelemetry.api.OpenTelemetry)) by supplying an
[`OpenTelemetry`](https://javadoc.io/static/io.opentelemetry/opentelemetry-api/1.64.0/io/opentelemetry/api/OpenTelemetry.html)
instance, then configure it with
[`Database.Builder::metricsCollector(...)`](https://javadoc.pyranid.com/com/pyranid/Database.Builder.html#metricsCollector(com.pyranid.MetricsCollector)):

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

Specifying [`poolName(null)`](https://otel.javadoc.pyranid.com/com/pyranid/otel/OpenTelemetryMetricsCollector.Builder.html#poolName(java.lang.String)) or leaving it unset skips pool-name-gated connection metrics. [`namespace(null)`](https://otel.javadoc.pyranid.com/com/pyranid/otel/OpenTelemetryMetricsCollector.Builder.html#namespace(java.lang.String)) omits `db.namespace`; no JDBC metadata lookup is performed by the builder. [`recordCollectionName(false)`](https://otel.javadoc.pyranid.com/com/pyranid/otel/OpenTelemetryMetricsCollector.Builder.html#recordCollectionName(java.lang.Boolean)) is the default and avoids table-name cardinality unless you opt in.

When `recordCollectionName(true)` is enabled, a conservative SQL lexer extracts `db.collection.name` for simple single-table `SELECT`, `INSERT`, `UPDATE`, and `DELETE` statements. It understands SQL comments, string literals, common quoted-string forms, quoted identifiers, and schema-qualified names. It omits the attribute for CTEs, subqueries, functions, joins, and other ambiguous forms instead of guessing. This remains an opt-in convenience, not a full dialect-specific SQL parser.

Full SQLSTATE values are emitted as `db.response.status_code` by default. Use [`recordFullSqlState(false)`](https://otel.javadoc.pyranid.com/com/pyranid/otel/OpenTelemetryMetricsCollector.Builder.html#recordFullSqlState(java.lang.Boolean)) to emit only the two-character SQLSTATE class. `error.type` prefers the underlying [`SQLException`](https://docs.oracle.com/en/java/javase/26/docs/api/java.sql/java/sql/SQLException.html) or deepest application cause when Pyranid wraps an error.

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
- `pyranid.post_transaction.duration`
- `pyranid.notification.session.open.duration`
- `pyranid.notification.session.duration`
- `pyranid.notification.session.active`
- `pyranid.notification.batch.size`
- `pyranid.notification.connection.losses`

Pyranid does not synthesize pool-internal metrics such as idle counts, max connections, pending requests, timeouts, or create time. Those belong to the connection pool or proxy.

### Notification Metrics

`pyranid.notification.session.open.duration` is a double histogram in seconds with the bounded `pyranid.notification.session.open_outcome` attribute (`success` or `failure`). Failures also include `error.type`.

`pyranid.notification.session.duration` is a double histogram in seconds for sessions that opened successfully. Its bounded `pyranid.notification.session.outcome` attribute is `callback_returned`, `interrupted`, or `failed`; `error.type` is present when the terminal callback supplies a failure. The duration includes opening and cleanup, matching Pyranid's [`MetricsCollector::didCloseNotificationSession(...)`](https://javadoc.pyranid.com/com/pyranid/MetricsCollector.html#didCloseNotificationSession(com.pyranid.DatabaseType,java.util.UUID,com.pyranid.MetricsCollector.NotificationSessionOutcome,java.time.Duration,java.lang.Throwable)) lifecycle callback.

`pyranid.notification.session.active` is a long up/down counter with unit `{session}`. It increments after a session has opened and all requested channels are registered, and decrements after terminal cleanup. Session identifiers are used only for internal balancing and are never exported.

`pyranid.notification.batch.size` is a long histogram with unit `{notification}`. It records only nonempty batches delivered to application code: the histogram count is the number of delivered batches and its sum is the number of delivered notifications. `pyranid.notification.connection.losses` is a monotonic long counter with unit `{connection}` and includes `error.type`.

All five notification metrics include `db.system.name`. They also include the configured `db.namespace` and `db.client.connection.pool.name` when those builder settings are present. They never include channel names, payloads, notification-session identifiers, or backend process identifiers. [`Database::sendNotification(...)`](https://javadoc.pyranid.com/com/pyranid/Database.html#sendNotification(java.lang.String,java.lang.String)) uses Pyranid's ordinary statement path, so sends appear in the existing statement metrics instead of a duplicate notification-send instrument.

## Attribute Notes

`db.system.name` is mapped from Pyranid's configured or detected [`DatabaseType`](https://javadoc.pyranid.com/com/pyranid/DatabaseType.html):

| Pyranid `DatabaseType` | `db.system.name` |
| --- | --- |
| `POSTGRESQL` | `postgresql` |
| `MYSQL` | `mysql` |
| `MARIA_DB` | `mariadb` |
| `SQLITE` | `sqlite` |
| `SQL_SERVER` | `microsoft.sql_server` |
| `ORACLE` | `oracle.db` |
| `GENERIC` | `other_sql` |

`db.operation.name` is the first SQL operation, uppercased. For CTEs, it reports the main operation after the CTE definitions. Batch operations report `BATCH <operation>`, or `BATCH` when no operation can be identified. For SQL containing multiple top-level statements, only the first statement is classified.

## Development Verification

From the `pyranid-otel/` project directory, run:

```bash
mvn -q verify
mvn -q javadoc:javadoc
```

Artifact signing and Maven Central publishing are isolated in the `release` profile. Use `mvn -P release deploy` only when publishing a release or snapshot; normal local and CI `verify` runs do not require GPG credentials. Automatic publishing is intentionally disabled: after deployment validation succeeds, review and publish the deployment in the Central Publisher Portal.

## Compatibility

This snapshot depends on Pyranid `4.6.0-SNAPSHOT` and pins OpenTelemetry Java BOM `1.64.0`. Future minor releases may bump the OTel BOM when database semantic conventions or instrument behavior changes.

The jar declares the automatic JPMS module name `com.pyranid.otel`.

## Notes

[`OpenTelemetryMetricsCollector::snapshot()`](https://otel.javadoc.pyranid.com/com/pyranid/otel/OpenTelemetryMetricsCollector.html#snapshot()) and the inherited [`MetricsCollector::notificationSnapshot()`](https://javadoc.pyranid.com/com/pyranid/MetricsCollector.html#notificationSnapshot()) return [`Optional.empty()`](https://docs.oracle.com/en/java/javase/26/docs/api/java.base/java/util/Optional.html#empty()) because metrics are read through the OTel SDK/exporter pipeline.

Builders resolve [`GlobalOpenTelemetry`](https://javadoc.io/static/io.opentelemetry/opentelemetry-api/1.64.0/io/opentelemetry/api/GlobalOpenTelemetry.html) only when [`build()`](https://otel.javadoc.pyranid.com/com/pyranid/otel/OpenTelemetryMetricsCollector.Builder.html#build()) needs it. Supplying an explicit [`Meter`](https://javadoc.io/static/io.opentelemetry/opentelemetry-api/1.64.0/io/opentelemetry/api/metrics/Meter.html) or [`OpenTelemetry`](https://javadoc.io/static/io.opentelemetry/opentelemetry-api/1.64.0/io/opentelemetry/api/OpenTelemetry.html) instance never initializes or freezes the global singleton.

This adapter does not emit [`Statement::getId()`](https://javadoc.pyranid.com/com/pyranid/Statement.html#getId()), full SQL text, `db.query.text`, trace/span data, or arbitrary query parameters. Those values are user-controlled or high-cardinality and belong in tracing or custom instrumentation when needed. Custom collectors can still read the full [`StatementContext`](https://javadoc.pyranid.com/com/pyranid/StatementContext.html).

`db.client.connection.use_time` measures connection hold time, not exact physical transaction lifetime. It includes transaction setup and cleanup around the commit/rollback operation.

`pyranid.transaction.active` increments when a physical JDBC transaction begins and decrements once at the terminal physical transaction outcome. Commit failure is not terminal by itself because Pyranid attempts rollback afterward; the rollback callback owns the single decrement. Physical-begin failures do not decrement unless the transaction was first recorded as active.

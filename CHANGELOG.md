# Changelog

All notable changes to Pyranid's OpenTelemetry Integration will be documented in this file.

## 1.3.0-SNAPSHOT

### Added

- Added notification-session open duration, total duration, and active-session metrics for Pyranid 4.6.0 notification listeners.
- Added delivered notification-batch size and terminal notification-connection loss metrics.
- Added bounded notification open and terminal outcome attributes. Failure metrics include `error.type`; channel names, payloads, session identifiers, and backend process identifiers are never exported.

### Fixed

- Ensured active-transaction metrics and internal state are drained at logical transaction exit if a physical terminal callback is absent or cannot finish.

### Changed

- Pyranid `4.6.0` is now required. Because Pyranid remains a `provided` dependency, applications must upgrade both artifacts together.
- Updated the provided Pyranid dependency to `4.6.0-SNAPSHOT`.
- Updated the OpenTelemetry Java BOM to `1.64.0`.
- Configured namespace and pool names are included on notification metrics when present. Notification sends continue through the ordinary statement metrics.

## 1.2.0

### Fixed

- Explicit `Meter` and `OpenTelemetry` builders no longer initialize `GlobalOpenTelemetry`.
- Failed batches now retain their batch size and use the same `BATCH <operation>` attributes as successful batches.
- Failure metrics now prefer the underlying SQL or application exception for `error.type`, include errors on transaction-closure and stream lifecycle metrics, and include transaction isolation on physical-begin failures.
- Active-transaction bookkeeping now restores its internal state if an OpenTelemetry counter rejects an update.
- SQL operation and collection extraction no longer treats text inside comments or string literals as SQL syntax. Ambiguous collection names are omitted instead of guessed.

### Changed

- Full SQLSTATE values are now recorded by default; `recordFullSqlState(false)` retains the former two-character class behavior.
- Batch operation names follow the OpenTelemetry `BATCH <operation>` convention, and CTEs report their main operation instead of `WITH`.
- Added OpenTelemetry's advised histogram boundaries for operation duration and returned rows.

### Added

- Added `pyranid.post_transaction.duration` alongside the existing post-transaction operation count.

## 1.1.0

### Fixed

- `db.system.name` is now reported for every Pyranid `DatabaseType`. Previously only PostgreSQL, Oracle, and the generic fallback were mapped, so the MySQL, MariaDB, SQLite, and SQL Server types added in Pyranid 4.3.0 were unhandled. They now map to the OpenTelemetry registry values `mysql`, `mariadb`, `sqlite`, `microsoft.sql_server`, and `oracle.db`, with a forward-compatible `other_sql` fallback for any database type added in the future.

### Changed

- Updated the provided `pyranid` dependency to 4.4.0.

## 1.0.0

### Added

- Initial release: `OpenTelemetryMetricsCollector`, which exports Pyranid `MetricsCollector` events (statement execution, connection acquisition/release, logical and physical transaction lifecycle, savepoints, streaming result sets, and post-transaction operations) through the OpenTelemetry API.

# Changelog

All notable changes to Pyranid's OpenTelemetry Integration will be documented in this file.

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

# Changelog

All notable changes to Pyranid's OpenTelemetry Integration will be documented in this file.

## 1.1.0

### Fixed

- `db.system.name` is now reported for every Pyranid `DatabaseType`. Previously only PostgreSQL, Oracle, and the generic fallback were mapped, so the MySQL, MariaDB, SQLite, and SQL Server types added in Pyranid 4.3.0 were unhandled. They now map to the OpenTelemetry registry values `mysql`, `mariadb`, `sqlite`, `microsoft.sql_server`, and `oracle.db`, with a forward-compatible `other_sql` fallback for any database type added in the future.

### Changed

- Updated the provided `pyranid` dependency to 4.4.0.

## 1.0.0

### Added

- Initial release: `OpenTelemetryMetricsCollector`, which exports Pyranid `MetricsCollector` events (statement execution, connection acquisition/release, logical and physical transaction lifecycle, savepoints, streaming result sets, and post-transaction operations) through the OpenTelemetry API.

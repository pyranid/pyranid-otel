/*
 * Copyright 2026 Revetware LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pyranid.otel;

import com.pyranid.Database;
import com.pyranid.DatabaseException;
import com.pyranid.DatabaseType;
import com.pyranid.MetricsCollector;
import com.pyranid.Transaction;
import com.pyranid.TransactionIsolation;
import com.pyranid.TransactionOptions;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.hsqldb.jdbc.JDBCDataSource;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.concurrent.ThreadSafe;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
@ThreadSafe
public class OpenTelemetryMetricsCollectorTests {
	private static final AttributeKey<String> DB_SYSTEM_NAME_ATTRIBUTE_KEY = AttributeKey.stringKey("db.system.name");
	private static final AttributeKey<String> DB_OPERATION_NAME_ATTRIBUTE_KEY = AttributeKey.stringKey("db.operation.name");
	private static final AttributeKey<String> DB_NAMESPACE_ATTRIBUTE_KEY = AttributeKey.stringKey("db.namespace");
	private static final AttributeKey<String> DB_COLLECTION_NAME_ATTRIBUTE_KEY = AttributeKey.stringKey("db.collection.name");
	private static final AttributeKey<String> DB_CLIENT_CONNECTION_POOL_NAME_ATTRIBUTE_KEY = AttributeKey.stringKey("db.client.connection.pool.name");
	private static final AttributeKey<String> TRANSACTION_CLOSURE_OUTCOME_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.transaction.closure_outcome");
	private static final AttributeKey<String> TRANSACTION_COMMIT_OUTCOME_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.transaction.commit_outcome");
	private static final AttributeKey<String> TRANSACTION_ROLLBACK_OUTCOME_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.transaction.rollback_outcome");
	private static final AttributeKey<String> TRANSACTION_BEGIN_PHASE_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.transaction.physical_begin_phase");
	private static final AttributeKey<String> SAVEPOINT_OPERATION_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.savepoint.operation");
	private static final AttributeKey<String> STREAM_OUTCOME_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.stream.outcome");

	@Test
	public void testDbSystemNameCoversEveryDatabaseType() {
		// Every DatabaseType must map to a non-null OTel db.system.name. Guards against the previous
		// exhaustive-switch-without-default that threw for DatabaseType constants added after this module compiled.
		Assertions.assertEquals("postgresql", OpenTelemetryMetricsCollector.dbSystemName(DatabaseType.POSTGRESQL));
		Assertions.assertEquals("mysql", OpenTelemetryMetricsCollector.dbSystemName(DatabaseType.MYSQL));
		Assertions.assertEquals("mariadb", OpenTelemetryMetricsCollector.dbSystemName(DatabaseType.MARIA_DB));
		Assertions.assertEquals("sqlite", OpenTelemetryMetricsCollector.dbSystemName(DatabaseType.SQLITE));
		Assertions.assertEquals("microsoft.sql_server", OpenTelemetryMetricsCollector.dbSystemName(DatabaseType.SQL_SERVER));
		Assertions.assertEquals("oracle.db", OpenTelemetryMetricsCollector.dbSystemName(DatabaseType.ORACLE));
		Assertions.assertEquals("other_sql", OpenTelemetryMetricsCollector.dbSystemName(DatabaseType.GENERIC));

		for (DatabaseType databaseType : DatabaseType.values()) {
			String dbSystemName = OpenTelemetryMetricsCollector.dbSystemName(databaseType);
			Assertions.assertNotNull(dbSystemName, "db.system.name must be non-null for " + databaseType);
			if (databaseType != DatabaseType.GENERIC)
				Assertions.assertNotEquals("other_sql", dbSystemName,
						databaseType + " should map to a specific db.system.name, not the generic fallback");
		}
	}

	@Test
	public void recordsStatementResponseAndPoolMetrics() {
		TestHarness harness = TestHarness.create();
		OpenTelemetryMetricsCollector collector = OpenTelemetryMetricsCollector
				.withMeter(harness.openTelemetrySdk().getMeter("test-statements"))
				.poolName("primary")
				.namespace("app")
				.recordCollectionName(true)
				.build();
		Database database = database("otel_statements", collector);

		database.query("CREATE TABLE widgets (id INT)").execute();
		database.query("INSERT INTO widgets VALUES (1)").execute();
		List<Integer> ids = database.query("""
				/* trace_id=abc */
				-- generated by test
				SELECT id FROM widgets
				""").fetchList(Integer.class);

		Assertions.assertEquals(List.of(1), ids);

		Collection<MetricData> metrics = harness.metricReader().collectAllMetrics();

		Assertions.assertEquals(1L, histogramCount(metrics, "db.client.operation.duration",
				attributes -> "postgresql".equals(attributes.get(DB_SYSTEM_NAME_ATTRIBUTE_KEY))
						&& "SELECT".equals(attributes.get(DB_OPERATION_NAME_ATTRIBUTE_KEY))
						&& "app".equals(attributes.get(DB_NAMESPACE_ATTRIBUTE_KEY))
						&& "widgets".equals(attributes.get(DB_COLLECTION_NAME_ATTRIBUTE_KEY))));
		Assertions.assertEquals(1L, histogramCount(metrics, "db.client.response.returned_rows",
				attributes -> "SELECT".equals(attributes.get(DB_OPERATION_NAME_ATTRIBUTE_KEY))
						&& "widgets".equals(attributes.get(DB_COLLECTION_NAME_ATTRIBUTE_KEY))));
		Assertions.assertEquals(1L, histogramCount(metrics, "pyranid.statement.rows_affected",
				attributes -> "INSERT".equals(attributes.get(DB_OPERATION_NAME_ATTRIBUTE_KEY))));
		Assertions.assertEquals(3L, histogramTotalCount(metrics, "db.client.connection.wait_time",
				attributes -> "primary".equals(attributes.get(DB_CLIENT_CONNECTION_POOL_NAME_ATTRIBUTE_KEY))));
		Assertions.assertEquals(3L, histogramTotalCount(metrics, "db.client.connection.use_time",
				attributes -> "primary".equals(attributes.get(DB_CLIENT_CONNECTION_POOL_NAME_ATTRIBUTE_KEY))));
	}

	@Test
	public void omitsPoolMetricsWhenPoolNameIsUnset() {
		TestHarness harness = TestHarness.create();
		OpenTelemetryMetricsCollector collector = OpenTelemetryMetricsCollector
				.withMeter(harness.openTelemetrySdk().getMeter("test-no-pool"))
				.build();
		Database database = database("otel_no_pool", collector);

		database.query("CREATE TABLE t (id INT)").execute();
		database.query("INSERT INTO t VALUES (1)").execute();

		Set<String> metricNames = harness.metricReader().collectAllMetrics().stream()
				.map(MetricData::getName)
				.collect(Collectors.toSet());

		Assertions.assertFalse(metricNames.contains("db.client.connection.wait_time"));
		Assertions.assertFalse(metricNames.contains("db.client.connection.use_time"));
	}

	@Test
	public void failedStatementConnectionAcquireWithNoPoolNameDoesNotTriggerDatabaseTypeDetection() {
		TestHarness harness = TestHarness.create();
		OpenTelemetryMetricsCollector collector = OpenTelemetryMetricsCollector
				.withMeter(harness.openTelemetrySdk().getMeter("test-failed-acquire-no-pool"))
				.build();
		CountingFailingDataSource dataSource = new CountingFailingDataSource();
		Database database = Database.withDataSource(dataSource)
				.metricsCollector(collector)
				.build();

		Assertions.assertThrows(DatabaseException.class, () -> database.query("SELECT 1").execute());
		Assertions.assertEquals(1, dataSource.connectionAttempts());
	}

	@Test
	public void failedStatementConnectionAcquireUsesConfiguredDatabaseType() {
		TestHarness harness = TestHarness.create();
		OpenTelemetryMetricsCollector collector = OpenTelemetryMetricsCollector
				.withMeter(harness.openTelemetrySdk().getMeter("test-failed-acquire-database-type"))
				.poolName("primary")
				.build();
		CountingFailingDataSource dataSource = new CountingFailingDataSource();
		Database database = Database.withDataSource(dataSource)
				.databaseType(DatabaseType.POSTGRESQL)
				.metricsCollector(collector)
				.build();

		Assertions.assertThrows(DatabaseException.class, () -> database.query("SELECT 1").execute());
		Assertions.assertEquals(1, dataSource.connectionAttempts());

		Collection<MetricData> metrics = harness.metricReader().collectAllMetrics();

		Assertions.assertEquals(1L, histogramTotalCount(metrics, "db.client.connection.wait_time",
				attributes -> "postgresql".equals(attributes.get(DB_SYSTEM_NAME_ATTRIBUTE_KEY))
						&& "primary".equals(attributes.get(DB_CLIENT_CONNECTION_POOL_NAME_ATTRIBUTE_KEY))));
		Assertions.assertEquals(0L, histogramTotalCountOrZero(metrics, "db.client.connection.wait_time",
				attributes -> "other_sql".equals(attributes.get(DB_SYSTEM_NAME_ATTRIBUTE_KEY))));
		Assertions.assertEquals(1L, histogramCount(metrics, "db.client.operation.duration",
				attributes -> "postgresql".equals(attributes.get(DB_SYSTEM_NAME_ATTRIBUTE_KEY))
						&& "SELECT".equals(attributes.get(DB_OPERATION_NAME_ATTRIBUTE_KEY))));
		Assertions.assertEquals(1L, longSumValue(metrics, "pyranid.statement.errors",
				attributes -> "postgresql".equals(attributes.get(DB_SYSTEM_NAME_ATTRIBUTE_KEY))
						&& "SELECT".equals(attributes.get(DB_OPERATION_NAME_ATTRIBUTE_KEY))));
		Assertions.assertEquals(0L, longSumValueOrZero(metrics, "pyranid.statement.errors",
				attributes -> "other_sql".equals(attributes.get(DB_SYSTEM_NAME_ATTRIBUTE_KEY))));
	}

	@Test
	public void recordsTransactionMetricsAndActiveGaugeReturnsToZero() {
		TestHarness harness = TestHarness.create();
		OpenTelemetryMetricsCollector collector = OpenTelemetryMetricsCollector
				.withMeter(harness.openTelemetrySdk().getMeter("test-transactions"))
				.build();
		Database database = database("otel_transactions", collector);

		database.query("CREATE TABLE t (id INT)").execute();
		database.transaction(() -> database.query("INSERT INTO t VALUES (1)").execute());

		Collection<MetricData> metrics = harness.metricReader().collectAllMetrics();

		Assertions.assertEquals(0L, longSumValue(metrics, "pyranid.transaction.active",
				attributes -> "postgresql".equals(attributes.get(DB_SYSTEM_NAME_ATTRIBUTE_KEY))));
		Assertions.assertEquals(1L, histogramCount(metrics, "pyranid.transaction.commit.duration",
				attributes -> "success".equals(attributes.get(TRANSACTION_COMMIT_OUTCOME_ATTRIBUTE_KEY))));
		Assertions.assertEquals(1L, longSumValue(metrics, "pyranid.transaction.count",
				attributes -> "committed".equals(attributes.get(TRANSACTION_CLOSURE_OUTCOME_ATTRIBUTE_KEY))));
	}

	@Test
	public void activeGaugeReturnsToZeroAfterCommitFailureAndRollbackSuccess() {
		TestHarness harness = TestHarness.create();
		OpenTelemetryMetricsCollector collector = OpenTelemetryMetricsCollector
				.withMeter(harness.openTelemetrySdk().getMeter("test-commit-failure"))
				.build();
		AtomicInteger rollbacks = new AtomicInteger();
		Database database = Database.withDataSource(commitFailingDataSource("otel_commit_failure", rollbacks))
				.databaseType(DatabaseType.POSTGRESQL)
				.metricsCollector(collector)
				.build();

		database.query("CREATE TABLE t (id INT)").execute();

		Assertions.assertThrows(DatabaseException.class, () -> database.transaction(() ->
				database.query("INSERT INTO t VALUES (1)").execute()));

		Collection<MetricData> metrics = harness.metricReader().collectAllMetrics();

		Assertions.assertEquals(1, rollbacks.get());
		Assertions.assertEquals(0L, longSumValue(metrics, "pyranid.transaction.active",
				attributes -> "postgresql".equals(attributes.get(DB_SYSTEM_NAME_ATTRIBUTE_KEY))));
		Assertions.assertEquals(1L, histogramCount(metrics, "pyranid.transaction.commit.duration",
				attributes -> "failure".equals(attributes.get(TRANSACTION_COMMIT_OUTCOME_ATTRIBUTE_KEY))));
		Assertions.assertEquals(1L, histogramCount(metrics, "pyranid.transaction.rollback.duration",
				attributes -> "success".equals(attributes.get(TRANSACTION_ROLLBACK_OUTCOME_ATTRIBUTE_KEY))));
		Assertions.assertEquals(1L, longSumValue(metrics, "pyranid.transaction.count",
				attributes -> "failed".equals(attributes.get(TRANSACTION_CLOSURE_OUTCOME_ATTRIBUTE_KEY))));
	}

	@Test
	public void activeGaugeDoesNotGoNegativeWhenPhysicalBeginFailsBeforeBeginCallback() {
		TestHarness harness = TestHarness.create();
		OpenTelemetryMetricsCollector collector = OpenTelemetryMetricsCollector
				.withMeter(harness.openTelemetrySdk().getMeter("test-begin-failure-active"))
				.build();
		AtomicInteger rollbacks = new AtomicInteger();
		Database database = Database.withDataSource(beginFailingDataSource("otel_begin_failure_active", rollbacks))
				.databaseType(DatabaseType.POSTGRESQL)
				.metricsCollector(collector)
				.build();

		database.query("CREATE TABLE t (id INT)").execute();

		Assertions.assertThrows(DatabaseException.class, () -> database.transaction(
				TransactionOptions.withIsolation(TransactionIsolation.SERIALIZABLE).build(), () ->
					database.query("INSERT INTO t VALUES (1)").execute()));

		Collection<MetricData> metrics = harness.metricReader().collectAllMetrics();

		Assertions.assertEquals(1, rollbacks.get());
		Assertions.assertEquals(0L, longSumValueOrZero(metrics, "pyranid.transaction.active",
				attributes -> "postgresql".equals(attributes.get(DB_SYSTEM_NAME_ATTRIBUTE_KEY))));
		Assertions.assertEquals(1L, longSumValue(metrics, "pyranid.transaction.physical.begin_failures",
				attributes -> "set_isolation".equals(attributes.get(TRANSACTION_BEGIN_PHASE_ATTRIBUTE_KEY))));
		Assertions.assertEquals(1L, histogramCount(metrics, "pyranid.transaction.rollback.duration",
				attributes -> "success".equals(attributes.get(TRANSACTION_ROLLBACK_OUTCOME_ATTRIBUTE_KEY))));
	}

	@Test
	public void recordsStreamTerminalOutcomeAfterCallbackCompletes() {
		TestHarness harness = TestHarness.create();
		OpenTelemetryMetricsCollector collector = OpenTelemetryMetricsCollector
				.withMeter(harness.openTelemetrySdk().getMeter("test-streams"))
				.build();
		Database database = database("otel_streams", collector);

		database.query("CREATE TABLE t (id INT)").execute();
		database.query("INSERT INTO t VALUES (1)").execute();
		database.query("INSERT INTO t VALUES (2)").execute();
		List<Integer> rows = database.query("SELECT id FROM t ORDER BY id")
				.fetchStream(Integer.class, stream -> stream.limit(1).collect(Collectors.toList()));

		Assertions.assertEquals(List.of(1), rows);

		Collection<MetricData> metrics = harness.metricReader().collectAllMetrics();

		Assertions.assertEquals(1L, histogramCount(metrics, "pyranid.fetchstream.duration",
				attributes -> "early_close".equals(attributes.get(STREAM_OUTCOME_ATTRIBUTE_KEY))));
		Assertions.assertEquals(1L, histogramCount(metrics, "pyranid.fetchstream.rows_consumed",
				attributes -> "early_close".equals(attributes.get(STREAM_OUTCOME_ATTRIBUTE_KEY))));
	}

	@Test
	public void recordsOpenFailureWithoutRowsConsumedSample() {
		TestHarness harness = TestHarness.create();
		OpenTelemetryMetricsCollector collector = OpenTelemetryMetricsCollector
				.withMeter(harness.openTelemetrySdk().getMeter("test-open-failure"))
				.build();
		Database database = database("otel_open_failure", collector);

		Assertions.assertThrows(DatabaseException.class, () -> database.query("SELECT id FROM missing_table")
				.fetchStream(Integer.class, stream -> stream.collect(Collectors.toList())));

		Collection<MetricData> metrics = harness.metricReader().collectAllMetrics();

		Assertions.assertEquals(1L, histogramCount(metrics, "pyranid.fetchstream.duration",
				attributes -> "open_failure".equals(attributes.get(STREAM_OUTCOME_ATTRIBUTE_KEY))));
		Assertions.assertEquals(0L, histogramTotalCountOrZero(metrics, "pyranid.fetchstream.rows_consumed",
				attributes -> "open_failure".equals(attributes.get(STREAM_OUTCOME_ATTRIBUTE_KEY))));
	}

	@Test
	public void recordsPhysicalBeginFailurePhaseAttributes() {
		TestHarness harness = TestHarness.create();
		OpenTelemetryMetricsCollector collector = OpenTelemetryMetricsCollector
				.withMeter(harness.openTelemetrySdk().getMeter("test-begin-failure-phases"))
				.build();
		Database database = database("otel_begin_failure_phases", collector);

		database.transaction(() -> {
			Transaction transaction = database.currentTransaction().orElseThrow();

			for (MetricsCollector.PhysicalTransactionBeginFailurePhase phase : MetricsCollector.PhysicalTransactionBeginFailurePhase.values())
				collector.didFailToBeginPhysicalTransaction(transaction, TransactionIsolation.DEFAULT, phase, DatabaseType.POSTGRESQL, new RuntimeException("begin failed"));
		});

		Collection<MetricData> metrics = harness.metricReader().collectAllMetrics();

		for (MetricsCollector.PhysicalTransactionBeginFailurePhase phase : MetricsCollector.PhysicalTransactionBeginFailurePhase.values())
			Assertions.assertEquals(1L, longSumValue(metrics, "pyranid.transaction.physical.begin_failures",
					attributes -> enumValue(phase).equals(attributes.get(TRANSACTION_BEGIN_PHASE_ATTRIBUTE_KEY))));
	}

	@Test
	public void recordsSavepointOperations() {
		TestHarness harness = TestHarness.create();
		OpenTelemetryMetricsCollector collector = OpenTelemetryMetricsCollector
				.withMeter(harness.openTelemetrySdk().getMeter("test-savepoints"))
				.build();
		Database database = database("otel_savepoints", collector);

		database.query("CREATE TABLE t (id INT)").execute();
		database.transaction(() -> {
			Transaction transaction = database.currentTransaction().orElseThrow();
			transaction.withSavepoint(() -> database.query("INSERT INTO t VALUES (1)").execute());
			Assertions.assertThrows(RuntimeException.class, () -> transaction.withSavepoint(() -> {
				database.query("INSERT INTO t VALUES (2)").execute();
				throw new RuntimeException("rollback to savepoint");
			}));
		});

		Collection<MetricData> metrics = harness.metricReader().collectAllMetrics();

		Assertions.assertEquals(2L, longSumValue(metrics, "pyranid.savepoint.operations",
				attributes -> "created".equals(attributes.get(SAVEPOINT_OPERATION_ATTRIBUTE_KEY))));
		Assertions.assertEquals(1L, longSumValue(metrics, "pyranid.savepoint.operations",
				attributes -> "rolled_back".equals(attributes.get(SAVEPOINT_OPERATION_ATTRIBUTE_KEY))));
		Assertions.assertEquals(1L, longSumValue(metrics, "pyranid.savepoint.operations",
				attributes -> "released".equals(attributes.get(SAVEPOINT_OPERATION_ATTRIBUTE_KEY))));
	}

	@NonNull
	private static Database database(@NonNull String name,
																	 @NonNull OpenTelemetryMetricsCollector collector) {
		return Database.withDataSource(createInMemoryDataSource(name))
				.databaseType(DatabaseType.POSTGRESQL)
				.metricsCollector(collector)
				.build();
	}

	@NonNull
	private static DataSource createInMemoryDataSource(@NonNull String databaseName) {
		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setUrl(format("jdbc:hsqldb:mem:%s;sql.syntax_pgs=true", databaseName));
		dataSource.setUser("SA");
		return dataSource;
	}

	@NonNull
	private static DataSource commitFailingDataSource(@NonNull String databaseName,
																										@NonNull AtomicInteger rollbacks) {
		return new WrappingDataSource(createInMemoryDataSource(databaseName), connection ->
				(Connection) Proxy.newProxyInstance(
						Connection.class.getClassLoader(),
						new Class<?>[]{Connection.class},
						(proxy, method, args) -> {
							if ("commit".equals(method.getName()))
								throw new SQLException("commit failed", "40001");

							if ("rollback".equals(method.getName()) && (args == null || args.length == 0))
								rollbacks.incrementAndGet();

							try {
								return method.invoke(connection, args);
							} catch (InvocationTargetException e) {
								throw e.getCause();
							}
						}));
	}

	@NonNull
	private static DataSource beginFailingDataSource(@NonNull String databaseName,
																									 @NonNull AtomicInteger rollbacks) {
		return new WrappingDataSource(createInMemoryDataSource(databaseName), connection ->
				(Connection) Proxy.newProxyInstance(
						Connection.class.getClassLoader(),
						new Class<?>[]{Connection.class},
						(proxy, method, args) -> {
							if ("setTransactionIsolation".equals(method.getName()))
								throw new SQLException("set isolation failed", "08000");

							if ("rollback".equals(method.getName()) && (args == null || args.length == 0))
								rollbacks.incrementAndGet();

							try {
								return method.invoke(connection, args);
							} catch (InvocationTargetException e) {
								throw e.getCause();
							}
						}));
	}

	private static long longSumValue(Collection<MetricData> metrics,
																	 String metricName,
																	 java.util.function.Predicate<Attributes> attributesMatcher) {
		return metricByName(metrics, metricName).getLongSumData().getPoints().stream()
				.filter(point -> attributesMatcher.test(point.getAttributes()))
				.mapToLong(LongPointData::getValue)
				.findFirst()
				.orElseThrow();
	}

	private static long longSumValueOrZero(Collection<MetricData> metrics,
																				 String metricName,
																				 java.util.function.Predicate<Attributes> attributesMatcher) {
		return metrics.stream()
				.filter(metric -> metricName.equals(metric.getName()))
				.findFirst()
				.map(metric -> metric.getLongSumData().getPoints().stream()
						.filter(point -> attributesMatcher.test(point.getAttributes()))
						.mapToLong(LongPointData::getValue)
						.sum())
				.orElse(0L);
	}

	private static long histogramCount(Collection<MetricData> metrics,
																	 String metricName,
																	 java.util.function.Predicate<Attributes> attributesMatcher) {
		return metricByName(metrics, metricName).getHistogramData().getPoints().stream()
				.filter(point -> attributesMatcher.test(point.getAttributes()))
				.mapToLong(HistogramPointData::getCount)
				.findFirst()
				.orElseThrow();
	}

	private static long histogramTotalCount(Collection<MetricData> metrics,
																					String metricName,
																					java.util.function.Predicate<Attributes> attributesMatcher) {
		return metricByName(metrics, metricName).getHistogramData().getPoints().stream()
				.filter(point -> attributesMatcher.test(point.getAttributes()))
				.mapToLong(HistogramPointData::getCount)
				.sum();
	}

	private static long histogramTotalCountOrZero(Collection<MetricData> metrics,
																								String metricName,
																								java.util.function.Predicate<Attributes> attributesMatcher) {
		return metrics.stream()
				.filter(metric -> metricName.equals(metric.getName()))
				.findFirst()
				.map(metric -> metric.getHistogramData().getPoints().stream()
						.filter(point -> attributesMatcher.test(point.getAttributes()))
						.mapToLong(HistogramPointData::getCount)
						.sum())
				.orElse(0L);
	}

	@NonNull
	private static String enumValue(@NonNull Enum<?> value) {
		return value.name().toLowerCase(java.util.Locale.ROOT);
	}

	private static MetricData metricByName(Collection<MetricData> metrics,
																				 String metricName) {
		return metrics.stream()
				.filter(metric -> metricName.equals(metric.getName()))
				.findFirst()
				.orElseThrow();
	}

	private record TestHarness(@NonNull InMemoryMetricReader metricReader,
														 @NonNull OpenTelemetrySdk openTelemetrySdk,
														 @NonNull SdkMeterProvider sdkMeterProvider) {
		@NonNull
		static TestHarness create() {
			InMemoryMetricReader metricReader = InMemoryMetricReader.create();
			SdkMeterProvider sdkMeterProvider = SdkMeterProvider.builder()
					.registerMetricReader(metricReader)
					.build();
			OpenTelemetrySdk openTelemetrySdk = OpenTelemetrySdk.builder()
					.setMeterProvider(sdkMeterProvider)
					.build();

			return new TestHarness(metricReader, openTelemetrySdk, sdkMeterProvider);
		}
	}

	@FunctionalInterface
	private interface ConnectionWrapper {
		@NonNull
		Connection wrap(@NonNull Connection connection) throws SQLException;
	}

	private static final class WrappingDataSource implements DataSource {
		@NonNull
		private final DataSource delegate;
		@NonNull
		private final ConnectionWrapper connectionWrapper;

		private WrappingDataSource(@NonNull DataSource delegate,
															 @NonNull ConnectionWrapper connectionWrapper) {
			this.delegate = delegate;
			this.connectionWrapper = connectionWrapper;
		}

		@Override
		public Connection getConnection() throws SQLException {
			return this.connectionWrapper.wrap(this.delegate.getConnection());
		}

		@Override
		public Connection getConnection(String username,
																		String password) throws SQLException {
			return this.connectionWrapper.wrap(this.delegate.getConnection(username, password));
		}

		@Override
		public PrintWriter getLogWriter() throws SQLException {
			return this.delegate.getLogWriter();
		}

		@Override
		public void setLogWriter(PrintWriter out) throws SQLException {
			this.delegate.setLogWriter(out);
		}

		@Override
		public void setLoginTimeout(int seconds) throws SQLException {
			this.delegate.setLoginTimeout(seconds);
		}

		@Override
		public int getLoginTimeout() throws SQLException {
			return this.delegate.getLoginTimeout();
		}

		@Override
		public Logger getParentLogger() throws SQLFeatureNotSupportedException {
			return this.delegate.getParentLogger();
		}

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			return this.delegate.unwrap(iface);
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) throws SQLException {
			return this.delegate.isWrapperFor(iface);
		}
	}

	private static final class CountingFailingDataSource implements DataSource {
		@NonNull
		private final AtomicInteger connectionAttempts;

		private CountingFailingDataSource() {
			this.connectionAttempts = new AtomicInteger();
		}

		private int connectionAttempts() {
			return this.connectionAttempts.get();
		}

		@Override
		public Connection getConnection() throws SQLException {
			this.connectionAttempts.incrementAndGet();
			throw new SQLException("connection unavailable");
		}

		@Override
		public Connection getConnection(String username,
																		String password) throws SQLException {
			return getConnection();
		}

		@Override
		public PrintWriter getLogWriter() {
			return null;
		}

		@Override
		public void setLogWriter(PrintWriter out) {
			// No-op
		}

		@Override
		public void setLoginTimeout(int seconds) {
			// No-op
		}

		@Override
		public int getLoginTimeout() {
			return 0;
		}

		@Override
		public Logger getParentLogger() throws SQLFeatureNotSupportedException {
			throw new SQLFeatureNotSupportedException();
		}

		@Override
		public <T> T unwrap(Class<T> iface) throws SQLException {
			throw new SQLException("Not a wrapper");
		}

		@Override
		public boolean isWrapperFor(Class<?> iface) {
			return false;
		}
	}
}

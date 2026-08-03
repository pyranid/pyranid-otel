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
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
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
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
	private static final AttributeKey<String> DB_RESPONSE_STATUS_CODE_ATTRIBUTE_KEY = AttributeKey.stringKey("db.response.status_code");
	private static final AttributeKey<String> DB_CLIENT_CONNECTION_POOL_NAME_ATTRIBUTE_KEY = AttributeKey.stringKey("db.client.connection.pool.name");
	private static final AttributeKey<String> ERROR_TYPE_ATTRIBUTE_KEY = AttributeKey.stringKey("error.type");
	private static final AttributeKey<String> TRANSACTION_CLOSURE_OUTCOME_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.transaction.closure_outcome");
	private static final AttributeKey<String> TRANSACTION_COMMIT_OUTCOME_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.transaction.commit_outcome");
	private static final AttributeKey<String> TRANSACTION_ROLLBACK_OUTCOME_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.transaction.rollback_outcome");
	private static final AttributeKey<String> TRANSACTION_BEGIN_PHASE_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.transaction.physical_begin_phase");
	private static final AttributeKey<String> TRANSACTION_RESULT_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.transaction.result");
	private static final AttributeKey<String> TRANSACTION_ISOLATION_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.transaction.isolation");
	private static final AttributeKey<String> SAVEPOINT_OPERATION_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.savepoint.operation");
	private static final AttributeKey<String> STREAM_OUTCOME_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.stream.outcome");
	private static final AttributeKey<String> NOTIFICATION_SESSION_OPEN_OUTCOME_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.notification.session.open_outcome");
	private static final AttributeKey<String> NOTIFICATION_SESSION_OUTCOME_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.notification.session.outcome");

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
	public void explicitMeterDoesNotInitializeGlobalOpenTelemetry() {
		GlobalOpenTelemetry.resetForTest();
		try {
			Assertions.assertFalse(GlobalOpenTelemetry.isSet());
			OpenTelemetryMetricsCollector.builder().build();
			Assertions.assertFalse(GlobalOpenTelemetry.isSet());
			TestHarness harness = TestHarness.create();
			OpenTelemetryMetricsCollector.withMeter(harness.openTelemetrySdk().getMeter("test-explicit-meter")).build();
			Assertions.assertFalse(GlobalOpenTelemetry.isSet());
		} finally {
			GlobalOpenTelemetry.resetForTest();
		}
	}

	@Test
	public void parsesOperationAndCollectionNamesConservatively() {
		Assertions.assertEquals("SELECT", OpenTelemetryMetricsCollector.operationNameForSql("""
				/* DELETE FROM audit_log */
				SELECT 'FROM credentials' FROM public.widgets
				""", false));
		Assertions.assertEquals("public.widgets", OpenTelemetryMetricsCollector.collectionNameForSql("""
				/* DELETE FROM audit_log */
				SELECT 'FROM credentials' FROM public.widgets
				"""));
		Assertions.assertEquals("widgets", OpenTelemetryMetricsCollector.collectionNameForSql(
				"SELECT q'[FROM credentials]' FROM \"widgets\""));
		Assertions.assertEquals("widgets", OpenTelemetryMetricsCollector.collectionNameForSql(
				"SELECT $$FROM credentials$$ FROM `widgets`"));
		Assertions.assertEquals("widgets", OpenTelemetryMetricsCollector.collectionNameForSql(
				"SELECT 1 FROM [widgets] /* nested /* FROM credentials */ comment */"));

		String cteSql = "WITH selected AS (SELECT id FROM widgets) SELECT id FROM selected";
		Assertions.assertEquals("SELECT", OpenTelemetryMetricsCollector.operationNameForSql(cteSql, false));
		Assertions.assertNull(OpenTelemetryMetricsCollector.collectionNameForSql(cteSql));
		Assertions.assertEquals("BATCH INSERT", OpenTelemetryMetricsCollector.operationNameForSql(
				"INSERT INTO widgets VALUES (:id)", true));
		Assertions.assertEquals("BATCH", OpenTelemetryMetricsCollector.operationNameForSql("-- no operation", true));

		Assertions.assertNull(OpenTelemetryMetricsCollector.collectionNameForSql(
				"SELECT * FROM widgets JOIN owners ON owners.id = widgets.owner_id"));
		Assertions.assertNull(OpenTelemetryMetricsCollector.collectionNameForSql(
				"SELECT * FROM widgets, owners"));
		Assertions.assertNull(OpenTelemetryMetricsCollector.collectionNameForSql(
				"SELECT * FROM generate_series(1, 5)"));
		Assertions.assertNull(OpenTelemetryMetricsCollector.collectionNameForSql(
				"SELECT (SELECT owner_id FROM owners LIMIT 1) FROM widgets"));
		Assertions.assertNull(OpenTelemetryMetricsCollector.collectionNameForSql(
				"INSERT INTO widgets (id) SELECT id FROM archived_widgets"));
		Assertions.assertNull(OpenTelemetryMetricsCollector.collectionNameForSql(
				"SELECT * FROM LATERAL generate_series(1, 5)"));
		Assertions.assertEquals("widgets", OpenTelemetryMetricsCollector.collectionNameForSql(
				"SELECT * FROM widgets; DELETE FROM credentials"));
	}

	@Test
	public void cachesSqlClassificationsAndRetainsHashCollisions() throws Exception {
		OpenTelemetryMetricsCollector collector = OpenTelemetryMetricsCollector
				.withOpenTelemetry(OpenTelemetry.noop())
				.recordCollectionName(true)
				.build();
		Method classificationMethod = OpenTelemetryMetricsCollector.class
				.getDeclaredMethod("sqlClassificationFor", String.class);
		classificationMethod.setAccessible(true);
		String firstSql = "SELECT * FROM Aa";
		String secondSql = "SELECT * FROM BB";
		Assertions.assertEquals(firstSql.hashCode(), secondSql.hashCode());

		Object firstClassification = classificationMethod.invoke(collector, firstSql);
		Object secondClassification = classificationMethod.invoke(collector, secondSql);

		Assertions.assertNotSame(firstClassification, secondClassification);
		Assertions.assertSame(firstClassification, classificationMethod.invoke(collector, new String(firstSql)));
		Assertions.assertSame(secondClassification, classificationMethod.invoke(collector, new String(secondSql)));
	}

	@Test
	public void usesSemanticConventionHistogramBoundaries() {
		TestHarness harness = TestHarness.create();
		OpenTelemetryMetricsCollector collector = OpenTelemetryMetricsCollector
				.withMeter(harness.openTelemetrySdk().getMeter("test-boundaries"))
				.build();
		Database database = database("otel_boundaries", collector);

		database.query("SELECT 1 FROM (VALUES (0))").fetchList(Integer.class);

		Collection<MetricData> metrics = harness.metricReader().collectAllMetrics();
		Assertions.assertEquals(List.of(0.001D, 0.005D, 0.01D, 0.05D, 0.1D, 0.5D, 1D, 5D, 10D),
				firstHistogramPoint(metrics, "db.client.operation.duration").getBoundaries());
		Assertions.assertEquals(List.of(1D, 2D, 5D, 10D, 20D, 50D, 100D, 200D, 500D, 1_000D, 2_000D, 5_000D, 10_000D),
				firstHistogramPoint(metrics, "db.client.response.returned_rows").getBoundaries());
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
	public void recordsFailedBatchSizeAndBatchOperationName() {
		TestHarness harness = TestHarness.create();
		OpenTelemetryMetricsCollector collector = OpenTelemetryMetricsCollector
				.withMeter(harness.openTelemetrySdk().getMeter("test-failed-batch"))
				.build();
		Database database = database("otel_failed_batch", collector);

		database.query("CREATE TABLE widgets (id INT PRIMARY KEY)").execute();
		Assertions.assertThrows(DatabaseException.class, () -> database.query("INSERT INTO widgets VALUES (:id)")
				.executeBatch(List.of(Map.of("id", 1), Map.of("id", 1))));

		Collection<MetricData> metrics = harness.metricReader().collectAllMetrics();
		Assertions.assertEquals(1L, histogramCount(metrics, "pyranid.statement.batch.size",
				attributes -> "BATCH INSERT".equals(attributes.get(DB_OPERATION_NAME_ATTRIBUTE_KEY))));
		Assertions.assertEquals(1L, longSumValue(metrics, "pyranid.statement.errors",
				attributes -> "BATCH INSERT".equals(attributes.get(DB_OPERATION_NAME_ATTRIBUTE_KEY))));
	}

	@Test
	public void recordsFullSqlStateAndUnderlyingSqlExceptionByDefault() {
		TestHarness harness = TestHarness.create();
		OpenTelemetryMetricsCollector collector = OpenTelemetryMetricsCollector
				.withMeter(harness.openTelemetrySdk().getMeter("test-error-attributes"))
				.build();
		Database database = database("otel_error_attributes", collector);

		Assertions.assertThrows(DatabaseException.class, () -> database.query("SELECT id FROM missing_table").execute());

		Attributes attributes = firstHistogramPoint(harness.metricReader().collectAllMetrics(),
				"db.client.operation.duration").getAttributes();
		String sqlState = attributes.get(DB_RESPONSE_STATUS_CODE_ATTRIBUTE_KEY);
		String errorType = attributes.get(ERROR_TYPE_ATTRIBUTE_KEY);
		Assertions.assertNotNull(sqlState);
		Assertions.assertTrue(sqlState.length() > 2, sqlState);
		Assertions.assertNotNull(errorType);
		Assertions.assertTrue(errorType.startsWith("java.sql."), errorType);
		Assertions.assertNotEquals(DatabaseException.class.getName(), errorType);
	}

	@Test
	public void canReduceSqlStateToItsClass() {
		TestHarness harness = TestHarness.create();
		OpenTelemetryMetricsCollector collector = OpenTelemetryMetricsCollector
				.withMeter(harness.openTelemetrySdk().getMeter("test-sql-state-class"))
				.recordFullSqlState(false)
				.build();
		Database database = database("otel_sql_state_class", collector);

		Assertions.assertThrows(DatabaseException.class, () -> database.query("SELECT id FROM missing_table").execute());

		String sqlState = firstHistogramPoint(harness.metricReader().collectAllMetrics(),
				"db.client.operation.duration").getAttributes().get(DB_RESPONSE_STATUS_CODE_ATTRIBUTE_KEY);
		Assertions.assertNotNull(sqlState);
		Assertions.assertEquals(2, sqlState.length());
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
	public void transactionExitDrainsActiveStateWithoutPhysicalTerminalCallback() {
		TestHarness harness = TestHarness.create();
		OpenTelemetryMetricsCollector collector = OpenTelemetryMetricsCollector
				.withMeter(harness.openTelemetrySdk().getMeter("test-transaction-exit-fallback"))
				.build();
		Database database = database("otel_transaction_exit_fallback", collector);
		AtomicReference<Transaction> transactionReference = new AtomicReference<>();

		database.transaction(() -> transactionReference.set(database.currentTransaction().orElseThrow()));
		Transaction transaction = transactionReference.get();
		collector.didBeginPhysicalTransaction(
				transaction, transaction.getTransactionIsolation(), DatabaseType.POSTGRESQL);

		Assertions.assertEquals(1L, longSumValue(harness.metricReader().collectAllMetrics(),
				"pyranid.transaction.active", attributes -> true));

		collector.didExitTransactionClosure(transaction, MetricsCollector.TransactionClosureOutcome.FAILED,
				DatabaseType.POSTGRESQL, Duration.ZERO, new IllegalStateException("terminal callback missing"));

		Assertions.assertEquals(0L, longSumValue(harness.metricReader().collectAllMetrics(),
				"pyranid.transaction.active", attributes -> true));

		// Reusing the same identity proves logical exit removed it from the strong-reference set rather than only
		// compensating the exported counter. A retained entry would suppress this second increment.
		collector.didBeginPhysicalTransaction(
				transaction, transaction.getTransactionIsolation(), DatabaseType.POSTGRESQL);
		Assertions.assertEquals(1L, longSumValue(harness.metricReader().collectAllMetrics(),
				"pyranid.transaction.active", attributes -> true));

		collector.didRollbackPhysicalTransaction(transaction, DatabaseType.POSTGRESQL, Duration.ZERO);
		Assertions.assertEquals(0L, longSumValue(harness.metricReader().collectAllMetrics(),
				"pyranid.transaction.active", attributes -> true));
	}

	@Test
	public void recordsTransactionClosureFailuresAndPostTransactionDurations() {
		TestHarness harness = TestHarness.create();
		OpenTelemetryMetricsCollector collector = OpenTelemetryMetricsCollector
				.withMeter(harness.openTelemetrySdk().getMeter("test-transaction-lifecycle"))
				.build();
		Database database = database("otel_transaction_lifecycle", collector);

		database.transaction(() -> {
			Transaction transaction = database.currentTransaction().orElseThrow();
			transaction.addPostTransactionOperation(result -> {
				// No-op
			});
			database.query("VALUES (1)").fetchList(Integer.class);
		});
		Assertions.assertThrows(IllegalStateException.class, () -> database.transaction(() -> {
			throw new IllegalStateException("closure failed");
		}));

		Collection<MetricData> metrics = harness.metricReader().collectAllMetrics();
		Assertions.assertEquals(1L, histogramCount(metrics, "pyranid.transaction.closure.duration",
				attributes -> IllegalStateException.class.getName().equals(attributes.get(ERROR_TYPE_ATTRIBUTE_KEY))));
		Assertions.assertEquals(1L, histogramCount(metrics, "pyranid.post_transaction.duration",
				attributes -> "committed".equals(attributes.get(TRANSACTION_RESULT_ATTRIBUTE_KEY))));
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
				attributes -> "set_isolation".equals(attributes.get(TRANSACTION_BEGIN_PHASE_ATTRIBUTE_KEY))
						&& "serializable".equals(attributes.get(TRANSACTION_ISOLATION_ATTRIBUTE_KEY))));
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
				attributes -> "open_failure".equals(attributes.get(STREAM_OUTCOME_ATTRIBUTE_KEY))
						&& attributes.get(ERROR_TYPE_ATTRIBUTE_KEY) != null));
		Assertions.assertEquals(0L, histogramTotalCountOrZero(metrics, "pyranid.fetchstream.rows_consumed",
				attributes -> "open_failure".equals(attributes.get(STREAM_OUTCOME_ATTRIBUTE_KEY))));
	}

	@Test
	public void recordsStreamCallbackFailureType() {
		TestHarness harness = TestHarness.create();
		OpenTelemetryMetricsCollector collector = OpenTelemetryMetricsCollector
				.withMeter(harness.openTelemetrySdk().getMeter("test-stream-callback-failure"))
				.build();
		Database database = database("otel_stream_callback_failure", collector);

		database.query("CREATE TABLE t (id INT)").execute();
		database.query("INSERT INTO t VALUES (1)").execute();
		Assertions.assertThrows(IllegalStateException.class, () -> database.query("SELECT id FROM t")
				.fetchStream(Integer.class, stream -> {
					stream.findFirst();
					throw new IllegalStateException("consumer failed");
				}));

		Collection<MetricData> metrics = harness.metricReader().collectAllMetrics();
		Assertions.assertEquals(1L, histogramCount(metrics, "pyranid.fetchstream.duration",
				attributes -> "callback_failure".equals(attributes.get(STREAM_OUTCOME_ATTRIBUTE_KEY))
						&& IllegalStateException.class.getName().equals(attributes.get(ERROR_TYPE_ATTRIBUTE_KEY))));
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

	@Test
	public void recordsNotificationMetricsWithBoundedAttributes() {
		TestHarness harness = TestHarness.create();
		OpenTelemetryMetricsCollector collector = OpenTelemetryMetricsCollector
				.withMeter(harness.openTelemetrySdk().getMeter("test-notifications"))
				.poolName("listener")
				.namespace("orders")
				.build();
		UUID failedSessionId = UUID.randomUUID();
		UUID returnedSessionId = UUID.randomUUID();
		UUID interruptedSessionId = UUID.randomUUID();
		SQLException connectionFailure = new SQLException("connection lost", "08006");

		collector.willOpenNotificationSession(DatabaseType.POSTGRESQL, failedSessionId);
		collector.didOpenNotificationSession(DatabaseType.POSTGRESQL, failedSessionId, Duration.ofMillis(20));
		collector.didDeliverNotificationBatch(DatabaseType.POSTGRESQL, failedSessionId, 3L);
		collector.didLoseNotificationConnection(DatabaseType.POSTGRESQL, failedSessionId, connectionFailure);
		collector.didCloseNotificationSession(DatabaseType.POSTGRESQL, failedSessionId,
				MetricsCollector.NotificationSessionOutcome.FAILED, Duration.ofSeconds(2), connectionFailure);

		collector.willOpenNotificationSession(DatabaseType.POSTGRESQL, returnedSessionId);
		collector.didOpenNotificationSession(DatabaseType.POSTGRESQL, returnedSessionId, Duration.ofMillis(10));
		collector.didCloseNotificationSession(DatabaseType.POSTGRESQL, returnedSessionId,
				MetricsCollector.NotificationSessionOutcome.CALLBACK_RETURNED, Duration.ofSeconds(1), null);

		collector.willOpenNotificationSession(DatabaseType.POSTGRESQL, interruptedSessionId);
		collector.didOpenNotificationSession(DatabaseType.POSTGRESQL, interruptedSessionId, Duration.ofMillis(15));
		collector.didCloseNotificationSession(DatabaseType.POSTGRESQL, interruptedSessionId,
				MetricsCollector.NotificationSessionOutcome.INTERRUPTED, Duration.ofMillis(500), null);

		collector.didFailToOpenNotificationSession(DatabaseType.POSTGRESQL, UUID.randomUUID(),
				Duration.ofMillis(30), new IllegalStateException("open failed"));

		Collection<MetricData> metrics = harness.metricReader().collectAllMetrics();
		Assertions.assertEquals(3L, histogramCount(metrics, "pyranid.notification.session.open.duration",
				attributes -> "success".equals(attributes.get(NOTIFICATION_SESSION_OPEN_OUTCOME_ATTRIBUTE_KEY))));
		Assertions.assertEquals(0.045D, histogramSum(metrics, "pyranid.notification.session.open.duration",
				attributes -> "success".equals(attributes.get(NOTIFICATION_SESSION_OPEN_OUTCOME_ATTRIBUTE_KEY))), 0.000_000_001D);
		Assertions.assertEquals(1L, histogramCount(metrics, "pyranid.notification.session.open.duration",
				attributes -> "failure".equals(attributes.get(NOTIFICATION_SESSION_OPEN_OUTCOME_ATTRIBUTE_KEY))
						&& IllegalStateException.class.getName().equals(attributes.get(ERROR_TYPE_ATTRIBUTE_KEY))));
		Assertions.assertEquals(0.03D, histogramSum(metrics, "pyranid.notification.session.open.duration",
				attributes -> "failure".equals(attributes.get(NOTIFICATION_SESSION_OPEN_OUTCOME_ATTRIBUTE_KEY))), 0.000_000_001D);
		Assertions.assertEquals(1L, histogramCount(metrics, "pyranid.notification.session.duration",
				attributes -> "failed".equals(attributes.get(NOTIFICATION_SESSION_OUTCOME_ATTRIBUTE_KEY))
						&& SQLException.class.getName().equals(attributes.get(ERROR_TYPE_ATTRIBUTE_KEY))));
		Assertions.assertEquals(2D, histogramSum(metrics, "pyranid.notification.session.duration",
				attributes -> "failed".equals(attributes.get(NOTIFICATION_SESSION_OUTCOME_ATTRIBUTE_KEY))), 0.000_000_001D);
		Assertions.assertEquals(1L, histogramCount(metrics, "pyranid.notification.session.duration",
				attributes -> "callback_returned".equals(attributes.get(NOTIFICATION_SESSION_OUTCOME_ATTRIBUTE_KEY))
						&& attributes.get(ERROR_TYPE_ATTRIBUTE_KEY) == null));
		Assertions.assertEquals(1L, histogramCount(metrics, "pyranid.notification.session.duration",
				attributes -> "interrupted".equals(attributes.get(NOTIFICATION_SESSION_OUTCOME_ATTRIBUTE_KEY))
						&& attributes.get(ERROR_TYPE_ATTRIBUTE_KEY) == null));
		Assertions.assertEquals(0L, longSumValue(metrics, "pyranid.notification.session.active",
				attributes -> "postgresql".equals(attributes.get(DB_SYSTEM_NAME_ATTRIBUTE_KEY))));
		Assertions.assertEquals(1L, histogramCount(metrics, "pyranid.notification.batch.size",
				attributes -> "listener".equals(attributes.get(DB_CLIENT_CONNECTION_POOL_NAME_ATTRIBUTE_KEY))));
		Assertions.assertEquals(3D, histogramSum(metrics, "pyranid.notification.batch.size",
				attributes -> "listener".equals(attributes.get(DB_CLIENT_CONNECTION_POOL_NAME_ATTRIBUTE_KEY))));
		Assertions.assertEquals(1L, longSumValue(metrics, "pyranid.notification.connection.losses",
				attributes -> SQLException.class.getName().equals(attributes.get(ERROR_TYPE_ATTRIBUTE_KEY))));

		Assertions.assertEquals("s", metricByName(metrics, "pyranid.notification.session.open.duration").getUnit());
		Assertions.assertEquals("s", metricByName(metrics, "pyranid.notification.session.duration").getUnit());
		Assertions.assertEquals("{session}", metricByName(metrics, "pyranid.notification.session.active").getUnit());
		Assertions.assertEquals("{notification}", metricByName(metrics, "pyranid.notification.batch.size").getUnit());
		Assertions.assertEquals("{connection}", metricByName(metrics, "pyranid.notification.connection.losses").getUnit());

		for (String metricName : List.of("pyranid.notification.session.open.duration",
				"pyranid.notification.session.duration", "pyranid.notification.batch.size")) {
			MetricData metric = metricByName(metrics, metricName);
			metric.getHistogramData().getPoints().forEach(point -> assertBoundedNotificationAttributes(point.getAttributes()));
		}

		for (String metricName : List.of("pyranid.notification.session.active",
				"pyranid.notification.connection.losses"))
			metricByName(metrics, metricName).getLongSumData().getPoints()
					.forEach(point -> assertBoundedNotificationAttributes(point.getAttributes()));

		Assertions.assertTrue(collector.snapshot().isEmpty());
		Assertions.assertTrue(collector.notificationSnapshot().isEmpty());
	}

	@Test
	public void balancesActiveNotificationSessionsBySessionId() {
		TestHarness harness = TestHarness.create();
		OpenTelemetryMetricsCollector collector = OpenTelemetryMetricsCollector
				.withMeter(harness.openTelemetrySdk().getMeter("test-notification-active"))
				.build();
		UUID notificationSessionId = UUID.randomUUID();

		collector.didOpenNotificationSession(DatabaseType.POSTGRESQL, notificationSessionId, Duration.ZERO);
		collector.didOpenNotificationSession(DatabaseType.POSTGRESQL, notificationSessionId, Duration.ZERO);
		Collection<MetricData> activeMetrics = harness.metricReader().collectAllMetrics();
		Assertions.assertEquals(1L, longSumValue(activeMetrics,
				"pyranid.notification.session.active", attributes -> true));
		Attributes activeAttributes = metricByName(activeMetrics, "pyranid.notification.session.active")
				.getLongSumData().getPoints().stream().findFirst().orElseThrow().getAttributes();
		Assertions.assertNull(activeAttributes.get(DB_NAMESPACE_ATTRIBUTE_KEY));
		Assertions.assertNull(activeAttributes.get(DB_CLIENT_CONNECTION_POOL_NAME_ATTRIBUTE_KEY));

		collector.didCloseNotificationSession(DatabaseType.POSTGRESQL, notificationSessionId,
				MetricsCollector.NotificationSessionOutcome.CALLBACK_RETURNED, Duration.ZERO, null);
		collector.didCloseNotificationSession(DatabaseType.POSTGRESQL, notificationSessionId,
				MetricsCollector.NotificationSessionOutcome.CALLBACK_RETURNED, Duration.ZERO, null);
		collector.didCloseNotificationSession(DatabaseType.POSTGRESQL, UUID.randomUUID(),
				MetricsCollector.NotificationSessionOutcome.CALLBACK_RETURNED, Duration.ZERO, null);

		Assertions.assertEquals(0L, longSumValue(harness.metricReader().collectAllMetrics(),
				"pyranid.notification.session.active", attributes -> true));
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

	private static double histogramSum(Collection<MetricData> metrics,
																	 String metricName,
																	 java.util.function.Predicate<Attributes> attributesMatcher) {
		return metricByName(metrics, metricName).getHistogramData().getPoints().stream()
				.filter(point -> attributesMatcher.test(point.getAttributes()))
				.mapToDouble(HistogramPointData::getSum)
				.findFirst()
				.orElseThrow();
	}

	private static void assertBoundedNotificationAttributes(@NonNull Attributes attributes) {
		Set<String> attributeNames = attributes.asMap().keySet().stream()
				.map(AttributeKey::getKey)
				.collect(Collectors.toSet());
		Assertions.assertTrue(Set.of("db.system.name", "db.namespace", "db.client.connection.pool.name",
				"pyranid.notification.session.open_outcome", "pyranid.notification.session.outcome", "error.type")
				.containsAll(attributeNames), attributeNames.toString());
		Assertions.assertEquals("postgresql", attributes.get(DB_SYSTEM_NAME_ATTRIBUTE_KEY));
		Assertions.assertEquals("orders", attributes.get(DB_NAMESPACE_ATTRIBUTE_KEY));
		Assertions.assertEquals("listener", attributes.get(DB_CLIENT_CONNECTION_POOL_NAME_ATTRIBUTE_KEY));
	}

	@NonNull
	private static HistogramPointData firstHistogramPoint(@NonNull Collection<MetricData> metrics,
																								@NonNull String metricName) {
		return metricByName(metrics, metricName).getHistogramData().getPoints().stream()
				.findFirst()
				.orElseThrow();
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

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

import com.pyranid.DatabaseType;
import com.pyranid.MetricsCollector;
import com.pyranid.StatementContext;
import com.pyranid.StatementLog;
import com.pyranid.StatementResult;
import com.pyranid.Transaction;
import com.pyranid.TransactionIsolation;
import com.pyranid.TransactionResult;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterBuilder;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static java.util.Objects.requireNonNull;

/**
 * OpenTelemetry-backed {@link MetricsCollector} for Pyranid database metrics.
 * <p>
 * Standard database-client metrics use OpenTelemetry semantic-convention names directly. Pyranid-specific lifecycle
 * events use {@code pyranid.*} metric and attribute names.
 * <p>
 * Notification metrics use bounded lifecycle attributes. Channel names, payloads, notification-session identifiers,
 * and backend process identifiers are never exported.
 * <p>
 * This implementation is thread-safe and expects the supplied {@link Meter} to be backed by a non-blocking exporter.
 * Pyranid core catches collector exceptions, but exporters that block can still stall the calling JDBC thread.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 1.0.0
 */
@ThreadSafe
public final class OpenTelemetryMetricsCollector implements MetricsCollector {
	@NonNull
	private static final String DEFAULT_INSTRUMENTATION_NAME;
	@NonNull
	private static final String UNKNOWN_OPERATION;
	@NonNull
	private static final String OUTCOME_SUCCESS;
	@NonNull
	private static final String OUTCOME_FAILURE;
	private static final int SQL_CLASSIFICATION_CACHE_CAPACITY;
	private static final int SQL_CLASSIFICATION_CACHE_MASK;
	private static final int SQL_CLASSIFICATION_CACHE_PROBES;
	@NonNull
	private static final List<Double> OPERATION_DURATION_BUCKET_BOUNDARIES;
	@NonNull
	private static final List<Long> RETURNED_ROWS_BUCKET_BOUNDARIES;

	@NonNull
	private static final AttributeKey<String> DB_SYSTEM_NAME_ATTRIBUTE_KEY;
	@NonNull
	private static final AttributeKey<String> DB_OPERATION_NAME_ATTRIBUTE_KEY;
	@NonNull
	private static final AttributeKey<String> DB_NAMESPACE_ATTRIBUTE_KEY;
	@NonNull
	private static final AttributeKey<String> DB_COLLECTION_NAME_ATTRIBUTE_KEY;
	@NonNull
	private static final AttributeKey<String> DB_RESPONSE_STATUS_CODE_ATTRIBUTE_KEY;
	@NonNull
	private static final AttributeKey<String> DB_CLIENT_CONNECTION_POOL_NAME_ATTRIBUTE_KEY;
	@NonNull
	private static final AttributeKey<String> ERROR_TYPE_ATTRIBUTE_KEY;
	@NonNull
	private static final AttributeKey<String> TRANSACTION_CLOSURE_OUTCOME_ATTRIBUTE_KEY;
	@NonNull
	private static final AttributeKey<String> TRANSACTION_COMMIT_OUTCOME_ATTRIBUTE_KEY;
	@NonNull
	private static final AttributeKey<String> TRANSACTION_ROLLBACK_OUTCOME_ATTRIBUTE_KEY;
	@NonNull
	private static final AttributeKey<String> TRANSACTION_BEGIN_PHASE_ATTRIBUTE_KEY;
	@NonNull
	private static final AttributeKey<String> TRANSACTION_RESULT_ATTRIBUTE_KEY;
	@NonNull
	private static final AttributeKey<String> TRANSACTION_ISOLATION_ATTRIBUTE_KEY;
	@NonNull
	private static final AttributeKey<String> SAVEPOINT_OPERATION_ATTRIBUTE_KEY;
	@NonNull
	private static final AttributeKey<String> STREAM_OUTCOME_ATTRIBUTE_KEY;
	@NonNull
	private static final AttributeKey<String> NOTIFICATION_SESSION_OPEN_OUTCOME_ATTRIBUTE_KEY;
	@NonNull
	private static final AttributeKey<String> NOTIFICATION_SESSION_OUTCOME_ATTRIBUTE_KEY;

	static {
		DEFAULT_INSTRUMENTATION_NAME = "com.pyranid.otel";
		UNKNOWN_OPERATION = "UNKNOWN";
		OUTCOME_SUCCESS = "success";
		OUTCOME_FAILURE = "failure";
		SQL_CLASSIFICATION_CACHE_CAPACITY = 1024;
		SQL_CLASSIFICATION_CACHE_MASK = SQL_CLASSIFICATION_CACHE_CAPACITY - 1;
		SQL_CLASSIFICATION_CACHE_PROBES = 4;
		OPERATION_DURATION_BUCKET_BOUNDARIES = List.of(0.001D, 0.005D, 0.01D, 0.05D, 0.1D, 0.5D, 1D, 5D, 10D);
		RETURNED_ROWS_BUCKET_BOUNDARIES = List.of(1L, 2L, 5L, 10L, 20L, 50L, 100L, 200L, 500L, 1_000L, 2_000L, 5_000L, 10_000L);

		DB_SYSTEM_NAME_ATTRIBUTE_KEY = AttributeKey.stringKey("db.system.name");
		DB_OPERATION_NAME_ATTRIBUTE_KEY = AttributeKey.stringKey("db.operation.name");
		DB_NAMESPACE_ATTRIBUTE_KEY = AttributeKey.stringKey("db.namespace");
		DB_COLLECTION_NAME_ATTRIBUTE_KEY = AttributeKey.stringKey("db.collection.name");
		DB_RESPONSE_STATUS_CODE_ATTRIBUTE_KEY = AttributeKey.stringKey("db.response.status_code");
		DB_CLIENT_CONNECTION_POOL_NAME_ATTRIBUTE_KEY = AttributeKey.stringKey("db.client.connection.pool.name");
		ERROR_TYPE_ATTRIBUTE_KEY = AttributeKey.stringKey("error.type");
		TRANSACTION_CLOSURE_OUTCOME_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.transaction.closure_outcome");
		TRANSACTION_COMMIT_OUTCOME_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.transaction.commit_outcome");
		TRANSACTION_ROLLBACK_OUTCOME_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.transaction.rollback_outcome");
		TRANSACTION_BEGIN_PHASE_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.transaction.physical_begin_phase");
		TRANSACTION_RESULT_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.transaction.result");
		TRANSACTION_ISOLATION_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.transaction.isolation");
		SAVEPOINT_OPERATION_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.savepoint.operation");
		STREAM_OUTCOME_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.stream.outcome");
		NOTIFICATION_SESSION_OPEN_OUTCOME_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.notification.session.open_outcome");
		NOTIFICATION_SESSION_OUTCOME_ATTRIBUTE_KEY = AttributeKey.stringKey("pyranid.notification.session.outcome");
	}

	@NonNull
	private final DoubleHistogram operationDurationHistogram;
	@NonNull
	private final LongHistogram returnedRowsHistogram;
	@NonNull
	private final DoubleHistogram connectionWaitTimeHistogram;
	@NonNull
	private final DoubleHistogram connectionUseTimeHistogram;
	@NonNull
	private final DoubleHistogram statementPreparationDurationHistogram;
	@NonNull
	private final DoubleHistogram statementExecutionDurationHistogram;
	@NonNull
	private final DoubleHistogram statementMappingDurationHistogram;
	@NonNull
	private final LongCounter statementErrorsCounter;
	@NonNull
	private final LongHistogram statementBatchSizeHistogram;
	@NonNull
	private final LongHistogram statementRowsAffectedHistogram;
	@NonNull
	private final DoubleHistogram transactionClosureDurationHistogram;
	@NonNull
	private final DoubleHistogram transactionCommitDurationHistogram;
	@NonNull
	private final DoubleHistogram transactionRollbackDurationHistogram;
	@NonNull
	private final LongCounter transactionPhysicalBeginFailuresCounter;
	@NonNull
	private final LongCounter transactionCountCounter;
	@NonNull
	private final LongUpDownCounter activeTransactionsCounter;
	@NonNull
	private final Set<Transaction> activeTransactions;
	@NonNull
	private final LongCounter savepointOperationsCounter;
	@NonNull
	private final DoubleHistogram streamDurationHistogram;
	@NonNull
	private final LongHistogram streamRowsConsumedHistogram;
	@NonNull
	private final LongCounter postTransactionOperationsCounter;
	@NonNull
	private final DoubleHistogram postTransactionDurationHistogram;
	@NonNull
	private final DoubleHistogram notificationSessionOpenDurationHistogram;
	@NonNull
	private final DoubleHistogram notificationSessionDurationHistogram;
	@NonNull
	private final LongUpDownCounter activeNotificationSessionsCounter;
	@NonNull
	private final Map<UUID, Attributes> activeNotificationSessions;
	@NonNull
	private final LongHistogram notificationBatchSizeHistogram;
	@NonNull
	private final LongCounter notificationConnectionLossesCounter;
	@Nullable
	private final String poolName;
	@Nullable
	private final String namespace;
	@NonNull
	private final Boolean recordCollectionName;
	@NonNull
	private final Boolean recordFullSqlState;
	@NonNull
	private final AtomicReferenceArray<SqlClassificationCacheEntry> sqlClassificationCache;

	/**
	 * Acquires a builder for {@link OpenTelemetryMetricsCollector} instances, using {@link GlobalOpenTelemetry}
	 * by default.
	 *
	 * @return the builder
	 */
	@NonNull
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Acquires a builder seeded with a required {@link Meter}.
	 *
	 * @param meter the meter used to build instruments
	 * @return the builder
	 */
	@NonNull
	public static Builder withMeter(@NonNull Meter meter) {
		requireNonNull(meter);
		return builder().meter(meter);
	}

	/**
	 * Acquires a builder seeded with a required {@link OpenTelemetry} instance.
	 *
	 * @param openTelemetry the OpenTelemetry instance used to build a meter
	 * @return the builder
	 */
	@NonNull
	public static Builder withOpenTelemetry(@NonNull OpenTelemetry openTelemetry) {
		requireNonNull(openTelemetry);
		return builder().openTelemetry(openTelemetry);
	}

	/**
	 * Creates an instance from a required {@link Meter} without additional customization.
	 *
	 * @param meter the meter used to build instruments
	 * @return an {@link OpenTelemetryMetricsCollector} instance
	 */
	@NonNull
	public static OpenTelemetryMetricsCollector fromMeter(@NonNull Meter meter) {
		return withMeter(meter).build();
	}

	/**
	 * Creates an instance from a required {@link OpenTelemetry} without additional customization.
	 *
	 * @param openTelemetry the OpenTelemetry instance used to build a meter
	 * @return an {@link OpenTelemetryMetricsCollector} instance
	 */
	@NonNull
	public static OpenTelemetryMetricsCollector fromOpenTelemetry(@NonNull OpenTelemetry openTelemetry) {
		return withOpenTelemetry(openTelemetry).build();
	}

	private OpenTelemetryMetricsCollector(@NonNull Builder builder) {
		requireNonNull(builder);
		Meter meter = requireNonNull(builder.resolveMeter());
		this.poolName = builder.poolName;
		this.namespace = builder.namespace;
		this.recordCollectionName = builder.recordCollectionName;
		this.recordFullSqlState = builder.recordFullSqlState;
		this.sqlClassificationCache = new AtomicReferenceArray<>(SQL_CLASSIFICATION_CACHE_CAPACITY);

		this.operationDurationHistogram = meter.histogramBuilder("db.client.operation.duration")
				.setDescription("Duration of database client operations.")
				.setUnit("s")
				.setExplicitBucketBoundariesAdvice(OPERATION_DURATION_BUCKET_BOUNDARIES)
				.build();
		this.returnedRowsHistogram = meter.histogramBuilder("db.client.response.returned_rows")
				.ofLongs()
				.setDescription("Number of rows returned by a database response.")
				.setUnit("{row}")
				.setExplicitBucketBoundariesAdvice(RETURNED_ROWS_BUCKET_BOUNDARIES)
				.build();
		this.connectionWaitTimeHistogram = meter.histogramBuilder("db.client.connection.wait_time")
				.setDescription("Time spent waiting for a database connection.")
				.setUnit("s")
				.build();
		this.connectionUseTimeHistogram = meter.histogramBuilder("db.client.connection.use_time")
				.setDescription("Time a database connection was held by Pyranid.")
				.setUnit("s")
				.build();
		this.statementPreparationDurationHistogram = meter.histogramBuilder("pyranid.statement.preparation.duration")
				.setDescription("Duration spent preparing and binding a JDBC statement.")
				.setUnit("s")
				.build();
		this.statementExecutionDurationHistogram = meter.histogramBuilder("pyranid.statement.execution.duration")
				.setDescription("Duration spent executing a JDBC statement.")
				.setUnit("s")
				.build();
		this.statementMappingDurationHistogram = meter.histogramBuilder("pyranid.statement.mapping.duration")
				.setDescription("Duration spent mapping JDBC result-set rows.")
				.setUnit("s")
				.build();
		this.statementErrorsCounter = meter.counterBuilder("pyranid.statement.errors")
				.setDescription("Total number of failed Pyranid statement operations.")
				.setUnit("{statement}")
				.build();
		this.statementBatchSizeHistogram = meter.histogramBuilder("pyranid.statement.batch.size")
				.ofLongs()
				.setDescription("Number of statement parameter groups submitted in a batch.")
				.setUnit("{statement}")
				.build();
		this.statementRowsAffectedHistogram = meter.histogramBuilder("pyranid.statement.rows_affected")
				.ofLongs()
				.setDescription("Rows affected by DML/update statements.")
				.setUnit("{row}")
				.build();
		this.transactionClosureDurationHistogram = meter.histogramBuilder("pyranid.transaction.closure.duration")
				.setDescription("Logical Pyranid transaction closure duration.")
				.setUnit("s")
				.build();
		this.transactionCommitDurationHistogram = meter.histogramBuilder("pyranid.transaction.commit.duration")
				.setDescription("JDBC transaction commit operation duration.")
				.setUnit("s")
				.build();
		this.transactionRollbackDurationHistogram = meter.histogramBuilder("pyranid.transaction.rollback.duration")
				.setDescription("JDBC transaction rollback operation duration.")
				.setUnit("s")
				.build();
		this.transactionPhysicalBeginFailuresCounter = meter.counterBuilder("pyranid.transaction.physical.begin_failures")
				.setDescription("Total number of physical transaction begin failures.")
				.setUnit("{transaction}")
				.build();
		this.transactionCountCounter = meter.counterBuilder("pyranid.transaction.count")
				.setDescription("Total number of logical transaction closures.")
				.setUnit("{transaction}")
				.build();
		this.activeTransactionsCounter = meter.upDownCounterBuilder("pyranid.transaction.active")
				.setDescription("Number of active physical JDBC transactions.")
				.setUnit("{transaction}")
				.build();
		this.activeTransactions = ConcurrentHashMap.newKeySet();
		this.savepointOperationsCounter = meter.counterBuilder("pyranid.savepoint.operations")
				.setDescription("Total number of savepoint operations.")
				.setUnit("{operation}")
				.build();
		this.streamDurationHistogram = meter.histogramBuilder("pyranid.fetchstream.duration")
				.setDescription("Duration of Pyranid fetchStream consumption.")
				.setUnit("s")
				.build();
		this.streamRowsConsumedHistogram = meter.histogramBuilder("pyranid.fetchstream.rows_consumed")
				.ofLongs()
				.setDescription("Rows consumed through Pyranid fetchStream.")
				.setUnit("{row}")
				.build();
		this.postTransactionOperationsCounter = meter.counterBuilder("pyranid.post_transaction.operations")
				.setDescription("Total number of post-transaction operations.")
				.setUnit("{operation}")
				.build();
		this.postTransactionDurationHistogram = meter.histogramBuilder("pyranid.post_transaction.duration")
				.setDescription("Duration of post-transaction operations.")
				.setUnit("s")
				.build();
		this.notificationSessionOpenDurationHistogram = meter.histogramBuilder("pyranid.notification.session.open.duration")
				.setDescription("Duration of notification-session opening attempts.")
				.setUnit("s")
				.build();
		this.notificationSessionDurationHistogram = meter.histogramBuilder("pyranid.notification.session.duration")
				.setDescription("Duration of opened notification sessions, including cleanup.")
				.setUnit("s")
				.build();
		this.activeNotificationSessionsCounter = meter.upDownCounterBuilder("pyranid.notification.session.active")
				.setDescription("Number of active notification sessions.")
				.setUnit("{session}")
				.build();
		this.activeNotificationSessions = new ConcurrentHashMap<>();
		this.notificationBatchSizeHistogram = meter.histogramBuilder("pyranid.notification.batch.size")
				.ofLongs()
				.setDescription("Number of notifications delivered to application code in a batch.")
				.setUnit("{notification}")
				.build();
		this.notificationConnectionLossesCounter = meter.counterBuilder("pyranid.notification.connection.losses")
				.setDescription("Total number of terminal notification-connection losses.")
				.setUnit("{connection}")
				.build();
	}

	@Override
	public void didAcquireStatementConnection(@NonNull StatementContext<?> ctx,
																						@NonNull Duration acquisitionDuration) {
		if (this.poolName == null)
			return;

		requireNonNull(ctx);
		recordConnectionWaitTime(ctx.getDatabaseType(), acquisitionDuration, null);
	}

	@Override
	public void didFailToAcquireStatementConnection(@NonNull StatementContext<?> ctx,
																									@NonNull DatabaseType databaseType,
																									@NonNull Duration acquisitionDuration,
																									@NonNull Throwable throwable) {
		if (this.poolName == null)
			return;

		requireNonNull(ctx);
		requireNonNull(databaseType);
		recordConnectionWaitTime(databaseType, acquisitionDuration, requireNonNull(throwable));
	}

	@Override
	public void didAcquireTransactionConnection(@NonNull Transaction transaction,
																						 @NonNull DatabaseType databaseType,
																						 @NonNull Duration acquisitionDuration) {
		recordConnectionWaitTime(databaseType, acquisitionDuration, null);
	}

	@Override
	public void didFailToAcquireTransactionConnection(@NonNull Transaction transaction,
																									 @NonNull DatabaseType databaseType,
																									 @NonNull Duration acquisitionDuration,
																									 @NonNull Throwable throwable) {
		recordConnectionWaitTime(databaseType, acquisitionDuration, requireNonNull(throwable));
	}

	@Override
	public void didReleaseStatementConnection(@NonNull StatementContext<?> ctx,
																						@NonNull Duration heldDuration) {
		if (this.poolName == null)
			return;

		requireNonNull(ctx);
		recordConnectionUseTime(ctx.getDatabaseType(), heldDuration, null);
	}

	@Override
	public void didFailToReleaseStatementConnection(@NonNull StatementContext<?> ctx,
																									@NonNull Duration heldDuration,
																									@NonNull Throwable throwable) {
		if (this.poolName == null)
			return;

		requireNonNull(ctx);
		recordConnectionUseTime(ctx.getDatabaseType(), heldDuration, requireNonNull(throwable));
	}

	@Override
	public void didReleaseTransactionConnection(@NonNull Transaction transaction,
																						 @NonNull DatabaseType databaseType,
																						 @NonNull Duration heldDuration) {
		recordConnectionUseTime(databaseType, heldDuration, null);
	}

	@Override
	public void didFailToReleaseTransactionConnection(@NonNull Transaction transaction,
																									 @NonNull DatabaseType databaseType,
																									 @NonNull Duration heldDuration,
																									 @NonNull Throwable throwable) {
		recordConnectionUseTime(databaseType, heldDuration, requireNonNull(throwable));
	}

	@Override
	public void didExitTransactionClosure(@NonNull Transaction transaction,
															@NonNull TransactionClosureOutcome outcome,
															@NonNull DatabaseType databaseType,
															@NonNull Duration logicalDuration,
															@Nullable Throwable thrown) {
		requireNonNull(outcome);
		Attributes attributes = transactionClosureAttributes(databaseType, transaction.getTransactionIsolation(), outcome, thrown);
		this.transactionClosureDurationHistogram.record(seconds(logicalDuration), attributes);
		this.transactionCountCounter.add(1, attributes);
	}

	@Override
	public void didBeginPhysicalTransaction(@NonNull Transaction transaction,
																					@NonNull TransactionIsolation isolation,
																					@NonNull DatabaseType databaseType) {
		markPhysicalTransactionActive(transaction, databaseType);
	}

	@Override
	public void didFailToBeginPhysicalTransaction(@NonNull Transaction transaction,
																							 @NonNull TransactionIsolation isolation,
																							 @NonNull PhysicalTransactionBeginFailurePhase phase,
																							 @NonNull DatabaseType databaseType,
																							 @NonNull Throwable throwable) {
		Attributes attributes = Attributes.builder()
				.putAll(databaseAttributes(databaseType))
				.put(TRANSACTION_ISOLATION_ATTRIBUTE_KEY, enumValue(isolation))
				.put(TRANSACTION_BEGIN_PHASE_ATTRIBUTE_KEY, enumValue(phase))
				.put(ERROR_TYPE_ATTRIBUTE_KEY, errorType(throwable))
				.build();
		this.transactionPhysicalBeginFailuresCounter.add(1, attributes);
	}

	@Override
	public void didCommitPhysicalTransaction(@NonNull Transaction transaction,
																					 @NonNull DatabaseType databaseType,
																					 @NonNull Duration physicalDuration) {
		this.transactionCommitDurationHistogram.record(seconds(physicalDuration),
				transactionOperationAttributes(databaseType, transaction.getTransactionIsolation(), TRANSACTION_COMMIT_OUTCOME_ATTRIBUTE_KEY, OUTCOME_SUCCESS, null));
		markPhysicalTransactionInactive(transaction, databaseType);
	}

	@Override
	public void didFailToCommitPhysicalTransaction(@NonNull Transaction transaction,
																								@NonNull DatabaseType databaseType,
																								@NonNull Duration physicalDuration,
																								@NonNull Throwable throwable) {
		this.transactionCommitDurationHistogram.record(seconds(physicalDuration),
				transactionOperationAttributes(databaseType, transaction.getTransactionIsolation(), TRANSACTION_COMMIT_OUTCOME_ATTRIBUTE_KEY, OUTCOME_FAILURE, throwable));
		// No active-transaction decrement here: Pyranid attempts rollback after commit failure, and the following
		// rollback callback owns the single terminal decrement. Decrementing here would double-count.
	}

	@Override
	public void didRollbackPhysicalTransaction(@NonNull Transaction transaction,
																						@NonNull DatabaseType databaseType,
																						@NonNull Duration physicalDuration) {
		this.transactionRollbackDurationHistogram.record(seconds(physicalDuration),
				transactionOperationAttributes(databaseType, transaction.getTransactionIsolation(), TRANSACTION_ROLLBACK_OUTCOME_ATTRIBUTE_KEY, OUTCOME_SUCCESS, null));
		markPhysicalTransactionInactive(transaction, databaseType);
	}

	@Override
	public void didFailToRollbackPhysicalTransaction(@NonNull Transaction transaction,
																									 @NonNull DatabaseType databaseType,
																									 @NonNull Duration physicalDuration,
																									 @NonNull Throwable throwable) {
		this.transactionRollbackDurationHistogram.record(seconds(physicalDuration),
				transactionOperationAttributes(databaseType, transaction.getTransactionIsolation(), TRANSACTION_ROLLBACK_OUTCOME_ATTRIBUTE_KEY, OUTCOME_FAILURE, throwable));
		markPhysicalTransactionInactive(transaction, databaseType);
	}

	@Override
	public void didCreateSavepoint(@NonNull Transaction transaction,
																 @NonNull DatabaseType databaseType) {
		recordSavepointOperation(databaseType, "created");
	}

	@Override
	public void didRollbackToSavepoint(@NonNull Transaction transaction,
																		 @NonNull DatabaseType databaseType) {
		recordSavepointOperation(databaseType, "rolled_back");
	}

	@Override
	public void didReleaseSavepoint(@NonNull Transaction transaction,
																	@NonNull DatabaseType databaseType) {
		recordSavepointOperation(databaseType, "released");
	}

	@Override
	public void didExecuteStatement(@NonNull StatementContext<?> ctx,
																	@NonNull StatementLog<?> statementLog,
																	@NonNull StatementResult result) {
		requireNonNull(ctx);
		requireNonNull(statementLog);
		requireNonNull(result);

		Integer batchSize = statementLog.getBatchSize().orElse(null);
		Attributes attributes = statementAttributes(ctx, null, null, batchSize);
		this.operationDurationHistogram.record(seconds(statementLog.getTotalDuration()), attributes);
		recordStatementComponentDurations(statementLog, attributes);

		if (batchSize != null)
			this.statementBatchSizeHistogram.record(batchSize.longValue(), attributes);

		Long rowsReturned = result.rowsReturned();
		if (rowsReturned != null)
			this.returnedRowsHistogram.record(rowsReturned, attributes);

		Long rowsAffected = result.rowsAffected();
		if (rowsAffected != null)
			this.statementRowsAffectedHistogram.record(rowsAffected, attributes);
	}

	@Override
	public void didFailToExecuteStatement(@NonNull StatementContext<?> ctx,
																				@NonNull StatementLog<?> statementLog,
																				@NonNull DatabaseType databaseType,
																				@NonNull Throwable throwable) {
		requireNonNull(ctx);
		requireNonNull(statementLog);
		requireNonNull(databaseType);
		requireNonNull(throwable);

		Integer batchSize = statementLog.getBatchSize().orElse(null);
		Attributes attributes = statementAttributes(ctx, throwable, databaseType, batchSize);
		this.operationDurationHistogram.record(seconds(statementLog.getTotalDuration()), attributes);
		recordStatementComponentDurations(statementLog, attributes);
		if (batchSize != null)
			this.statementBatchSizeHistogram.record(batchSize.longValue(), attributes);
		this.statementErrorsCounter.add(1, attributes);
	}

	@Override
	public void didFailToOpenStream(@NonNull StatementContext<?> ctx,
																	@NonNull DatabaseType databaseType,
																	@NonNull Duration openDuration,
																	@NonNull Throwable throwable) {
		requireNonNull(ctx);
		requireNonNull(databaseType);
		requireNonNull(throwable);

		Attributes attributes = streamAttributes(databaseType, StreamTerminalOutcome.OPEN_FAILURE, throwable);
		this.streamDurationHistogram.record(seconds(openDuration), attributes);
	}

	@Override
	public void didCloseStream(@NonNull StatementContext<?> ctx,
														 @NonNull StreamTerminalOutcome outcome,
														 @NonNull Long rowsConsumed,
														 @NonNull Duration streamDuration,
														 @Nullable Throwable throwable) {
		requireNonNull(ctx);
		requireNonNull(outcome);
		requireNonNull(rowsConsumed);
		requireNonNull(streamDuration);

		Attributes attributes = streamAttributes(ctx.getDatabaseType(), outcome, throwable);
		this.streamDurationHistogram.record(seconds(streamDuration), attributes);
		this.streamRowsConsumedHistogram.record(rowsConsumed, attributes);
	}

	@Override
	public void willOpenNotificationSession(@NonNull DatabaseType databaseType,
																						@NonNull UUID notificationSessionId) {
		// The matching callbacks supply the authoritative duration and outcome; there is no metric to record yet.
		requireNonNull(databaseType);
		requireNonNull(notificationSessionId);
	}

	@Override
	public void didOpenNotificationSession(@NonNull DatabaseType databaseType,
																					 @NonNull UUID notificationSessionId,
																					 @NonNull Duration openDuration) {
		requireNonNull(openDuration);
		markNotificationSessionActive(databaseType, notificationSessionId);
		this.notificationSessionOpenDurationHistogram.record(seconds(openDuration),
				notificationAttributes(databaseType, NOTIFICATION_SESSION_OPEN_OUTCOME_ATTRIBUTE_KEY, OUTCOME_SUCCESS, null));
	}

	@Override
	public void didFailToOpenNotificationSession(@NonNull DatabaseType databaseType,
																						 @NonNull UUID notificationSessionId,
																						 @NonNull Duration openDuration,
																						 @NonNull Throwable throwable) {
		requireNonNull(notificationSessionId);
		requireNonNull(openDuration);
		requireNonNull(throwable);
		this.notificationSessionOpenDurationHistogram.record(seconds(openDuration),
				notificationAttributes(databaseType, NOTIFICATION_SESSION_OPEN_OUTCOME_ATTRIBUTE_KEY, OUTCOME_FAILURE, throwable));
	}

	@Override
	public void didDeliverNotificationBatch(@NonNull DatabaseType databaseType,
																						@NonNull UUID notificationSessionId,
																						@NonNull Long notificationCount) {
		requireNonNull(notificationSessionId);
		this.notificationBatchSizeHistogram.record(requireNonNull(notificationCount), notificationAttributes(databaseType));
	}

	@Override
	public void didLoseNotificationConnection(@NonNull DatabaseType databaseType,
																							@NonNull UUID notificationSessionId,
																							@NonNull Throwable throwable) {
		requireNonNull(notificationSessionId);
		this.notificationConnectionLossesCounter.add(1,
				notificationAttributes(databaseType, null, null, requireNonNull(throwable)));
	}

	@Override
	public void didCloseNotificationSession(@NonNull DatabaseType databaseType,
																						 @NonNull UUID notificationSessionId,
																						 @NonNull NotificationSessionOutcome outcome,
																						 @NonNull Duration sessionDuration,
																						 @Nullable Throwable throwable) {
		requireNonNull(outcome);
		requireNonNull(sessionDuration);

		try {
			this.notificationSessionDurationHistogram.record(seconds(sessionDuration),
					notificationAttributes(databaseType, NOTIFICATION_SESSION_OUTCOME_ATTRIBUTE_KEY, enumValue(outcome), throwable));
		} finally {
			markNotificationSessionInactive(notificationSessionId);
		}
	}

	@Override
	public void didRunPostTransactionOperation(@NonNull Transaction transaction,
																						 @NonNull TransactionResult result,
																						 @NonNull DatabaseType databaseType,
																						 @NonNull Duration duration,
																						 @Nullable Throwable throwable) {
		AttributesBuilder builder = Attributes.builder()
				.putAll(databaseAttributes(databaseType))
				.put(TRANSACTION_RESULT_ATTRIBUTE_KEY, enumValue(result));

		if (throwable != null)
			builder.put(ERROR_TYPE_ATTRIBUTE_KEY, errorType(throwable));

		Attributes attributes = builder.build();
		this.postTransactionOperationsCounter.add(1, attributes);
		this.postTransactionDurationHistogram.record(seconds(duration), attributes);
	}

	@Override
	@NonNull
	public Optional<Snapshot> snapshot() {
		return Optional.empty();
	}

	@NonNull
	private Attributes statementAttributes(@NonNull StatementContext<?> ctx,
																 @Nullable Throwable throwable,
																 @Nullable DatabaseType databaseTypeOverride,
																 @Nullable Integer batchSize) {
		requireNonNull(ctx);
		SqlClassification sqlClassification = sqlClassificationFor(ctx.getStatement().getSql());

		AttributesBuilder builder = Attributes.builder()
				.putAll(databaseAttributes(databaseTypeOverride == null ? ctx.getDatabaseType() : databaseTypeOverride))
				.put(DB_OPERATION_NAME_ATTRIBUTE_KEY, operationName(sqlClassification.operation(), batchSize != null));

		if (this.namespace != null)
			builder.put(DB_NAMESPACE_ATTRIBUTE_KEY, this.namespace);

		if (this.recordCollectionName) {
			String collectionName = sqlClassification.collectionName();
			if (collectionName != null)
				builder.put(DB_COLLECTION_NAME_ATTRIBUTE_KEY, collectionName);
		}

		String statusCode = dbResponseStatusCode(throwable);
		if (statusCode != null)
			builder.put(DB_RESPONSE_STATUS_CODE_ATTRIBUTE_KEY, statusCode);

		if (throwable != null)
			builder.put(ERROR_TYPE_ATTRIBUTE_KEY, errorType(throwable));

		return builder.build();
	}

	@NonNull
	private Attributes databaseAttributes(@NonNull DatabaseType databaseType) {
		requireNonNull(databaseType);
		return Attributes.of(DB_SYSTEM_NAME_ATTRIBUTE_KEY, dbSystemName(databaseType));
	}

	@NonNull
	private Attributes notificationAttributes(@NonNull DatabaseType databaseType) {
		return notificationAttributes(databaseType, null, null, null);
	}

	@NonNull
	private Attributes notificationAttributes(@NonNull DatabaseType databaseType,
																						@Nullable AttributeKey<String> outcomeAttributeKey,
																						@Nullable String outcome,
																						@Nullable Throwable throwable) {
		AttributesBuilder builder = Attributes.builder().putAll(databaseAttributes(databaseType));

		if (this.namespace != null)
			builder.put(DB_NAMESPACE_ATTRIBUTE_KEY, this.namespace);

		if (this.poolName != null)
			builder.put(DB_CLIENT_CONNECTION_POOL_NAME_ATTRIBUTE_KEY, this.poolName);

		if (outcomeAttributeKey != null)
			builder.put(outcomeAttributeKey, requireNonNull(outcome));

		if (throwable != null)
			builder.put(ERROR_TYPE_ATTRIBUTE_KEY, errorType(throwable));

		return builder.build();
	}

	private void markNotificationSessionActive(@NonNull DatabaseType databaseType,
																								@NonNull UUID notificationSessionId) {
		requireNonNull(notificationSessionId);
		Attributes attributes = notificationAttributes(databaseType);

		if (this.activeNotificationSessions.putIfAbsent(notificationSessionId, attributes) != null)
			return;

		try {
			this.activeNotificationSessionsCounter.add(1, attributes);
		} catch (RuntimeException | Error throwable) {
			this.activeNotificationSessions.remove(notificationSessionId, attributes);
			throw throwable;
		}
	}

	private void markNotificationSessionInactive(@NonNull UUID notificationSessionId) {
		requireNonNull(notificationSessionId);
		Attributes attributes = this.activeNotificationSessions.remove(notificationSessionId);

		if (attributes == null)
			return;

		try {
			this.activeNotificationSessionsCounter.add(-1, attributes);
		} catch (RuntimeException | Error throwable) {
			this.activeNotificationSessions.putIfAbsent(notificationSessionId, attributes);
			throw throwable;
		}
	}

	private void markPhysicalTransactionActive(@NonNull Transaction transaction,
																						 @NonNull DatabaseType databaseType) {
		requireNonNull(transaction);
		requireNonNull(databaseType);

		if (!this.activeTransactions.add(transaction))
			return;

		try {
			this.activeTransactionsCounter.add(1, databaseAttributes(databaseType));
		} catch (RuntimeException | Error throwable) {
			this.activeTransactions.remove(transaction);
			throw throwable;
		}
	}

	private void markPhysicalTransactionInactive(@NonNull Transaction transaction,
																							 @NonNull DatabaseType databaseType) {
		requireNonNull(transaction);
		requireNonNull(databaseType);

		if (!this.activeTransactions.remove(transaction))
			return;

		try {
			this.activeTransactionsCounter.add(-1, databaseAttributes(databaseType));
		} catch (RuntimeException | Error throwable) {
			this.activeTransactions.add(transaction);
			throw throwable;
		}
	}

	@NonNull
	private Attributes transactionClosureAttributes(@NonNull DatabaseType databaseType,
																	@NonNull TransactionIsolation isolation,
																	@NonNull TransactionClosureOutcome outcome,
																	@Nullable Throwable throwable) {
		AttributesBuilder builder = Attributes.builder()
				.putAll(databaseAttributes(databaseType))
				.put(TRANSACTION_ISOLATION_ATTRIBUTE_KEY, enumValue(isolation))
				.put(TRANSACTION_CLOSURE_OUTCOME_ATTRIBUTE_KEY, enumValue(outcome));

		if (throwable != null)
			builder.put(ERROR_TYPE_ATTRIBUTE_KEY, errorType(throwable));

		return builder.build();
	}

	@NonNull
	private Attributes transactionOperationAttributes(@NonNull DatabaseType databaseType,
																										@NonNull TransactionIsolation isolation,
																										@NonNull AttributeKey<String> outcomeAttributeKey,
																										@NonNull String outcome,
																										@Nullable Throwable throwable) {
		AttributesBuilder builder = Attributes.builder()
				.putAll(databaseAttributes(databaseType))
				.put(TRANSACTION_ISOLATION_ATTRIBUTE_KEY, enumValue(isolation))
				.put(outcomeAttributeKey, outcome);

		if (throwable != null)
			builder.put(ERROR_TYPE_ATTRIBUTE_KEY, errorType(throwable));

		return builder.build();
	}

	@NonNull
	private Attributes streamAttributes(@NonNull DatabaseType databaseType,
														@NonNull StreamTerminalOutcome outcome,
														@Nullable Throwable throwable) {
		AttributesBuilder builder = Attributes.builder()
				.putAll(databaseAttributes(databaseType))
				.put(STREAM_OUTCOME_ATTRIBUTE_KEY, enumValue(outcome));

		if (throwable != null)
			builder.put(ERROR_TYPE_ATTRIBUTE_KEY, errorType(throwable));

		return builder.build();
	}

	private void recordStatementComponentDurations(@NonNull StatementLog<?> statementLog,
																								 @NonNull Attributes attributes) {
		requireNonNull(statementLog);
		requireNonNull(attributes);
		statementLog.getPreparationDuration().ifPresent(duration -> this.statementPreparationDurationHistogram.record(seconds(duration), attributes));
		statementLog.getExecutionDuration().ifPresent(duration -> this.statementExecutionDurationHistogram.record(seconds(duration), attributes));
		statementLog.getResultSetMappingDuration().ifPresent(duration -> this.statementMappingDurationHistogram.record(seconds(duration), attributes));
	}

	private void recordConnectionWaitTime(@NonNull DatabaseType databaseType,
																				@NonNull Duration duration,
																				@Nullable Throwable throwable) {
		requireNonNull(databaseType);
		requireNonNull(duration);

		if (this.poolName == null)
			return;

		this.connectionWaitTimeHistogram.record(seconds(duration), connectionAttributes(databaseType, throwable));
	}

	private void recordConnectionUseTime(@NonNull DatabaseType databaseType,
																			 @NonNull Duration duration,
																			 @Nullable Throwable throwable) {
		requireNonNull(databaseType);
		requireNonNull(duration);

		if (this.poolName == null)
			return;

		this.connectionUseTimeHistogram.record(seconds(duration), connectionAttributes(databaseType, throwable));
	}

	@NonNull
	private Attributes connectionAttributes(@NonNull DatabaseType databaseType,
																					@Nullable Throwable throwable) {
		AttributesBuilder builder = Attributes.builder()
				.put(DB_CLIENT_CONNECTION_POOL_NAME_ATTRIBUTE_KEY, requireNonNull(this.poolName))
				.putAll(databaseAttributes(databaseType));

		if (throwable != null)
			builder.put(ERROR_TYPE_ATTRIBUTE_KEY, errorType(throwable));

		return builder.build();
	}

	private void recordSavepointOperation(@NonNull DatabaseType databaseType,
																				@NonNull String operation) {
		this.savepointOperationsCounter.add(1, Attributes.builder()
				.putAll(databaseAttributes(databaseType))
				.put(SAVEPOINT_OPERATION_ATTRIBUTE_KEY, operation)
				.build());
	}

	@NonNull
	static String dbSystemName(@NonNull DatabaseType databaseType) {
		requireNonNull(databaseType);
		// Maps to OpenTelemetry's db.system.name registry values. A default arm keeps this forward-compatible: a future
		// DatabaseType added to the core falls back to "other_sql" instead of throwing at runtime (the previous switch had
		// no default, so values added after this module was compiled would fail).
		return switch (databaseType) {
			case POSTGRESQL -> "postgresql";
			case MYSQL -> "mysql";
			case MARIA_DB -> "mariadb";
			case SQLITE -> "sqlite";
			case SQL_SERVER -> "microsoft.sql_server";
			case ORACLE -> "oracle.db";
			default -> "other_sql"; // GENERIC and any future DatabaseType
		};
	}

	@NonNull
	static String operationNameForSql(@NonNull String sql,
																	 boolean batch) {
		String operation = analyzeSql(requireNonNull(sql)).operation();
		return operationName(operation, batch);
	}

	@NonNull
	private static String operationName(@NonNull String operation,
															 boolean batch) {
		requireNonNull(operation);

		if (!batch)
			return operation;

		return UNKNOWN_OPERATION.equals(operation) ? "BATCH" : "BATCH " + operation;
	}

	@Nullable
	static String collectionNameForSql(@NonNull String sql) {
		return collectionName(analyzeSql(requireNonNull(sql)));
	}

	@NonNull
	private SqlClassification sqlClassificationFor(@NonNull String sql) {
		requireNonNull(sql);
		int firstIndex = spreadHash(sql.hashCode()) & SQL_CLASSIFICATION_CACHE_MASK;
		int emptyIndex = -1;

		// Adjacent probes let colliding hot statements coexist while keeping the cache bounded and lock-free.
		for (int offset = 0; offset < SQL_CLASSIFICATION_CACHE_PROBES; offset++) {
			int index = (firstIndex + offset) & SQL_CLASSIFICATION_CACHE_MASK;
			SqlClassificationCacheEntry entry = this.sqlClassificationCache.get(index);

			if (entry == null) {
				if (emptyIndex < 0)
					emptyIndex = index;
			} else if (entry.sql().equals(sql)) {
				return entry.classification();
			}
		}

		SqlAnalysis analysis = analyzeSql(sql);
		SqlClassification classification = new SqlClassification(analysis.operation(),
				this.recordCollectionName ? collectionName(analysis) : null);
		SqlClassificationCacheEntry newEntry = new SqlClassificationCacheEntry(sql, classification);

		if (emptyIndex < 0 || !this.sqlClassificationCache.compareAndSet(emptyIndex, null, newEntry))
			this.sqlClassificationCache.set(firstIndex, newEntry);

		return classification;
	}

	private static int spreadHash(int hash) {
		return hash ^ (hash >>> 16);
	}

	@Nullable
	private static String collectionName(@NonNull SqlAnalysis analysis) {
		requireNonNull(analysis);
		if (analysis.operationIndex() < 0 || analysis.commonTableExpression() || hasSecondaryOperation(analysis))
			return null;

		return switch (analysis.operation()) {
			case "INSERT" -> insertCollectionName(analysis);
			case "UPDATE" -> updateCollectionName(analysis);
			case "DELETE" -> deleteCollectionName(analysis);
			case "SELECT" -> selectCollectionName(analysis);
			default -> null;
		};
	}

	private static boolean hasSecondaryOperation(@NonNull SqlAnalysis analysis) {
		for (int i = analysis.operationIndex() + 1; i < analysis.endIndex(); i++)
			if (operationWord(analysis.tokens().get(i)))
				return true;

		return false;
	}

	@Nullable
	private static String insertCollectionName(@NonNull SqlAnalysis analysis) {
		int index = analysis.operationIndex() + 1;
		if (!isWord(analysis.tokens(), index, "INTO"))
			return null;

		ParsedIdentifier identifier = parseIdentifier(analysis.tokens(), index + 1, analysis.endIndex());
		return identifier == null ? null : identifier.value();
	}

	@Nullable
	private static String updateCollectionName(@NonNull SqlAnalysis analysis) {
		int index = analysis.operationIndex() + 1;
		if (isWord(analysis.tokens(), index, "ONLY"))
			index++;

		ParsedIdentifier identifier = parseIdentifier(analysis.tokens(), index, analysis.endIndex());
		if (identifier == null)
			return null;

		int depth = 0;
		for (int i = identifier.nextIndex(); i < analysis.endIndex(); i++) {
			SqlToken token = analysis.tokens().get(i);
			if (token.isSymbol("("))
				depth++;
			else if (token.isSymbol(")"))
				depth = Math.max(0, depth - 1);
			else if (depth == 0 && token.isWord("SET"))
				return identifier.value();
			else if (depth == 0 && (token.isSymbol(",") || token.isWord("JOIN")))
				return null;
		}

		return null;
	}

	@Nullable
	private static String deleteCollectionName(@NonNull SqlAnalysis analysis) {
		int index = analysis.operationIndex() + 1;
		if (!isWord(analysis.tokens(), index, "FROM"))
			return null;

		ParsedIdentifier identifier = parseIdentifier(analysis.tokens(), index + 1, analysis.endIndex());
		if (identifier == null)
			return null;

		int depth = 0;
		for (int i = identifier.nextIndex(); i < analysis.endIndex(); i++) {
			SqlToken token = analysis.tokens().get(i);
			if (token.isSymbol("("))
				depth++;
			else if (token.isSymbol(")"))
				depth = Math.max(0, depth - 1);
			else if (depth == 0 && (token.isSymbol(",") || token.isWord("JOIN") || token.isWord("USING")))
				return null;
		}

		return identifier.value();
	}

	@Nullable
	private static String selectCollectionName(@NonNull SqlAnalysis analysis) {
		List<SqlToken> tokens = analysis.tokens();
		int depth = 0;
		int fromIndex = -1;

		for (int i = analysis.operationIndex() + 1; i < analysis.endIndex(); i++) {
			SqlToken token = tokens.get(i);
			if (token.isSymbol("("))
				depth++;
			else if (token.isSymbol(")"))
				depth = Math.max(0, depth - 1);
			else if (depth == 0 && token.isWord("FROM")) {
				fromIndex = i;
				break;
			}
		}

		if (fromIndex < 0 || fromIndex + 1 >= analysis.endIndex()
				|| tokens.get(fromIndex + 1).isWord("LATERAL")
				|| tokens.get(fromIndex + 1).isSymbol("("))
			return null;

		ParsedIdentifier identifier = parseIdentifier(tokens, fromIndex + 1, analysis.endIndex());
		if (identifier == null || (identifier.nextIndex() < analysis.endIndex()
				&& tokens.get(identifier.nextIndex()).isSymbol("(")))
			return null;

		depth = 0;
		for (int i = identifier.nextIndex(); i < analysis.endIndex(); i++) {
			SqlToken token = tokens.get(i);
			if (token.isSymbol("("))
				depth++;
			else if (token.isSymbol(")"))
				depth = Math.max(0, depth - 1);
			else if (depth == 0 && (token.isSymbol(",") || token.isWord("JOIN") || token.isWord("APPLY")
					|| token.isWord("UNION") || token.isWord("INTERSECT") || token.isWord("EXCEPT")
					|| token.isWord("FROM")))
				return null;
		}

		return identifier.value();
	}

	@Nullable
	private static ParsedIdentifier parseIdentifier(@NonNull List<SqlToken> tokens,
																			 int startIndex,
																			 int endIndex) {
		if (startIndex >= endIndex || !tokens.get(startIndex).identifier())
			return null;

		StringBuilder value = new StringBuilder(tokens.get(startIndex).text());
		int index = startIndex + 1;

		while (index + 1 < endIndex && tokens.get(index).isSymbol(".") && tokens.get(index + 1).identifier()) {
			value.append('.').append(tokens.get(index + 1).text());
			index += 2;
		}

		return new ParsedIdentifier(value.toString(), index);
	}

	private static boolean isWord(@NonNull List<SqlToken> tokens,
														int index,
														@NonNull String word) {
		return index >= 0 && index < tokens.size() && tokens.get(index).isWord(word);
	}

	@NonNull
	private static SqlAnalysis analyzeSql(@NonNull String sql) {
		List<SqlToken> tokens = tokenizeSql(requireNonNull(sql));
		int endIndex = firstStatementEnd(tokens);
		if (endIndex == 0 || !tokens.get(0).word())
			return new SqlAnalysis(tokens, -1, endIndex, UNKNOWN_OPERATION, false);

		if (!tokens.get(0).isWord("WITH"))
			return new SqlAnalysis(tokens, 0, endIndex, tokens.get(0).upperText(), false);

		int depth = 0;
		boolean completedCommonTableExpression = false;
		for (int i = 1; i < endIndex; i++) {
			SqlToken token = tokens.get(i);
			if (token.isSymbol("(")) {
				depth++;
			} else if (token.isSymbol(")")) {
				if (depth == 1)
					completedCommonTableExpression = true;
				depth = Math.max(0, depth - 1);
			} else if (depth == 0 && completedCommonTableExpression && operationWord(token)) {
				return new SqlAnalysis(tokens, i, endIndex, token.upperText(), true);
			}
		}

		return new SqlAnalysis(tokens, -1, endIndex, UNKNOWN_OPERATION, true);
	}

	private static int firstStatementEnd(@NonNull List<SqlToken> tokens) {
		int depth = 0;
		for (int i = 0; i < tokens.size(); i++) {
			SqlToken token = tokens.get(i);
			if (token.isSymbol("("))
				depth++;
			else if (token.isSymbol(")"))
				depth = Math.max(0, depth - 1);
			else if (depth == 0 && token.isSymbol(";"))
				return i;
		}

		return tokens.size();
	}

	private static boolean operationWord(@NonNull SqlToken token) {
		if (!token.word())
			return false;

		return switch (token.upperText()) {
			case "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "CALL", "EXECUTE", "CREATE", "ALTER",
					"DROP", "TRUNCATE", "GRANT", "REVOKE" -> true;
			default -> false;
		};
	}

	@NonNull
	private static List<SqlToken> tokenizeSql(@NonNull String sql) {
		List<SqlToken> tokens = new ArrayList<>();
		int index = 0;

		while (index < sql.length()) {
			char character = sql.charAt(index);
			if (Character.isWhitespace(character)) {
				index++;
				continue;
			}

			if (character == '-' && index + 1 < sql.length() && sql.charAt(index + 1) == '-') {
				index = skipLine(sql, index + 2);
				continue;
			}

			if (character == '#') {
				index = skipLine(sql, index + 1);
				continue;
			}

			if (character == '/' && index + 1 < sql.length() && sql.charAt(index + 1) == '*') {
				index = skipBlockComment(sql, index + 2);
				continue;
			}

			if ((character == 'q' || character == 'Q') && index + 2 < sql.length() && sql.charAt(index + 1) == '\'') {
				int nextIndex = skipOracleQuotedString(sql, index);
				if (nextIndex > index) {
					index = nextIndex;
					continue;
				}
			}

			if (character == '\'') {
				index = skipQuoted(sql, index + 1, '\'', true);
				continue;
			}

			if (character == '$') {
				int nextIndex = skipDollarQuotedString(sql, index);
				if (nextIndex > index) {
					index = nextIndex;
					continue;
				}
			}

			if (character == '\"' || character == '`' || character == '[') {
				QuotedIdentifier quotedIdentifier = readQuotedIdentifier(sql, index, character);
				tokens.add(new SqlToken(SqlTokenType.IDENTIFIER, quotedIdentifier.value()));
				index = quotedIdentifier.nextIndex();
				continue;
			}

			if (Character.isLetter(character) || character == '_') {
				int startIndex = index++;
				while (index < sql.length()) {
					char nextCharacter = sql.charAt(index);
					if (!Character.isLetterOrDigit(nextCharacter) && nextCharacter != '_' && nextCharacter != '$')
						break;
					index++;
				}
				tokens.add(new SqlToken(SqlTokenType.WORD, sql.substring(startIndex, index)));
				continue;
			}

			tokens.add(new SqlToken(SqlTokenType.SYMBOL, String.valueOf(character)));
			index++;
		}

		return List.copyOf(tokens);
	}

	private static int skipLine(@NonNull String sql,
														int startIndex) {
		int newlineIndex = sql.indexOf('\n', startIndex);
		return newlineIndex < 0 ? sql.length() : newlineIndex + 1;
	}

	private static int skipBlockComment(@NonNull String sql,
																		int startIndex) {
		int depth = 1;
		int index = startIndex;
		while (index < sql.length() && depth > 0) {
			if (index + 1 < sql.length() && sql.charAt(index) == '/' && sql.charAt(index + 1) == '*') {
				depth++;
				index += 2;
			} else if (index + 1 < sql.length() && sql.charAt(index) == '*' && sql.charAt(index + 1) == '/') {
				depth--;
				index += 2;
			} else {
				index++;
			}
		}
		return index;
	}

	private static int skipQuoted(@NonNull String sql,
														int startIndex,
														char quote,
														boolean backslashEscapes) {
		int index = startIndex;
		while (index < sql.length()) {
			char character = sql.charAt(index);
			if (backslashEscapes && character == '\\' && index + 1 < sql.length()) {
				index += 2;
			} else if (character == quote) {
				if (index + 1 < sql.length() && sql.charAt(index + 1) == quote)
					index += 2;
				else
					return index + 1;
			} else {
				index++;
			}
		}
		return sql.length();
	}

	private static int skipDollarQuotedString(@NonNull String sql,
																				int startIndex) {
		int delimiterEnd = startIndex + 1;
		while (delimiterEnd < sql.length()) {
			char character = sql.charAt(delimiterEnd);
			if (character == '$')
				break;
			if (!Character.isLetterOrDigit(character) && character != '_')
				return startIndex;
			delimiterEnd++;
		}

		if (delimiterEnd >= sql.length())
			return startIndex;

		String delimiter = sql.substring(startIndex, delimiterEnd + 1);
		int closingIndex = sql.indexOf(delimiter, delimiterEnd + 1);
		return closingIndex < 0 ? sql.length() : closingIndex + delimiter.length();
	}

	private static int skipOracleQuotedString(@NonNull String sql,
																			int startIndex) {
		char openingDelimiter = sql.charAt(startIndex + 2);
		char closingDelimiter = switch (openingDelimiter) {
			case '[' -> ']';
			case '(' -> ')';
			case '{' -> '}';
			case '<' -> '>';
			default -> openingDelimiter;
		};

		for (int index = startIndex + 3; index + 1 < sql.length(); index++) {
			if (sql.charAt(index) == closingDelimiter && sql.charAt(index + 1) == '\'')
				return index + 2;
		}
		return sql.length();
	}

	@NonNull
	private static QuotedIdentifier readQuotedIdentifier(@NonNull String sql,
																								int startIndex,
																								char openingQuote) {
		char closingQuote = openingQuote == '[' ? ']' : openingQuote;
		StringBuilder value = new StringBuilder();
		int index = startIndex + 1;
		while (index < sql.length()) {
			char character = sql.charAt(index);
			if (character == closingQuote) {
				if (index + 1 < sql.length() && sql.charAt(index + 1) == closingQuote) {
					value.append(closingQuote);
					index += 2;
					continue;
				}
				return new QuotedIdentifier(value.toString(), index + 1);
			}
			value.append(character);
			index++;
		}

		return new QuotedIdentifier(value.toString(), sql.length());
	}

	private enum SqlTokenType {
		WORD,
		IDENTIFIER,
		SYMBOL
	}

	private record SqlToken(@NonNull SqlTokenType type,
											@NonNull String text) {
		private boolean word() {
			return this.type == SqlTokenType.WORD;
		}

		private boolean identifier() {
			return this.type == SqlTokenType.WORD || this.type == SqlTokenType.IDENTIFIER;
		}

		private boolean isWord(@NonNull String word) {
			return word() && this.text.equalsIgnoreCase(word);
		}

		private boolean isSymbol(@NonNull String symbol) {
			return this.type == SqlTokenType.SYMBOL && this.text.equals(symbol);
		}

		@NonNull
		private String upperText() {
			return this.text.toUpperCase(Locale.ROOT);
		}
	}

	private record SqlAnalysis(@NonNull List<SqlToken> tokens,
												 int operationIndex,
												 int endIndex,
												 @NonNull String operation,
												 boolean commonTableExpression) {
	}

	private record SqlClassification(@NonNull String operation,
														 @Nullable String collectionName) {
	}

	private record SqlClassificationCacheEntry(@NonNull String sql,
																			 @NonNull SqlClassification classification) {
	}

	private record ParsedIdentifier(@NonNull String value,
														int nextIndex) {
	}

	private record QuotedIdentifier(@NonNull String value,
														int nextIndex) {
	}

	@NonNull
	private static String enumValue(@NonNull Enum<?> value) {
		requireNonNull(value);
		return value.name().toLowerCase(Locale.ROOT);
	}

	@Nullable
	private String dbResponseStatusCode(@Nullable Throwable throwable) {
		SQLException sqlException = sqlExceptionFor(throwable);
		if (sqlException == null)
			return null;

		String sqlState = sqlException.getSQLState();
		if (sqlState == null || sqlState.isBlank())
			return null;

		if (this.recordFullSqlState || sqlState.length() < 2)
			return sqlState;

		return sqlState.substring(0, 2);
	}

	@Nullable
	private static SQLException sqlExceptionFor(@Nullable Throwable throwable) {
		Throwable current = throwable;
		Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());

		while (current != null && visited.add(current)) {
			if (current instanceof SQLException sqlException)
				return sqlException;

			current = current.getCause();
		}

		return null;
	}

	@NonNull
	private static String errorType(@NonNull Throwable throwable) {
		requireNonNull(throwable);
		Throwable current = throwable;
		Throwable mostRelevant = throwable;
		Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());

		while (current != null && visited.add(current)) {
			if (current instanceof SQLException)
				return current.getClass().getName();
			mostRelevant = current;
			current = current.getCause();
		}

		return mostRelevant.getClass().getName();
	}

	private static double seconds(@NonNull Duration duration) {
		requireNonNull(duration);
		return duration.toNanos() / 1_000_000_000D;
	}

	/**
	 * Builder used to construct instances of {@link OpenTelemetryMetricsCollector}.
	 */
	@NotThreadSafe
	public static final class Builder {
		@Nullable
		private Meter meter;
		@Nullable
		private OpenTelemetry openTelemetry;
		@NonNull
		private String instrumentationName;
		@Nullable
		private String instrumentationVersion;
		@Nullable
		private String poolName;
		@Nullable
		private String namespace;
		@NonNull
		private Boolean recordCollectionName;
		@NonNull
		private Boolean recordFullSqlState;

		private Builder() {
			this.openTelemetry = null;
			this.instrumentationName = DEFAULT_INSTRUMENTATION_NAME;
			this.instrumentationVersion = packageImplementationVersion();
			this.poolName = null;
			this.namespace = null;
			this.recordCollectionName = false;
			this.recordFullSqlState = true;
		}

		/**
		 * Sets a specific meter to use for metric instruments.
		 *
		 * @param meter the meter to use
		 * @return this builder
		 */
		@NonNull
		public Builder meter(@NonNull Meter meter) {
			this.meter = requireNonNull(meter);
			return this;
		}

		/**
		 * Sets the OpenTelemetry API object used to construct a meter if {@link #meter(Meter)} is not set.
		 *
		 * @param openTelemetry the OpenTelemetry instance
		 * @return this builder
		 */
		@NonNull
		public Builder openTelemetry(@NonNull OpenTelemetry openTelemetry) {
			this.openTelemetry = requireNonNull(openTelemetry);
			return this;
		}

		/**
		 * Sets the instrumentation scope name to use when constructing a meter.
		 *
		 * @param instrumentationName the instrumentation scope name
		 * @return this builder
		 */
		@NonNull
		public Builder instrumentationName(@NonNull String instrumentationName) {
			this.instrumentationName = requireNonNull(instrumentationName);
			return this;
		}

		/**
		 * Sets an optional instrumentation scope version to use when constructing a meter.
		 *
		 * @param instrumentationVersion the instrumentation scope version, or {@code null}
		 * @return this builder
		 */
		@NonNull
		public Builder instrumentationVersion(@Nullable String instrumentationVersion) {
			this.instrumentationVersion = instrumentationVersion;
			return this;
		}

		/**
		 * Sets the database connection pool name to emit as {@code db.client.connection.pool.name}.
		 * <p>
		 * When unset or {@code null}, {@code db.client.connection.wait_time} and
		 * {@code db.client.connection.use_time} are not emitted, and notification metrics omit the pool-name attribute.
		 *
		 * @param poolName pool name, or {@code null}
		 * @return this builder
		 */
		@NonNull
		public Builder poolName(@Nullable String poolName) {
			this.poolName = poolName;
			return this;
		}

		/**
		 * Sets the logical database namespace to emit as {@code db.namespace}.
		 * <p>
		 * No JDBC metadata lookup is performed. When unset or {@code null}, the attribute is omitted.
		 *
		 * @param namespace logical database namespace, or {@code null}
		 * @return this builder
		 */
		@NonNull
		public Builder namespace(@Nullable String namespace) {
			this.namespace = namespace;
			return this;
		}

		/**
		 * Controls whether parsed collection/table names are emitted as {@code db.collection.name}.
		 * <p>
		 * The default is {@code false}. When enabled, the flag applies consistently to
		 * {@code db.client.operation.duration} and {@code db.client.response.returned_rows}.
		 *
		 * @param recordCollectionName whether to record collection/table names
		 * @return this builder
		 */
		@NonNull
		public Builder recordCollectionName(@NonNull Boolean recordCollectionName) {
			this.recordCollectionName = requireNonNull(recordCollectionName);
			return this;
		}

		/**
		 * Controls whether full SQLSTATE values are emitted as {@code db.response.status_code}.
		 * <p>
		 * The default is {@code true}. Set this to {@code false} to record only the two-character SQLSTATE class.
		 *
		 * @param recordFullSqlState whether to record full SQLSTATE values
		 * @return this builder
		 */
		@NonNull
		public Builder recordFullSqlState(@NonNull Boolean recordFullSqlState) {
			this.recordFullSqlState = requireNonNull(recordFullSqlState);
			return this;
		}

		@NonNull
		private Meter resolveMeter() {
			if (this.meter != null)
				return this.meter;

			OpenTelemetry openTelemetry = this.openTelemetry == null ? GlobalOpenTelemetry.getOrNoop() : this.openTelemetry;
			MeterBuilder meterBuilder = openTelemetry.meterBuilder(this.instrumentationName);

			if (this.instrumentationVersion != null)
				meterBuilder = meterBuilder.setInstrumentationVersion(this.instrumentationVersion);

			return meterBuilder.build();
		}

		/**
		 * Builds the collector.
		 *
		 * @return the collector instance
		 */
		@NonNull
		public OpenTelemetryMetricsCollector build() {
			return new OpenTelemetryMetricsCollector(this);
		}

		@Nullable
		private static String packageImplementationVersion() {
			Package implementationPackage = OpenTelemetryMetricsCollector.class.getPackage();
			return implementationPackage == null ? null : implementationPackage.getImplementationVersion();
		}
	}
}

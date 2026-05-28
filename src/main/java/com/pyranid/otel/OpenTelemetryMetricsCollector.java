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

import com.pyranid.DatabaseException;
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
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * OpenTelemetry-backed {@link MetricsCollector} for Pyranid database metrics.
 * <p>
 * Standard database-client metrics use OpenTelemetry semantic-convention names directly. Pyranid-specific lifecycle
 * events use {@code pyranid.*} metric and attribute names.
 * <p>
 * This implementation is thread-safe and expects the supplied {@link Meter} to be backed by a non-blocking exporter.
 * Pyranid core catches collector exceptions, but exporters that block can still stall the calling JDBC thread.
 *
 * @author <a href="https://www.revetkn.com">Mark Allen</a>
 * @since 4.2.0
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
	@NonNull
	private static final Pattern OPERATION_PATTERN;
	@NonNull
	private static final Pattern INSERT_COLLECTION_PATTERN;
	@NonNull
	private static final Pattern UPDATE_COLLECTION_PATTERN;
	@NonNull
	private static final Pattern DELETE_COLLECTION_PATTERN;
	@NonNull
	private static final Pattern SELECT_COLLECTION_PATTERN;

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

	static {
		DEFAULT_INSTRUMENTATION_NAME = "com.pyranid.otel";
		UNKNOWN_OPERATION = "UNKNOWN";
		OUTCOME_SUCCESS = "success";
		OUTCOME_FAILURE = "failure";
		OPERATION_PATTERN = Pattern.compile("^\\s*([A-Za-z]+)");
		INSERT_COLLECTION_PATTERN = Pattern.compile("(?is)^\\s*insert\\s+into\\s+([^\\s(]+)");
		UPDATE_COLLECTION_PATTERN = Pattern.compile("(?is)^\\s*update\\s+([^\\s(]+)");
		DELETE_COLLECTION_PATTERN = Pattern.compile("(?is)^\\s*delete\\s+from\\s+([^\\s(]+)");
		SELECT_COLLECTION_PATTERN = Pattern.compile("(?is)\\bfrom\\s+([^\\s,()]+)");

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
	private final LongCounter savepointOperationsCounter;
	@NonNull
	private final DoubleHistogram streamDurationHistogram;
	@NonNull
	private final LongHistogram streamRowsConsumedHistogram;
	@NonNull
	private final LongCounter postTransactionOperationsCounter;
	@Nullable
	private final String poolName;
	@Nullable
	private final String namespace;
	@NonNull
	private final Boolean recordCollectionName;
	@NonNull
	private final Boolean recordFullSqlState;

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

		this.operationDurationHistogram = meter.histogramBuilder("db.client.operation.duration")
				.setDescription("Duration of database client operations.")
				.setUnit("s")
				.build();
		this.returnedRowsHistogram = meter.histogramBuilder("db.client.response.returned_rows")
				.ofLongs()
				.setDescription("Number of rows returned by a database response.")
				.setUnit("{row}")
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
																									@NonNull Duration acquisitionDuration,
																									@NonNull Throwable throwable) {
		if (this.poolName == null)
			return;

		requireNonNull(ctx);
		recordConnectionWaitTime(ctx.getDatabaseType(), acquisitionDuration, requireNonNull(throwable));
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
		Attributes attributes = transactionClosureAttributes(databaseType, transaction.getTransactionIsolation(), outcome);
		this.transactionClosureDurationHistogram.record(seconds(logicalDuration), attributes);
		this.transactionCountCounter.add(1, attributes);
	}

	@Override
	public void didBeginPhysicalTransaction(@NonNull Transaction transaction,
																					@NonNull TransactionIsolation isolation,
																					@NonNull DatabaseType databaseType) {
		this.activeTransactionsCounter.add(1, databaseAttributes(databaseType));
	}

	@Override
	public void didFailToBeginPhysicalTransaction(@NonNull Transaction transaction,
																							 @NonNull TransactionIsolation isolation,
																							 @NonNull PhysicalTransactionBeginFailurePhase phase,
																							 @NonNull DatabaseType databaseType,
																							 @NonNull Throwable throwable) {
		Attributes attributes = Attributes.builder()
				.putAll(databaseAttributes(databaseType))
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
		this.activeTransactionsCounter.add(-1, databaseAttributes(databaseType));
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
		this.activeTransactionsCounter.add(-1, databaseAttributes(databaseType));
	}

	@Override
	public void didFailToRollbackPhysicalTransaction(@NonNull Transaction transaction,
																									 @NonNull DatabaseType databaseType,
																									 @NonNull Duration physicalDuration,
																									 @NonNull Throwable throwable) {
		this.transactionRollbackDurationHistogram.record(seconds(physicalDuration),
				transactionOperationAttributes(databaseType, transaction.getTransactionIsolation(), TRANSACTION_ROLLBACK_OUTCOME_ATTRIBUTE_KEY, OUTCOME_FAILURE, throwable));
		this.activeTransactionsCounter.add(-1, databaseAttributes(databaseType));
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

		Attributes attributes = statementAttributes(ctx, null);
		this.operationDurationHistogram.record(seconds(statementLog.getTotalDuration()), attributes);
		recordStatementComponentDurations(statementLog, attributes);

		statementLog.getBatchSize().ifPresent(batchSize -> this.statementBatchSizeHistogram.record(batchSize.longValue(), attributes));

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
																				@NonNull Throwable throwable) {
		requireNonNull(ctx);
		requireNonNull(statementLog);
		requireNonNull(throwable);

		Attributes attributes = statementAttributes(ctx, throwable);
		this.operationDurationHistogram.record(seconds(statementLog.getTotalDuration()), attributes);
		recordStatementComponentDurations(statementLog, attributes);
		this.statementErrorsCounter.add(1, attributes);
	}

	@Override
	public void didFailToOpenStream(@NonNull StatementContext<?> ctx,
																	@NonNull Duration openDuration,
																	@NonNull Throwable throwable) {
		requireNonNull(ctx);
		requireNonNull(throwable);

		Attributes attributes = streamAttributes(ctx.getDatabaseType(), StreamTerminalOutcome.OPEN_FAILURE);
		this.streamDurationHistogram.record(seconds(openDuration), attributes);
		this.streamRowsConsumedHistogram.record(0L, attributes);
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

		Attributes attributes = streamAttributes(ctx.getDatabaseType(), outcome);
		this.streamDurationHistogram.record(seconds(streamDuration), attributes);
		this.streamRowsConsumedHistogram.record(rowsConsumed, attributes);
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

		this.postTransactionOperationsCounter.add(1, builder.build());
	}

	@Override
	@NonNull
	public Optional<Snapshot> snapshot() {
		return Optional.empty();
	}

	@NonNull
	private Attributes statementAttributes(@NonNull StatementContext<?> ctx,
																				 @Nullable Throwable throwable) {
		requireNonNull(ctx);

		AttributesBuilder builder = Attributes.builder()
				.putAll(databaseAttributes(ctx.getDatabaseType()))
				.put(DB_OPERATION_NAME_ATTRIBUTE_KEY, operationNameFor(ctx));

		if (this.namespace != null)
			builder.put(DB_NAMESPACE_ATTRIBUTE_KEY, this.namespace);

		if (this.recordCollectionName) {
			String collectionName = collectionNameFor(ctx);
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
	private Attributes transactionClosureAttributes(@NonNull DatabaseType databaseType,
																									@NonNull TransactionIsolation isolation,
																									@NonNull TransactionClosureOutcome outcome) {
		return Attributes.builder()
				.putAll(databaseAttributes(databaseType))
				.put(TRANSACTION_ISOLATION_ATTRIBUTE_KEY, enumValue(isolation))
				.put(TRANSACTION_CLOSURE_OUTCOME_ATTRIBUTE_KEY, enumValue(outcome))
				.build();
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
																			@NonNull StreamTerminalOutcome outcome) {
		return Attributes.builder()
				.putAll(databaseAttributes(databaseType))
				.put(STREAM_OUTCOME_ATTRIBUTE_KEY, enumValue(outcome))
				.build();
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
	private static String dbSystemName(@NonNull DatabaseType databaseType) {
		requireNonNull(databaseType);
		return switch (databaseType) {
			case POSTGRESQL -> "postgresql";
			case ORACLE -> "oracle.db";
			case GENERIC -> "other_sql";
		};
	}

	@NonNull
	private static String operationNameFor(@NonNull StatementContext<?> ctx) {
		requireNonNull(ctx);
		Matcher matcher = OPERATION_PATTERN.matcher(ctx.getStatement().getSql());
		if (!matcher.find())
			return UNKNOWN_OPERATION;
		return matcher.group(1).toUpperCase(Locale.ROOT);
	}

	@Nullable
	private static String collectionNameFor(@NonNull StatementContext<?> ctx) {
		requireNonNull(ctx);
		String sql = ctx.getStatement().getSql();
		String operation = operationNameFor(ctx);
		Pattern pattern = switch (operation) {
			case "INSERT" -> INSERT_COLLECTION_PATTERN;
			case "UPDATE" -> UPDATE_COLLECTION_PATTERN;
			case "DELETE" -> DELETE_COLLECTION_PATTERN;
			case "SELECT", "WITH" -> SELECT_COLLECTION_PATTERN;
			default -> null;
		};

		if (pattern == null)
			return null;

		Matcher matcher = pattern.matcher(sql);
		if (!matcher.find())
			return null;

		return normalizeIdentifier(matcher.group(1));
	}

	@NonNull
	private static String normalizeIdentifier(@NonNull String identifier) {
		requireNonNull(identifier);
		String normalized = identifier.trim();
		while (normalized.endsWith(";") || normalized.endsWith(","))
			normalized = normalized.substring(0, normalized.length() - 1);

		if ((normalized.startsWith("\"") && normalized.endsWith("\""))
				|| (normalized.startsWith("`") && normalized.endsWith("`"))
				|| (normalized.startsWith("[") && normalized.endsWith("]")))
			normalized = normalized.substring(1, normalized.length() - 1);

		return normalized;
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

		while (current != null) {
			if (current instanceof SQLException sqlException)
				return sqlException;

			if (current instanceof DatabaseException && current.getCause() instanceof SQLException sqlException)
				return sqlException;

			current = current.getCause();
		}

		return null;
	}

	@NonNull
	private static String errorType(@NonNull Throwable throwable) {
		requireNonNull(throwable);
		return throwable.getClass().getName();
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
			this.openTelemetry = GlobalOpenTelemetry.get();
			this.instrumentationName = DEFAULT_INSTRUMENTATION_NAME;
			this.instrumentationVersion = packageImplementationVersion();
			this.poolName = null;
			this.namespace = null;
			this.recordCollectionName = false;
			this.recordFullSqlState = false;
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
		 * Sets the database connection pool name used for pool-name-gated connection metrics.
		 * <p>
		 * When unset or {@code null}, {@code db.client.connection.wait_time} and
		 * {@code db.client.connection.use_time} are not emitted.
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
		 * The default is {@code false}, which records only the two-character SQLSTATE class.
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

			MeterBuilder meterBuilder = requireNonNull(this.openTelemetry).meterBuilder(this.instrumentationName);

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

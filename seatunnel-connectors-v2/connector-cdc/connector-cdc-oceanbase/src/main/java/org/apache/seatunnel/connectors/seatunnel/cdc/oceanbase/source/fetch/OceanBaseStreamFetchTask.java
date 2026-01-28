/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source.fetch;

import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source.offset.OceanBaseOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.utils.OceanBaseUtils;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import com.oceanbase.clogproxy.client.LogProxyClient;
import com.oceanbase.clogproxy.client.listener.RecordListener;
import com.oceanbase.oms.logmessage.LogMessage;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.data.Envelope;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import io.debezium.util.SchemaNameAdjuster;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/** OceanBase stream fetch task for incremental reading via LogProxy. */
@Slf4j
public class OceanBaseStreamFetchTask implements FetchTask<SourceSplitBase> {

    private static final SchemaNameAdjuster SCHEMA_NAME_ADJUSTER = SchemaNameAdjuster.create();

    // Debezium source schema for OceanBase
    private static final Schema SOURCE_SCHEMA =
            SchemaBuilder.struct()
                    .name(
                            SCHEMA_NAME_ADJUSTER.adjust(
                                    "org.apache.seatunnel.connectors.oceanbase.Source"))
                    .field("version", Schema.STRING_SCHEMA)
                    .field("connector", Schema.STRING_SCHEMA)
                    .field("snapshot", Schema.STRING_SCHEMA)
                    .field("db", Schema.STRING_SCHEMA)
                    .field("table", Schema.STRING_SCHEMA)
                    .field("ts_ms", Schema.INT64_SCHEMA)
                    .field("checkpoint", Schema.OPTIONAL_STRING_SCHEMA)
                    .build();

    /**
     * Create Debezium envelope schema with dynamic before/after schemas
     *
     * @param rowSchema Schema for the row data (before/after fields)
     * @return Envelope schema
     */
    private static Schema createEnvelopeSchema(Schema rowSchema) {
        return SchemaBuilder.struct()
                .name(
                        SCHEMA_NAME_ADJUSTER.adjust(
                                "org.apache.seatunnel.connectors.oceanbase.Envelope"))
                .field(Envelope.FieldName.OPERATION, Schema.STRING_SCHEMA)
                .field(Envelope.FieldName.SOURCE, SOURCE_SCHEMA)
                .field(Envelope.FieldName.BEFORE, rowSchema)
                .field(Envelope.FieldName.AFTER, rowSchema)
                .field(Envelope.FieldName.TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA)
                .build();
    }

    private final IncrementalSplit incrementalSplit;
    private volatile boolean taskRunning = false;
    private LogProxyClient logProxyClient;

    public OceanBaseStreamFetchTask(IncrementalSplit incrementalSplit) {
        this.incrementalSplit = incrementalSplit;
    }

    @Override
    public void execute(Context context) throws Exception {
        OceanBaseFetchTaskContext taskContext = (OceanBaseFetchTaskContext) context;
        ChangeEventQueue<DataChangeEvent> changeEventQueue = taskContext.getQueue();

        taskRunning = true;

        // IncrementalSplit has getTableIds() that returns List<TableId>
        List<TableId> tableIds = incrementalSplit.getTableIds();
        if (tableIds == null || tableIds.isEmpty()) {
            log.error("No table IDs found in incremental split");
            return;
        }

        // Support multiple tables in one split
        log.info("Starting stream reading for {} tables", tableIds.size());
        for (TableId tableId : tableIds) {
            log.info("  - Table: {}", tableId);
        }

        OceanBaseOffset startOffset = (OceanBaseOffset) incrementalSplit.getStartupOffset();
        OceanBaseOffset stopOffset = (OceanBaseOffset) incrementalSplit.getStopOffset();

        log.info("Starting stream reading from {} to {}", startOffset, stopOffset);

        try {
            startLogProxyClient(taskContext, startOffset, tableIds, changeEventQueue);
        } catch (Exception e) {
            log.error(
                    "Execute stream read subtask for OceanBase split {} fail", incrementalSplit, e);
            throw e;
        } finally {
            taskRunning = false;
        }
    }

    /** Start LogProxy client */
    private void startLogProxyClient(
            OceanBaseFetchTaskContext taskContext,
            OceanBaseOffset startOffset,
            List<TableId> tableIds,
            ChangeEventQueue<DataChangeEvent> changeEventQueue)
            throws Exception {

        OceanBaseSourceConfig sourceConfig = taskContext.getSourceConfig();
        OceanBaseOffset stopOffset = (OceanBaseOffset) incrementalSplit.getStopOffset();

        try {
            // Get or create LogProxy client from context
            logProxyClient = OceanBaseUtils.createLogProxyClient(sourceConfig, startOffset);

            // Add record listener to handle incoming log messages
            logProxyClient.addListener(
                    new RecordListener() {
                        private final List<LogMessage> logMessageList = new LinkedList<>();

                        private void commitLogMessage() throws InterruptedException {
                            if (logMessageList.isEmpty()) {
                                return;
                            }

                            // Process each log message in the transaction batch
                            for (LogMessage logMessage : logMessageList) {
                                // Match table ID from LogMessage against split's table list
                                TableId matchedTableId = matchTableId(logMessage, tableIds);

                                if (matchedTableId == null) {
                                    // Skip messages for tables not in this split
                                    continue;
                                }

                                // Extract offset from log message
                                OceanBaseOffset currentOffset =
                                        new OceanBaseOffset(
                                                logMessage.getFileNameOffset(),
                                                String.valueOf(logMessage.getFileNameOffset()));

                                // Check if we've reached the stop offset
                                if (stopOffset != null
                                        && currentOffset.getTimestamp()
                                                >= stopOffset.getTimestamp()) {
                                    log.info("Reached stop offset {}", stopOffset);
                                    shutdown();
                                    logMessageList.clear();
                                    return;
                                }

                                // Convert LogMessage to SourceRecord
                                SourceRecord sourceRecord =
                                        convertLogMessageToSourceRecord(
                                                sourceConfig, matchedTableId, logMessage);
                                if (sourceRecord != null) {
                                    // Enqueue the change event
                                    changeEventQueue.enqueue(new DataChangeEvent(sourceRecord));
                                }
                            }
                            logMessageList.clear();
                        }

                        @Override
                        public void notify(LogMessage logMessage) {
                            try {
                                switch (logMessage.getOpt()) {
                                    case HEARTBEAT:
                                    case BEGIN:
                                    case DDL:
                                        break;
                                    case INSERT:
                                    case UPDATE:
                                    case DELETE:
                                        logMessageList.add(logMessage);
                                        break;
                                    case COMMIT:
                                        commitLogMessage();
                                        break;
                                    default:
                                        throw new UnsupportedOperationException(
                                                "Unsupported type: " + logMessage.getOpt());
                                }
                            } catch (Exception e) {
                                log.error("Failed to process LogMessage", e);
                            }
                        }

                        @Override
                        public void onException(
                                com.oceanbase.clogproxy.client.exception.LogProxyClientException
                                        e) {
                            log.error("LogProxy client exception occurred", e);
                            // Classify exception types for better error handling
                            String errorMsg = e.getMessage();
                            if (errorMsg != null) {
                                if (errorMsg.contains("connect")
                                        || errorMsg.contains("connection")) {
                                    log.error(
                                            "Connection error: Check LogProxy host/port and network connectivity");
                                } else if (errorMsg.contains("auth")
                                        || errorMsg.contains("permission")) {
                                    log.error(
                                            "Authentication error: Check username/password and tenant configuration");
                                } else if (errorMsg.contains("timeout")) {
                                    log.error(
                                            "Timeout error: Consider increasing connect.timeout.ms configuration");
                                }
                            }
                            // Shutdown task on critical exception
                            shutdown();
                        }
                    });

            // Start the LogProxy client
            logProxyClient.start();

            log.info("LogProxy client started successfully for split {}", incrementalSplit);

            logProxyClient.join();

            // Emit END watermark for all tables
            log.info("Stream reading completed for split {}", incrementalSplit);
            for (TableId tableId : tableIds) {
                changeEventQueue.enqueue(
                        new DataChangeEvent(
                                WatermarkEvent.create(
                                        createWatermarkPartitionMap(tableId.identifier()),
                                        "__oceanbase_watermarks",
                                        incrementalSplit.splitId(),
                                        WatermarkKind.END,
                                        stopOffset)));
            }

        } catch (Exception e) {
            log.error("Error during LogProxy client execution", e);
            throw e;
        } finally {
            shutdown();
        }
    }

    /** Convert LogMessage to SourceRecord with complete Debezium envelope */
    private SourceRecord convertLogMessageToSourceRecord(
            OceanBaseSourceConfig sourceConfig, TableId tableId, LogMessage logMessage) {

        try {
            // Parse LogMessage into OceanBaseCDCRecord to properly separate before/after values
            OceanBaseCDCRecord cdcRecord = new OceanBaseCDCRecord(logMessage);
            String operation = cdcRecord.getType().toString();

            // Build partition map (source partition)
            Map<String, String> partitionMap = new HashMap<>();
            partitionMap.put("database", cdcRecord.getDatabase());
            partitionMap.put("table", cdcRecord.getTable());

            // Build offset map
            Map<String, Object> offsetMap = new HashMap<>();
            offsetMap.put("timestamp", logMessage.getTimestamp());
            offsetMap.put("checkpoint", logMessage.getCheckpoint());
            if (logMessage.getSafeTimestamp() != null) {
                offsetMap.put("safe_timestamp", logMessage.getSafeTimestamp());
            }

            // Build Debezium source metadata as Struct
            Long timestampMs = cdcRecord.getTimestamp() * 1000; // Convert seconds to milliseconds
            Struct sourceStruct =
                    new Struct(SOURCE_SCHEMA)
                            .put("version", "1.0.0")
                            .put("connector", "oceanbase")
                            .put("snapshot", "false")
                            .put("db", cdcRecord.getDatabase())
                            .put("table", cdcRecord.getTable())
                            .put("ts_ms", timestampMs)
                            .put(
                                    "checkpoint",
                                    logMessage.getCheckpoint() != null
                                            ? logMessage.getCheckpoint()
                                            : null);

            // Extract field data using OceanBaseCDCRecord which correctly separates before/after
            Map<String, Object> beforeData =
                    OceanBaseCDCRecord.toValueMap(cdcRecord.getFieldsBefore());
            Map<String, Object> afterData =
                    OceanBaseCDCRecord.toValueMap(cdcRecord.getFieldsAfter());

            // Build key (primary key fields) - keep as Map for schemaless
            Map<String, Object> key = new HashMap<>();
            // Use after data for key, fallback to before for DELETE
            if (afterData != null && !afterData.isEmpty()) {
                key.putAll(afterData);
            } else if (beforeData != null && !beforeData.isEmpty()) {
                key.putAll(beforeData);
            }

            // Build row schema from OceanBaseCDCRecord fields
            Schema rowSchema = createRowSchema(tableId, cdcRecord);

            // Map operation type to Debezium operation and build before/after Structs
            String debeziumOp;
            Struct beforeStruct = null;
            Struct afterStruct = null;

            switch (operation.toUpperCase()) {
                case "INSERT":
                    debeziumOp = Envelope.Operation.CREATE.code();
                    afterStruct = createRowStruct(rowSchema, afterData);
                    break;
                case "UPDATE":
                    debeziumOp = Envelope.Operation.UPDATE.code();
                    beforeStruct = createRowStruct(rowSchema, beforeData);
                    afterStruct = createRowStruct(rowSchema, afterData);
                    break;
                case "DELETE":
                    debeziumOp = Envelope.Operation.DELETE.code();
                    beforeStruct = createRowStruct(rowSchema, beforeData);
                    break;
                case "BEGIN":
                case "COMMIT":
                case "HEARTBEAT":
                    // Skip transaction control events
                    return null;
                default:
                    log.warn("Unknown operation type: {}", operation);
                    return null;
            }

            // Build Debezium envelope as Struct
            Schema envelopeSchema = createEnvelopeSchema(rowSchema);
            Struct envelope =
                    new Struct(envelopeSchema)
                            .put(Envelope.FieldName.OPERATION, debeziumOp)
                            .put(Envelope.FieldName.SOURCE, sourceStruct)
                            .put(Envelope.FieldName.BEFORE, beforeStruct)
                            .put(Envelope.FieldName.AFTER, afterStruct)
                            .put(Envelope.FieldName.TIMESTAMP, timestampMs);

            // Create SourceRecord with Schema and Struct
            return new SourceRecord(
                    partitionMap,
                    offsetMap,
                    tableId.toString(), // topic
                    null, // key schema (schemaless mode)
                    key,
                    envelopeSchema, // value schema
                    envelope); // value as Struct

        } catch (Exception e) {
            log.warn("Failed to convert LogMessage to SourceRecord for table {}", tableId, e);
            return null;
        }
    }

    /**
     * Create row schema from OceanBaseCDCRecord fields All fields are treated as optional strings
     * for simplicity
     *
     * @param tableId Table identifier
     * @param cdcRecord OceanBaseCDCRecord containing field definitions
     * @return Row schema
     */
    private Schema createRowSchema(TableId tableId, OceanBaseCDCRecord cdcRecord) {
        SchemaBuilder builder =
                SchemaBuilder.struct()
                        .optional()
                        .name(
                                SCHEMA_NAME_ADJUSTER.adjust(
                                        "org.apache.seatunnel.connectors.oceanbase.Data."
                                                + tableId.table()));

        // Collect all unique field names from both before and after maps
        // No need to worry about duplicates as we use Map keys
        java.util.Set<String> fieldNames = new java.util.HashSet<>();
        fieldNames.addAll(cdcRecord.getFieldsBefore().keySet());
        fieldNames.addAll(cdcRecord.getFieldsAfter().keySet());

        // Add all fields to schema
        for (String fieldName : fieldNames) {
            // All fields are optional strings for now
            // A more complete implementation would map field types properly
            builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
        }

        return builder.build();
    }

    /**
     * Create Struct from row data
     *
     * @param schema Row schema
     * @param data Row data
     * @return Struct representing the row, or null if data is empty
     */
    private Struct createRowStruct(Schema schema, Map<String, Object> data) {
        if (data == null || data.isEmpty()) {
            return null;
        }

        Struct struct = new Struct(schema);
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            // Set field value if it exists in schema
            if (schema.field(fieldName) != null) {
                // Convert value to string as per schema definition
                String stringValue = value != null ? value.toString() : null;
                struct.put(fieldName, stringValue);
            }
        }
        return struct;
    }

    /**
     * Match table ID from LogMessage against split's table list
     *
     * @param logMessage LogMessage to match
     * @param tableIds List of table IDs in this split
     * @return Matched TableId, or null if no match
     */
    private TableId matchTableId(LogMessage logMessage, List<TableId> tableIds) {
        if (logMessage == null || tableIds == null || tableIds.isEmpty()) {
            return null;
        }

        String dbName = OceanBaseUtils.extractOceanBaseDbName(logMessage.getDbName());
        String tableName = logMessage.getTableName();

        if (dbName == null || tableName == null) {
            return null;
        }

        // Match against split's table list
        for (TableId tableId : tableIds) {
            // OceanBase uses catalog for database name
            if (dbName.equals(tableId.catalog()) && tableName.equals(tableId.table())) {
                return tableId;
            }
        }

        // No match found - this message is not for tables in this split
        return null;
    }

    /** Create watermark partition map */
    private Map<String, String> createWatermarkPartitionMap(String tableIdentifier) {
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put("table", tableIdentifier);
        return partitionMap;
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public void shutdown() {
        // Cleanup LogProxyClient
        if (logProxyClient != null) {
            try {
                log.info("Stopping LogProxyClient");
                logProxyClient.stop();
                logProxyClient = null;
            } catch (Exception e) {
                log.warn("Failed to stop LogProxy client", e);
            }
        }
        taskRunning = false;
    }

    @Override
    public SourceSplitBase getSplit() {
        return incrementalSplit;
    }
}

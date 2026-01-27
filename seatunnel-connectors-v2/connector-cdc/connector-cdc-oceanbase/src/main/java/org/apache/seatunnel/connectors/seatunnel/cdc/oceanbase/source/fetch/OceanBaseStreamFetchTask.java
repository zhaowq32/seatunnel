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

import org.apache.kafka.connect.source.SourceRecord;

import com.oceanbase.clogproxy.client.LogProxyClient;
import com.oceanbase.clogproxy.client.listener.RecordListener;
import com.oceanbase.oms.logmessage.DataMessage;
import com.oceanbase.oms.logmessage.LogMessage;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** OceanBase stream fetch task for incremental reading via LogProxy. */
@Slf4j
public class OceanBaseStreamFetchTask implements FetchTask<SourceSplitBase> {

    private final IncrementalSplit incrementalSplit;
    private volatile boolean taskRunning = false;
    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_RETRY_DELAY_MS = 1000L;

    public OceanBaseStreamFetchTask(IncrementalSplit incrementalSplit) {
        this.incrementalSplit = incrementalSplit;
    }

    @Override
    public void execute(Context context) throws Exception {
        OceanBaseFetchTaskContext taskContext = (OceanBaseFetchTaskContext) context;
        OceanBaseSourceConfig sourceConfig = taskContext.getSourceConfig();
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
            shutdown(taskContext);
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
            LogProxyClient logProxyClient = taskContext.createLogProxyClient(startOffset);

            // Add record listener to handle incoming log messages
            logProxyClient.addListener(
                    new RecordListener() {
                        @Override
                        public void notify(LogMessage logMessage) {
                            try {
                                // Check if the message is for any of the current tables
                                TableId matchedTableId =
                                        findMatchedTable(
                                                logMessage.getDbName(),
                                                logMessage.getTableName(),
                                                tableIds);

                                if (matchedTableId == null) {
                                    // Message is not for any table in this split
                                    return;
                                }

                                // Extract offset from log message
                                OceanBaseOffset currentOffset =
                                        extractOffsetFromLogMessage(logMessage);

                                // Check if we've reached the stop offset
                                if (stopOffset != null
                                        && currentOffset.getTimestamp()
                                                >= stopOffset.getTimestamp()) {
                                    log.info("Reached stop offset {}", stopOffset);
                                    shutdown(taskContext);
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
                            shutdown(taskContext);
                        }
                    });

            // Start the LogProxy client
            logProxyClient.start();

            log.info("LogProxy client started successfully for split {}", incrementalSplit);

            // Wait for the client to run with efficient waiting mechanism
            synchronized (this) {
                while (taskRunning) {
                    try {
                        // Wait indefinitely until notified
                        this.wait();
                    } catch (InterruptedException e) {
                        log.info("Stream reading interrupted for split {}", incrementalSplit);
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }

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
        }
    }

    /** Convert LogMessage to SourceRecord with complete Debezium envelope */
    private SourceRecord convertLogMessageToSourceRecord(
            OceanBaseSourceConfig sourceConfig, TableId tableId, LogMessage logMessage) {

        try {
            // Filter by table if needed
            if (!isTableMatched(logMessage.getDbName(), logMessage.getTableName(), tableId)) {
                return null;
            }

            String operation =
                    logMessage.getOpt() != null ? logMessage.getOpt().toString() : "UNKNOWN";

            // Build partition map (source partition)
            Map<String, String> partitionMap = new HashMap<>();
            partitionMap.put("server", sourceConfig.getLogproxyHost());
            partitionMap.put("database", logMessage.getDbName());
            partitionMap.put("table", logMessage.getTableName());

            // Build offset map
            Map<String, Object> offsetMap = new HashMap<>();
            offsetMap.put("timestamp", logMessage.getTimestamp());
            offsetMap.put("checkpoint", logMessage.getCheckpoint());
            if (logMessage.getSafeTimestamp() != null) {
                offsetMap.put("safe_timestamp", logMessage.getSafeTimestamp());
            }

            // Parse timestamp
            long timestampMs;
            try {
                long timestamp = Long.parseLong(logMessage.getTimestamp());
                timestampMs = timestamp * 1000L; // Convert seconds to milliseconds
            } catch (NumberFormatException e) {
                log.warn(
                        "Failed to parse timestamp: {}, using current time",
                        logMessage.getTimestamp());
                timestampMs = System.currentTimeMillis();
            }

            // Build Debezium source metadata
            Map<String, Object> sourceMetadata = new HashMap<>();
            sourceMetadata.put("version", "1.0.0");
            sourceMetadata.put("connector", "oceanbase");
            sourceMetadata.put("name", sourceConfig.getTenantName());
            sourceMetadata.put("ts_ms", timestampMs);
            sourceMetadata.put("snapshot", "false");
            sourceMetadata.put("db", logMessage.getDbName());
            sourceMetadata.put("table", logMessage.getTableName());
            sourceMetadata.put("server_id", sourceConfig.getLogproxyHost());
            if (logMessage.getCheckpoint() != null) {
                sourceMetadata.put("checkpoint", logMessage.getCheckpoint());
            }

            // Extract field data from LogMessage
            Map<String, Object> beforeData = extractFieldDataFromLogMessage(logMessage, true);
            Map<String, Object> afterData = extractFieldDataFromLogMessage(logMessage, false);

            // Build key (primary key fields)
            Map<String, Object> key = new HashMap<>();
            // Use after data for key, fallback to before for DELETE
            if (afterData != null && !afterData.isEmpty()) {
                key.putAll(afterData);
            } else if (beforeData != null && !beforeData.isEmpty()) {
                key.putAll(beforeData);
            }

            // Build Debezium envelope
            Map<String, Object> envelope = new HashMap<>();

            // Map operation type to Debezium operation
            String debeziumOp;
            switch (operation.toUpperCase()) {
                case "INSERT":
                    debeziumOp = "c"; // create
                    envelope.put("before", null);
                    envelope.put("after", afterData);
                    break;
                case "UPDATE":
                    debeziumOp = "u"; // update
                    envelope.put("before", beforeData);
                    envelope.put("after", afterData);
                    break;
                case "DELETE":
                    debeziumOp = "d"; // delete
                    envelope.put("before", beforeData);
                    envelope.put("after", null);
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

            envelope.put("op", debeziumOp);
            envelope.put("source", sourceMetadata);
            envelope.put("ts_ms", timestampMs);

            // Create SourceRecord with complete Debezium format
            return new SourceRecord(
                    partitionMap,
                    offsetMap,
                    tableId.toString(), // topic
                    null, // key schema (schemaless mode)
                    key,
                    null, // value schema (schemaless mode)
                    envelope);

        } catch (Exception e) {
            log.warn("Failed to convert LogMessage to SourceRecord for table {}", tableId, e);
            return null;
        }
    }

    /**
     * Extract field data from LogMessage
     *
     * @param logMessage LogMessage to extract data from
     * @param extractBefore true to extract before image (old values), false for after image
     * @return Map of field name to value
     */
    private Map<String, Object> extractFieldDataFromLogMessage(
            LogMessage logMessage, boolean extractBefore) {
        if (logMessage == null) {
            return null;
        }

        try {
            Map<String, Object> data = new HashMap<>();

            // Get field list from LogMessage
            java.util.List<com.oceanbase.oms.logmessage.DataMessage.Record.Field> fields =
                    logMessage.getFieldList();

            if (fields == null || fields.isEmpty()) {
                return null;
            }

            // Extract field values
            for (com.oceanbase.oms.logmessage.DataMessage.Record.Field field : fields) {
                if (field != null) {
                    String fieldName = field.getFieldname();
                    // For UPDATE operations, before image might be in old values
                    // This is a simplified approach - actual implementation might need
                    // to check field.isPrev() or similar method
                    Object fieldValue = parseFieldValueFromLogMessage(field);
                    data.put(fieldName, fieldValue);
                }
            }

            return data;
        } catch (Exception e) {
            log.warn("Failed to extract field data from LogMessage", e);
            return null;
        }
    }

    /**
     * Parse field value from LogMessage Field Note: This is a simplified implementation that treats
     * all values as strings A complete implementation should handle type conversions based on field
     * metadata
     */
    private Object parseFieldValueFromLogMessage(
            com.oceanbase.oms.logmessage.DataMessage.Record.Field field) {
        if (field == null) {
            return null;
        }

        try {
            // Get the string value from the field
            // The actual field API might be different, adjust as needed
            Object value = field.getValue();

            if (value == null) {
                return null;
            }

            // For now, return as string - a complete implementation would
            // convert to proper types based on field.getType() or similar metadata
            return value.toString();
        } catch (Exception e) {
            log.warn("Failed to parse field value", e);
            return null;
        }
    }

    /**
     * Extract offset from LogMessage
     *
     * <p>For OceanBase, the safe checkpoint is determined as follows: - Heartbeat type: Use
     * timestamp field as the safe checkpoint (in seconds) - Other types (DDL, DML): Use
     * fileNameOffset field as the safe checkpoint
     *
     * <p>This is because libobcdc doesn't guarantee strict time ordering for data changes, so for
     * DDL/DML types, fileNameOffset (which contains the most recent heartbeat timestamp) should be
     * used as the safe checkpoint.
     */
    private OceanBaseOffset extractOffsetFromLogMessage(LogMessage logMessage) {
        long checkpoint;
        if (DataMessage.Record.Type.HEARTBEAT.equals(logMessage.getOpt())) {
            checkpoint = Long.parseLong(logMessage.getTimestamp());
        } else {
            checkpoint = logMessage.getFileNameOffset();
        }
        return new OceanBaseOffset(checkpoint, String.valueOf(checkpoint));
    }

    /** Check if table matches the split */
    private boolean isTableMatched(String database, String table, TableId targetTableId) {
        if (database == null || table == null || targetTableId == null) {
            return false;
        }
        // In OceanBase, use schema() instead of catalog()
        String targetDatabase =
                targetTableId.schema() != null ? targetTableId.schema() : targetTableId.catalog();
        return database.equals(targetDatabase) && table.equals(targetTableId.table());
    }

    /**
     * Find the matched table from a list of tables
     *
     * @param database Database name from LogMessage
     * @param table Table name from LogMessage
     * @param tableIds List of TableId to match against
     * @return Matched TableId or null if no match found
     */
    private TableId findMatchedTable(String database, String table, List<TableId> tableIds) {
        if (database == null || table == null || tableIds == null || tableIds.isEmpty()) {
            return null;
        }

        for (TableId tableId : tableIds) {
            if (isTableMatched(database, table, tableId)) {
                return tableId;
            }
        }

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
        synchronized (this) {
            taskRunning = false;
            // Notify waiting thread to exit
            this.notifyAll();
        }
    }

    @Override
    public SourceSplitBase getSplit() {
        return incrementalSplit;
    }

    /**
     * Shutdown the fetch task
     *
     * @param taskContext Context to clean up, can be null
     */
    private void shutdown(OceanBaseFetchTaskContext taskContext) {
        synchronized (this) {
            taskRunning = false;
            // Notify waiting thread to exit
            this.notifyAll();
        }
        // LogProxyClient is now managed by taskContext
        // Do not stop it here directly
        log.info("OceanBaseStreamFetchTask shutdown called");
    }
}

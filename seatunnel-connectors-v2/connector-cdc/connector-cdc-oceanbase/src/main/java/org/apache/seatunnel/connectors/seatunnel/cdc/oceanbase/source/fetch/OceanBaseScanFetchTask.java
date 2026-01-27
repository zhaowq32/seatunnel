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
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source.dialect.OceanBaseDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source.offset.OceanBaseOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.utils.OceanBaseUtils;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * OceanBase scan fetch task for snapshot reading. This follows the MySQL CDC pattern: low watermark
 * -> snapshot -> high watermark -> backfill
 */
@Slf4j
public class OceanBaseScanFetchTask implements FetchTask<SourceSplitBase> {

    private final SnapshotSplit snapshotSplit;
    private volatile boolean taskRunning = false;
    private List<String> primaryKeys;
    private Connection jdbcConnection;

    public OceanBaseScanFetchTask(SnapshotSplit snapshotSplit) {
        this.snapshotSplit = snapshotSplit;
    }

    @Override
    public void execute(Context context) throws Exception {
        OceanBaseFetchTaskContext taskContext = (OceanBaseFetchTaskContext) context;
        OceanBaseSourceConfig sourceConfig = taskContext.getSourceConfig();
        OceanBaseDialect dialect = taskContext.getDialect();
        ChangeEventQueue<DataChangeEvent> changeEventQueue = taskContext.getQueue();

        taskRunning = true;
        TableId tableId = snapshotSplit.getTableId();

        try {
            // Create connection with retry
            jdbcConnection = OceanBaseUtils.createJdbcConnectionWithRetry(sourceConfig, 3);

            // Get primary keys for the table
            primaryKeys = OceanBaseUtils.getPrimaryKeys(jdbcConnection, tableId);
            if (primaryKeys.isEmpty()) {
                log.warn(
                        "Table {} has no primary keys, CDC operations may not work correctly",
                        tableId);
            }

            // Step 1: Determine low watermark (timestamp before snapshot starts)
            final OceanBaseOffset lowWatermark = dialect.displayCurrentOffset();
            log.info(
                    "Snapshot step 1 - Determining low watermark {} for split {}",
                    lowWatermark,
                    snapshotSplit);

            changeEventQueue.enqueue(
                    new DataChangeEvent(
                            WatermarkEvent.create(
                                    createWatermarkPartitionMap(tableId.identifier()),
                                    "__oceanbase_watermarks",
                                    snapshotSplit.splitId(),
                                    WatermarkKind.LOW,
                                    lowWatermark)));

            // Step 2: Snapshot data reading
            log.info("Snapshot step 2 - Snapshotting data for table {}", tableId);
            readSnapshotData(taskContext, tableId, changeEventQueue);

            // Step 3: Determine high watermark (timestamp after snapshot completes)
            OceanBaseOffset highWatermark = dialect.displayCurrentOffset();
            log.info(
                    "Snapshot step 3 - Determining high watermark {} for split {}",
                    highWatermark,
                    snapshotSplit);

            changeEventQueue.enqueue(
                    new DataChangeEvent(
                            WatermarkEvent.create(
                                    createWatermarkPartitionMap(tableId.identifier()),
                                    "__oceanbase_watermarks",
                                    snapshotSplit.splitId(),
                                    WatermarkKind.HIGH,
                                    highWatermark)));

            // Step 4: Backfill incremental changes that occurred during snapshot
            log.info(
                    "Snapshot step 4 - Back fill stream split for snapshot split {}",
                    snapshotSplit);

            final IncrementalSplit dataBackfillSplit =
                    createBackfillStreamSplit(lowWatermark, highWatermark, tableId);

            final boolean streamBackfillRequired =
                    dataBackfillSplit.getStopOffset().isAfter(dataBackfillSplit.getStartupOffset());

            if (!streamBackfillRequired) {
                log.info("No backfill required for split {}", snapshotSplit);
                changeEventQueue.enqueue(
                        new DataChangeEvent(
                                WatermarkEvent.create(
                                        createWatermarkPartitionMap(tableId.identifier()),
                                        "__oceanbase_watermarks",
                                        dataBackfillSplit.splitId(),
                                        WatermarkKind.END,
                                        dataBackfillSplit.getStopOffset())));
            } else {
                log.info("Executing backfill for split {}", snapshotSplit);
                OceanBaseStreamFetchTask dataBackfillTask =
                        new OceanBaseStreamFetchTask(dataBackfillSplit);
                dataBackfillTask.execute(taskContext);
            }

        } catch (Exception e) {
            log.error(
                    "Execute snapshot read subtask for OceanBase split {} fail", snapshotSplit, e);
            throw e;
        } finally {
            taskRunning = false;
            OceanBaseUtils.closeConnection(jdbcConnection);
        }
    }

    /** Read snapshot data from OceanBase via JDBC */
    private void readSnapshotData(
            OceanBaseFetchTaskContext taskContext,
            TableId tableId,
            ChangeEventQueue<DataChangeEvent> changeEventQueue)
            throws Exception {

        OceanBaseSourceConfig sourceConfig = taskContext.getSourceConfig();

        // Reuse the connection created in execute()
        try (java.sql.PreparedStatement stmt =
                        prepareSnapshotStatement(jdbcConnection, sourceConfig, tableId);
                java.sql.ResultSet rs = stmt.executeQuery()) {

            int recordCount = 0;
            while (rs.next() && taskRunning) {
                checkTaskRunning();

                try {
                    // Build source record from result set
                    SourceRecord record = buildSnapshotRecord(sourceConfig, tableId, rs);
                    if (record != null) {
                        changeEventQueue.enqueue(new DataChangeEvent(record));
                        recordCount++;

                        if (recordCount % 1000 == 0) {
                            log.debug(
                                    "Read {} records from table {} chunk [{}, {}]",
                                    recordCount,
                                    tableId,
                                    snapshotSplit.getSplitStart(),
                                    snapshotSplit.getSplitEnd());
                        }
                    }
                } catch (Exception e) {
                    log.error("Failed to process row from table {}", tableId, e);
                    // Continue to process next row instead of failing entire snapshot
                }
            }

            log.info(
                    "Snapshot completed for table {} chunk [{}, {}], total records: {}",
                    tableId,
                    snapshotSplit.getSplitStart(),
                    snapshotSplit.getSplitEnd(),
                    recordCount);
        }
    }

    /** Prepare snapshot statement with chunk boundaries */
    private java.sql.PreparedStatement prepareSnapshotStatement(
            java.sql.Connection connection, OceanBaseSourceConfig sourceConfig, TableId tableId)
            throws Exception {

        String tableName = String.format("%s.%s", tableId.catalog(), tableId.table());

        Object[] splitStart = snapshotSplit.getSplitStart();
        Object[] splitEnd = snapshotSplit.getSplitEnd();
        org.apache.seatunnel.api.table.type.SeaTunnelRowType splitKeyType =
                snapshotSplit.getSplitKeyType();

        log.info(
                "Initializing snapshot split processing: TableId={}, StartKey={}, EndKey={}",
                tableId,
                splitStart,
                splitEnd);

        String sql = "SELECT * FROM " + tableName;
        java.sql.PreparedStatement stmt = null;

        if (splitKeyType != null && splitKeyType.getFieldNames().length > 0) {
            String splitKey = splitKeyType.getFieldNames()[0];
            StringBuilder whereClause = new StringBuilder();
            int paramIndex = 1;

            if (splitStart != null && splitStart.length > 0 && splitStart[0] != null) {
                whereClause.append(splitKey).append(" >= ?");
            }

            if (splitEnd != null && splitEnd.length > 0 && splitEnd[0] != null) {
                if (whereClause.length() > 0) {
                    whereClause.append(" AND ");
                }
                whereClause.append(splitKey).append(" < ?");
            }

            if (whereClause.length() > 0) {
                sql += " WHERE " + whereClause.toString();
            }

            // Add order by to ensure consistent chunking
            sql += " ORDER BY " + splitKey;

            stmt = connection.prepareStatement(sql);

            if (splitStart != null && splitStart.length > 0 && splitStart[0] != null) {
                stmt.setObject(paramIndex++, splitStart[0]);
            }

            if (splitEnd != null && splitEnd.length > 0 && splitEnd[0] != null) {
                stmt.setObject(paramIndex++, splitEnd[0]);
            }
        } else {
            stmt = connection.prepareStatement(sql);
        }

        // Set batch size for better performance
        stmt.setFetchSize(sourceConfig.getBatchSize());

        return stmt;
    }

    /** Build snapshot source record */
    private SourceRecord buildSnapshotRecord(
            OceanBaseSourceConfig sourceConfig, TableId tableId, java.sql.ResultSet rs)
            throws Exception {

        // Build partition map
        Map<String, Object> partitionMap = new HashMap<>();
        partitionMap.put("server", sourceConfig.getLogproxyHost());
        partitionMap.put("database", tableId.catalog());
        partitionMap.put("table", tableId.table());

        // Build offset map
        Map<String, Object> offsetMap = new HashMap<>();
        offsetMap.put("snapshot", true);
        offsetMap.put("timestamp", System.currentTimeMillis() / 1000);

        // Build key and value
        Map<String, Object> key = new HashMap<>();
        Map<String, Object> value = new HashMap<>();

        java.sql.ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            Object columnValue;

            try {
                columnValue = rs.getObject(i);

                // Handle NULL values
                if (rs.wasNull()) {
                    columnValue = null;
                }
            } catch (Exception e) {
                log.warn("Failed to read column {} from table {}", columnName, tableId, e);
                columnValue = null;
            }

            value.put(columnName, columnValue);

            // Add primary key columns to key map
            if (primaryKeys != null && primaryKeys.contains(columnName)) {
                key.put(columnName, columnValue);
            }
        }

        return new SourceRecord(
                partitionMap,
                offsetMap,
                tableId.toString(),
                null, // key schema (should be properly defined)
                key.isEmpty() ? null : key, // Use null if no primary keys
                null, // value schema (should be properly defined)
                value);
    }

    /** Create watermark partition map */
    private Map<String, String> createWatermarkPartitionMap(String tableIdentifier) {
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put("table", tableIdentifier);
        return partitionMap;
    }

    /** Create backfill incremental split */
    private IncrementalSplit createBackfillStreamSplit(
            OceanBaseOffset lowWatermark, OceanBaseOffset highWatermark, TableId tableId) {

        return new IncrementalSplit(
                snapshotSplit.splitId() + "_backfill",
                Collections.singletonList(tableId),
                lowWatermark,
                highWatermark,
                new ArrayList<>(),
                null,
                null);
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public SourceSplitBase getSplit() {
        return snapshotSplit;
    }

    private void checkTaskRunning() throws InterruptedException {
        if (!taskRunning) {
            throw new InterruptedException("Task has been stopped");
        }
    }

    @Override
    public void shutdown() {
        taskRunning = false;
    }
}

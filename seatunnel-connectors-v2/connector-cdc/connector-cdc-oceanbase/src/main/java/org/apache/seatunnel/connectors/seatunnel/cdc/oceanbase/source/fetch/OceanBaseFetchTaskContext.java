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

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source.dialect.OceanBaseDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source.offset.OceanBaseOffset;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.util.LoggingContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/** OceanBase fetch task context. */
@Slf4j
public class OceanBaseFetchTaskContext implements FetchTask.Context {

    @Getter private final OceanBaseDialect dialect;
    @Getter private final OceanBaseSourceConfig sourceConfig;
    private ChangeEventQueue<DataChangeEvent> changeEventQueue;

    public OceanBaseFetchTaskContext(OceanBaseDialect dialect, OceanBaseSourceConfig sourceConfig) {
        this.dialect = dialect;
        this.sourceConfig = sourceConfig;
    }

    public void configure(@Nonnull SourceSplitBase sourceSplitBase) {
        // Configure the queue size based on split type and exactly-once semantics
        final int queueSize =
                sourceSplitBase.isSnapshotSplit() && isExactlyOnce()
                        ? Integer.MAX_VALUE
                        : sourceConfig.getBatchSize();

        this.changeEventQueue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(Duration.ofMillis(1000)) // Default poll interval
                        .maxBatchSize(1024) // Default max batch size
                        .maxQueueSize(queueSize)
                        .loggingContextSupplier(
                                () ->
                                        LoggingContext.forConnector(
                                                "oceanbase-cdc",
                                                "oceanbase-cdc-connector",
                                                "oceanbase-cdc-connector-task"))
                        .build();
    }

    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return changeEventQueue;
    }

    @Override
    public TableId getTableId(SourceRecord record) {
        // Extract table ID from OceanBase record
        String database = extractStringFromRecord(record, "db");
        String table = extractStringFromRecord(record, "table");

        if (database == null || table == null) {
            log.warn("Failed to extract database or table from record");
            return null;
        }

        return TableId.parse(database + "." + table);
    }

    @Override
    public Tables.TableFilter getTableFilter() {
        // Filters have been pushed down to LogProxy
        return Tables.TableFilter.includeAll();
    }

    @Override
    public boolean isExactlyOnce() {
        return sourceConfig.isExactlyOnce();
    }

    @Override
    public Offset getStreamOffset(SourceRecord record) {
        // Extract timestamp and checkpoint from record
        Long timestamp = extractTimestampFromRecord(record);
        String checkpoint = extractStringFromRecord(record, "checkpoint");
        return new OceanBaseOffset(timestamp, checkpoint);
    }

    @Override
    public boolean isDataChangeRecord(SourceRecord record) {
        // Check if this is a data change record (INSERT/UPDATE/DELETE)
        if (record.value() == null) {
            return false;
        }
        String op = extractStringFromRecord(record, "op");
        return op != null && (op.equals("INSERT") || op.equals("UPDATE") || op.equals("DELETE"));
    }

    @Override
    public boolean isRecordBetween(
            SourceRecord record, @Nonnull Object[] splitStart, @Nonnull Object[] splitEnd) {
        // For OceanBase, we rely on timestamp-based filtering
        // This is typically handled by LogProxy
        return true;
    }

    @Override
    public void rewriteOutputBuffer(
            Map<org.apache.kafka.connect.data.Struct, SourceRecord> outputBuffer,
            @Nonnull SourceRecord changeRecord) {
        // For OceanBase CDC, we handle updates by removing old records
        // and adding new records based on primary key
        org.apache.kafka.connect.data.Struct key =
                (org.apache.kafka.connect.data.Struct) changeRecord.key();

        if (key != null) {
            String op = extractStringFromRecord(changeRecord, "op");

            if ("UPDATE".equals(op) || "DELETE".equals(op)) {
                // Remove old record
                outputBuffer.remove(key);
            }

            if ("INSERT".equals(op) || "UPDATE".equals(op)) {
                // Add new record
                outputBuffer.put(key, changeRecord);
            }
        }
    }

    @Override
    public List<SourceRecord> formatMessageTimestamp(Collection<SourceRecord> records) {
        // Format timestamp for records if needed
        // OceanBase LogProxy already provides formatted timestamps
        return new ArrayList<>(records);
    }

    /** Extract string value from record */
    private String extractStringFromRecord(SourceRecord record, String fieldName) {
        if (record == null || record.value() == null) {
            return null;
        }

        try {
            Object value = record.value();

            // Handle Struct type (Debezium format)
            if (value instanceof org.apache.kafka.connect.data.Struct) {
                org.apache.kafka.connect.data.Struct struct =
                        (org.apache.kafka.connect.data.Struct) value;
                return struct.getString(fieldName);
            }

            // Handle Map type
            if (value instanceof Map) {
                Map<?, ?> map = (Map<?, ?>) value;
                Object fieldValue = map.get(fieldName);
                return fieldValue != null ? fieldValue.toString() : null;
            }
        } catch (Exception e) {
            log.debug("Failed to extract field {} from record", fieldName, e);
        }
        return null;
    }

    /** Extract timestamp from record */
    private Long extractTimestampFromRecord(SourceRecord record) {
        if (record == null || record.value() == null) {
            return null;
        }

        try {
            Object value = record.value();

            if (value instanceof org.apache.kafka.connect.data.Struct) {
                org.apache.kafka.connect.data.Struct struct =
                        (org.apache.kafka.connect.data.Struct) value;
                Long tsMs = struct.getInt64("ts_ms");
                return tsMs != null ? tsMs / 1000 : null; // Convert to seconds
            }

            if (value instanceof Map) {
                Map<?, ?> map = (Map<?, ?>) value;
                Object tsMs = map.get("ts_ms");
                if (tsMs instanceof Long) {
                    return (Long) tsMs / 1000;
                } else if (tsMs != null) {
                    return Long.parseLong(tsMs.toString()) / 1000;
                }
            }
        } catch (Exception e) {
            log.debug("Failed to extract timestamp from record", e);
        }
        return null;
    }

    @Override
    public void close() {
        log.info("Stopping OceanBaseFetchTaskContext");
    }
}

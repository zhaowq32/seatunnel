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
/**
 * OceanBase CDC 读取任务上下文
 *
 * <p>为 OceanBase CDC 读取任务提供上下文信息和工具方法。
 *
 * <p>主要功能:
 *
 * <ul>
 *   <li>管理数据变更事件队列
 *   <li>提供 SourceRecord 解析方法
 *   <li>处理偏移量提取
 *   <li>处理输出缓冲区重写（Exactly-Once 语义）
 * </ul>
 */
@Slf4j
public class OceanBaseFetchTaskContext implements FetchTask.Context {

    /** OceanBase 数据源方言 */
    @Getter private final OceanBaseDialect dialect;

    /** OceanBase 数据源配置 */
    @Getter private final OceanBaseSourceConfig sourceConfig;

    /** 数据变更事件队列 */
    private ChangeEventQueue<DataChangeEvent> changeEventQueue;

    public OceanBaseFetchTaskContext(OceanBaseDialect dialect, OceanBaseSourceConfig sourceConfig) {
        this.dialect = dialect;
        this.sourceConfig = sourceConfig;
    }

    /**
     * 配置上下文
     *
     * <p>根据分片类型和 Exactly-Once 语义配置队列大小。
     *
     * @param sourceSplitBase 源分片
     */
    public void configure(@Nonnull SourceSplitBase sourceSplitBase) {
        // 根据分片类型和 Exactly-Once 语义配置队列大小
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

    /**
     * 从 SourceRecord 中提取表 ID
     *
     * <p>从 Debezium source 元数据中提取 db 和 table 字段。
     *
     * @param record SourceRecord
     * @return 表 ID,如果提取失败则返回 null
     */
    @Override
    public TableId getTableId(SourceRecord record) {
        // 从 OceanBase 记录中提取表 ID
        String database = extractStringFromRecord(record, "db");
        String table = extractStringFromRecord(record, "table");

        if (database == null || table == null) {
            log.warn("Failed to extract database or table from record");
            return null;
        }

        return TableId.parse(database + "." + table);
    }

    /**
     * 获取表过滤器
     *
     * <p>OceanBase CDC 将过滤逻辑下推到 LogProxy,故此处返回包含所有表。
     *
     * @return 包含所有表的过滤器
     */
    @Override
    public Tables.TableFilter getTableFilter() {
        // 过滤器已经下推到 LogProxy
        return Tables.TableFilter.includeAll();
    }

    @Override
    public boolean isExactlyOnce() {
        return sourceConfig.isExactlyOnce();
    }

    /**
     * 从 SourceRecord 提取流偏移量
     *
     * <p>提取 timestamp 和 checkpoint 字段构建 OceanBaseOffset。
     *
     * @param record SourceRecord
     * @return OceanBase 偏移量
     */
    @Override
    public Offset getStreamOffset(SourceRecord record) {
        // 从记录中提取 timestamp 和 checkpoint
        Long timestamp = extractTimestampFromRecord(record);
        String checkpoint = extractStringFromRecord(record, "checkpoint");
        return new OceanBaseOffset(timestamp, checkpoint);
    }

    /**
     * 判断是否为数据变更记录
     *
     * <p>检查 op 字段是否为 INSERT/UPDATE/DELETE。
     *
     * @param record SourceRecord
     * @return true 表示是数据变更记录
     */
    @Override
    public boolean isDataChangeRecord(SourceRecord record) {
        // 检查是否为数据变更记录 (INSERT/UPDATE/DELETE)
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

    /**
     * 重写输出缓冲区
     *
     * <p>用于 Exactly-Once 语义,根据主键删除旧记录并添加新记录。
     *
     * @param outputBuffer 输出缓冲区
     * @param changeRecord 数据变更记录
     */
    @Override
    public void rewriteOutputBuffer(
            Map<org.apache.kafka.connect.data.Struct, SourceRecord> outputBuffer,
            @Nonnull SourceRecord changeRecord) {
        // 处理 UPDATE 和 DELETE 操作,根据主键删除旧记录并添加新记录
        org.apache.kafka.connect.data.Struct key =
                (org.apache.kafka.connect.data.Struct) changeRecord.key();

        if (key != null) {
            String op = extractStringFromRecord(changeRecord, "op");

            if ("UPDATE".equals(op) || "DELETE".equals(op)) {
                // 删除旧记录
                outputBuffer.remove(key);
            }

            if ("INSERT".equals(op) || "UPDATE".equals(op)) {
                // 添加新记录
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

    /**
     * 从记录中提取字符串值
     *
     * <p>支持从 Struct 和 Map 两种格式提取。
     *
     * @param record SourceRecord
     * @param fieldName 字段名
     * @return 字段值,如果不存在则返回 null
     */
    private String extractStringFromRecord(SourceRecord record, String fieldName) {
        if (record == null || record.value() == null) {
            return null;
        }

        try {
            Object value = record.value();

            // 处理 Struct 类型 (Debezium 格式)
            if (value instanceof org.apache.kafka.connect.data.Struct) {
                org.apache.kafka.connect.data.Struct struct =
                        (org.apache.kafka.connect.data.Struct) value;
                return struct.getString(fieldName);
            }

            // 处理 Map 类型
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

    /**
     * 从记录中提取时间戳
     *
     * <p>提取 ts_ms 字段并转换为秒。
     *
     * @param record SourceRecord
     * @return 时间戳（秒）,如果不存在则返回 null
     */
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
                return tsMs != null ? tsMs / 1000 : null; // 转换为秒
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

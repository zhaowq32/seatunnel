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

/**
 * OceanBase 流式读取任务
 *
 * <p>负责通过 OceanBase LogProxy 进行增量数据读取，是 OceanBase CDC 连接器的核心组件。
 *
 * <p>主要职责:
 *
 * <ul>
 *   <li>创建并启动 LogProxyClient 连接到 OceanBase 集群
 *   <li>监听 LogProxy 返回的数据变更事件（INSERT/UPDATE/DELETE/COMMIT 等）
 *   <li>将 LogMessage 转换为 Debezium 格式的 SourceRecord
 *   <li>处理事务边界（BEGIN/COMMIT），确保数据一致性
 *   <li>管理任务生命周期（启动/停止/错误处理）
 *   <li>检查和处理停止偏移量
 * </ul>
 *
 * <p><strong>注意</strong>:
 *
 * <ul>
 *   <li>仅支持流式模式,不支持快照读取
 *   <li>支持多表同时读取，通过单个 LogProxyClient 处理
 *   <li>支持按时间戳范围读取，通过 startOffset 和 stopOffset 控制
 *   <li>数据处理是事务级别的，只有收到 COMMIT 消息才会提交事务批次
 * </ul>
 *
 * <p><strong>核心处理流程</strong>:
 *
 * <ol>
 *   <li>初始化任务和必要的组件
 *   <li>从 IncrementalSplit 中提取表列表和偏移量信息
 *   <li>创建 LogProxyClient 并注册 RecordListener
 *   <li>启动 LogProxyClient 开始接收数据
 *   <li>在 RecordListener 中处理不同类型的 LogMessage
 *   <li>将处理后的消息转换为 SourceRecord 并入队
 *   <li>检查是否达到停止偏移量
 *   <li>任务完成后发送 END watermark
 * </ol>
 */
@Slf4j
public class OceanBaseStreamFetchTask implements FetchTask<SourceSplitBase> {

    /** Schema 名称调整器,用于确保 Schema 名称符合 Kafka Connect 规范 */
    private static final SchemaNameAdjuster SCHEMA_NAME_ADJUSTER = SchemaNameAdjuster.create();

    /**
     * Debezium source 元数据 Schema
     *
     * <p>包含 CDC 记录的来源信息:
     *
     * <ul>
     *   <li>version: 连接器版本
     *   <li>connector: 连接器名称 (oceanbase)
     *   <li>snapshot: 是否为快照数据 (OceanBase CDC 恒为 false)
     *   <li>db: 数据库名
     *   <li>table: 表名
     *   <li>ts_ms: 时间戳（毫秒）
     *   <li>checkpoint: LogProxy checkpoint
     * </ul>
     */
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

    /** 当前读取的增量分片 */
    private final IncrementalSplit incrementalSplit;

    /** 任务是否正在运行 */
    private volatile boolean taskRunning = false;

    /** LogProxy 客户端,用于接收数据变更事件 */
    private LogProxyClient logProxyClient;

    public OceanBaseStreamFetchTask(IncrementalSplit incrementalSplit) {
        this.incrementalSplit = incrementalSplit;
    }

    /**
     * 执行流式读取任务
     *
     * <p>此方法是 FetchTask 的核心方法，负责启动整个流式读取过程。
     *
     * <p>任务执行流程:
     *
     * <ol>
     *   <li>从任务上下文中获取事件队列
     *   <li>设置任务运行状态为 true
     *   <li>从 IncrementalSplit 提取表列表和偏移量信息
     *   <li>验证表列表是否为空
     *   <li>记录启动日志，包括表列表和偏移量范围
     *   <li>调用 startLogProxyClient 方法启动 LogProxy 客户端并开始读取
     *   <li>捕获并处理执行过程中的异常
     *   <li>无论成功与否，最终设置任务运行状态为 false
     * </ol>
     *
     * @param context 任务上下文,包含配置信息和事件队列
     * @throws Exception 如果读取过程中发生错误，会向上抛出
     */
    @Override
    public void execute(Context context) throws Exception {
        OceanBaseFetchTaskContext taskContext = (OceanBaseFetchTaskContext) context;
        ChangeEventQueue<DataChangeEvent> changeEventQueue = taskContext.getQueue();

        taskRunning = true;

        // 从 IncrementalSplit 中获取要读取的表列表
        List<TableId> tableIds = incrementalSplit.getTableIds();
        if (tableIds == null || tableIds.isEmpty()) {
            log.error("No table IDs found in incremental split");
            return;
        }

        // 支持在一个 split 中同时读取多个表
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

    /**
     * 启动 LogProxy 客户端并开始流式读取
     *
     * <p>该方法是流式读取的核心逻辑，负责建立与 OceanBase LogProxy 的连接并处理数据变更事件。
     *
     * <p>核心处理逻辑:
     *
     * <ol>
     *   <li>创建 LogProxyClient 实例，使用提供的配置和起始偏移量
     *   <li>注册 RecordListener 来处理 LogProxy 推送的消息:
     *       <ul>
     *         <li>INSERT/UPDATE/DELETE: 添加到事务批次，等待 COMMIT
     *         <li>COMMIT: 提交当前事务批次，将所有变更转换并入队
     *         <li>BEGIN/DDL/HEARTBEAT: 忽略处理
     *       </ul>
     *   <li>启动 LogProxyClient 开始接收数据
     *   <li>等待 LogProxyClient 完成（阻塞调用）
     *   <li>为所有表发送 END watermark，标记读取完成
     *   <li>处理异常情况并确保资源正确释放
     * </ol>
     *
     * <p><strong>事务处理机制</strong>:
     *
     * <ul>
     *   <li>使用 LinkedList 缓存事务中的所有变更
     *   <li>只有收到 COMMIT 消息才会处理整个事务批次
     *   <li>每个事务批次处理完成后清空缓存
     * </ul>
     *
     * @param taskContext 任务上下文，包含数据源配置等信息
     * @param startOffset 起始偏移量，指定从哪里开始读取
     * @param tableIds 要读取的表列表，用于过滤和匹配消息
     * @param changeEventQueue 数据变更事件队列，用于存储处理后的事件
     * @throws Exception 如果启动或读取过程中发生错误
     */
    private void startLogProxyClient(
            OceanBaseFetchTaskContext taskContext,
            OceanBaseOffset startOffset,
            List<TableId> tableIds,
            ChangeEventQueue<DataChangeEvent> changeEventQueue)
            throws Exception {

        OceanBaseSourceConfig sourceConfig = taskContext.getSourceConfig();
        OceanBaseOffset stopOffset = (OceanBaseOffset) incrementalSplit.getStopOffset();

        try {
            // 创建 LogProxy 客户端
            logProxyClient = OceanBaseUtils.createLogProxyClient(sourceConfig, startOffset);

            // 注册记录监听器来处理 LogProxy 返回的数据
            logProxyClient.addListener(
                    new RecordListener() {
                        // 用于缓存当前事务的 LogMessage 列表
                        private final List<LogMessage> logMessageList = new LinkedList<>();

                        /**
                         * 提交事务批次,将缓存的 LogMessage 转换为 SourceRecord 并入队
                         *
                         * <p>该方法在收到 COMMIT 消息时被调用。
                         */
                        private void commitLogMessage() throws InterruptedException {
                            if (logMessageList.isEmpty()) {
                                return;
                            }

                            // 处理事务中的每个 LogMessage
                            for (LogMessage logMessage : logMessageList) {
                                // 将 LogMessage 的表信息与 split 的表列表匹配
                                TableId matchedTableId = matchTableId(logMessage, tableIds);

                                if (matchedTableId == null) {
                                    // 跳过不在此 split 中的表
                                    continue;
                                }

                                // 从 LogMessage 中提取偏移量
                                OceanBaseOffset currentOffset =
                                        new OceanBaseOffset(
                                                logMessage.getFileNameOffset(),
                                                String.valueOf(logMessage.getFileNameOffset()));

                                // 检查是否达到停止偏移量
                                if (stopOffset != null
                                        && currentOffset.getTimestamp()
                                                >= stopOffset.getTimestamp()) {
                                    log.info("Reached stop offset {}", stopOffset);
                                    shutdown();
                                    logMessageList.clear();
                                    return;
                                }

                                // 将 LogMessage 转换为 SourceRecord
                                SourceRecord sourceRecord =
                                        convertLogMessageToSourceRecord(
                                                sourceConfig, matchedTableId, logMessage);
                                if (sourceRecord != null) {
                                    // 将数据变更事件入队
                                    changeEventQueue.enqueue(new DataChangeEvent(sourceRecord));
                                }
                            }
                            logMessageList.clear();
                        }

                        /**
                         * 处理 LogProxy 推送的 LogMessage
                         *
                         * <p>根据操作类型进行不同处理:
                         *
                         * <ul>
                         *   <li>INSERT/UPDATE/DELETE: 添加到事务批次,等待 COMMIT
                         *   <li>COMMIT: 提交当前事务批次
                         *   <li>BEGIN/DDL/HEARTBEAT: 忽略
                         * </ul>
                         *
                         * @param logMessage LogProxy 推送的日志消息
                         */
                        @Override
                        public void notify(LogMessage logMessage) {
                            try {
                                switch (logMessage.getOpt()) {
                                    case HEARTBEAT: // 心跳消息,忽略
                                    case BEGIN: // 事务开始,忽略
                                    case DDL: // DDL 语句,忽略
                                        break;
                                    case INSERT: // 插入操作
                                    case UPDATE: // 更新操作
                                    case DELETE: // 删除操作
                                        logMessageList.add(logMessage);
                                        break;
                                    case COMMIT: // 事务提交,处理批次
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

                        /**
                         * 处理 LogProxyClient 异常
                         *
                         * <p>对常见异常类型进行分类和提示:
                         *
                         * <ul>
                         *   <li>连接错误: 检查 LogProxy 主机/端口和网络连接
                         *   <li>认证错误: 检查用户名/密码和租户配置
                         *   <li>超时错误: 考虑增加连接超时配置
                         * </ul>
                         *
                         * @param e LogProxy 客户端异常
                         */
                        @Override
                        public void onException(
                                com.oceanbase.clogproxy.client.exception.LogProxyClientException
                                        e) {
                            log.error("LogProxy client exception occurred", e);
                            // 对异常类型进行分类,提供更好的错误处理提示
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
                            // 发生严重异常时关闭任务
                            shutdown();
                        }
                    });

            // 启动 LogProxy 客户端
            logProxyClient.start();

            log.info("LogProxy client started successfully for split {}", incrementalSplit);

            // 等待 LogProxyClient 完成(阻塞调用)
            logProxyClient.join();

            // 为所有表发送 END watermark
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

    /**
     * 将 LogMessage 转换为 Debezium 格式的 SourceRecord
     *
     * <p>该方法是 CDC 数据转换的核心,将 OceanBase LogProxy 的数据格式 转换为 SeaTunnel 能够识别的 Debezium 格式。这是整个连接器的关键转换点。
     *
     * <p><strong>转换步骤</strong>:
     *
     * <ol>
     *   <li>将 LogMessage 封装为 OceanBaseCDCRecord,自动分离 before/after 字段
     *   <li>构建 partition map (包含 database 和 table 信息)
     *   <li>构建 offset map (包含 timestamp 和 checkpoint 信息)
     *   <li>构建 Debezium source 元数据 struct (包含版本、连接器、时间戳等)
     *   <li>从 OceanBaseCDCRecord 提取 before 和 after 数据
     *   <li>构建 row schema (字段定义)
     *   <li>根据操作类型构建 before/after struct:
     *       <ul>
     *         <li>INSERT: 只有 after struct
     *         <li>UPDATE: 同时包含 before 和 after struct
     *         <li>DELETE: 只有 before struct
     *       </ul>
     *   <li>构建 Debezium envelope struct (包含操作类型、源信息、before/after 数据)
     *   <li>创建并返回 SourceRecord
     * </ol>
     *
     * <p><strong>重要提示</strong>:
     *
     * <ul>
     *   <li>时间戳需要从秒转换为毫秒 (* 1000)，以符合 Debezium 格式要求
     *   <li>所有字段都被处理为可选的字符串类型，以简化处理
     *   <li>使用 HashSet 自动去重字段名，避免 Schema 构建失败
     *   <li>转换失败时会记录警告并返回 null，不会影响整个任务
     * </ul>
     *
     * @param sourceConfig 数据源配置，包含必要的连接信息
     * @param tableId 表标识符，用于构建 topic 和 partition 信息
     * @param logMessage LogProxy 返回的原始日志消息，包含变更数据
     * @return Debezium 格式的 SourceRecord,如果转换失败则返回 null
     */
    private SourceRecord convertLogMessageToSourceRecord(
            OceanBaseSourceConfig sourceConfig, TableId tableId, LogMessage logMessage) {

        try {
            // 将 LogMessage 解析为 OceanBaseCDCRecord,正确分离 before/after 值
            OceanBaseCDCRecord cdcRecord = new OceanBaseCDCRecord(logMessage);
            String operation = cdcRecord.getType().toString();

            // 构建 partition map (源分区)
            Map<String, String> partitionMap = new HashMap<>();
            partitionMap.put("database", cdcRecord.getDatabase());
            partitionMap.put("table", cdcRecord.getTable());

            // 构建 offset map
            Map<String, Object> offsetMap = new HashMap<>();
            offsetMap.put("timestamp", logMessage.getTimestamp());
            offsetMap.put("checkpoint", logMessage.getCheckpoint());
            if (logMessage.getSafeTimestamp() != null) {
                offsetMap.put("safe_timestamp", logMessage.getSafeTimestamp());
            }

            // 构建 Debezium source 元数据 Struct
            Long timestampMs = cdcRecord.getTimestamp() * 1000; // 将秒转换为毫秒
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

            // 使用 OceanBaseCDCRecord 提取字段数据,自动正确分离 before/after
            Map<String, Object> beforeData =
                    OceanBaseCDCRecord.toValueMap(cdcRecord.getFieldsBefore());
            Map<String, Object> afterData =
                    OceanBaseCDCRecord.toValueMap(cdcRecord.getFieldsAfter());

            // 构建 key (主键字段) - 使用 Map 存储，无 schema 模式
            Map<String, Object> key = new HashMap<>();
            // 使用 after 数据作为 key,DELETE 操作使用 before
            if (afterData != null && !afterData.isEmpty()) {
                key.putAll(afterData);
            } else if (beforeData != null && !beforeData.isEmpty()) {
                key.putAll(beforeData);
            }

            // 从 OceanBaseCDCRecord 字段构建 row schema
            Schema rowSchema = createRowSchema(tableId, cdcRecord);

            // 将操作类型映射为 Debezium 操作,并构建 before/after Struct
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
                    // 跳过事务控制事件
                    return null;
                default:
                    log.warn("Unknown operation type: {}", operation);
                    return null;
            }

            // 构建 Debezium envelope Struct
            Schema envelopeSchema = createEnvelopeSchema(rowSchema);
            Struct envelope =
                    new Struct(envelopeSchema)
                            .put(Envelope.FieldName.OPERATION, debeziumOp)
                            .put(Envelope.FieldName.SOURCE, sourceStruct)
                            .put(Envelope.FieldName.BEFORE, beforeStruct)
                            .put(Envelope.FieldName.AFTER, afterStruct)
                            .put(Envelope.FieldName.TIMESTAMP, timestampMs);

            // 创建 SourceRecord,包含 Schema 和 Struct
            return new SourceRecord(
                    partitionMap,
                    offsetMap,
                    tableId.toString(), // topic
                    null, // key schema (无 schema 模式)
                    key,
                    envelopeSchema, // value schema
                    envelope); // value 为 Struct

        } catch (Exception e) {
            log.warn("Failed to convert LogMessage to SourceRecord for table {}", tableId, e);
            return null;
        }
    }

    /**
     * 从 OceanBaseCDCRecord 字段创建 row schema
     *
     * <p>所有字段都被处理为可选的字符串类型,以简化处理。
     *
     * <p>更完整的实现应该正确映射字段类型。
     *
     * <p><strong>重要</strong>: 使用 HashSet 自动去重字段名,避免 Schema 构建失败。
     *
     * @param tableId 表标识符
     * @param cdcRecord 包含字段定义的 OceanBaseCDCRecord
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

        // 收集 before 和 after map 中的所有字段名
        // 使用 Map key 自动去重,不需担心重复
        java.util.Set<String> fieldNames = new java.util.HashSet<>();
        fieldNames.addAll(cdcRecord.getFieldsBefore().keySet());
        fieldNames.addAll(cdcRecord.getFieldsAfter().keySet());

        // 将所有字段添加到 schema
        for (String fieldName : fieldNames) {
            // 所有字段目前都使用可选的字符串类型
            // 更完整的实现应该正确映射字段类型
            builder.field(fieldName, Schema.OPTIONAL_STRING_SCHEMA);
        }

        return builder.build();
    }

    /**
     * 从行数据创建 Struct
     *
     * <p>将 Map 格式的行数据转换为 Kafka Connect 的 Struct 格式。
     *
     * @param schema 行 schema
     * @param data 行数据 Map
     * @return 表示行的 Struct,如果数据为空则返回 null
     */
    private Struct createRowStruct(Schema schema, Map<String, Object> data) {
        if (data == null || data.isEmpty()) {
            return null;
        }

        Struct struct = new Struct(schema);
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            // 仅当字段在 schema 中存在时才设置值
            if (schema.field(fieldName) != null) {
                // 根据 schema 定义将值转换为字符串
                String stringValue = value != null ? value.toString() : null;
                struct.put(fieldName, stringValue);
            }
        }
        return struct;
    }

    /**
     * 将 LogMessage 的表信息与 split 的表列表匹配
     *
     * <p>OceanBase 的 dbName 格式为 'tenant.database',需要提取 database 部分。
     *
     * @param logMessage 要匹配的 LogMessage
     * @param tableIds 此 split 中的表 ID 列表
     * @return 匹配的 TableId,如果没有匹配则返回 null
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

        // 与 split 的表列表进行匹配
        for (TableId tableId : tableIds) {
            // OceanBase 使用 catalog 存储数据库名
            if (dbName.equals(tableId.catalog()) && tableName.equals(tableId.table())) {
                return tableId;
            }
        }

        // 未找到匹配 - 该消息不属于此 split 中的表
        return null;
    }

    /**
     * 创建 watermark 分区 map
     *
     * @param tableIdentifier 表标识符
     * @return Watermark 分区 map
     */
    private Map<String, String> createWatermarkPartitionMap(String tableIdentifier) {
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put("table", tableIdentifier);
        return partitionMap;
    }

    /**
     * 判断任务是否正在运行
     *
     * @return true 表示任务正在运行
     */
    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    /**
     * 关闭任务并清理资源
     *
     * <p>停止 LogProxyClient 并释放相关资源。
     */
    @Override
    public void shutdown() {
        // 清理 LogProxyClient
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

    /**
     * 获取当前任务处理的分片
     *
     * @return 增量分片
     */
    @Override
    public SourceSplitBase getSplit() {
        return incrementalSplit;
    }
}

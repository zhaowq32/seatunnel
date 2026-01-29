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

package org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.table.CatalogOptions;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.cdc.debezium.DeserializeFormat;
import org.apache.seatunnel.connectors.cdc.debezium.row.DebeziumJsonDeserializeSchema;
import org.apache.seatunnel.connectors.cdc.debezium.row.SeaTunnelRowDebeziumDeserializeSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceConfigProvider;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source.dialect.OceanBaseDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source.offset.OceanBaseOffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;

import javax.annotation.Nonnull;

import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

/**
 * OceanBase CDC 增量数据源
 *
 * <p>此类是 OceanBase CDC 连接器的核心实现，继承自通用的 IncrementalSource 类， 负责：
 *
 * <ul>
 *   <li>创建和配置 OceanBase 数据源
 *   <li>定义启动和停止模式
 *   <li>创建反序列化 schema
 *   <li>创建数据源方言和偏移量工厂
 * </ul>
 *
 * <p>OceanBase CDC 连接器的工作流程：
 *
 * <ol>
 *   <li>通过 OceanBaseIncrementalSourceFactory 创建实例
 *   <li>初始化配置和必要的组件
 *   <li>通过 LogProxyClient 连接到 OceanBase 集群
 *   <li>监听并处理数据变更事件
 *   <li>将事件转换为 SeaTunnel 可处理的格式
 * </ol>
 */
public class OceanBaseIncrementalSource<T> extends IncrementalSource<T, OceanBaseSourceConfig>
        implements SupportParallelism {

    /** 连接器标识符，用于在配置中引用 */
    public static final String IDENTIFIER = "OceanBase-CDC";

    public static final String DRIVER_NAME = "com.oceanbase.jdbc.Driver";

    /**
     * 构造函数
     *
     * @param options 配置选项
     * @param catalogTables 表结构列表
     */
    public OceanBaseIncrementalSource(ReadonlyConfig options, List<CatalogTable> catalogTables) {
        super(options, catalogTables);
    }

    /**
     * 获取启动模式选项
     *
     * @return 启动模式选项
     */
    @Override
    public Option<StartupMode> getStartupModeOption() {
        return OceanBaseSourceOptions.STARTUP_MODE;
    }

    /**
     * 获取停止模式选项
     *
     * @return 停止模式选项
     */
    @Override
    public Option<StopMode> getStopModeOption() {
        return OceanBaseSourceOptions.STOP_MODE;
    }

    /**
     * 获取插件名称
     *
     * @return 插件名称
     */
    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    /**
     * 创建数据源配置工厂
     *
     * <p>此方法是配置处理的核心，负责从 ReadonlyConfig 中提取配置项并构建 OceanBaseSourceConfig。
     *
     * @param config 只读配置
     * @return 数据源配置工厂
     */
    @Override
    public SourceConfig.Factory<OceanBaseSourceConfig> createSourceConfigFactory(
            @Nonnull ReadonlyConfig config) {
        // 创建配置构建器
        OceanBaseSourceConfigProvider.Builder builder =
                OceanBaseSourceConfigProvider.newBuilder()
                        // 基本连接配置
                        .url(config.get(JdbcCommonOptions.URL))
                        .username(config.get(OceanBaseSourceOptions.USERNAME))
                        .password(config.get(OceanBaseSourceOptions.PASSWORD))
                        // LogProxy 配置
                        .logProxyHost(config.get(OceanBaseSourceOptions.LOG_PROXY_HOST))
                        .logProxyPort(config.get(OceanBaseSourceOptions.LOG_PROXY_PORT))
                        // 同步表
                        .tableNames(config.get(CatalogOptions.TABLE_NAMES));

        // 可选配置项
        Optional.ofNullable(config.get(OceanBaseSourceOptions.CLUSTER_URL))
                .ifPresent(builder::clusterUrl);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.ROOT_SERVER_LIST))
                .ifPresent(builder::rootServerList);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.BLACK_TABLE_LIST))
                .ifPresent(builder::blackTableList);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.STARTUP_TIMESTAMP))
                .ifPresent(builder::startTimestamp);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.SERVER_TIME_ZONE))
                .ifPresent(builder::serverTimeZone);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.WORKING_MODE))
                .ifPresent(builder::workingMode);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.CLUSTER_ID))
                .ifPresent(builder::clusterId);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.SYS_USERNAME))
                .ifPresent(builder::sysUsername);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.SYS_PASSWORD))
                .ifPresent(builder::sysPassword);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.BATCH_SIZE))
                .ifPresent(builder::batchSize);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.EXACTLY_ONCE))
                .ifPresent(builder::exactlyOnce);
        // 启动和停止配置
        Optional.ofNullable(startupConfig).ifPresent(builder::startupConfig);
        Optional.ofNullable(stopConfig).ifPresent(builder::stopConfig);

        // 验证并返回配置工厂
        return builder.validate();
    }

    /**
     * 创建 Debezium 反序列化 schema
     *
     * <p>根据配置的格式，选择不同的反序列化 schema：
     *
     * <ul>
     *   <li>COMPATIBLE_DEBEZIUM_JSON：使用 DebeziumJsonDeserializeSchema
     *   <li>默认：使用 SeaTunnelRowDebeziumDeserializeSchema
     * </ul>
     *
     * @param config 配置
     * @return 反序列化 schema
     */
    @SuppressWarnings("unchecked")
    @Override
    public DebeziumDeserializationSchema<T> createDebeziumDeserializationSchema(
            ReadonlyConfig config) {
        // 检查是否显式设置了 COMPATIBLE_DEBEZIUM_JSON 格式
        if (DeserializeFormat.COMPATIBLE_DEBEZIUM_JSON.equals(
                config.get(JdbcSourceOptions.FORMAT))) {
            return (DebeziumDeserializationSchema<T>)
                    new DebeziumJsonDeserializeSchema(
                            config.get(JdbcSourceOptions.DEBEZIUM_PROPERTIES));
        }

        // 使用默认的 SeaTunnelRowDebeziumDeserializeSchema，结合表结构信息
        String zoneId = config.get(JdbcSourceOptions.SERVER_TIME_ZONE);
        return (DebeziumDeserializationSchema<T>)
                SeaTunnelRowDebeziumDeserializeSchema.builder()
                        .setTables(catalogTables)
                        .setServerTimeZone(ZoneId.of(zoneId))
                        .build();
    }

    /**
     * 创建数据源方言
     *
     * @param config 配置
     * @return 数据源方言
     */
    @Override
    public DataSourceDialect<OceanBaseSourceConfig> createDataSourceDialect(ReadonlyConfig config) {
        return new OceanBaseDialect();
    }

    /**
     * 创建偏移量工厂
     *
     * @param config 配置
     * @return 偏移量工厂
     */
    @Override
    public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
        return new OceanBaseOffsetFactory();
    }

    /**
     * 获取 JDBC 驱动名称
     *
     * @return JDBC 驱动名称
     */
    @Override
    public Optional<String> driverName() {
        return Optional.of(DRIVER_NAME);
    }
}

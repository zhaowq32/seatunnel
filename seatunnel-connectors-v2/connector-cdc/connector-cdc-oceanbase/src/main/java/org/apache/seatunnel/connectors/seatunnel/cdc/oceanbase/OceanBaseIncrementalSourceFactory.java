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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.source.BaseChangeStreamTableSourceFactory;
import org.apache.seatunnel.connectors.cdc.base.utils.CatalogTableUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.seatunnel.api.options.table.CatalogOptions.TABLE_NAMES;
import static org.apache.seatunnel.connectors.cdc.base.option.SourceOptions.STARTUP_TIMESTAMP;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.BLACK_TABLE_LIST;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.CLUSTER_ID;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.CLUSTER_URL;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.EXACTLY_ONCE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.LOG_PROXY_HOST;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.LOG_PROXY_PORT;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.ROOT_SERVER_LIST;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.SERVER_TIME_ZONE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.SYS_PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.SYS_USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.WORKING_MODE;

/**
 * OceanBase CDC 增量数据源工厂类
 *
 * <p>负责创建和配置 OceanBase CDC 数据源，处理以下核心功能：
 *
 * <ul>
 *   <li>定义连接器的配置选项规则
 *   <li>从配置创建数据源实例
 *   <li>支持从检查点恢复数据源
 *   <li>管理表结构信息
 * </ul>
 *
 * <p>配置选项说明：
 *
 * <h3>必填选项：</h3>
 *
 * <ul>
 *   <li>{@code url} - OceanBase 数据库连接 URL
 *   <li>{@code log_proxy_host} - LogProxy 服务主机地址
 *   <li>{@code log_proxy_port} - LogProxy 服务端口
 *   <li>{@code username} - 数据库用户名
 *   <li>{@code password} - 数据库密码
 *   <li>{@code table_names} - 要监控的表名列表
 * </ul>
 *
 * <h3>可选选项：</h3>
 *
 * <ul>
 *   <li>{@code cluster_url} - OceanBase 集群配置 URL（企业版使用）
 *   <li>{@code root_server_list} - 根服务器列表（社区版使用）
 *   <li>{@code black_table_list} - 表黑名单
 *   <li>{@code startup.timestamp} - 启动时间戳
 *   <li>{@code server_time_zone} - 服务器时区
 *   <li>{@code working_mode} - 工作模式（memory 或 storage）
 *   <li>{@code start_timestamp_us} - 启动时间戳（微秒级）
 *   <li>{@code cluster_id} - 集群 ID
 *   <li>{@code sys_username} - 系统租户用户名
 *   <li>{@code sys_password} - 系统租户密码
 *   <li>{@code batch_size} - 批处理大小(用于设置变更事件队列的最大队列大小)
 *   <li>{@code exactly_once} - 是否启用精确一次语义
 *   <li>{@code startup.mode} - 启动模式
 *   <li>{@code stop.mode} - 停止模式
 *   <li>{@code stop.timestamp} - 停止时间戳
 * </ul>
 */
@AutoService(Factory.class)
@Slf4j
public class OceanBaseIncrementalSourceFactory extends BaseChangeStreamTableSourceFactory {

    /**
     * 获取工厂标识符
     *
     * @return 工厂标识符，用于在配置中引用此连接器
     */
    @Override
    public String factoryIdentifier() {
        return OceanBaseIncrementalSource.IDENTIFIER;
    }

    /**
     * 定义连接器的配置选项规则
     *
     * <p>指定哪些配置项是必填的，哪些是可选的。
     *
     * @return 配置选项规则
     */
    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                // 必填配置项
                .required(
                        JdbcCommonOptions.URL,
                        LOG_PROXY_HOST,
                        LOG_PROXY_PORT,
                        USERNAME,
                        PASSWORD,
                        TABLE_NAMES)
                // 可选配置项
                .optional(
                        CLUSTER_URL,
                        ROOT_SERVER_LIST,
                        BLACK_TABLE_LIST,
                        STARTUP_TIMESTAMP,
                        SERVER_TIME_ZONE,
                        WORKING_MODE,
                        CLUSTER_ID,
                        SYS_USERNAME,
                        SYS_PASSWORD,
                        BATCH_SIZE,
                        EXACTLY_ONCE,
                        OceanBaseSourceOptions.STARTUP_MODE,
                        OceanBaseSourceOptions.STOP_MODE,
                        SourceOptions.STARTUP_TIMESTAMP,
                        SourceOptions.STOP_TIMESTAMP)
                .build();
    }

    /**
     * 从配置和恢复表创建数据源
     *
     * <p>此方法是工厂的核心方法，负责：
     *
     * <ul>
     *   <li>如果有恢复表，则使用恢复表的结构（不支持 schema evolution）
     *   <li>如果没有恢复表，则从配置加载表结构
     *   <li>创建并返回 OceanBaseIncrementalSource 实例
     * </ul>
     *
     * @param context 表源工厂上下文，包含配置信息
     * @param restoreTables 从检查点恢复的表列表
     * @param <T> 数据类型
     * @param <SplitT> 分片类型
     * @param <StateT> 状态类型
     * @return 表源实例
     */
    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> restoreSource(
                    TableSourceFactoryContext context, List<CatalogTable> restoreTables) {
        return () -> {
            List<CatalogTable> catalogTables;

            // OceanBase CDC 暂时不支持 schema evolution
            // 从检查点恢复时，直接使用检查点中的表结构
            // 以确保与创建检查点时的状态一致
            if (!restoreTables.isEmpty()) {
                log.info(
                        "Restoring OceanBase CDC source from checkpoint with {} tables",
                        restoreTables.size());
                catalogTables = restoreTables;
            } else {
                // 填充 JdbcCommonOptions.URL 和 JdbcCommonOptions.COMPATIBLE_MODE
                Map<String, Object> newConfigMap = context.getOptions().getSourceMap();
                newConfigMap.put(JdbcCommonOptions.COMPATIBLE_MODE.key(), "mysql");
                newConfigMap.put(
                        JdbcCommonOptions.DRIVER.key(), OceanBaseIncrementalSource.DRIVER_NAME);
                // 从配置加载表结构
                catalogTables =
                        CatalogTableUtil.getCatalogTables(
                                ReadonlyConfig.fromMap(newConfigMap), context.getClassLoader());
                // 处理表配置列表
                Optional<List<JdbcSourceTableConfig>> tableConfigs =
                        context.getOptions().getOptional(JdbcSourceOptions.TABLE_NAMES_CONFIG);
                if (tableConfigs.isPresent()) {
                    catalogTables =
                            CatalogTableUtils.mergeCatalogTableConfig(
                                    catalogTables,
                                    tableConfigs.get(),
                                    text -> TablePath.of(text, true));
                }
                log.info(
                        "Starting OceanBase CDC source with {} tables from configuration",
                        catalogTables.size());
            }
            // 创建并返回 OceanBaseIncrementalSource 实例
            return (SeaTunnelSource<T, SplitT, StateT>)
                    new OceanBaseIncrementalSource<>(context.getOptions(), catalogTables);
        };
    }

    /**
     * 获取数据源类
     *
     * @return 数据源类
     */
    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return OceanBaseIncrementalSource.class;
    }
}

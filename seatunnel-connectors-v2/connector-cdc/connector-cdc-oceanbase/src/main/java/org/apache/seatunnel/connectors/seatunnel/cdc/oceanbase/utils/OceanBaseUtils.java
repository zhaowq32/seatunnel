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

package org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.utils;

import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.OceanBaseIncrementalSource;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.exception.OceanBaseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source.offset.OceanBaseOffset;

import com.oceanbase.clogproxy.client.LogProxyClient;
import com.oceanbase.clogproxy.client.config.ObReaderConfig;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.common.exception.CommonErrorCode.ILLEGAL_ARGUMENT;

@Slf4j
public class OceanBaseUtils {
    public static String buildWhiteTableList(String tenant, List<String> tableNames) {
        return tableNames.stream()
                .map(tableName -> tenant + "." + tableName)
                .collect(Collectors.joining("|"));
    }

    public static String extractTenant(String username) {
        if (username == null) {
            return null;
        }

        int atIndex = username.indexOf('@');
        if (atIndex < 0 || atIndex == username.length() - 1) {
            return null;
        }

        int hashIndex = username.indexOf('#', atIndex + 1);

        if (hashIndex < 0) {
            // username@tenant
            return username.substring(atIndex + 1);
        }

        // username@tenant#cluster
        return username.substring(atIndex + 1, hashIndex);
    }

    public static String extractOceanBaseDbName(String oceanBaseDbName) {
        if (oceanBaseDbName == null) {
            return null;
        }
        int atIndex = oceanBaseDbName.indexOf('.');
        if (atIndex < 0 || atIndex == oceanBaseDbName.length() - 1) {
            return null;
        }
        return oceanBaseDbName.substring(atIndex + 1);
    }

    /** Create JDBC connection to OceanBase */
    public static Connection createJdbcConnection(OceanBaseSourceConfig sourceConfig) {
        try {
            // Use default OceanBase JDBC driver
            Class.forName(OceanBaseIncrementalSource.DRIVER_NAME);

            // Use JDBC URL from source config
            String jdbcUrl = sourceConfig.getUrl();

            return DriverManager.getConnection(
                    jdbcUrl, sourceConfig.getUsername(), sourceConfig.getPassword());
        } catch (ClassNotFoundException e) {
            throw new OceanBaseConnectorException(
                    ILLEGAL_ARGUMENT, "Failed to load JDBC driver: com.oceanbase.jdbc.Driver");
        } catch (SQLException e) {
            throw new OceanBaseConnectorException(
                    ILLEGAL_ARGUMENT, "Failed to create JDBC connection");
        }
    }

    /**
     * 构建 LogProxyClient 所需的 ObReaderConfig
     *
     * <p>此方法是 LogProxyClient 配置的核心，负责从 OceanBaseSourceConfig 中提取配置项 并构建 ObReaderConfig 对象，用于初始化
     * LogProxyClient。
     *
     * <p><strong>配置优先级</strong>:
     *
     * <ol>
     *   <li>集群配置: cluster_url > root_server_list
     *   <li>时间戳配置: startOffset.timestamp > sourceConfig.startTimestamp >
     *       sourceConfig.startTimestampUS
     * </ol>
     *
     * <p><strong>核心配置项</strong>:
     *
     * <ul>
     *   <li>集群配置 (cluster_url 或 root_server_list)
     *   <li>数据库 credentials (username, password)
     *   <li>系统租户 credentials (sys_username, sys_password)
     *   <li>表过滤器 (white_table_list, black_table_list)
     *   <li>工作模式 (working_mode)
     *   <li>时区 (server_time_zone)
     *   <li>集群 ID (cluster_id)
     *   <li>启动时间戳 (start_timestamp)
     * </ul>
     *
     * @param sourceConfig 数据源配置，包含所有必要的连接和过滤信息
     * @param startOffset 起始偏移量，用于确定从哪个时间点开始读取
     * @return 构建好的 ObReaderConfig 对象
     */
    private static ObReaderConfig buildObReaderConfig(
            OceanBaseSourceConfig sourceConfig, OceanBaseOffset startOffset) {
        ObReaderConfig obReaderConfig = new ObReaderConfig();

        // 设置集群配置
        if (sourceConfig.getClusterUrl() != null && !sourceConfig.getClusterUrl().isEmpty()) {
            obReaderConfig.setClusterUrl(sourceConfig.getClusterUrl());
        } else if (sourceConfig.getRootServerList() != null
                && !sourceConfig.getRootServerList().isEmpty()) {
            obReaderConfig.setRsList(sourceConfig.getRootServerList());
        }

        // 设置数据库凭证
        obReaderConfig.setUsername(sourceConfig.getUsername());
        obReaderConfig.setPassword(sourceConfig.getPassword());

        // 如果提供了系统租户凭证，则设置
        if (sourceConfig.getSysUsername() != null && !sourceConfig.getSysUsername().isEmpty()) {
            obReaderConfig.setSysUsername(sourceConfig.getSysUsername());
        }
        if (sourceConfig.getSysPassword() != null && !sourceConfig.getSysPassword().isEmpty()) {
            obReaderConfig.setSysPassword(sourceConfig.getSysPassword());
        }

        // 设置表过滤器
        obReaderConfig.setTableWhiteList(sourceConfig.getWhiteTableList());
        if (sourceConfig.getBlackTableList() != null
                && !sourceConfig.getBlackTableList().isEmpty()) {
            obReaderConfig.setTableBlackList(sourceConfig.getBlackTableList());
        }

        // 设置工作模式
        if (sourceConfig.getWorkingMode() != null) {
            obReaderConfig.setWorkingMode(sourceConfig.getWorkingMode());
        }

        // 设置时区
        if (sourceConfig.getServerTimeZone() != null) {
            obReaderConfig.setTimezone(sourceConfig.getServerTimeZone());
        }

        // 设置集群 ID
        if (sourceConfig.getClusterId() != null) {
            obReaderConfig.setClusterId(sourceConfig.getClusterId());
        }

        // 设置启动时间戳（按优先级）
        if (startOffset != null && startOffset.getTimestamp() > 0) {
            obReaderConfig.setStartTimestamp(startOffset.getTimestamp());
            log.info("Set LogProxy start timestamp: {}", startOffset.getTimestamp());
        } else if (sourceConfig.getStartTimestamp() != null) {
            obReaderConfig.setStartTimestamp(sourceConfig.getStartTimestamp());
            log.info(
                    "Set LogProxy start timestamp from config: {}",
                    sourceConfig.getStartTimestamp());
        }
        return obReaderConfig;
    }

    /**
     * 创建新的 LogProxyClient 实例
     *
     * <p>此方法是 LogProxyClient 创建的入口点，负责：
     *
     * <ol>
     *   <li>构建 ObReaderConfig 配置对象
     *   <li>使用配置创建 LogProxyClient 实例
     *   <li>记录创建过程的日志
     * </ol>
     *
     * <p><strong>重要说明</strong>:
     *
     * <ul>
     *   <li>LogProxyClient 是与 OceanBase 集群通信的核心组件
     *   <li>创建的 client 实例需要在使用完成后调用 stop() 方法关闭
     *   <li>此方法只负责创建实例，不负责启动 client
     * </ul>
     *
     * @param sourceConfig 数据源配置，包含 LogProxy 连接信息和 OceanBase 配置
     * @param startOffset 起始偏移量，用于确定从哪个时间点开始读取数据
     * @return 创建好的 LogProxyClient 实例
     */
    public static LogProxyClient createLogProxyClient(
            OceanBaseSourceConfig sourceConfig, OceanBaseOffset startOffset) {
        log.info("Creating LogProxyClient with start offset: {}", startOffset);

        // 构建 ObReader 配置
        ObReaderConfig obReaderConfig =
                OceanBaseUtils.buildObReaderConfig(sourceConfig, startOffset);

        // 创建 LogProxyClient
        LogProxyClient client =
                new LogProxyClient(
                        sourceConfig.getLogProxyHost(),
                        sourceConfig.getLogProxyPort(),
                        obReaderConfig);

        log.info("LogProxyClient created successfully");
        return client;
    }

    /** Format table list for logging */
    public static String formatTableList(List<String> tableList) {
        if (tableList == null || tableList.isEmpty()) {
            return "ALL_TABLES";
        }
        return String.join(", ", tableList);
    }
}

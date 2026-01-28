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

import static org.apache.seatunnel.common.exception.CommonErrorCode.ILLEGAL_ARGUMENT;

@Slf4j
public class OceanBaseUtils {
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
            String jdbcDriver = "com.oceanbase.jdbc.Driver";
            Class.forName(jdbcDriver);

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

    /** Build ObReaderConfig for LogProxyClient */
    private static ObReaderConfig buildObReaderConfig(
            OceanBaseSourceConfig sourceConfig, OceanBaseOffset startOffset) {
        ObReaderConfig obReaderConfig = new ObReaderConfig();

        // Set cluster configuration
        if (sourceConfig.getClusterUrl() != null && !sourceConfig.getClusterUrl().isEmpty()) {
            obReaderConfig.setClusterUrl(sourceConfig.getClusterUrl());
        } else if (sourceConfig.getRootServerList() != null
                && !sourceConfig.getRootServerList().isEmpty()) {
            obReaderConfig.setRsList(sourceConfig.getRootServerList());
        }

        // Set credentials
        obReaderConfig.setUsername(sourceConfig.getUsername());
        obReaderConfig.setPassword(sourceConfig.getPassword());

        // Set system tenant credentials if provided
        if (sourceConfig.getSysUsername() != null && !sourceConfig.getSysUsername().isEmpty()) {
            obReaderConfig.setSysUsername(sourceConfig.getSysUsername());
        }
        if (sourceConfig.getSysPassword() != null && !sourceConfig.getSysPassword().isEmpty()) {
            obReaderConfig.setSysPassword(sourceConfig.getSysPassword());
        }

        // Set table filter
        obReaderConfig.setTableWhiteList(sourceConfig.getWhiteTableList());
        if (sourceConfig.getBlackTableList() != null
                && !sourceConfig.getBlackTableList().isEmpty()) {
            obReaderConfig.setTableBlackList(sourceConfig.getBlackTableList());
        }

        // Set working mode
        if (sourceConfig.getWorkingMode() != null) {
            obReaderConfig.setWorkingMode(sourceConfig.getWorkingMode());
        }

        // Set timezone
        if (sourceConfig.getServerTimeZone() != null) {
            obReaderConfig.setTimezone(sourceConfig.getServerTimeZone());
        }

        if (sourceConfig.getClusterId() != null) {
            obReaderConfig.setClusterId(sourceConfig.getClusterId());
        }

        // Set start timestamp if provided
        if (startOffset != null && startOffset.getTimestamp() > 0) {
            obReaderConfig.setStartTimestamp(startOffset.getTimestamp());
            log.info("Set LogProxy start timestamp: {}", startOffset.getTimestamp());
        } else if (sourceConfig.getStartTimestamp() != null) {
            obReaderConfig.setStartTimestamp(sourceConfig.getStartTimestamp());
            log.info(
                    "Set LogProxy start timestamp from config: {}",
                    sourceConfig.getStartTimestamp());
        } else if (sourceConfig.getStartTimestampUS() != null) {
            obReaderConfig.setStartTimestampUs(sourceConfig.getStartTimestampUS());
            log.info(
                    "Set LogProxy start timestamp us from config: {}",
                    sourceConfig.getStartTimestampUS());
        }

        return obReaderConfig;
    }

    /**
     * Create a new LogProxyClient instance
     *
     * @param startOffset Starting offset for LogProxy
     * @return LogProxyClient instance
     */
    public static LogProxyClient createLogProxyClient(
            OceanBaseSourceConfig sourceConfig, OceanBaseOffset startOffset) {
        log.info("Creating LogProxyClient with start offset: {}", startOffset);

        // Build ObReader configuration
        ObReaderConfig obReaderConfig =
                OceanBaseUtils.buildObReaderConfig(sourceConfig, startOffset);

        // Create LogProxyClient
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

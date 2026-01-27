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

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.exception.OceanBaseConnectorException;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source.offset.OceanBaseOffset;

import com.oceanbase.clogproxy.client.LogProxyClient;
import com.oceanbase.clogproxy.client.config.ObReaderConfig;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.common.exception.CommonErrorCode.ILLEGAL_ARGUMENT;
import static org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils.rowToArray;

@Slf4j
public class OceanBaseUtils {
    /** Create JDBC connection to OceanBase */
    public static Connection createJdbcConnection(OceanBaseSourceConfig sourceConfig) {
        try {
            // Use default OceanBase JDBC driver
            String jdbcDriver = "com.oceanbase.jdbc.Driver";
            Class.forName(jdbcDriver);

            // Use JDBC URL from source config
            String jdbcUrl = sourceConfig.getJdbcUrl();

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
        obReaderConfig.setTableBlackList(sourceConfig.getBlackTableList());

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
                        sourceConfig.getLogproxyHost(),
                        sourceConfig.getLogproxyPort(),
                        obReaderConfig);

        log.info("LogProxyClient created successfully");
        return client;
    }

    /** Parse table identifier from database and table name */
    public static TableId parseTableId(String database, String table) {
        return TableId.parse(database + "." + table);
    }

    /** Get current timestamp in seconds */
    public static long getCurrentTimestampSeconds() {
        return System.currentTimeMillis() / 1000;
    }

    /**
     * Convert checkpoint string to timestamp Checkpoint format: "timestamp@sequence" or just
     * "timestamp"
     */
    public static long parseTimestampFromCheckpoint(String checkpoint) {
        if (checkpoint == null || checkpoint.isEmpty()) {
            return 0L;
        }

        int atIndex = checkpoint.indexOf('@');
        if (atIndex > 0) {
            return Long.parseLong(checkpoint.substring(0, atIndex));
        }

        return Long.parseLong(checkpoint);
    }

    /** Format table list for logging */
    public static String formatTableList(List<String> tableList) {
        if (tableList == null || tableList.isEmpty()) {
            return "ALL_TABLES";
        }
        return String.join(", ", tableList);
    }

    /** Check if table matches the filter */
    public static boolean isTableIncluded(String database, String table, List<String> tableList) {
        if (tableList == null || tableList.isEmpty()) {
            return true;
        }

        String fullTableName = database + "." + table;
        return tableList.stream().anyMatch(pattern -> matchesPattern(fullTableName, pattern));
    }

    /** Simple pattern matching (supports * wildcard) */
    private static boolean matchesPattern(String text, String pattern) {
        if (pattern.equals("*") || pattern.equals("*.*")) {
            return true;
        }

        String regex = pattern.replace(".", "\\.").replace("*", ".*");
        return text.matches(regex);
    }

    /** Close JDBC connection safely */
    public static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.warn("Failed to close JDBC connection", e);
            }
        }
    }

    /** Get primary key columns for a table */
    public static List<String> getPrimaryKeys(Connection connection, TableId tableId) {
        List<String> primaryKeys = new ArrayList<>();
        try {
            java.sql.DatabaseMetaData metaData = connection.getMetaData();
            String catalog = tableId.catalog();
            String schema = tableId.schema();
            String table = tableId.table();

            try (java.sql.ResultSet rs = metaData.getPrimaryKeys(catalog, schema, table)) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    int keySeq = rs.getInt("KEY_SEQ");
                    primaryKeys.add(columnName);
                    log.debug(
                            "Found primary key column {} (seq={}) for table {}",
                            columnName,
                            keySeq,
                            tableId);
                }
            }

            if (primaryKeys.isEmpty()) {
                log.warn("No primary keys found for table {}", tableId);
            }
        } catch (SQLException e) {
            log.error("Failed to get primary keys for table {}", tableId, e);
        }
        return primaryKeys;
    }

    /** Get table column metadata */
    public static Map<String, Integer> getTableColumns(Connection connection, TableId tableId) {
        Map<String, Integer> columns = new java.util.LinkedHashMap<>();
        try {
            java.sql.DatabaseMetaData metaData = connection.getMetaData();
            String catalog = tableId.catalog();
            String schema = tableId.schema();
            String table = tableId.table();

            try (java.sql.ResultSet rs = metaData.getColumns(catalog, schema, table, null)) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    int dataType = rs.getInt("DATA_TYPE");
                    columns.put(columnName, dataType);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get columns for table {}", tableId, e);
        }
        return columns;
    }

    /** Create JDBC connection with retry */
    public static Connection createJdbcConnectionWithRetry(
            OceanBaseSourceConfig sourceConfig, int maxRetries) {
        int retryCount = 0;
        SQLException lastException = null;

        while (retryCount < maxRetries) {
            try {
                return createJdbcConnection(sourceConfig);
            } catch (OceanBaseConnectorException e) {
                lastException = (SQLException) e.getCause();
                retryCount++;
                if (retryCount < maxRetries) {
                    long sleepTime = (long) Math.pow(2, retryCount) * 1000;
                    log.warn(
                            "Failed to create JDBC connection, retry {}/{} after {}ms",
                            retryCount,
                            maxRetries,
                            sleepTime);
                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        throw new OceanBaseConnectorException(
                ILLEGAL_ARGUMENT,
                "Failed to create JDBC connection after " + maxRetries + " retries",
                lastException);
    }

    // ==================== Chunk Splitting Query Methods ====================

    /** Query the minimum and maximum value of the column in the table. */
    public static Object[] queryMinMax(JdbcConnection jdbc, TableId tableId, String columnName)
            throws SQLException {
        final String minMaxQuery =
                String.format(
                        "SELECT MIN(%s), MAX(%s) FROM %s",
                        quote(columnName), quote(columnName), quote(tableId));
        return jdbc.queryAndMap(
                minMaxQuery,
                rs -> {
                    if (!rs.next()) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]",
                                        minMaxQuery));
                    }
                    return rowToArray(rs, 2);
                });
    }

    /** Query the minimum value of the column in the table, greater than excludedLowerBound. */
    public static Object queryMin(
            JdbcConnection jdbc, TableId tableId, String columnName, Object excludedLowerBound)
            throws SQLException {
        final String minQuery =
                String.format(
                        "SELECT MIN(%s) FROM %s WHERE %s > ?",
                        quote(columnName), quote(tableId), quote(columnName));
        return jdbc.prepareQueryAndMap(
                minQuery,
                ps -> ps.setObject(1, excludedLowerBound),
                rs -> {
                    if (!rs.next()) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]", minQuery));
                    }
                    return rs.getObject(1);
                });
    }

    /**
     * Query approximate row count of the table. OceanBase supports SHOW TABLE STATUS similar to
     * MySQL.
     */
    public static long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId)
            throws SQLException {
        // Use SHOW TABLE STATUS for approximate row count
        final String useDatabaseStatement = String.format("USE %s;", quote(tableId.catalog()));
        final String rowCountQuery = String.format("SHOW TABLE STATUS LIKE '%s';", tableId.table());

        jdbc.execute(useDatabaseStatement);
        return jdbc.queryAndMap(
                rowCountQuery,
                rs -> {
                    if (!rs.next() || rs.getMetaData().getColumnCount() < 5) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]",
                                        rowCountQuery));
                    }
                    return rs.getLong(5); // Rows column
                });
    }

    /**
     * Sample data from column using skip-read and sort approach. This reads every Nth row to get a
     * sample of the data distribution.
     */
    public static Object[] skipReadAndSortSampleData(
            JdbcConnection jdbc, TableId tableId, String columnName, int inverseSamplingRate)
            throws Exception {
        final String sampleQuery =
                String.format("SELECT %s FROM %s", quote(columnName), quote(tableId));

        Statement stmt = null;
        ResultSet rs = null;

        List<Object> results = new ArrayList<>();
        try {
            stmt =
                    jdbc.connection()
                            .createStatement(
                                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

            stmt.setFetchSize(Integer.MIN_VALUE);
            rs = stmt.executeQuery(sampleQuery);

            int count = 0;
            while (rs.next()) {
                count++;
                if (count % 100000 == 0) {
                    log.info("Processing row index: {}", count);
                }
                if (count % inverseSamplingRate == 0) {
                    results.add(rs.getObject(1));
                }
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Thread interrupted");
                }
            }
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    log.error("Failed to close ResultSet", e);
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    log.error("Failed to close Statement", e);
                }
            }
        }
        Object[] resultsArray = results.toArray();
        Arrays.sort(resultsArray);
        return resultsArray;
    }

    /** Query the next chunk's maximum value. */
    public static Object queryNextChunkMax(
            JdbcConnection jdbc,
            TableId tableId,
            String splitColumnName,
            int chunkSize,
            Object includedLowerBound)
            throws SQLException {
        String quotedColumn = quote(splitColumnName);
        String query =
                String.format(
                        "SELECT MAX(%s) FROM ("
                                + "SELECT %s FROM %s WHERE %s >= ? ORDER BY %s ASC LIMIT %s"
                                + ") AS T",
                        quotedColumn,
                        quotedColumn,
                        quote(tableId),
                        quotedColumn,
                        quotedColumn,
                        chunkSize);
        return jdbc.prepareQueryAndMap(
                query,
                ps -> ps.setObject(1, includedLowerBound),
                rs -> {
                    if (!rs.next()) {
                        throw new SQLException(
                                String.format(
                                        "No result returned after running query [%s]", query));
                    }
                    return rs.getObject(1);
                });
    }

    /** Build split scan query for snapshot reading. */
    public static String buildSplitScanQuery(
            TableId tableId, SeaTunnelRowType rowType, boolean isFirstSplit, boolean isLastSplit) {
        return buildSplitQuery(tableId, rowType, isFirstSplit, isLastSplit, -1, true);
    }

    /** Build split query with conditions based on primary key ranges. */
    private static String buildSplitQuery(
            TableId tableId,
            SeaTunnelRowType rowType,
            boolean isFirstSplit,
            boolean isLastSplit,
            int limitSize,
            boolean isScanningData) {
        final String condition;

        if (isFirstSplit && isLastSplit) {
            condition = null;
        } else if (isFirstSplit) {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(rowType, sql, " <= ?");
            if (isScanningData) {
                sql.append(" AND NOT (");
                addPrimaryKeyColumnsToCondition(rowType, sql, " = ?");
                sql.append(")");
            }
            condition = sql.toString();
        } else if (isLastSplit) {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(rowType, sql, " > ?");
            condition = sql.toString();
        } else {
            final StringBuilder sql = new StringBuilder();
            addPrimaryKeyColumnsToCondition(rowType, sql, " > ?");
            if (isScanningData) {
                sql.append(" AND NOT (");
                addPrimaryKeyColumnsToCondition(rowType, sql, " = ?");
                sql.append(")");
            }
            sql.append(" AND ");
            addPrimaryKeyColumnsToCondition(rowType, sql, " <= ?");
            condition = sql.toString();
        }

        if (limitSize > 0) {
            return buildSelectWithRowLimits(tableId, limitSize, "*", condition);
        }
        return buildSelectWithoutRowLimit(tableId, "*", condition);
    }

    private static String buildSelectWithoutRowLimit(
            TableId tableId, String projection, String condition) {
        final StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(projection).append(" FROM ");
        sql.append(quote(tableId));
        if (condition != null) {
            sql.append(" WHERE ").append(condition);
        }
        return sql.toString();
    }

    private static String buildSelectWithRowLimits(
            TableId tableId, int limit, String projection, String condition) {
        final StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(projection).append(" FROM ");
        sql.append(quote(tableId));
        if (condition != null) {
            sql.append(" WHERE ").append(condition);
        }
        sql.append(" LIMIT ").append(limit);
        return sql.toString();
    }

    private static void addPrimaryKeyColumnsToCondition(
            SeaTunnelRowType rowType, StringBuilder sql, String predicate) {
        for (int i = 0; i < rowType.getFieldNames().length; i++) {
            if (i > 0) {
                sql.append(" AND ");
            }
            sql.append(quote(rowType.getFieldNames()[i])).append(predicate);
        }
    }

    /** Quote identifier for SQL query. */
    private static String quote(String identifier) {
        return "`" + identifier + "`";
    }

    /** Quote table identifier for SQL query. */
    private static String quote(TableId tableId) {
        return tableId.catalog() != null
                ? quote(tableId.catalog()) + "." + quote(tableId.table())
                : quote(tableId.table());
    }
}

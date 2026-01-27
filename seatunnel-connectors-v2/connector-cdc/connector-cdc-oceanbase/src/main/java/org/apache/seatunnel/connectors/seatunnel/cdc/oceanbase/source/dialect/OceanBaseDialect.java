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

package org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source.dialect;

import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source.enumerator.OceanBaseChunkSplitter;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source.fetch.OceanBaseFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source.fetch.OceanBaseScanFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source.fetch.OceanBaseStreamFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source.offset.OceanBaseOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.utils.OceanBaseUtils;

import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.DIALECT_NAME;

@Slf4j
public class OceanBaseDialect implements DataSourceDialect<OceanBaseSourceConfig> {

    @Override
    public String getName() {
        return DIALECT_NAME;
    }

    @Override
    public List<TableId> discoverDataCollections(OceanBaseSourceConfig sourceConfig) {
        List<TableId> tableIds = new ArrayList<>();

        try (Connection connection = OceanBaseUtils.createJdbcConnection(sourceConfig)) {
            DatabaseMetaData metaData = connection.getMetaData();

            // If table list is specified, use it; otherwise discover all tables
            if (sourceConfig.getTableList() != null && !sourceConfig.getTableList().isEmpty()) {
                for (String tablePattern : sourceConfig.getTableList()) {
                    String[] parts = tablePattern.split("\\.");
                    if (parts.length == 2) {
                        String database = parts[0];
                        String table = parts[1];

                        // Handle wildcard patterns
                        if (database.contains("*") || table.contains("*")) {
                            List<TableId> matchedTables =
                                    discoverTablesByPattern(metaData, database, table);
                            tableIds.addAll(matchedTables);
                        } else {
                            tableIds.add(TableId.parse(tablePattern));
                        }
                    }
                }
            } else {
                // Discover all tables in all databases
                tableIds.addAll(discoverAllTables(metaData, sourceConfig.getTenantName()));
            }

            log.info(
                    "Discovered {} tables for OceanBase CDC: {}",
                    tableIds.size(),
                    OceanBaseUtils.formatTableList(
                            tableIds.stream()
                                    .map(TableId::toString)
                                    .collect(java.util.stream.Collectors.toList())));

        } catch (SQLException e) {
            log.error("Failed to discover tables", e);
        }

        return tableIds;
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(OceanBaseSourceConfig sourceConfig) {
        // OceanBase table names are case-sensitive depending on the underlying file system
        return true;
    }

    @Override
    public ChunkSplitter createChunkSplitter(OceanBaseSourceConfig sourceConfig) {
        return new OceanBaseChunkSplitter(sourceConfig, this);
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(@Nonnull SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new OceanBaseScanFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            return new OceanBaseStreamFetchTask(sourceSplitBase.asIncrementalSplit());
        }
    }

    @Override
    public FetchTask.Context createFetchTaskContext(
            SourceSplitBase sourceSplitBase, OceanBaseSourceConfig sourceConfig) {
        return new OceanBaseFetchTaskContext(this, sourceConfig);
    }

    /** Display current offset for OceanBase */
    public OceanBaseOffset displayCurrentOffset() {
        long currentTimestamp = OceanBaseUtils.getCurrentTimestampSeconds();
        OceanBaseOffset offset =
                new OceanBaseOffset(currentTimestamp, String.valueOf(currentTimestamp));

        log.info(
                "Current OceanBase offset: timestamp={}, checkpoint={}",
                offset.getTimestamp(),
                offset.getCheckpoint());

        return offset;
    }

    /** Discover tables matching the pattern */
    private List<TableId> discoverTablesByPattern(
            DatabaseMetaData metaData, String databasePattern, String tablePattern)
            throws SQLException {
        List<TableId> tableIds = new ArrayList<>();

        // Convert wildcard to SQL pattern
        String dbPattern = databasePattern.replace("*", "%");
        String tblPattern = tablePattern.replace("*", "%");

        try (ResultSet rs =
                metaData.getTables(null, dbPattern, tblPattern, new String[] {"TABLE"})) {
            while (rs.next()) {
                String database = rs.getString("TABLE_SCHEM");
                String table = rs.getString("TABLE_NAME");
                if (database != null && table != null) {
                    tableIds.add(TableId.parse(database + "." + table));
                }
            }
        }

        return tableIds;
    }

    /** Discover all tables in all databases */
    private List<TableId> discoverAllTables(DatabaseMetaData metaData, String tenantName)
            throws SQLException {
        List<TableId> tableIds = new ArrayList<>();

        try (ResultSet rs = metaData.getTables(null, null, null, new String[] {"TABLE"})) {
            while (rs.next()) {
                String database = rs.getString("TABLE_SCHEM");
                String table = rs.getString("TABLE_NAME");

                // Skip system databases
                if (database != null && !isSystemDatabase(database)) {
                    tableIds.add(TableId.parse(database + "." + table));
                }
            }
        }

        return tableIds;
    }

    /** Check if the database is a system database */
    private boolean isSystemDatabase(String database) {
        return database.equalsIgnoreCase("information_schema")
                || database.equalsIgnoreCase("mysql")
                || database.equalsIgnoreCase("oceanbase")
                || database.equalsIgnoreCase("performance_schema")
                || database.equalsIgnoreCase("sys");
    }
}

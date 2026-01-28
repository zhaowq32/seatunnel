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

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
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
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.START_TIMESTAMP_US;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.SYS_PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.SYS_USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.URL;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.WORKING_MODE;

/** Factory for OceanBase CDC incremental source. */
@AutoService(Factory.class)
@Slf4j
public class OceanBaseIncrementalSourceFactory extends BaseChangeStreamTableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return OceanBaseIncrementalSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URL, LOG_PROXY_HOST, LOG_PROXY_PORT, USERNAME, PASSWORD, TABLE_NAMES)
                .optional(
                        CLUSTER_URL,
                        ROOT_SERVER_LIST,
                        BLACK_TABLE_LIST,
                        STARTUP_TIMESTAMP,
                        SERVER_TIME_ZONE,
                        WORKING_MODE,
                        START_TIMESTAMP_US,
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

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> restoreSource(
                    TableSourceFactoryContext context, List<CatalogTable> restoreTables) {
        return () -> {
            List<CatalogTable> catalogTables;

            // OceanBase CDC does not support schema evolution yet.
            // When restoring from checkpoint, use the checkpoint table structure directly
            // to ensure consistency with the state when checkpoint was created.
            if (!restoreTables.isEmpty()) {
                log.info(
                        "Restoring OceanBase CDC source from checkpoint with {} tables",
                        restoreTables.size());
                catalogTables = restoreTables;
            } else {
                // Load the JDBC driver in to DriverManager
                try {
                    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                } catch (Exception e) {
                    log.warn(
                            "Failed to load JDBC driver {}",
                            "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                            e);
                }
                catalogTables =
                        CatalogTableUtil.getCatalogTables(
                                context.getOptions(), context.getClassLoader());
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
            return (SeaTunnelSource<T, SplitT, StateT>)
                    new OceanBaseIncrementalSource<>(context.getOptions(), catalogTables);
        };
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return OceanBaseIncrementalSource.class;
    }
}

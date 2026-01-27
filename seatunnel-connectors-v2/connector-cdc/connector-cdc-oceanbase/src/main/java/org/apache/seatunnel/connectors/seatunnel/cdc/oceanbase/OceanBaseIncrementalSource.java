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
import org.apache.seatunnel.connectors.cdc.debezium.row.DebeziumJsonDeserializeSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceConfigProvider;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source.dialect.OceanBaseDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.source.offset.OceanBaseOffsetFactory;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Optional;

/** OceanBase CDC incremental source. */
public class OceanBaseIncrementalSource<T> extends IncrementalSource<T, OceanBaseSourceConfig>
        implements SupportParallelism {

    static final String IDENTIFIER = "OceanBase-CDC";

    public OceanBaseIncrementalSource(ReadonlyConfig options, List<CatalogTable> catalogTables) {
        super(options, catalogTables);
    }

    @Override
    public Option<StartupMode> getStartupModeOption() {
        return OceanBaseSourceOptions.STARTUP_MODE;
    }

    @Override
    public Option<StopMode> getStopModeOption() {
        return OceanBaseSourceOptions.STOP_MODE;
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public SourceConfig.Factory<OceanBaseSourceConfig> createSourceConfigFactory(
            @Nonnull ReadonlyConfig config) {
        OceanBaseSourceConfigProvider.Builder builder =
                OceanBaseSourceConfigProvider.newBuilder()
                        .jdbcUrl(config.get(OceanBaseSourceOptions.JDBC_URL))
                        .username(config.get(OceanBaseSourceOptions.USERNAME))
                        .password(config.get(OceanBaseSourceOptions.PASSWORD))
                        .logproxyHost(config.get(OceanBaseSourceOptions.LOGPROXY_HOST))
                        .logproxyPort(config.get(OceanBaseSourceOptions.LOGPROXY_PORT));

        Optional.ofNullable(config.get(OceanBaseSourceOptions.CLUSTER_URL))
                .ifPresent(builder::clusterUrl);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.ROOT_SERVER_LIST))
                .ifPresent(builder::rootServerList);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.WHITE_TABLE_LIST))
                .ifPresent(builder::whiteTableList);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.BLACK_TABLE_LIST))
                .ifPresent(builder::blackTableList);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.START_TIMESTAMP))
                .ifPresent(builder::startTimestamp);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.SERVER_TIME_ZONE))
                .ifPresent(builder::serverTimeZone);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.WORKING_MODE))
                .ifPresent(builder::workingMode);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.START_TIMESTAMP_US))
                .ifPresent(builder::startTimestampUS);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.CLUSTER_ID))
                .ifPresent(builder::clusterId);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.SYS_USERNAME))
                .ifPresent(builder::sysUsername);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.SYS_PASSWORD))
                .ifPresent(builder::sysPassword);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.SPLIT_SIZE))
                .ifPresent(builder::splitSize);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.BATCH_SIZE))
                .ifPresent(builder::batchSize);
        Optional.ofNullable(config.get(OceanBaseSourceOptions.EXACTLY_ONCE))
                .ifPresent(builder::exactlyOnce);
        Optional.ofNullable(startupConfig).ifPresent(builder::startupConfig);
        Optional.ofNullable(stopConfig).ifPresent(builder::stopConfig);
        return builder.validate();
    }

    @SuppressWarnings("unchecked")
    @Override
    public DebeziumDeserializationSchema<T> createDebeziumDeserializationSchema(
            ReadonlyConfig config) {
        // For OceanBase, we use the Debezium JSON format
        return (DebeziumDeserializationSchema<T>)
                new DebeziumJsonDeserializeSchema(
                        config.get(JdbcSourceOptions.DEBEZIUM_PROPERTIES));
    }

    @Override
    public DataSourceDialect<OceanBaseSourceConfig> createDataSourceDialect(ReadonlyConfig config) {
        return new OceanBaseDialect();
    }

    @Override
    public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
        return new OceanBaseOffsetFactory();
    }

    @Override
    public Optional<String> driverName() {
        return Optional.of("com.oceanbase.jdbc.Driver");
    }
}

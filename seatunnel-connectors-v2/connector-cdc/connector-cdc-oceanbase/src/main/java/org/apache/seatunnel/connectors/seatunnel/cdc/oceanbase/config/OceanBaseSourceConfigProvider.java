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

package org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config;

import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.exception.OceanBaseConnectorException;

import java.util.Objects;

import static org.apache.seatunnel.common.exception.CommonErrorCode.ILLEGAL_ARGUMENT;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.EXACTLY_ONCE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.LOGPROXY_PORT;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.SERVER_TIME_ZONE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.SPLIT_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.START_TIMESTAMP;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.START_TIMESTAMP_US;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.WORKING_MODE;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class OceanBaseSourceConfigProvider {
    private OceanBaseSourceConfigProvider() {}

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder implements SourceConfig.Factory<OceanBaseSourceConfig> {
        private String jdbcUrl;
        private String logproxyHost;
        private int logproxyPort = LOGPROXY_PORT.defaultValue();
        private String clusterUrl;
        private String rootServerList;
        private String username;
        private String password;
        private String whiteTableList;
        private String blackTableList;
        private Long startTimestamp = START_TIMESTAMP.defaultValue();
        private String serverTimeZone = SERVER_TIME_ZONE.defaultValue();
        private String workingMode = WORKING_MODE.defaultValue();
        private Long startTimestampUS = START_TIMESTAMP_US.defaultValue();
        private String clusterId;
        private String sysUsername;
        private String sysPassword;
        private int splitSize = SPLIT_SIZE.defaultValue();
        private int batchSize = BATCH_SIZE.defaultValue();
        private boolean exactlyOnce = EXACTLY_ONCE.defaultValue();
        private StartupConfig startupConfig;
        private StopConfig stopConfig;

        public Builder jdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public Builder logproxyHost(String logproxyHost) {
            this.logproxyHost = logproxyHost;
            return this;
        }

        public Builder logproxyPort(int logproxyPort) {
            this.logproxyPort = logproxyPort;
            return this;
        }

        public Builder clusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;
            return this;
        }

        public Builder rootServerList(String rootServerList) {
            this.rootServerList = rootServerList;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder whiteTableList(String whiteTableList) {
            this.whiteTableList = whiteTableList;
            return this;
        }

        public Builder blackTableList(String blackTableList) {
            this.blackTableList = blackTableList;
            return this;
        }

        public Builder startTimestamp(Long startTimestamp) {
            this.startTimestamp = startTimestamp;
            return this;
        }

        public Builder serverTimeZone(String serverTimeZone) {
            this.serverTimeZone = serverTimeZone;
            return this;
        }

        public Builder workingMode(String workingMode) {
            this.workingMode = workingMode;
            return this;
        }

        public Builder startTimestampUS(Long startTimestampUS) {
            this.startTimestampUS = startTimestampUS;
            return this;
        }

        public Builder clusterId(String clusterId) {
            this.clusterId = clusterId;
            return this;
        }

        public Builder sysUsername(String sysUsername) {
            this.sysUsername = sysUsername;
            return this;
        }

        public Builder sysPassword(String sysPassword) {
            this.sysPassword = sysPassword;
            return this;
        }

        public Builder splitSize(int splitSize) {
            checkArgument(splitSize > 0, "splitSize must be positive");
            this.splitSize = splitSize;
            return this;
        }

        public Builder batchSize(int batchSize) {
            checkArgument(batchSize > 0, "batchSize must be positive");
            this.batchSize = batchSize;
            return this;
        }

        public Builder exactlyOnce(boolean exactlyOnce) {
            this.exactlyOnce = exactlyOnce;
            return this;
        }

        public Builder startupConfig(StartupConfig startupConfig) {
            this.startupConfig = Objects.requireNonNull(startupConfig);
            if (startupConfig.getStartupMode() != StartupMode.INITIAL
                    && startupConfig.getStartupMode() != StartupMode.LATEST
                    && startupConfig.getStartupMode() != StartupMode.TIMESTAMP) {
                throw new OceanBaseConnectorException(
                        ILLEGAL_ARGUMENT,
                        "Unsupported startup mode " + startupConfig.getStartupMode());
            }
            return this;
        }

        public Builder stopConfig(StopConfig stopConfig) {
            this.stopConfig = Objects.requireNonNull(stopConfig);
            if (stopConfig.getStopMode() != StopMode.NEVER) {
                throw new OceanBaseConnectorException(
                        ILLEGAL_ARGUMENT,
                        String.format("The %s mode is not supported.", stopConfig.getStopMode()));
            }
            return this;
        }

        public Builder validate() {
            checkNotNull(jdbcUrl, "jdbc-url must be provided");
            checkNotNull(username, "username must be provided");
            checkNotNull(password, "password must be provided");
            return this;
        }

        @Override
        public OceanBaseSourceConfig create(int subtask) {
            return new OceanBaseSourceConfig(
                    jdbcUrl,
                    logproxyHost,
                    logproxyPort,
                    clusterUrl,
                    rootServerList,
                    username,
                    password,
                    whiteTableList,
                    blackTableList,
                    startTimestamp,
                    serverTimeZone,
                    workingMode,
                    startTimestampUS,
                    clusterId,
                    sysUsername,
                    sysPassword,
                    splitSize,
                    batchSize,
                    exactlyOnce,
                    startupConfig,
                    stopConfig);
        }
    }
}

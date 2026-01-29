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
import org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.utils.OceanBaseUtils;

import java.util.List;
import java.util.Objects;

import static org.apache.seatunnel.common.exception.CommonErrorCode.ILLEGAL_ARGUMENT;
import static org.apache.seatunnel.connectors.cdc.base.option.SourceOptions.STARTUP_TIMESTAMP;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.EXACTLY_ONCE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.LOG_PROXY_PORT;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.SERVER_TIME_ZONE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oceanbase.config.OceanBaseSourceOptions.WORKING_MODE;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkState;

public class OceanBaseSourceConfigProvider {
    private OceanBaseSourceConfigProvider() {}

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder implements SourceConfig.Factory<OceanBaseSourceConfig> {
        private String url;
        private String logProxyHost;
        private int logProxyPort = LOG_PROXY_PORT.defaultValue();
        private String clusterUrl;
        private String rootServerList;
        private String username;
        private String password;
        private String tenant;
        private List<String> tableNames;
        private String blackTableList;
        private Long startTimestamp = STARTUP_TIMESTAMP.defaultValue();
        private String serverTimeZone = SERVER_TIME_ZONE.defaultValue();
        private String workingMode = WORKING_MODE.defaultValue();
        private String clusterId;
        private String sysUsername;
        private String sysPassword;
        private int batchSize = BATCH_SIZE.defaultValue();
        private boolean exactlyOnce = EXACTLY_ONCE.defaultValue();
        private StartupConfig startupConfig;
        private StopConfig stopConfig;

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public Builder logProxyHost(String logProxyHost) {
            this.logProxyHost = logProxyHost;
            return this;
        }

        public Builder logProxyPort(int logProxyPort) {
            this.logProxyPort = logProxyPort;
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
            this.tenant = OceanBaseUtils.extractTenant(username);
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder tableNames(List<String> tableNames) {
            this.tableNames = tableNames;
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
            if (startupConfig.getStartupMode() != StartupMode.TIMESTAMP) {
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
            String tenantRegex = "^[^@#]+@[^@#]+(#[^@#]+)?$";
            checkState(username.matches(tenantRegex), "The username format is username@tenant.");
            return this;
        }

        @Override
        public OceanBaseSourceConfig create(int subtask) {
            return new OceanBaseSourceConfig(
                    url,
                    logProxyHost,
                    logProxyPort,
                    clusterUrl,
                    rootServerList,
                    username,
                    password,
                    tenant,
                    OceanBaseUtils.buildWhiteTableList(tenant, tableNames),
                    blackTableList,
                    startTimestamp,
                    serverTimeZone,
                    workingMode,
                    clusterId,
                    sysUsername,
                    sysPassword,
                    batchSize,
                    exactlyOnce,
                    startupConfig,
                    stopConfig);
        }
    }
}

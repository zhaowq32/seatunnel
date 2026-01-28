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

import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class OceanBaseSourceConfig implements SourceConfig {

    private static final long serialVersionUID = 1L;

    private final String url;
    private final String logProxyHost;
    private final int logProxyPort;
    private final String clusterUrl;
    private final String rootServerList;
    private final String username;
    private final String password;
    private final String tenant;
    private final String whiteTableList;
    private final String blackTableList;
    private final Long startTimestamp;
    private final String serverTimeZone;
    private final String workingMode;
    private final Long startTimestampUS;
    private final String clusterId;
    private final String sysUsername;
    private final String sysPassword;
    private final int batchSize;
    private final boolean exactlyOnce;
    private final StartupConfig startupConfig;
    private final StopConfig stopConfig;

    public OceanBaseSourceConfig(
            String url,
            String logProxyHost,
            int logProxyPort,
            String clusterUrl,
            String rootServerList,
            String username,
            String password,
            String tenant,
            String whiteTableList,
            String blackTableList,
            Long startTimestamp,
            String serverTimeZone,
            String workingMode,
            Long startTimestampUS,
            String clusterId,
            String sysUsername,
            String sysPassword,
            int batchSize,
            boolean exactlyOnce,
            StartupConfig startupConfig,
            StopConfig stopConfig) {
        this.url = url;
        this.logProxyHost = logProxyHost;
        this.logProxyPort = logProxyPort;
        this.clusterUrl = clusterUrl;
        this.rootServerList = rootServerList;
        this.username = username;
        this.password = password;
        this.tenant = tenant;
        this.whiteTableList = whiteTableList;
        this.blackTableList = blackTableList;
        this.startTimestamp = startTimestamp;
        this.serverTimeZone = serverTimeZone;
        this.workingMode = workingMode;
        this.startTimestampUS = startTimestampUS;
        this.clusterId = clusterId;
        this.sysUsername = sysUsername;
        this.sysPassword = sysPassword;
        this.batchSize = batchSize;
        this.exactlyOnce = exactlyOnce;
        this.startupConfig = startupConfig;
        this.stopConfig = stopConfig;
    }

    @Override
    public StartupConfig getStartupConfig() {
        return startupConfig;
    }

    @Override
    public StopConfig getStopConfig() {
        return stopConfig;
    }

    @Override
    public int getSplitSize() {
        throw new UnsupportedOperationException(
                "OceanBase CDC only supports streaming mode, splitSize is not supported");
    }

    @Override
    public boolean isExactlyOnce() {
        return exactlyOnce;
    }
}

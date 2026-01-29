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

/**
 * OceanBase CDC 数据源配置类
 *
 * <p>该类封装了 OceanBase CDC 连接器所需的所有配置参数,包括:
 *
 * <ul>
 *   <li>数据库连接配置(JDBC URL, 用户名, 密码等)
 *   <li>LogProxy 服务配置(主机, 端口等)
 *   <li>集群配置(集群URL, Root Server列表等)
 *   <li>表过滤配置(白名单, 黑名单)
 *   <li>启动和停止配置
 * </ul>
 *
 * <p>注意: OceanBase CDC 仅支持流式读取模式,不支持快照分片。
 */
@Getter
@EqualsAndHashCode
public class OceanBaseSourceConfig implements SourceConfig {

    private static final long serialVersionUID = 1L;

    /** JDBC 连接 URL,用于连接 OceanBase 数据库 */
    private final String url;

    /** LogProxy 服务的主机地址 */
    private final String logProxyHost;

    /** LogProxy 服务的端口号,默认为 2983 */
    private final int logProxyPort;

    /** OceanBase 集群 URL (企业版使用) */
    private final String clusterUrl;

    /** OceanBase Root Server 列表 (社区版使用),格式: ip1:rpc_port1:sql_port1;ip2:rpc_port2:sql_port2 */
    private final String rootServerList;

    /** 数据库用户名 */
    private final String username;

    /** 数据库密码 */
    private final String password;

    /** 租户名称 */
    private final String tenant;

    /** 表白名单,格式: tenant.database.table,多个表用 | 分隔 */
    private final String whiteTableList;

    /** 表黑名单,格式: tenant.database.table,多个表用 | 分隔 */
    private final String blackTableList;

    /** 启动时间戳(秒),用于指定从哪个时间点开始读取数据 */
    private final Long startTimestamp;

    /** 服务器时区,影响时间相关字段的读取,默认 +08:00 */
    private final String serverTimeZone;

    /** libobcdc 工作模式,可选值: "storage" 或 "memory" */
    private final String workingMode;

    /** OceanBase 集群 ID */
    private final String clusterId;

    /** 系统租户用户名,用于访问系统租户 */
    private final String sysUsername;

    /** 系统租户密码 */
    private final String sysPassword;

    /** 批次大小,用于设置变更事件队列的最大队列大小,默认 1024 */
    private final int batchSize;

    /** 是否启用 Exactly-Once 语义,默认 false */
    private final boolean exactlyOnce;

    /** 启动配置,定义如何启动 CDC 读取 */
    private final StartupConfig startupConfig;

    /** 停止配置,定义何时停止 CDC 读取 */
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

    /**
     * 获取分片大小
     *
     * <p>OceanBase CDC 仅支持流式模式,不支持快照分片,因此此方法会抛出异常。
     *
     * @return 永远不会返回
     * @throws UnsupportedOperationException 因为 OceanBase CDC 不支持分片
     */
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

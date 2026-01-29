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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.SingleChoiceOption;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;

import java.util.Collections;

/**
 * OceanBase CDC 数据源配置选项类
 *
 * <p>定义了 OceanBase CDC 连接器的所有可配置选项,包括必选和可选参数。
 *
 * <p>必选配置包括: JDBC URL, LogProxy 主机/端口, 用户名, 密码, 表名列表
 *
 * <p>可选配置包括: 集群配置, 时区, 工作模式, 租户信息等
 */
public class OceanBaseSourceOptions extends SourceOptions {

    /** OceanBase 方言名称 */
    public static final String DIALECT_NAME = "OceanBase";

    /**
     * LogProxy 服务主机地址
     *
     * <p>OceanBase 日志代理服务的主机名或 IP 地址。
     *
     * <p>LogProxy 是 OceanBase CDC 的核心组件,负责提供实时数据变更流。
     */
    public static final Option<String> LOG_PROXY_HOST =
            Options.key("log_proxy_host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hostname or IP address of the OceanBase log proxy service.");

    /**
     * LogProxy 服务端口号
     *
     * <p>OceanBase 日志代理服务的端口号,默认为 2983。
     */
    public static final Option<Integer> LOG_PROXY_PORT =
            Options.key("log_proxy_port")
                    .intType()
                    .defaultValue(2983)
                    .withDescription("Port number of the OceanBase log proxy service.");

    /**
     * OceanBase 集群 URL (企业版)
     *
     * <p>仅在使用 OceanBase 企业版时需要配置。
     *
     * <p>该 URL 用于获取 OceanBase 集群节点信息。
     */
    public static final Option<String> CLUSTER_URL =
            Options.key("cluster_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "This URL is used to obtain information about OceanBase cluster nodes, and it needs to be configured only when using the enterprise version of OceanBase.");

    /**
     * Root Server 列表 (社区版)
     *
     * <p>仅在使用 OceanBase 社区版时需要配置。
     *
     * <p>格式: ip1:rpc_port1:sql_port1;ip2:rpc_port2:sql_port2
     *
     * <p>多个节点地址用分号(;)分隔。
     */
    public static final Option<String> ROOT_SERVER_LIST =
            Options.key("root_server_list")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The list of nodes in the OceanBase cluster needs to be configured only when using the community edition of OceanBase.Format: (multiple addresses are supported, separated by ';') ip1:rpc_port1:sql_port1;ip2:rpc_port2:sql_port2.");

    /** 数据库用户名,用于连接 OceanBase 数据库 */
    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Username for connecting to OceanBase.");

    /** 数据库密码,用于连接 OceanBase 数据库 */
    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password for connecting to OceanBase.");

    /**
     * 表黑名单
     *
     * <p>用于排除不需要监控的表。
     *
     * <p>格式: tenant.database.table,支持 fnmatch 模式匹配。
     *
     * <p>多个表用竖线(|)分隔。
     */
    public static final Option<String> BLACK_TABLE_LIST =
            Options.key("black_table_list")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The blacklist for monitoring data changes uses `fnmatch` to match patterns in the format `tenant.database.table`, with multiple values separated by `|`.");

    /**
     * 服务器时区
     *
     * <p>连接使用的时区,默认为 +08:00 (北京时间)。
     *
     * <p>该配置会影响时间相关字段的读取和解析。
     */
    public static final Option<String> SERVER_TIME_ZONE =
            Options.key("server_time_zone")
                    .stringType()
                    .defaultValue("+08:00")
                    .withDescription(
                            "The time zone used for the connection; this value will affect how time-related fields are read.");

    /**
     * libobcdc 工作模式
     *
     * <p>可选值: "storage" 或 "memory",默认为 "storage"。
     *
     * <p>storage 模式会将数据存储在磁盘,memory 模式全部使用内存。
     */
    public static final Option<String> WORKING_MODE =
            Options.key("working_mode")
                    .stringType()
                    .defaultValue("storage")
                    .withDescription(
                            "The working mode of libobcdc can be set to either \"storage\" or \"memory\".");

    /** OceanBase 集群 ID */
    public static final Option<String> CLUSTER_ID =
            Options.key("cluster_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The cluster ID of the OceanBase cluster.");

    /**
     * 系统租户用户名
     *
     * <p>OceanBase 集群中 sys 租户的用户名。
     *
     * <p>某些操作需要 sys 租户权限才能执行。
     */
    public static final Option<String> SYS_USERNAME =
            Options.key("sys_username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The username of the sys tenant user in the OceanBase cluster..");

    /** 系统租户密码 */
    public static final Option<String> SYS_PASSWORD =
            Options.key("sys_password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The password of the sys tenant user in the OceanBase cluster.");

    /**
     * 批次大小
     *
     * <p>用于设置数据变更事件队列的最大队列大小,默认为 1024。
     *
     * <p>该参数影响内存使用和吞吐量,较大的值可以提高吞吐量但会增加内存占用。
     *
     * <p>注意: OceanBase CDC 仅支持流式模式,此参数用于流式读取时的队列配置。
     */
    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "The max queue size for change event queue in streaming mode. Default is 1024.");

    /**
     * 是否启用 Exactly-Once 语义
     *
     * <p>默认为 false。
     *
     * <p>启用后可以保证每条数据只被处理一次,但会增加性能开销。
     */
    public static final Option<Boolean> EXACTLY_ONCE =
            Options.key("exactly_once")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable exactly-once semantic. Default is false.");

    /**
     * CDC 源的启动模式
     *
     * <p>OceanBase CDC 仅支持 TIMESTAMP 模式。
     *
     * <p>需要配合 startup_timestamp 参数指定具体的启动时间点。
     */
    public static final SingleChoiceOption<StartupMode> STARTUP_MODE =
            Options.key(SourceOptions.STARTUP_MODE_KEY)
                    .singleChoice(
                            StartupMode.class, Collections.singletonList(StartupMode.TIMESTAMP))
                    .defaultValue(StartupMode.TIMESTAMP)
                    .withDescription(
                            "Optional startup mode for CDC source, valid enumerations are \"timestamp\"");

    /**
     * CDC 源的停止模式
     *
     * <p>OceanBase CDC 仅支持 NEVER 模式,即持续读取直到任务被取消。
     */
    public static final SingleChoiceOption<StopMode> STOP_MODE =
            Options.key(SourceOptions.STOP_MODE_KEY)
                    .singleChoice(StopMode.class, Collections.singletonList(StopMode.NEVER))
                    .defaultValue(StopMode.NEVER)
                    .withDescription(
                            "Optional stop mode for CDC source, valid enumerations are \"never\"");
}

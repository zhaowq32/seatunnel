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

public class OceanBaseSourceOptions extends SourceOptions {

    public static final String DIALECT_NAME = "OceanBase";

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("JDBC url of the OceanBase LogProxy server.");

    public static final Option<String> LOG_PROXY_HOST =
            Options.key("log_proxy_host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Hostname or IP address of OceanBase log proxy service.");

    public static final Option<Integer> LOG_PROXY_PORT =
            Options.key("log_proxy_port")
                    .intType()
                    .defaultValue(2983)
                    .withDescription("Port number of OceanBase log proxy service.");

    public static final Option<String> CLUSTER_URL =
            Options.key("cluster_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "This URL is used to obtain information about OceanBase cluster nodes, and it needs to be configured only when using the enterprise version of OceanBase.");

    public static final Option<String> ROOT_SERVER_LIST =
            Options.key("root_server_list")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The list of nodes in the OceanBase cluster needs to be configured only when using the community edition of OceanBase.Format: (multiple addresses are supported, separated by ';') ip1:rpc_port1:sql_port1;ip2:rpc_port2:sql_port2.");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Username for connecting to OceanBase.");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password for connecting to OceanBase.");

    public static final Option<String> BLACK_TABLE_LIST =
            Options.key("black_table_list")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The blacklist for monitoring data changes uses `fnmatch` to match patterns in the format `tenant.database.table`, with multiple values separated by `|`.");

    public static final Option<String> SERVER_TIME_ZONE =
            Options.key("server_time_zone")
                    .stringType()
                    .defaultValue("+08:00")
                    .withDescription(
                            "The time zone used for the connection; this value will affect how time-related fields are read.");

    public static final Option<String> WORKING_MODE =
            Options.key("working_mode")
                    .stringType()
                    .defaultValue("storage")
                    .withDescription(
                            "The working mode of libobcdc can be set to either \"storage\" or \"memory\".");

    public static final Option<Long> START_TIMESTAMP_US =
            Options.key("start_timestamp_us")
                    .longType()
                    .defaultValue(0L)
                    .withDescription(
                            "The starting timestamp for the subscription data, in microseconds. A value of 0 means it will start from the current time.");

    public static final Option<String> CLUSTER_ID =
            Options.key("cluster_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The cluster ID of the OceanBase cluster.");

    public static final Option<String> SYS_USERNAME =
            Options.key("sys_username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The username of the sys tenant user in the OceanBase cluster..");

    public static final Option<String> SYS_PASSWORD =
            Options.key("sys_password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The password of the sys tenant user in the OceanBase cluster.");

    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription("The batch size for reading snapshot data. Default is 1024.");

    public static final Option<Boolean> EXACTLY_ONCE =
            Options.key("exactly_once")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable exactly-once semantic. Default is false.");

    public static final SingleChoiceOption<StartupMode> STARTUP_MODE =
            Options.key(SourceOptions.STARTUP_MODE_KEY)
                    .singleChoice(
                            StartupMode.class, Collections.singletonList(StartupMode.TIMESTAMP))
                    .defaultValue(StartupMode.TIMESTAMP)
                    .withDescription(
                            "Optional startup mode for CDC source, valid enumerations are \"timestamp\"");

    public static final SingleChoiceOption<StopMode> STOP_MODE =
            Options.key(SourceOptions.STOP_MODE_KEY)
                    .singleChoice(StopMode.class, Collections.singletonList(StopMode.NEVER))
                    .defaultValue(StopMode.NEVER)
                    .withDescription(
                            "Optional stop mode for CDC source, valid enumerations are \"never\"");
}

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

package org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;

import java.util.List;

/** TDengine CDC incremental source options. */
public class TdengineIncrementalSourceOptions {

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The TDengine JDBC URL. Use WebSocket mode (recommended, no native library): "
                                    + "jdbc:TAOS-WS://host:6041/db");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The username for TDengine authentication");

    public static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password for TDengine authentication");

    public static final Option<String> DATABASE =
            Options.key("database")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The TDengine database name");

    public static final Option<String> STABLE =
            Options.key("stable")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The TDengine super table name to subscribe to");

    public static final Option<List<String>> TABLE_NAMES =
            Options.key("table-names")
                    .listType()
                    .noDefaultValue()
                    .withDescription(
                            "The TDengine sub-table names to capture, separated by comma. If not specified, all sub-tables will be captured.");

    public static final Option<String> TOPIC =
            Options.key("topic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The TDengine TMQ topic name. If not specified, one will be auto-created based on database and stable name.");

    public static final Option<String> GROUP_ID =
            Options.key("group.id")
                    .stringType()
                    .defaultValue("seatunnel-tdengine-cdc")
                    .withDescription("The TDengine TMQ consumer group id");

    public static final Option<Integer> POLL_INTERVAL_MS =
            Options.key("poll-interval.ms")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The poll interval in milliseconds for fetching data from TDengine TMQ");

    public static final Option<Integer> POLL_MAX_BATCH_SIZE =
            Options.key("poll-max-batch-size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "The maximum number of records to fetch per poll from TMQ consumer");

    public static final Option<StartupMode> STARTUP_MODE =
            Options.key("startup.mode")
                    .enumType(StartupMode.class)
                    .defaultValue(StartupMode.INITIAL)
                    .withDescription(
                            "The startup mode for CDC. "
                                    + "INITIAL: snapshot existing data then stream, "
                                    + "LATEST: stream from latest offset only, "
                                    + "EARLIEST: stream from earliest available offset");

    public static final Option<StopMode> STOP_MODE =
            Options.key("stop.mode")
                    .enumType(StopMode.class)
                    .defaultValue(StopMode.NEVER)
                    .withDescription(
                            "The stop mode for CDC. Default is NEVER (unbounded streaming).");

    public static final Option<Boolean> ENABLE_AUTO_COMMIT =
            Options.key("enable.auto.commit")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to enable auto commit of TMQ offsets. Default is false for exactly-once semantics.");

    public static final Option<Boolean> MSG_WITH_TABLE_NAME =
            Options.key("msg.with.table.name")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to include the table name in the consumed message. Default is true.");
}

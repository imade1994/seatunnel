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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;

import java.util.List;

/** Factory for creating {@link TdengineSourceConfig}. */
public class TdengineSourceConfigFactory implements SourceConfig.Factory<TdengineSourceConfig> {

    private static final long serialVersionUID = 1L;

    private String url;
    private String username;
    private String password;
    private String database;
    private String stable;
    private List<String> tableNames;
    private String topic;
    private String groupId;
    private int pollIntervalMs;
    private int pollMaxBatchSize;
    private boolean enableAutoCommit;
    private boolean msgWithTableName;
    private StartupConfig startupConfig;
    private StopConfig stopConfig;
    private int splitSize;
    private boolean exactlyOnce;

    public TdengineSourceConfigFactory url(String url) {
        this.url = url;
        return this;
    }

    public TdengineSourceConfigFactory username(String username) {
        this.username = username;
        return this;
    }

    public TdengineSourceConfigFactory password(String password) {
        this.password = password;
        return this;
    }

    public TdengineSourceConfigFactory database(String database) {
        this.database = database;
        return this;
    }

    public TdengineSourceConfigFactory stable(String stable) {
        this.stable = stable;
        return this;
    }

    public TdengineSourceConfigFactory tableNames(List<String> tableNames) {
        this.tableNames = tableNames;
        return this;
    }

    public TdengineSourceConfigFactory topic(String topic) {
        this.topic = topic;
        return this;
    }

    public TdengineSourceConfigFactory groupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public TdengineSourceConfigFactory pollIntervalMs(int pollIntervalMs) {
        this.pollIntervalMs = pollIntervalMs;
        return this;
    }

    public TdengineSourceConfigFactory pollMaxBatchSize(int pollMaxBatchSize) {
        this.pollMaxBatchSize = pollMaxBatchSize;
        return this;
    }

    public TdengineSourceConfigFactory enableAutoCommit(boolean enableAutoCommit) {
        this.enableAutoCommit = enableAutoCommit;
        return this;
    }

    public TdengineSourceConfigFactory msgWithTableName(boolean msgWithTableName) {
        this.msgWithTableName = msgWithTableName;
        return this;
    }

    public TdengineSourceConfigFactory startupOptions(StartupConfig startupConfig) {
        this.startupConfig = startupConfig;
        return this;
    }

    public TdengineSourceConfigFactory stopOptions(StopConfig stopConfig) {
        this.stopConfig = stopConfig;
        return this;
    }

    public TdengineSourceConfigFactory splitSize(int splitSize) {
        this.splitSize = splitSize;
        return this;
    }

    public TdengineSourceConfigFactory exactlyOnce(boolean exactlyOnce) {
        this.exactlyOnce = exactlyOnce;
        return this;
    }

    /** Populate this factory from a ReadonlyConfig. */
    public TdengineSourceConfigFactory fromReadonlyConfig(ReadonlyConfig config) {
        this.url = config.get(TdengineIncrementalSourceOptions.URL);
        this.username = config.get(TdengineIncrementalSourceOptions.USERNAME);
        this.password = config.get(TdengineIncrementalSourceOptions.PASSWORD);
        this.database = config.get(TdengineIncrementalSourceOptions.DATABASE);
        this.stable = config.get(TdengineIncrementalSourceOptions.STABLE);
        this.tableNames = config.get(TdengineIncrementalSourceOptions.TABLE_NAMES);
        this.topic = config.get(TdengineIncrementalSourceOptions.TOPIC);
        this.groupId = config.get(TdengineIncrementalSourceOptions.GROUP_ID);
        this.pollIntervalMs = config.get(TdengineIncrementalSourceOptions.POLL_INTERVAL_MS);
        this.pollMaxBatchSize = config.get(TdengineIncrementalSourceOptions.POLL_MAX_BATCH_SIZE);
        this.enableAutoCommit = config.get(TdengineIncrementalSourceOptions.ENABLE_AUTO_COMMIT);
        this.msgWithTableName = config.get(TdengineIncrementalSourceOptions.MSG_WITH_TABLE_NAME);
        this.splitSize = config.get(SourceOptions.SNAPSHOT_SPLIT_SIZE);
        return this;
    }

    @Override
    public TdengineSourceConfig create(int subtask) {
        return new TdengineSourceConfig(
                url,
                username,
                password,
                database,
                stable,
                tableNames,
                topic,
                groupId,
                pollIntervalMs,
                pollMaxBatchSize,
                enableAutoCommit,
                msgWithTableName,
                startupConfig,
                stopConfig,
                splitSize,
                exactlyOnce);
    }
}

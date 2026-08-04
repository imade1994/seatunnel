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

import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;

import com.taosdata.jdbc.tmq.TMQConstants;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.List;
import java.util.Objects;
import java.util.Properties;

/** TDengine CDC Source configuration. */
@Getter
@EqualsAndHashCode
public class TdengineSourceConfig implements SourceConfig {

    private static final long serialVersionUID = 1L;

    private final String url;
    private final String username;
    private final String password;
    private final String database;
    private final String stable;
    private final List<String> tableNames;
    private final String topic;
    private final String groupId;
    private final int pollIntervalMs;
    private final int pollMaxBatchSize;
    private final boolean enableAutoCommit;
    private final boolean msgWithTableName;
    private final StartupConfig startupConfig;
    private final StopConfig stopConfig;
    private final int splitSize;
    private final boolean exactlyOnce;

    public TdengineSourceConfig(
            String url,
            String username,
            String password,
            String database,
            String stable,
            List<String> tableNames,
            String topic,
            String groupId,
            int pollIntervalMs,
            int pollMaxBatchSize,
            boolean enableAutoCommit,
            boolean msgWithTableName,
            StartupConfig startupConfig,
            StopConfig stopConfig,
            int splitSize,
            boolean exactlyOnce) {
        this.url = Objects.requireNonNull(url);
        this.username = username;
        this.password = password;
        this.database = Objects.requireNonNull(database);
        this.stable = Objects.requireNonNull(stable);
        this.tableNames = tableNames;
        this.topic = topic;
        this.groupId = groupId;
        this.pollIntervalMs = pollIntervalMs;
        this.pollMaxBatchSize = pollMaxBatchSize;
        this.enableAutoCommit = enableAutoCommit;
        this.msgWithTableName = msgWithTableName;
        this.startupConfig = startupConfig;
        this.stopConfig = stopConfig;
        this.splitSize = splitSize;
        this.exactlyOnce = exactlyOnce;
    }

    /** Build the TMQ consumer properties for TaosConsumer (WebSocket mode, 3.2.5+). */
    public Properties buildTmqProperties() {
        Properties props = new Properties();
        // Parse JDBC URL to extract host and port for TMQ bootstrap servers
        String host = "127.0.0.1";
        String port = "6041";
        if (url != null) {
            String urlPart = url;
            if (urlPart.startsWith("jdbc:TAOS-WS://")) {
                urlPart = urlPart.substring("jdbc:TAOS-WS://".length());
            } else if (urlPart.startsWith("jdbc:TAOS-RS://")) {
                urlPart = urlPart.substring("jdbc:TAOS-RS://".length());
            } else if (urlPart.startsWith("jdbc:TAOS://")) {
                urlPart = urlPart.substring("jdbc:TAOS://".length());
            }
            if (urlPart.contains("/")) {
                urlPart = urlPart.substring(0, urlPart.indexOf("/"));
            }
            if (urlPart.contains("?")) {
                urlPart = urlPart.substring(0, urlPart.indexOf("?"));
            }
            if (urlPart.contains(":")) {
                String[] parts = urlPart.split(":");
                host = parts[0];
                port = parts[1];
            } else if (!urlPart.isEmpty()) {
                host = urlPart;
            }
        }

        // WebSocket-based TMQ config (taos-jdbcdriver 3.2.5+)
        props.setProperty(TMQConstants.BOOTSTRAP_SERVERS, host + ":" + port);
        // Force WebSocket connection type (no native libs needed)
        props.setProperty(TMQConstants.CONNECT_TYPE, "ws");
        props.setProperty(TMQConstants.GROUP_ID, groupId);
        props.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, String.valueOf(enableAutoCommit));
        props.setProperty(TMQConstants.AUTO_OFFSET_RESET, "earliest");
        props.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, String.valueOf(msgWithTableName));
        if (username != null) {
            props.setProperty(TMQConstants.CONNECT_USER, username);
        }
        if (password != null) {
            props.setProperty(TMQConstants.CONNECT_PASS, password);
        }
        // Deliberately omit VALUE_DESERIALIZER so TaosConsumer defaults
        // to `new MapDeserializer()` internally (bytecode offset 102-110)
        // instead of `Class.forName("MapDeserializer")` (offset 85-99).
        // This avoids classloader delegation issues in SeaTunnel's
        // SeaTunnelChildFirstClassLoader environment.
        return props;
    }

    /**
     * Get the effective topic name, generating one from the database and stable name if not set.
     */
    public String getEffectiveTopic() {
        if (topic != null) {
            return topic;
        }
        return database + "_" + stable;
    }

    /**
     * Build the SQL to create the TMQ topic if it doesn't exist.
     *
     * <p>The topic is created as a SELECT on the super table. If specific table names are provided,
     * the SELECT is scoped to those sub-tables.
     */
    public String buildCreateTopicSQL() {
        String topicName = getEffectiveTopic();
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TOPIC IF NOT EXISTS ").append(topicName);
        sb.append(" AS SELECT tbname, * FROM ").append(database).append(".").append(stable);

        if (tableNames != null && !tableNames.isEmpty()) {
            sb.append(" WHERE tbname IN (");
            for (int i = 0; i < tableNames.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append("'").append(tableNames.get(i)).append("'");
            }
            sb.append(")");
        }
        return sb.toString();
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
        return splitSize;
    }

    @Override
    public boolean isExactlyOnce() {
        return exactlyOnce;
    }
}

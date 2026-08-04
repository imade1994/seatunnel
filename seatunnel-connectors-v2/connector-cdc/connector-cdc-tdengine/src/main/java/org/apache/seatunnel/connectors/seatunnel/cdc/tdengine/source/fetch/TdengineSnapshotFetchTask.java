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

package org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.fetch;

import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.config.TdengineSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.offset.TdengineOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.offset.TdengineOffsetConstants;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Map;

/**
 * Snapshot fetch task for TDengine CDC initial phase.
 *
 * <p>Queries existing data from the TDengine super table using JDBC and converts each row into a
 * SourceRecord with INSERT operation type.
 */
@Slf4j
public class TdengineSnapshotFetchTask implements FetchTask<SourceSplitBase> {

    private final SnapshotSplit snapshotSplit;
    private volatile boolean taskRunning = false;

    public TdengineSnapshotFetchTask(SnapshotSplit snapshotSplit) {
        this.snapshotSplit = snapshotSplit;
    }

    /** Register JDBC WebSocket driver manually (bypasses classloader checks). */
    private static void registerWebSocketDriver() {
        try {
            Class<?> clazz =
                    Class.forName(
                            "com.taosdata.jdbc.ws.WebSocketDriver",
                            true,
                            Thread.currentThread().getContextClassLoader());
            java.sql.Driver driver = (java.sql.Driver) clazz.getDeclaredConstructor().newInstance();
            java.sql.DriverManager.registerDriver(driver);
        } catch (Exception e) {
            log.warn(
                    "Failed to register WebSocketDriver, will try default DriverManager: {}",
                    e.getMessage());
        }
    }

    @Override
    public void execute(Context context) throws Exception {
        TdengineFetchTaskContext taskContext = (TdengineFetchTaskContext) context;
        TdengineSourceConfig sourceConfig = taskContext.getSourceConfig();
        ChangeEventQueue<DataChangeEvent> queue = taskContext.getQueue();

        this.taskRunning = true;

        String query = buildSnapshotQuery(sourceConfig);

        // Register WebSocket driver explicitly to avoid classloader issues
        registerWebSocketDriver();

        // Send LOW watermark before snapshot (same pattern as MongoDB CDC).
        // Use empty vgroupId so it sorts before any actual TMQ vgroup ID
        // (numeric strings like "1","2",...). Otherwise shouldEmit's isAfter
        // check would filter all incremental records.
        TdengineOffset lowWatermark = new TdengineOffset("", 0L);
        queue.enqueue(
                new DataChangeEvent(
                        WatermarkEvent.create(
                                TdengineRecordUtils.buildSourcePartition(""),
                                "__tdengine_snapshot_watermarks",
                                snapshotSplit.splitId(),
                                WatermarkKind.LOW,
                                lowWatermark)));

        log.info("Executing snapshot query: {}", query);

        int rowCount = 0;
        try (Connection connection =
                        DriverManager.getConnection(
                                sourceConfig.getUrl(),
                                sourceConfig.getUsername(),
                                sourceConfig.getPassword());
                Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next() && taskRunning) {
                SourceRecord record =
                        convertRowToSourceRecord(sourceConfig, rs, metaData, columnCount);
                if (record != null) {
                    queue.enqueue(new DataChangeEvent(record));
                    rowCount++;
                }
            }

            log.info(
                    "Snapshot completed, {} rows fetched for table {}",
                    rowCount,
                    sourceConfig.getStable());

            // Send HIGH watermark after snapshot — this triggers the split completion
            // and the transition from snapshot phase to incremental phase.
            if (taskRunning) {
                TdengineOffset highWatermark = new TdengineOffset("", rowCount);
                queue.enqueue(
                        new DataChangeEvent(
                                WatermarkEvent.create(
                                        TdengineRecordUtils.buildSourcePartition(""),
                                        "__tdengine_snapshot_watermarks",
                                        snapshotSplit.splitId(),
                                        WatermarkKind.HIGH,
                                        highWatermark)));
            }
        } finally {
            taskRunning = false;
        }
    }

    private String buildSnapshotQuery(TdengineSourceConfig sourceConfig) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT tbname, * FROM ").append(sourceConfig.getDatabase());
        sb.append(".").append(sourceConfig.getStable());

        // If specific table names were provided, filter by them
        if (sourceConfig.getTableNames() != null && !sourceConfig.getTableNames().isEmpty()) {
            sb.append(" WHERE tbname IN (");
            for (int i = 0; i < sourceConfig.getTableNames().size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append("'").append(sourceConfig.getTableNames().get(i)).append("'");
            }
            sb.append(")");
        }

        sb.append(" ORDER BY ts ASC");
        return sb.toString();
    }

    private SourceRecord convertRowToSourceRecord(
            TdengineSourceConfig sourceConfig,
            ResultSet rs,
            ResultSetMetaData metaData,
            int columnCount)
            throws Exception {

        // Build value schema dynamically
        org.apache.kafka.connect.data.SchemaBuilder valueSchemaBuilder =
                org.apache.kafka.connect.data.SchemaBuilder.struct()
                        .field(
                                TdengineOffsetConstants.OP_FIELD,
                                org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
                        .field(
                                TdengineOffsetConstants.DB_NAME_FIELD,
                                org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
                        .field(
                                TdengineOffsetConstants.TABLE_NAME_FIELD,
                                org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
                        .field(
                                TdengineOffsetConstants.TS_MS_FIELD,
                                org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA)
                        .field(
                                TdengineOffsetConstants.SOURCE_FIELD,
                                org.apache.kafka.connect.data.SchemaBuilder.struct()
                                        .field(
                                                TdengineOffsetConstants.TS_MS_FIELD,
                                                org.apache.kafka.connect.data.Schema
                                                        .OPTIONAL_INT64_SCHEMA)
                                        .field(
                                                "db",
                                                org.apache.kafka.connect.data.Schema
                                                        .OPTIONAL_STRING_SCHEMA)
                                        .field(
                                                "table",
                                                org.apache.kafka.connect.data.Schema
                                                        .OPTIONAL_STRING_SCHEMA)
                                        .optional()
                                        .build());

        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i).toLowerCase();
            String columnType = metaData.getColumnTypeName(i).toUpperCase();
            org.apache.kafka.connect.data.Schema fieldSchema;
            switch (columnType) {
                case "TINYINT":
                case "SMALLINT":
                case "INT":
                case "INTEGER":
                    fieldSchema = org.apache.kafka.connect.data.Schema.OPTIONAL_INT32_SCHEMA;
                    break;
                case "BIGINT":
                    fieldSchema = org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA;
                    break;
                case "FLOAT":
                    fieldSchema = org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT32_SCHEMA;
                    break;
                case "DOUBLE":
                    fieldSchema = org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA;
                    break;
                case "BOOL":
                    fieldSchema = org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA;
                    break;
                case "TIMESTAMP":
                    fieldSchema = org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA;
                    break;
                default:
                    fieldSchema = org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;
                    break;
            }
            valueSchemaBuilder.field(columnName, fieldSchema);
        }

        org.apache.kafka.connect.data.Schema valueSchema = valueSchemaBuilder.build();
        Struct value = new Struct(valueSchema);

        // Set metadata fields
        value.put(TdengineOffsetConstants.OP_FIELD, TdengineOffsetConstants.OP_INSERT);
        value.put(TdengineOffsetConstants.DB_NAME_FIELD, sourceConfig.getDatabase());

        // Extract sub-table name (tbname)
        String tbnameValue = null;
        try {
            tbnameValue = rs.getString("tbname");
        } catch (Exception ignored) {
            // tbname column may not be present
        }
        if (tbnameValue == null) {
            tbnameValue = sourceConfig.getStable(); // fallback to super table name
        }
        String tableName = sourceConfig.getDatabase() + "." + tbnameValue;
        value.put(TdengineOffsetConstants.TABLE_NAME_FIELD, tableName);
        value.put(TdengineOffsetConstants.TS_MS_FIELD, System.currentTimeMillis());

        // Build source struct using the schema defined in valueSchema.
        // Must include db/table fields for SourceRecordUtils.getTableId().
        org.apache.kafka.connect.data.Field sourceField =
                valueSchema.field(TdengineOffsetConstants.SOURCE_FIELD);
        if (sourceField != null) {
            Struct sourceStruct = new Struct(sourceField.schema());
            sourceStruct.put(TdengineOffsetConstants.TS_MS_FIELD, System.currentTimeMillis());
            sourceStruct.put("db", sourceConfig.getDatabase());
            sourceStruct.put("table", sourceConfig.getStable());
            value.put(sourceField, sourceStruct);
        }

        // Set column values
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i).toLowerCase();
            Object columnValue = rs.getObject(i);
            // TDengine JDBC driver returns VARCHAR/BINARY as byte[], convert to String.
            // TIMESTAMP columns use INT64 in the Kafka Connect schema, so keep as epoch-millis;
            // the deserializer's coerceType will convert Long → LocalDateTime.
            if (columnValue instanceof byte[]) {
                columnValue = new String((byte[]) columnValue);
            } else if (columnValue instanceof Timestamp) {
                columnValue = ((Timestamp) columnValue).getTime();
            }
            value.put(columnName, columnValue);
        }

        // Build key
        Struct key =
                new Struct(
                        org.apache.kafka.connect.data.SchemaBuilder.struct()
                                .field(
                                        TdengineOffsetConstants.TABLE_NAME_FIELD,
                                        org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
                                .build());
        key.put(TdengineOffsetConstants.TABLE_NAME_FIELD, tableName);

        String topic = sourceConfig.getDatabase() + "." + sourceConfig.getStable();
        Map<String, String> partition = TdengineRecordUtils.buildSourcePartition("");
        Map<String, String> offset = TdengineRecordUtils.buildSourceOffset("", 0);

        return TdengineRecordUtils.buildSourceRecord(partition, offset, topic, key, value);
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public void shutdown() {
        taskRunning = false;
    }

    @Override
    public SourceSplitBase getSplit() {
        return snapshotSplit;
    }
}

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

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.config.TdengineSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.offset.TdengineOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.offset.TdengineOffsetConstants;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import com.taosdata.jdbc.tmq.ConsumerRecord;
import com.taosdata.jdbc.tmq.ConsumerRecords;
import com.taosdata.jdbc.tmq.TaosConsumer;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Fetch task for TDengine TMQ streaming (incremental) phase.
 *
 * <p>Uses {@link TaosConsumer} to subscribe to a TDengine TMQ topic and poll for change events.
 * Each record is a {@code Map<String, Object>} that is converted to a {@link SourceRecord} and
 * enqueued into the {@link ChangeEventQueue}.
 */
@Slf4j
public class TdengineStreamFetchTask implements FetchTask<SourceSplitBase> {

    private final IncrementalSplit streamSplit;
    private volatile boolean taskRunning = false;
    private TaosConsumer<Map<String, Object>> taosConsumer;

    public TdengineStreamFetchTask(IncrementalSplit streamSplit) {
        this.streamSplit = streamSplit;
    }

    @Override
    public void execute(Context context) throws Exception {
        TdengineFetchTaskContext taskContext = (TdengineFetchTaskContext) context;
        TdengineSourceConfig sourceConfig = taskContext.getSourceConfig();
        ChangeEventQueue<DataChangeEvent> queue = taskContext.getQueue();

        this.taskRunning = true;

        try {
            // Step 1: Ensure the TMQ topic exists by creating it via JDBC
            String topicName = sourceConfig.getEffectiveTopic();
            String createTopicSQL = sourceConfig.buildCreateTopicSQL();
            ensureTopicExists(sourceConfig, createTopicSQL, topicName);

            // Step 2: Create the TaosConsumer and subscribe.
            // VALUE_DESERIALIZER is deliberately not set in TMQ properties so
            // TaosConsumer defaults to `new MapDeserializer()` internally.
            // Set context classloader so ServiceLoader inside ConsumerManager
            // can discover WSConsumerFactory from the connector jar's SPI file.
            log.info("Creating TaosConsumer for TDengine TMQ topic: {}", topicName);
            ClassLoader originalCL = Thread.currentThread().getContextClassLoader();
            Thread.currentThread()
                    .setContextClassLoader(TdengineStreamFetchTask.class.getClassLoader());
            try {
                taosConsumer = new TaosConsumer<>(sourceConfig.buildTmqProperties());
            } finally {
                Thread.currentThread().setContextClassLoader(originalCL);
            }
            taosConsumer.subscribe(Collections.singletonList(topicName));

            log.info("TDengine TMQ consumer started, polling for changes on topic: {}", topicName);

            while (taskRunning) {
                try {
                    ConsumerRecords<Map<String, Object>> records =
                            taosConsumer.poll(Duration.ofMillis(sourceConfig.getPollIntervalMs()));

                    if (records == null || records.isEmpty()) {
                        continue;
                    }

                    // ConsumerRecords implements Iterable<ConsumerRecord<Map<String,Object>>>
                    for (ConsumerRecord<Map<String, Object>> record : records) {
                        if (!taskRunning) {
                            break;
                        }

                        Map<String, Object> valueMap = record.value();
                        if (valueMap == null || valueMap.isEmpty()) {
                            continue;
                        }

                        // Use vGroupId and offset from the ConsumerRecord
                        String vgroupId = String.valueOf(record.getVGroupId());
                        long recordOffset = record.getOffset();

                        SourceRecord sourceRecord =
                                convertToSourceRecord(
                                        sourceConfig, valueMap, vgroupId, recordOffset);

                        if (sourceRecord != null) {
                            if (!isBoundedRead()) {
                                queue.enqueue(new DataChangeEvent(sourceRecord));
                            } else {
                                Offset currentOffset =
                                        TdengineRecordUtils.getStreamOffset(sourceRecord);
                                if (currentOffset.isAtOrBefore(streamSplit.getStopOffset())) {
                                    queue.enqueue(new DataChangeEvent(sourceRecord));
                                }
                                if (currentOffset.isAtOrAfter(streamSplit.getStopOffset())) {
                                    // Send end watermark
                                    SourceRecord watermark =
                                            WatermarkEvent.create(
                                                    TdengineRecordUtils.buildSourcePartition(
                                                            "stream"),
                                                    "__tdengine_watermarks",
                                                    streamSplit.splitId(),
                                                    WatermarkKind.END,
                                                    currentOffset);
                                    queue.enqueue(new DataChangeEvent(watermark));
                                    log.info("Reached stop offset, finishing stream read");
                                    taskRunning = false;
                                    break;
                                }
                            }
                        }
                    }

                    // Commit offset after processing batch
                    if (!sourceConfig.isEnableAutoCommit() && taskRunning) {
                        taosConsumer.commitSync();
                    }
                } catch (Exception e) {
                    if (taskRunning) {
                        log.warn("Error polling from TDengine TMQ: {}", e.getMessage(), e);
                        TimeUnit.MILLISECONDS.sleep(sourceConfig.getPollIntervalMs());
                    }
                }
            }
        } finally {
            taskRunning = false;
            if (taosConsumer != null) {
                try {
                    taosConsumer.unsubscribe();
                    taosConsumer.close();
                } catch (Exception e) {
                    log.warn("Error closing TaosConsumer", e);
                }
            }
        }
    }

    /**
     * Ensure the TMQ topic exists by executing {@code CREATE TOPIC IF NOT EXISTS} via JDBC. If the
     * topic already exists, this is a no-op.
     */
    private void ensureTopicExists(
            TdengineSourceConfig sourceConfig, String createTopicSQL, String topicName) {
        try (Connection conn =
                        DriverManager.getConnection(
                                sourceConfig.getUrl(),
                                sourceConfig.getUsername(),
                                sourceConfig.getPassword());
                Statement stmt = conn.createStatement()) {
            stmt.execute(createTopicSQL);
            log.info("Ensured TMQ topic exists: {} (SQL: {})", topicName, createTopicSQL);
        } catch (SQLException e) {
            log.warn(
                    "Failed to auto-create TMQ topic '{}' (may already exist or insufficient privileges): {}",
                    topicName,
                    e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private SourceRecord convertToSourceRecord(
            TdengineSourceConfig sourceConfig,
            Map<String, Object> valueMap,
            String vgroupId,
            long recordOffset) {
        try {
            long timestamp = System.currentTimeMillis();

            Map<String, String> partition = TdengineRecordUtils.buildSourcePartition(vgroupId);
            Map<String, String> sourceOffset =
                    TdengineRecordUtils.buildSourceOffset(vgroupId, recordOffset);

            // Build key: table name
            org.apache.kafka.connect.data.Schema keySchema =
                    org.apache.kafka.connect.data.SchemaBuilder.struct()
                            .field(
                                    TdengineOffsetConstants.TABLE_NAME_FIELD,
                                    org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
                            .build();
            Struct key = new Struct(keySchema);
            // Extract table name from the record
            String tableName = sourceConfig.getDatabase() + "." + sourceConfig.getStable();
            if (valueMap.containsKey("tbname")) {
                Object tbnameObj = valueMap.get("tbname");
                tableName = sourceConfig.getDatabase() + "." + tbnameObj;
            }
            key.put(TdengineOffsetConstants.TABLE_NAME_FIELD, tableName);

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

            // Add all data fields from the TMQ record
            for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
                String fieldName = entry.getKey();
                if ("_db".equals(fieldName)) {
                    continue;
                }
                Object val = entry.getValue();
                if (val == null) {
                    valueSchemaBuilder.field(
                            fieldName, org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA);
                } else if (val instanceof Long
                        || val instanceof Integer
                        || val instanceof Short
                        || val instanceof Byte) {
                    valueSchemaBuilder.field(
                            fieldName, org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA);
                } else if (val instanceof Double || val instanceof Float) {
                    valueSchemaBuilder.field(
                            fieldName,
                            org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA);
                } else if (val instanceof Boolean) {
                    valueSchemaBuilder.field(
                            fieldName,
                            org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA);
                } else if (val instanceof Timestamp || val instanceof java.util.Date) {
                    valueSchemaBuilder.field(
                            fieldName, org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA);
                } else if (val instanceof byte[]) {
                    valueSchemaBuilder.field(
                            fieldName, org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA);
                } else {
                    valueSchemaBuilder.field(
                            fieldName, org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA);
                }
            }

            org.apache.kafka.connect.data.Schema valueSchema = valueSchemaBuilder.build();
            Struct value = new Struct(valueSchema);

            value.put(TdengineOffsetConstants.OP_FIELD, TdengineOffsetConstants.OP_INSERT);
            value.put(TdengineOffsetConstants.DB_NAME_FIELD, sourceConfig.getDatabase());
            value.put(TdengineOffsetConstants.TABLE_NAME_FIELD, tableName);
            value.put(TdengineOffsetConstants.TS_MS_FIELD, timestamp);

            // Build source struct. Must include db/table for SourceRecordUtils.getTableId().
            org.apache.kafka.connect.data.Field sourceField =
                    valueSchema.field(TdengineOffsetConstants.SOURCE_FIELD);
            if (sourceField != null) {
                Struct sourceStruct = new Struct(sourceField.schema());
                sourceStruct.put(TdengineOffsetConstants.TS_MS_FIELD, timestamp);
                sourceStruct.put("db", sourceConfig.getDatabase());
                sourceStruct.put("table", sourceConfig.getStable());
                value.put(sourceField, sourceStruct);
            }

            for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
                String fieldName = entry.getKey();
                if ("_db".equals(fieldName)) {
                    continue;
                }
                Object val = entry.getValue();
                // TDengine JDBC driver returns VARCHAR/BINARY as byte[], convert to String.
                // TIMESTAMP columns use INT64 in the Kafka Connect schema, so keep as epoch-
                // millis; the deserializer's coerceType will convert Long → LocalDateTime.
                if (val instanceof byte[]) {
                    val = new String((byte[]) val);
                } else if (val instanceof Timestamp) {
                    val = ((Timestamp) val).getTime();
                } else if (val instanceof java.util.Date) {
                    val = ((java.util.Date) val).getTime();
                }
                value.put(fieldName, val);
            }

            String topic = sourceConfig.getDatabase() + "." + sourceConfig.getStable();

            return TdengineRecordUtils.buildSourceRecord(
                    partition, sourceOffset, topic, key, value);
        } catch (Exception e) {
            log.error("Failed to convert TDengine record to SourceRecord: {}", e.getMessage(), e);
            return null;
        }
    }

    private boolean isBoundedRead() {
        return !TdengineOffset.NO_STOPPING_OFFSET.equals(streamSplit.getStopOffset());
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
        return streamSplit;
    }
}

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

import org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.offset.TdengineOffset;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.relational.TableId;

import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.offset.TdengineOffsetConstants.DB_NAME_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.offset.TdengineOffsetConstants.OP_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.offset.TdengineOffsetConstants.SOURCE_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.offset.TdengineOffsetConstants.TABLE_NAME_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.offset.TdengineOffsetConstants.TS_MS_FIELD;

/** Utility methods for creating and working with TDengine CDC SourceRecord objects. */
public final class TdengineRecordUtils {

    private TdengineRecordUtils() {}

    /** Schema for the value portion of a TDengine CDC record. */
    public static Schema buildValueSchema() {
        return SchemaBuilder.struct()
                .field(OP_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                .field(DB_NAME_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                .field(TABLE_NAME_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                .field(TS_MS_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
                .field(
                        SOURCE_FIELD,
                        SchemaBuilder.struct()
                                .field(TS_MS_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
                                .optional()
                                .build())
                .optional()
                .build();
    }

    /** Build a SourceRecord for TDengine CDC data. */
    public static SourceRecord buildSourceRecord(
            Map<String, String> sourcePartition,
            Map<String, String> sourceOffset,
            String topic,
            Struct key,
            Struct value) {
        Schema keySchema =
                SchemaBuilder.struct()
                        .field(TABLE_NAME_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                        .build();

        // Use the value's own schema to ensure consistency
        Schema valueSchema = value.schema();
        return new SourceRecord(
                sourcePartition,
                sourceOffset,
                topic,
                null, // kafka partition
                keySchema,
                key,
                valueSchema,
                value);
    }

    /** Build the source partition map for a given vgroup. */
    public static Map<String, String> buildSourcePartition(String vgroupId) {
        Map<String, String> partition = new HashMap<>();
        partition.put("vgroupId", vgroupId);
        return partition;
    }

    /** Build the source offset map for a given vgroup and position. */
    public static Map<String, String> buildSourceOffset(String vgroupId, long position) {
        Map<String, String> offset = new HashMap<>();
        offset.put("vgroupId", vgroupId);
        offset.put("offset", String.valueOf(position));
        return offset;
    }

    /** Extract a TableId from the SourceRecord. */
    public static TableId getTableId(SourceRecord record) {
        if (record.value() instanceof Struct) {
            Struct value = (Struct) record.value();
            String dbName = value.getString(DB_NAME_FIELD);
            String tableName = value.getString(TABLE_NAME_FIELD);
            if (dbName != null && tableName != null) {
                return new TableId(dbName, null, tableName);
            }
            if (tableName != null) {
                return TableId.parse(tableName);
            }
        }
        if (record.topic() != null) {
            return TableId.parse(record.topic());
        }
        return TableId.parse("unknown");
    }

    /** Extract the stream offset from a SourceRecord. */
    public static TdengineOffset getStreamOffset(SourceRecord record) {
        return new TdengineOffset((Map<String, String>) record.sourceOffset());
    }

    /** Check if the record is a data change record (not a heartbeat or watermark). */
    public static boolean isDataChangeRecord(SourceRecord record) {
        if (record.value() instanceof Struct) {
            Struct value = (Struct) record.value();
            return value.getString(OP_FIELD) != null;
        }
        return false;
    }
}

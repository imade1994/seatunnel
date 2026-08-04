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

package org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.deserializer;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.MetadataUtil;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.utils.SourceRecordUtils;
import org.apache.seatunnel.connectors.cdc.debezium.AbstractDebeziumDeserializationSchema;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.offset.TdengineOffsetConstants.OP_DELETE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.offset.TdengineOffsetConstants.OP_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.offset.TdengineOffsetConstants.OP_INSERT;
import static org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.offset.TdengineOffsetConstants.OP_UPDATE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.offset.TdengineOffsetConstants.TABLE_NAME_FIELD;

/**
 * Deserialization schema for TDengine CDC records.
 *
 * <p>Converts SourceRecord objects created from TMQ consumer records into SeaTunnelRow with proper
 * RowKind (INSERT/UPDATE/DELETE).
 */
@Slf4j
public class TdengineConnectorDeserializationSchema
        extends AbstractDebeziumDeserializationSchema<SeaTunnelRow> {

    private final List<CatalogTable> tables;
    private final Map<String, SeaTunnelRowType> tableRowTypes;
    private final boolean singleTableMode;
    private final SeaTunnelRowType defaultRowType;

    public TdengineConnectorDeserializationSchema(List<CatalogTable> tables) {
        super(new HashMap<>());
        this.tables = tables;
        this.tableRowTypes = new HashMap<>();
        // Follow the same pattern as SeaTunnelRowDebeziumDeserializeSchema:
        // when there's only one catalog table, use a single-table mode that
        // accepts records from any sub-table without name matching.
        // This is essential for TDengine CDC because records carry sub-table
        // names (e.g. isa.subtable_1) while the catalog only has the super
        // table (e.g. isa.sz_point_data).
        this.singleTableMode = tables.size() == 1;
        if (this.singleTableMode) {
            CatalogTable table = tables.get(0);
            this.defaultRowType = table.getSeaTunnelRowType();
            // Still register with the super table path for exact matches (e.g. DDL events)
            tableRowTypes.put(table.getTablePath().toString(), this.defaultRowType);
        } else {
            this.defaultRowType = null;
            for (CatalogTable table : tables) {
                tableRowTypes.put(table.getTablePath().toString(), table.getSeaTunnelRowType());
            }
        }
    }

    @Override
    public void deserialize(@Nonnull SourceRecord record, Collector<SeaTunnelRow> out)
            throws Exception {
        super.deserialize(record, out);

        Struct value = (Struct) record.value();
        if (value == null) {
            return;
        }

        String op = value.getString(OP_FIELD);
        String tableId = value.getString(TABLE_NAME_FIELD);

        // Look up the row type for this record. In single-table mode (the common
        // case for TDengine CDC), accept records from any sub-table directly.
        // In multi-table mode, match by exact table path with fallback to the
        // short table name portion.
        final SeaTunnelRowType rowType;
        if (singleTableMode) {
            rowType = defaultRowType;
        } else {
            SeaTunnelRowType resolved = tableRowTypes.get(tableId);
            if (resolved == null && tableId != null && tableId.contains(".")) {
                resolved = tableRowTypes.get(tableId.substring(tableId.lastIndexOf(".") + 1));
            }
            if (resolved == null) {
                log.warn(
                        "Skip record for unknown table: {} (available: {})",
                        tableId,
                        tableRowTypes.keySet());
                return;
            }
            rowType = resolved;
        }

        // Build SeaTunnelRow from Struct value fields
        String[] fieldNames = rowType.getFieldNames();
        SeaTunnelRow row = new SeaTunnelRow(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = fieldNames[i];
            Object fieldValue = null;
            // TDengine column names are lowercase in TMQ output
            try {
                fieldValue = value.get(fieldName.toLowerCase());
            } catch (org.apache.kafka.connect.errors.DataException ignored) {
                // field not in schema
            }
            if (fieldValue == null) {
                try {
                    fieldValue = value.get(fieldName);
                } catch (org.apache.kafka.connect.errors.DataException ignored) {
                    // field not in schema
                }
            }
            // Type coercion: convert to match target type if needed
            if (fieldValue != null) {
                SeaTunnelDataType<?> targetType = rowType.getFieldType(i);
                fieldValue = coerceType(fieldValue, targetType);
            }
            row.setField(i, fieldValue);
        }

        // Set row kind based on operation type
        switch (op) {
            case OP_INSERT:
                row.setRowKind(RowKind.INSERT);
                break;
            case OP_UPDATE:
                row.setRowKind(RowKind.UPDATE_AFTER);
                break;
            case OP_DELETE:
                row.setRowKind(RowKind.DELETE);
                break;
            default:
                row.setRowKind(RowKind.INSERT);
                break;
        }

        row.setTableId(tableId);

        Long fetchTimestamp = SourceRecordUtils.getFetchTimestamp(record);
        Long messageTimestamp = SourceRecordUtils.getMessageTimestamp(record);
        long delay = -1L;
        if (fetchTimestamp != null && messageTimestamp != null) {
            delay = fetchTimestamp - messageTimestamp;
        }
        MetadataUtil.setDelay(row, delay);
        MetadataUtil.setEventTime(row, fetchTimestamp);

        out.collect(row);
    }

    /** Coerce value to match the target SeaTunnel data type. */
    private static Object coerceType(Object value, SeaTunnelDataType<?> targetType) {
        if (value == null) {
            return null;
        }
        switch (targetType.getSqlType()) {
            case STRING:
                if (value instanceof byte[]) {
                    return new String((byte[]) value);
                }
                if (!(value instanceof String)) {
                    return String.valueOf(value);
                }
                break;
            case BIGINT:
            case INT:
            case SMALLINT:
            case TINYINT:
                if (value instanceof String) {
                    return Long.parseLong((String) value);
                }
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                break;
            case DOUBLE:
            case FLOAT:
                if (value instanceof String) {
                    return Double.parseDouble((String) value);
                }
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                break;
            case BOOLEAN:
                if (value instanceof String) {
                    return Boolean.parseBoolean((String) value);
                }
                break;
            case TIMESTAMP:
                // TMQ may return Long epoch-millis for TIMESTAMP columns.
                // Convert to LocalDateTime to match the declared schema type.
                if (value instanceof Long) {
                    return java.time.LocalDateTime.ofInstant(
                            java.time.Instant.ofEpochMilli((Long) value),
                            java.time.ZoneId.systemDefault());
                }
                if (value instanceof java.sql.Timestamp) {
                    return ((java.sql.Timestamp) value).toLocalDateTime();
                }
                break;
            default:
                break;
        }
        return value;
    }

    @Override
    public List<CatalogTable> getProducedType() {
        return tables;
    }
}

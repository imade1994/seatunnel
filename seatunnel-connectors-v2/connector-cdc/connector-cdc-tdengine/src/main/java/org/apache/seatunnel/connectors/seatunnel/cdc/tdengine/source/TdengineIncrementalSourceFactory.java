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

package org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.BaseChangeStreamTableSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.config.TdengineIncrementalSourceOptions;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

/** Factory for creating TDengine CDC incremental source. */
@AutoService(Factory.class)
@Slf4j
public class TdengineIncrementalSourceFactory extends BaseChangeStreamTableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return TdengineIncrementalSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        TdengineIncrementalSourceOptions.URL,
                        TdengineIncrementalSourceOptions.USERNAME,
                        TdengineIncrementalSourceOptions.PASSWORD,
                        TdengineIncrementalSourceOptions.DATABASE,
                        TdengineIncrementalSourceOptions.STABLE)
                .optional(
                        TdengineIncrementalSourceOptions.TABLE_NAMES,
                        TdengineIncrementalSourceOptions.TOPIC,
                        TdengineIncrementalSourceOptions.GROUP_ID,
                        TdengineIncrementalSourceOptions.POLL_INTERVAL_MS,
                        TdengineIncrementalSourceOptions.POLL_MAX_BATCH_SIZE,
                        TdengineIncrementalSourceOptions.ENABLE_AUTO_COMMIT,
                        TdengineIncrementalSourceOptions.MSG_WITH_TABLE_NAME)
                .optional(
                        TdengineIncrementalSourceOptions.STARTUP_MODE,
                        TdengineIncrementalSourceOptions.STOP_MODE)
                .conditional(
                        TdengineIncrementalSourceOptions.STARTUP_MODE,
                        StartupMode.SPECIFIC,
                        SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE,
                        SourceOptions.STARTUP_SPECIFIC_OFFSET_POS)
                .conditional(
                        TdengineIncrementalSourceOptions.STOP_MODE,
                        StopMode.SPECIFIC,
                        SourceOptions.STOP_SPECIFIC_OFFSET_FILE,
                        SourceOptions.STOP_SPECIFIC_OFFSET_POS)
                .conditional(
                        TdengineIncrementalSourceOptions.STARTUP_MODE,
                        StartupMode.TIMESTAMP,
                        SourceOptions.STARTUP_TIMESTAMP)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return TdengineIncrementalSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> restoreSource(
                    TableSourceFactoryContext context, List<CatalogTable> restoreTables) {
        return () -> {
            ReadonlyConfig config = context.getOptions();
            List<CatalogTable> catalogTables;

            if (!restoreTables.isEmpty()) {
                catalogTables = new ArrayList<>(restoreTables);
            } else {
                // Try CatalogTableUtil first (needs connector-tdengine as catalog factory)
                try {
                    catalogTables =
                            CatalogTableUtil.getCatalogTables(config, context.getClassLoader());
                } catch (Exception e) {
                    log.warn(
                            "Failed to get catalog tables via CatalogTableUtil: {}. "
                                    + "Falling back to JDBC metadata discovery.",
                            e.getMessage());
                    // Fallback: build CatalogTable from TDengine JDBC metadata
                    catalogTables = discoverCatalogTables(config);
                }
            }

            return (SeaTunnelSource<T, SplitT, StateT>)
                    new TdengineIncrementalSource(config, catalogTables);
        };
    }

    /**
     * Discover catalog tables by calling TDengine REST API to get schema. Uses HTTP directly
     * instead of JDBC to avoid classloader isolation issues during factory initialization.
     */
    private List<CatalogTable> discoverCatalogTables(ReadonlyConfig config) {
        String url = config.get(TdengineIncrementalSourceOptions.URL);
        String username = config.get(TdengineIncrementalSourceOptions.USERNAME);
        String password = config.get(TdengineIncrementalSourceOptions.PASSWORD);
        String database = extractString(config, TdengineIncrementalSourceOptions.DATABASE);
        String stable = extractString(config, TdengineIncrementalSourceOptions.STABLE);

        try {
            String restUrl = buildRestUrl(url);
            String auth =
                    Base64.getEncoder()
                            .encodeToString((username + ":" + password).getBytes("UTF-8"));
            String sql = "DESCRIBE " + database + "." + stable;

            String response = httpPost(restUrl, auth, sql);
            if (response == null) {
                return new ArrayList<>();
            }
            return parseDescribeResponse(response, database, stable);
        } catch (Exception e) {
            log.warn(
                    "Failed to discover schema via REST API: {}. "
                            + "Will use empty catalog - schema discovered at runtime.",
                    e.getMessage());
            return new ArrayList<>();
        }
    }

    /** Simple HTTP POST with Basic Auth (JDK 8 compatible). */
    private String httpPost(String urlStr, String auth, String body) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization", "Basic " + auth);
        conn.setRequestProperty("Content-Type", "text/plain");
        conn.setDoOutput(true);
        conn.setConnectTimeout(10000);
        conn.setReadTimeout(10000);
        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes("UTF-8"));
        }
        if (conn.getResponseCode() != 200) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br =
                new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"))) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        }
        conn.disconnect();
        return sb.toString();
    }

    /** Build REST API URL from JDBC URL. */
    private String buildRestUrl(String jdbcUrl) {
        String hp = jdbcUrl;
        if (hp.startsWith("jdbc:TAOS-WS://")) hp = hp.substring("jdbc:TAOS-WS://".length());
        else if (hp.startsWith("jdbc:TAOS-RS://")) hp = hp.substring("jdbc:TAOS-RS://".length());
        else if (hp.startsWith("jdbc:TAOS://")) hp = hp.substring("jdbc:TAOS://".length());
        if (hp.contains("/")) hp = hp.substring(0, hp.indexOf("/"));
        if (hp.contains("?")) hp = hp.substring(0, hp.indexOf("?"));
        return "http://" + hp + "/rest/sql";
    }

    /**
     * Parse TDengine REST DESCRIBE response into CatalogTable. Each data row has 7+ columns; we
     * only need the first two: [fieldName, fieldType, ...].
     */
    private List<CatalogTable> parseDescribeResponse(String json, String database, String stable) {
        // Find "data":[[...]] section
        int dataKeyIdx = json.indexOf("\"data\":");
        if (dataKeyIdx < 0) {
            return new ArrayList<>();
        }
        int dataStart = json.indexOf("[[", dataKeyIdx);
        if (dataStart < 0) {
            return new ArrayList<>();
        }
        int dataEnd = json.indexOf("]]", dataStart);
        if (dataEnd < 0) {
            return new ArrayList<>();
        }
        String dataSection = json.substring(dataStart + 2, dataEnd);

        TableSchema.Builder schemaBuilder = TableSchema.builder();
        // Split by "],[ to get each data row
        String[] rows = dataSection.split("\\],\\[");
        for (String row : rows) {
            // Match first pair of "name","type" in the row
            java.util.regex.Matcher m =
                    java.util.regex.Pattern.compile("\"([^\"]+)\",\"([^\"]+)\"").matcher(row);
            if (m.find()) {
                String fieldName = m.group(1);
                String fieldType = m.group(2);
                // Skip metadata rows (column_meta, not data)
                if ("field".equals(fieldName) || "code".equals(fieldName)) continue;
                // Skip non-column entries (numbers, empty strings from extra columns)
                if (fieldName.isEmpty() || Character.isDigit(fieldName.charAt(0))) continue;

                SeaTunnelDataType<?> dataType = mapTDengineType(fieldType);
                schemaBuilder.column(
                        PhysicalColumn.builder()
                                .name(fieldName)
                                .dataType(dataType)
                                .sourceType(fieldType)
                                .nullable(true)
                                .build());
            }
        }

        if (schemaBuilder.build().getColumns().isEmpty()) {
            return new ArrayList<>();
        }

        TableSchema schema = schemaBuilder.build();

        // Reorder: ts, tbname must come first
        List<Column> reordered = new ArrayList<>();
        for (Column col : schema.getColumns()) {
            if ("ts".equals(col.getName())) {
                reordered.add(col);
                reordered.add(
                        PhysicalColumn.builder()
                                .name("tbname")
                                .dataType(BasicType.STRING_TYPE)
                                .sourceType("VARCHAR")
                                .nullable(false)
                                .build());
            } else {
                reordered.add(col);
            }
        }

        schema =
                TableSchema.builder()
                        .columns(reordered)
                        .primaryKey(
                                PrimaryKey.of(
                                        "pk_ts_tbname", java.util.Arrays.asList("ts", "tbname")))
                        .build();

        TableIdentifier tableId = TableIdentifier.of(null, database, null, stable);
        CatalogTable catalogTable =
                CatalogTable.of(
                        tableId,
                        schema,
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "TDengine CDC source table");
        log.info("Discovered catalog table via REST: {}", catalogTable);
        return Collections.singletonList(catalogTable);
    }

    /**
     * Map TDengine column types to SeaTunnel data types. Replicates the logic of
     * connector-tdengine's {@code TDengineTypeMapper.mapping()} to ensure consistency, but inlined
     * here to avoid cross-module classloader issues.
     */
    private static SeaTunnelDataType<?> mapTDengineType(String tdType) {
        // Match the exact type string used in connector-tdengine's TDengineTypeMapper
        switch (tdType.toUpperCase().trim()) {
            case "BOOL":
            case "BIT":
                return BasicType.BOOLEAN_TYPE;
            case "TINYINT":
            case "TINYINT UNSIGNED":
            case "SMALLINT":
            case "SMALLINT UNSIGNED":
            case "MEDIUMINT":
            case "MEDIUMINT UNSIGNED":
            case "INT":
            case "INTEGER":
            case "YEAR":
                return BasicType.INT_TYPE;
            case "INT UNSIGNED":
            case "INTEGER UNSIGNED":
            case "BIGINT":
                return BasicType.LONG_TYPE;
            case "BIGINT UNSIGNED":
                return new DecimalType(20, 0);
            case "FLOAT":
                return BasicType.FLOAT_TYPE;
            case "FLOAT UNSIGNED":
                // UNSIGNED float may overflow; same warning as connector-tdengine
                return BasicType.FLOAT_TYPE;
            case "DOUBLE":
                return BasicType.DOUBLE_TYPE;
            case "DOUBLE UNSIGNED":
                // UNSIGNED double may overflow; same warning as connector-tdengine
                return BasicType.DOUBLE_TYPE;
            case "DECIMAL":
            case "DECIMAL UNSIGNED":
                return new DecimalType(38, 18);
            case "TIMESTAMP":
            case "DATETIME":
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case "DATE":
                return LocalTimeType.LOCAL_DATE_TYPE;
            case "TIME":
                return LocalTimeType.LOCAL_TIME_TYPE;
            case "CHAR":
            case "NCHAR":
            case "TINYTEXT":
            case "MEDIUMTEXT":
            case "TEXT":
            case "VARCHAR":
            case "JSON":
            case "LONGTEXT":
                return BasicType.STRING_TYPE;
            case "TINYBLOB":
            case "MEDIUMBLOB":
            case "BLOB":
            case "LONGBLOB":
            case "VARBINARY":
            case "BINARY":
                return PrimitiveByteArrayType.INSTANCE;
            case "GEOMETRY":
            default:
                // connector-tdengine throws for GEOMETRY/UNKNOWN; we map to STRING
                // as a safe fallback since the CDC REST endpoint may report types
                // differently
                return BasicType.STRING_TYPE;
        }
    }

    private static String unquote(String s) {
        if (s.startsWith("\"")) s = s.substring(1);
        if (s.endsWith("\"")) s = s.substring(0, s.length() - 1);
        return s;
    }

    /**
     * Extract a string value from config, handling both plain string and single-element list
     * formats. When the user writes {@code "database":["isa"]}, the config parser may return a list
     * representation.
     */
    private static String extractString(
            ReadonlyConfig config, org.apache.seatunnel.api.configuration.Option<String> option) {
        String value = config.get(option);
        if (value != null) {
            // Handle list format: ["isa"] → isa
            String trimmed = value.trim();
            if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
                trimmed = trimmed.substring(1, trimmed.length() - 1).trim();
                if (trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
                    trimmed = trimmed.substring(1, trimmed.length() - 1);
                }
                return trimmed;
            }
        }
        return value;
    }
}

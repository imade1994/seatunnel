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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.cdc.debezium.DeserializeFormat;
import org.apache.seatunnel.connectors.cdc.debezium.row.DebeziumJsonDeserializeSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.config.TdengineIncrementalSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.config.TdengineSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.config.TdengineSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.deserializer.TdengineConnectorDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.dialect.TdengineDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.offset.TdengineOffsetFactory;

import java.util.List;
import java.util.Optional;

/** TDengine CDC incremental source. */
public class TdengineIncrementalSource extends IncrementalSource<SeaTunnelRow, TdengineSourceConfig>
        implements SupportParallelism {

    static final String IDENTIFIER = "TDengine-CDC";

    public TdengineIncrementalSource(ReadonlyConfig options, List<CatalogTable> catalogTables) {
        super(options, catalogTables);
    }

    @Override
    public Option<StartupMode> getStartupModeOption() {
        return TdengineIncrementalSourceOptions.STARTUP_MODE;
    }

    @Override
    public Option<StopMode> getStopModeOption() {
        return TdengineIncrementalSourceOptions.STOP_MODE;
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public SourceConfig.Factory<TdengineSourceConfig> createSourceConfigFactory(
            ReadonlyConfig config) {
        TdengineSourceConfigFactory configFactory = new TdengineSourceConfigFactory();
        configFactory.fromReadonlyConfig(config);
        configFactory.startupOptions(startupConfig);
        configFactory.stopOptions(stopConfig);
        return configFactory;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DebeziumDeserializationSchema<SeaTunnelRow> createDebeziumDeserializationSchema(
            ReadonlyConfig config) {
        if (DeserializeFormat.COMPATIBLE_DEBEZIUM_JSON.equals(
                config.get(JdbcSourceOptions.FORMAT))) {
            return (DebeziumDeserializationSchema<SeaTunnelRow>)
                    new DebeziumJsonDeserializeSchema(
                            config.get(JdbcSourceOptions.DEBEZIUM_PROPERTIES));
        }

        return (DebeziumDeserializationSchema<SeaTunnelRow>)
                new TdengineConnectorDeserializationSchema(catalogTables);
    }

    @Override
    public DataSourceDialect<TdengineSourceConfig> createDataSourceDialect(ReadonlyConfig config) {
        return new TdengineDialect();
    }

    @Override
    public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
        return new TdengineOffsetFactory();
    }

    @Override
    public Optional<String> driverName() {
        // Use WebSocket driver - no native library required
        return Optional.of("com.taosdata.jdbc.ws.WebSocketDriver");
    }
}

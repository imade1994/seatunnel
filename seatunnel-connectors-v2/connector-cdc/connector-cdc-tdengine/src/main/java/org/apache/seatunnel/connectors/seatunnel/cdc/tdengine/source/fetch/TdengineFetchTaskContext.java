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
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.config.TdengineSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.dialect.TdengineDialect;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.util.LoggingContext;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/** Fetch task context for TDengine CDC, managing connection lifecycle and queue. */
@Slf4j
public class TdengineFetchTaskContext implements FetchTask.Context {

    private final TdengineDialect dialect;
    private final TdengineSourceConfig sourceConfig;
    private ChangeEventQueue<DataChangeEvent> changeEventQueue;
    private Connection jdbcConnection;

    public TdengineFetchTaskContext(TdengineDialect dialect, TdengineSourceConfig sourceConfig) {
        this.dialect = dialect;
        this.sourceConfig = sourceConfig;
    }

    @Override
    public void configure(SourceSplitBase sourceSplitBase) {
        final int queueSize =
                sourceSplitBase.isSnapshotSplit() && isExactlyOnce()
                        ? Integer.MAX_VALUE
                        : sourceConfig.getPollMaxBatchSize();
        this.changeEventQueue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(Duration.ofMillis(sourceConfig.getPollIntervalMs()))
                        .maxBatchSize(sourceConfig.getPollMaxBatchSize())
                        .maxQueueSize(queueSize)
                        .loggingContextSupplier(
                                () ->
                                        LoggingContext.forConnector(
                                                "tdengine-cdc",
                                                "tdengine-cdc-connector",
                                                "tdengine-cdc-connector-task"))
                        .build();
    }

    public TdengineSourceConfig getSourceConfig() {
        return sourceConfig;
    }

    public TdengineDialect getDialect() {
        return dialect;
    }

    @Override
    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return changeEventQueue;
    }

    public Connection getJdbcConnection() {
        if (jdbcConnection == null) {
            try {
                Class.forName("com.taosdata.jdbc.TSDBDriver");
                jdbcConnection = DriverManager.getConnection(sourceConfig.getUrl());
            } catch (Exception e) {
                throw new RuntimeException("Failed to create JDBC connection to TDengine", e);
            }
        }
        return jdbcConnection;
    }

    @Override
    public TableId getTableId(SourceRecord record) {
        return TdengineRecordUtils.getTableId(record);
    }

    @Override
    public Tables.TableFilter getTableFilter() {
        return Tables.TableFilter.includeAll();
    }

    @Override
    public boolean isExactlyOnce() {
        return sourceConfig.isExactlyOnce();
    }

    @Override
    public Offset getStreamOffset(SourceRecord record) {
        return TdengineRecordUtils.getStreamOffset(record);
    }

    @Override
    public boolean isDataChangeRecord(SourceRecord record) {
        return TdengineRecordUtils.isDataChangeRecord(record);
    }

    @Override
    public boolean isRecordBetween(SourceRecord record, Object[] splitStart, Object[] splitEnd) {
        // Simple implementation: for TDengine time-series data, we compare timestamps
        return true;
    }

    @Override
    public void rewriteOutputBuffer(
            Map<Struct, SourceRecord> outputBuffer, SourceRecord changeRecord) {
        // For TDengine TMQ, the records are already in the correct format.
        // For stream records, simply put them into the buffer.
        outputBuffer.put((Struct) changeRecord.key(), changeRecord);
    }

    @Override
    public List<SourceRecord> formatMessageTimestamp(Collection<SourceRecord> snapshotRecords) {
        // Snapshot records already have timestamps from the database
        return (List<SourceRecord>) snapshotRecords;
    }

    @Override
    public void close() {
        if (jdbcConnection != null) {
            try {
                jdbcConnection.close();
            } catch (Exception e) {
                log.warn("Failed to close JDBC connection", e);
            }
        }
    }
}

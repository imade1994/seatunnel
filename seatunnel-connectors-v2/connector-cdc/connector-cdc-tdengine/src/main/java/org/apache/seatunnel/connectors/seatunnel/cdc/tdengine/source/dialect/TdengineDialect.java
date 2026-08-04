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

package org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.dialect;

import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.config.TdengineSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.fetch.TdengineFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.fetch.TdengineSnapshotFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.fetch.TdengineStreamFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.splitter.TdengineChunkSplitter;

import io.debezium.relational.TableId;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.offset.TdengineOffsetConstants.DIALECT_NAME;

/** TDengine data source dialect implementation for CDC. */
@Slf4j
public class TdengineDialect implements DataSourceDialect<TdengineSourceConfig> {

    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return DIALECT_NAME;
    }

    @Override
    public List<TableId> discoverDataCollections(TdengineSourceConfig sourceConfig) {
        List<TableId> tableIds = new ArrayList<>();
        // Super table covers all sub-tables via TMQ topic
        tableIds.add(TableId.parse(sourceConfig.getDatabase() + "." + sourceConfig.getStable()));
        log.info("Discovered data collection for TDengine CDC: {}", tableIds.get(0));
        return tableIds;
    }

    @Override
    public boolean isDataCollectionIdCaseSensitive(TdengineSourceConfig sourceConfig) {
        // TDengine identifiers are case-insensitive
        return false;
    }

    @Override
    public ChunkSplitter createChunkSplitter(TdengineSourceConfig sourceConfig) {
        return new TdengineChunkSplitter();
    }

    @Override
    public FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase) {
        if (sourceSplitBase.isSnapshotSplit()) {
            return new TdengineSnapshotFetchTask(sourceSplitBase.asSnapshotSplit());
        } else {
            return new TdengineStreamFetchTask(sourceSplitBase.asIncrementalSplit());
        }
    }

    @Override
    public FetchTask.Context createFetchTaskContext(
            SourceSplitBase sourceSplitBase, TdengineSourceConfig sourceConfig) {
        return new TdengineFetchTaskContext(this, sourceConfig);
    }
}

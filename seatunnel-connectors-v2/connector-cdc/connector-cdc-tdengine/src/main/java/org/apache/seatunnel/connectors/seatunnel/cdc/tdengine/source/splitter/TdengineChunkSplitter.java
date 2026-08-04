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

package org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.splitter;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.source.enumerator.splitter.ChunkSplitter;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;

import io.debezium.relational.TableId;

import java.util.Collection;
import java.util.Collections;

/** Simple chunk splitter for TDengine. Creates one snapshot split per table (no chunking). */
public class TdengineChunkSplitter implements ChunkSplitter {

    private static final SeaTunnelRowType SPLIT_KEY_TYPE =
            new SeaTunnelRowType(
                    new String[] {"splitKey"},
                    new org.apache.seatunnel.api.table.type.SeaTunnelDataType[] {
                        BasicType.LONG_TYPE
                    });

    @Override
    public Collection<SnapshotSplit> generateSplits(TableId tableId) {
        // TDengine time-series data: one split per table, no key-range chunking
        String splitId = tableId.identifier() + "-snapshot";
        SnapshotSplit split = new SnapshotSplit(splitId, tableId, SPLIT_KEY_TYPE, null, null);
        return Collections.singletonList(split);
    }
}

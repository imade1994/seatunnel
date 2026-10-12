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

package org.apache.seatunnel.engine.server.persistence;

import org.apache.seatunnel.shade.com.google.common.collect.Maps;

import org.apache.seatunnel.engine.imap.storage.mysql.config.StorageProperties;
import org.apache.seatunnel.engine.imap.storage.mysql.mysql.MysqlClient;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.engine.imap.storage.mysql.util.SerializationUtils.serialize;

/**
 * 只读的作业历史读取器，供列表类 REST 接口直查存储用。
 *
 * <p>历史 map 在启动时被跳过加载（见 {@link StorageProperties#isHistoryMap}），而 Hazelcast 的 {@code IMap#values()}
 * 不读穿、只扫内存，所以 {@code GET /job} 这类列表接口拿不到历史，必须直查存储。
 *
 * <p><b>只构造 {@link MysqlClient}，绝不构造 {@code RedisMysqlIMapStorage}</b>：后者的 {@code initialize()} 会启动
 * FlushScheduler 线程，REST 侧再造一个实例会导致两个 flusher 并发刷同一组 Redis key。历史作业的写入早已 flush 落库，列表查询读 MySQL 足够。
 */
@Slf4j
public class FinishedJobHistoryReader {

    private final MysqlClient mysqlClient;
    private final int queryLimit;

    private FinishedJobHistoryReader(MysqlClient mysqlClient, int queryLimit) {
        this.mysqlClient = mysqlClient;
        this.queryLimit = queryLimit;
    }

    /**
     * 按 map 名构造读取器，存储配置取自该 map 的 map-store properties（走 {@code Config#getMapConfigOrNull}，它能解析
     * {@code engine*} 这类通配配置）。
     *
     * @return 该 map 没有可用配置时返回 null，调用方应回退到内存 map
     */
    public static FinishedJobHistoryReader forMap(
            HazelcastInstance hazelcastInstance, String mapName) {
        try {
            MapConfig mapConfig = hazelcastInstance.getConfig().getMapConfigOrNull(mapName);
            if (mapConfig == null) {
                log.warn("No map config for {}, cannot create history reader", mapName);
                return null;
            }
            Map<String, Object> configuration =
                    new HashMap<>(
                            Maps.fromProperties(mapConfig.getMapStoreConfig().getProperties()));
            // FileMapStoreFactory 会就地注入 businessName，但不依赖那道改写，这里显式补齐
            configuration.put(StorageProperties.BUSINESS_KEY, mapName);
            configuration.put(
                    StorageProperties.CLUSTER_NAME, hazelcastInstance.getConfig().getClusterName());
            StorageProperties properties = StorageProperties.from(configuration);
            return new FinishedJobHistoryReader(
                    new MysqlClient(properties), properties.getHistoryQueryLimit());
        } catch (Exception e) {
            log.warn(
                    "Failed to create history reader for map {}, fall back to in-memory map",
                    mapName,
                    e);
            return null;
        }
    }

    /** 配置的列表条数上限（{@code history.query.limit}）。 */
    public int getQueryLimit() {
        return queryLimit;
    }

    /** 最近写入的 limit 条，按 update_time 倒序。 */
    public Map<Object, Object> loadLatest(int limit) {
        return mysqlClient.loadLatest(limit);
    }

    /** 按 jobId 批量取。缺失的 key 不会出现在结果里。 */
    public Map<Object, Object> loadByIds(Collection<Long> jobIds) {
        if (jobIds == null || jobIds.isEmpty()) {
            return Collections.emptyMap();
        }
        List<byte[]> keys = new ArrayList<>(jobIds.size());
        for (Long jobId : jobIds) {
            keys.add(serialize(jobId));
        }
        return mysqlClient.loadBatch(keys);
    }
}

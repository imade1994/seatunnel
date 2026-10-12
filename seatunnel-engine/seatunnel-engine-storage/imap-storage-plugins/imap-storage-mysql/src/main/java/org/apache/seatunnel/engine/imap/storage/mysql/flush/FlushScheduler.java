/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.seatunnel.engine.imap.storage.mysql.flush;

import org.apache.seatunnel.engine.imap.storage.mysql.bean.RedisIMapData;
import org.apache.seatunnel.engine.imap.storage.mysql.mysql.MysqlClient;
import org.apache.seatunnel.engine.imap.storage.mysql.redis.RedisClientManager;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisCluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.engine.imap.storage.mysql.util.SerializationUtils.deserialize;

/**
 * Periodically replays the Redis dirty set into the MySQL state table. Only after a batch is
 * persisted successfully are the dirty marks removed, so a failure retries on the next cycle
 * (at-least-once).
 */
@Slf4j
public class FlushScheduler implements Runnable {

    private final RedisClientManager redisClient;
    private final MysqlClient mysqlClient;
    private final int batchSize;
    private final long batchBytes;

    public FlushScheduler(
            RedisClientManager redisClient,
            MysqlClient mysqlClient,
            int batchSize,
            long batchBytes) {
        this.redisClient = redisClient;
        this.mysqlClient = mysqlClient;
        this.batchSize = batchSize;
        this.batchBytes = batchBytes;
    }

    @Override
    public void run() {
        JedisCluster cluster = redisClient.getCluster();
        Set<byte[]> dirty;
        try {
            dirty = cluster.smembers(redisClient.getDirtyKey());
        } catch (Exception e) {
            log.error("read dirty set from redis failed", e);
            return;
        }
        if (dirty == null || dirty.isEmpty()) {
            return;
        }

        List<byte[]> dirtyList = new ArrayList<>(dirty);
        // process in batches to bound a single SQL statement size
        for (int start = 0; start < dirtyList.size(); start += batchSize) {
            int end = Math.min(start + batchSize, dirtyList.size());
            List<byte[]> batch = dirtyList.subList(start, end);
            try {
                flushBatch(cluster, batch);
            } catch (Exception e) {
                log.error("flush imap dirty data to mysql failed, will retry next cycle", e);
                return;
            }
        }
    }

    private void flushBatch(JedisCluster cluster, List<byte[]> dirtyBatch) {
        List<Object[]> upsertRows = new ArrayList<>();
        List<byte[]> deleteKeys = new ArrayList<>();
        List<byte[]> processed = new ArrayList<>(dirtyBatch.size());
        long pendingBytes = 0;

        for (byte[] raw : dirtyBatch) {
            RedisIMapData data = deserialize(raw, RedisIMapData.class);
            byte[] current = cluster.hget(redisClient.getStateKey(), data.getKey());
            if (current != null) {
                // current is the serialized RedisIMapData envelope; store the ACTUAL value bytes
                // with the ACTUAL value class, otherwise loadAll deserializes the envelope bytes
                // as the value class and fails (e.g. "The map was incorrectly serialized").
                RedisIMapData live = deserialize(current, RedisIMapData.class);
                byte[] value = live.getValue();
                upsertRows.add(
                        new Object[] {
                            live.getKey(), live.getKeyClassName(), value, live.getValueClassName()
                        });
                pendingBytes += (value == null ? 0 : value.length);
            } else {
                deleteKeys.add(data.getKey());
            }
            processed.add(raw);
            // 单条 upsert SQL 同时受「行数」与「总字节」约束，避免超过 MySQL max_allowed_packet
            if (pendingBytes >= batchBytes || processed.size() >= batchSize) {
                flushPartial(cluster, upsertRows, deleteKeys, processed);
                upsertRows = new ArrayList<>();
                deleteKeys = new ArrayList<>();
                processed = new ArrayList<>();
                pendingBytes = 0;
            }
        }
        if (!upsertRows.isEmpty() || !deleteKeys.isEmpty()) {
            flushPartial(cluster, upsertRows, deleteKeys, processed);
        }
    }

    /** 写一批到 MySQL，成功后才移除对应 dirty 标记（at-least-once） */
    private void flushPartial(
            JedisCluster cluster,
            List<Object[]> upsertRows,
            List<byte[]> deleteKeys,
            List<byte[]> processed) {
        mysqlClient.batchUpsert(upsertRows);
        mysqlClient.batchDelete(deleteKeys);
        cluster.srem(redisClient.getDirtyKey(), processed.toArray(new byte[0][]));
    }
}

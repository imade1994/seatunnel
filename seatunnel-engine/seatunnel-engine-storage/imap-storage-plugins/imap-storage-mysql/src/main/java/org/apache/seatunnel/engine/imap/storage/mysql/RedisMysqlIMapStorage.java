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

package org.apache.seatunnel.engine.imap.storage.mysql;

import org.apache.seatunnel.engine.imap.storage.api.IMapStorage;
import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;
import org.apache.seatunnel.engine.imap.storage.mysql.bean.RedisIMapData;
import org.apache.seatunnel.engine.imap.storage.mysql.config.StorageProperties;
import org.apache.seatunnel.engine.imap.storage.mysql.flush.FlushScheduler;
import org.apache.seatunnel.engine.imap.storage.mysql.mysql.MysqlClient;
import org.apache.seatunnel.engine.imap.storage.mysql.redis.RedisClientManager;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.engine.imap.storage.mysql.util.SerializationUtils.deserialize;
import static org.apache.seatunnel.engine.imap.storage.mysql.util.SerializationUtils.serialize;

/**
 * IMAP storage backed by Redis (write buffer, low latency) and MySQL (durable state table).
 *
 * <p>Writes go to Redis immediately and are acknowledged fast; a periodic {@link FlushScheduler}
 * persists the accumulated dirty keys to MySQL. {@link #loadAll()} merges the MySQL snapshot with
 * the un-flushed Redis dirty delta, so the current state is always correct. This keeps the storage
 * footprint bounded by the live state size instead of the append-only WAL of the file plugin.
 */
@Slf4j
public class RedisMysqlIMapStorage implements IMapStorage {

    private StorageProperties properties;
    private RedisClientManager redisClient;
    private MysqlClient mysqlClient;
    private ScheduledExecutorService flushExecutor;

    public RedisMysqlIMapStorage(Map<String, Object> configuration) {
        this.initialize(configuration);
    }

    /** 仅测试用：直接注入依赖，跳过 Redis/MySQL 建连与 flush 调度。 */
    RedisMysqlIMapStorage(
            StorageProperties properties, RedisClientManager redisClient, MysqlClient mysqlClient) {
        this.properties = properties;
        this.redisClient = redisClient;
        this.mysqlClient = mysqlClient;
        this.flushExecutor = null;
    }

    @Override
    public void initialize(Map<String, Object> configuration) {
        this.properties = StorageProperties.from(configuration);
        this.redisClient = new RedisClientManager(properties);
        this.mysqlClient = new MysqlClient(properties);
        this.mysqlClient.ensureTable();
        this.flushExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread t = new Thread(r, "imap-mysql-flush");
                            t.setDaemon(true);
                            return t;
                        });
        this.flushExecutor.scheduleWithFixedDelay(
                new FlushScheduler(
                        redisClient,
                        mysqlClient,
                        properties.getFlushBatchSize(),
                        properties.getFlushBatchBytes()),
                properties.getFlushIntervalMs(),
                properties.getFlushIntervalMs(),
                TimeUnit.MILLISECONDS);
        log.info(
                "RedisMysqlIMapStorage initialized, business={}, cluster={}, flushInterval={}ms",
                properties.getBusinessName(),
                properties.getClusterName(),
                properties.getFlushIntervalMs());
    }

    @Override
    public boolean store(Object key, Object value) {
        try {
            JedisCluster cluster = redisClient.getCluster();
            byte[] field = serialize(key);
            RedisIMapData data = buildData(key, value);
            cluster.hset(redisClient.getStateKey(), field, serialize(data));
            markDirty(cluster, key);
            return true;
        } catch (Exception e) {
            log.error("store imap data to redis failed, key is {}", key, e);
            return false;
        }
    }

    @Override
    public Set<Object> storeAll(Map<Object, Object> map) {
        Set<Object> failures = new HashSet<>();
        if (map == null || map.isEmpty()) {
            return failures;
        }
        JedisCluster cluster = redisClient.getCluster();
        map.forEach(
                (key, value) -> {
                    try {
                        byte[] field = serialize(key);
                        RedisIMapData data = buildData(key, value);
                        cluster.hset(redisClient.getStateKey(), field, serialize(data));
                        markDirty(cluster, key);
                    } catch (Exception e) {
                        failures.add(key);
                    }
                });
        return failures;
    }

    @Override
    public boolean delete(Object key) {
        try {
            JedisCluster cluster = redisClient.getCluster();
            cluster.hdel(redisClient.getStateKey(), serialize(key));
            markDirty(cluster, key);
            return true;
        } catch (Exception e) {
            log.error("delete imap data from redis failed, key is {}", key, e);
            return false;
        }
    }

    @Override
    public Set<Object> deleteAll(Collection<Object> keys) {
        Set<Object> failures = new HashSet<>();
        if (keys == null || keys.isEmpty()) {
            return failures;
        }
        JedisCluster cluster = redisClient.getCluster();
        for (Object key : keys) {
            try {
                cluster.hdel(redisClient.getStateKey(), serialize(key));
                markDirty(cluster, key);
            } catch (Exception e) {
                failures.add(key);
            }
        }
        return failures;
    }

    @Override
    public Map<Object, Object> loadAll() throws IOException {
        if (properties.isHistoryMap()) {
            // 历史 map（作业完成后只写不读）启动不加载：全量加载会把整张表拉进内存，表大到
            // 一定程度主节点会起不来。查询改走单 key 读穿(#load) 与 REST 侧直查存储。
            log.info(
                    "imap history map {} skipped loadAll at startup", properties.getBusinessName());
            return Collections.emptyMap();
        }
        // 1. durable snapshot from MySQL
        Map<Object, Object> result = mysqlClient.loadAll();
        // 2. apply un-flushed Redis dirty delta on top
        JedisCluster cluster = redisClient.getCluster();
        Set<byte[]> dirty = cluster.smembers(redisClient.getDirtyKey());
        if (dirty != null) {
            for (byte[] raw : dirty) {
                RedisIMapData data = deserialize(raw, RedisIMapData.class);
                byte[] current = cluster.hget(redisClient.getStateKey(), data.getKey());
                Object key = deserialize(data.getKey(), loadClass(data.getKeyClassName()));
                if (current != null) {
                    RedisIMapData live = deserialize(current, RedisIMapData.class);
                    result.put(
                            key, deserialize(live.getValue(), loadClass(live.getValueClassName())));
                } else {
                    result.remove(key);
                }
            }
        }
        return result;
    }

    @Override
    public Set<Object> loadAllKeys() {
        if (properties.isHistoryMap()) {
            // 与 loadAll() 保持一致：两者同为空 -> Hazelcast 加载 0 条，不会出现 null value
            log.info(
                    "imap history map {} skipped loadAllKeys at startup",
                    properties.getBusinessName());
            return Collections.emptySet();
        }
        Set<Object> keys = mysqlClient.loadAllKeys();
        JedisCluster cluster = redisClient.getCluster();
        Set<byte[]> dirty = cluster.smembers(redisClient.getDirtyKey());
        if (dirty != null) {
            for (byte[] raw : dirty) {
                RedisIMapData data = deserialize(raw, RedisIMapData.class);
                Object key = deserialize(data.getKey(), loadClass(data.getKeyClassName()));
                byte[] current = cluster.hget(redisClient.getStateKey(), data.getKey());
                if (current != null) {
                    keys.add(key);
                } else {
                    keys.remove(key);
                }
            }
        }
        return keys;
    }

    /**
     * 单 key 读穿：Redis 优先，MySQL 兜底。
     *
     * <p>Redis 一定不比 MySQL 旧 —— {@link #store} 与 {@link #delete} 都先改 Redis，再等 flush 落库 —— 所以优先读
     * Redis 能拿到尚未 flush 的最新值。仅当 Redis 没有该值时才需要区分两种情况：
     *
     * <ul>
     *   <li>key 在 dirty 集合里 → Redis 是权威的，说明确实被删了，直接返回 null（**不能**回落到 MySQL，否则会把已删除的数据复活）；
     *   <li>key 不在 dirty 集合里 → Redis 从未写过，或 Redis 丢过数据（如重启），回落 MySQL。
     * </ul>
     */
    @Override
    public Object load(Object key) {
        try {
            JedisCluster cluster = redisClient.getCluster();
            byte[] field = serialize(key);
            byte[] raw = cluster.hget(redisClient.getStateKey(), field);
            if (raw != null) {
                RedisIMapData live = deserialize(raw, RedisIMapData.class);
                return live.getValue() == null
                        ? null
                        : deserialize(live.getValue(), loadClass(live.getValueClassName()));
            }
            if (isDirty(cluster, field)) {
                return null;
            }
            return mysqlClient.load(field);
        } catch (Exception e) {
            log.error("load imap data failed, key is {}", key, e);
            return null;
        }
    }

    /**
     * key 是否在 Redis dirty 集合里。dirty 成员是带时间戳的 {@link RedisIMapData} marker，重建字节 不会一致，所以只能逐个比较 marker
     * 里的 key 字节 —— 好在该集合通常接近空（flush 每 5s 清一次）， 且只在 Redis 查不到值时才会走到这里。
     */
    private boolean isDirty(JedisCluster cluster, byte[] keyBytes) {
        Set<byte[]> dirty = cluster.smembers(redisClient.getDirtyKey());
        if (dirty == null) {
            return false;
        }
        for (byte[] marker : dirty) {
            if (Arrays.equals(deserialize(marker, RedisIMapData.class).getKey(), keyBytes)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void destroy(boolean deleteAllFileFlag) {
        if (flushExecutor != null) {
            flushExecutor.shutdown();
        }
        try {
            if (deleteAllFileFlag) {
                redisClient.getCluster().del(redisClient.getStateKey(), redisClient.getDirtyKey());
                mysqlClient.deleteAll();
            }
        } catch (Exception e) {
            log.warn(
                    "destroy imap mysql storage failed, deleteAllFileFlag={}",
                    deleteAllFileFlag,
                    e);
        } finally {
            redisClient.close();
        }
    }

    /**
     * 只把 key 标记进 dirty 集合（不携带 value，避免脏集合随 value 体积无限膨胀）； flush 时 FlushScheduler 从 state hash
     * 读当前值，marker 仅作 key 级脏标记。
     */
    private void markDirty(JedisCluster cluster, Object key) {
        RedisIMapData marker =
                RedisIMapData.builder()
                        .key(serialize(key))
                        .keyClassName(key.getClass().getName())
                        .value(null)
                        .valueClassName(null)
                        .timestamp(System.currentTimeMillis())
                        .build();
        cluster.sadd(redisClient.getDirtyKey(), serialize(marker));
    }

    private RedisIMapData buildData(Object key, Object value) {
        return RedisIMapData.builder()
                .key(serialize(key))
                .keyClassName(key.getClass().getName())
                .value(value == null ? null : serialize(value))
                .valueClassName(value == null ? null : value.getClass().getName())
                .timestamp(System.currentTimeMillis())
                .build();
    }

    private static Class<?> loadClass(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IMapStorageException("Class not found: " + className, e);
        }
    }
}

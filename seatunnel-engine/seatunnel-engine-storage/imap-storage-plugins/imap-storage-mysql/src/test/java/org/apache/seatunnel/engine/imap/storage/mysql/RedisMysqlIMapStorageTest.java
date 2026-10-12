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

package org.apache.seatunnel.engine.imap.storage.mysql;

import org.apache.seatunnel.engine.imap.storage.mysql.bean.RedisIMapData;
import org.apache.seatunnel.engine.imap.storage.mysql.config.StorageProperties;
import org.apache.seatunnel.engine.imap.storage.mysql.mysql.MysqlClient;
import org.apache.seatunnel.engine.imap.storage.mysql.redis.RedisClientManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import redis.clients.jedis.JedisCluster;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.engine.imap.storage.mysql.util.SerializationUtils.serialize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RedisMysqlIMapStorageTest {

    private static final String HISTORY_MAP = "engine_finishedJobState";
    private static final String NORMAL_MAP = "engine_runningJobState";

    @Mock private RedisClientManager redisClient;
    @Mock private MysqlClient mysqlClient;
    @Mock private JedisCluster jedisCluster;

    private static StorageProperties properties(String businessName) {
        Map<String, Object> config = new HashMap<>();
        config.put(StorageProperties.BUSINESS_KEY, businessName);
        config.put(StorageProperties.CLUSTER_NAME, "test-cluster");
        config.put(StorageProperties.REDIS_HOST, "localhost");
        config.put(StorageProperties.REDIS_PASSWORD, "pwd");
        config.put(StorageProperties.MYSQL_URL, "jdbc:mysql://localhost:3306/test");
        config.put(StorageProperties.MYSQL_USERNAME, "root");
        config.put(StorageProperties.MYSQL_PASSWORD, "pwd");
        return StorageProperties.from(config);
    }

    private RedisMysqlIMapStorage storage(String businessName) {
        return new RedisMysqlIMapStorage(properties(businessName), redisClient, mysqlClient);
    }

    /** 历史 map 启动零加载：这是本改动的主要目的（表变大后主节点仍能起来）。 */
    @Test
    void testHistoryMapSkipsLoadAtStartup() throws Exception {
        RedisMysqlIMapStorage storage = storage(HISTORY_MAP);

        assertTrue(storage.loadAll().isEmpty());
        assertTrue(storage.loadAllKeys().isEmpty());
        verifyNoInteractions(mysqlClient);
    }

    /** 非历史 map（running 等）必须保持全量加载，主节点故障恢复依赖它。 */
    @Test
    void testNonHistoryMapStillLoads() throws Exception {
        when(redisClient.getCluster()).thenReturn(jedisCluster);
        when(redisClient.getDirtyKey()).thenReturn("dirty".getBytes(StandardCharsets.UTF_8));
        when(jedisCluster.smembers(any(byte[].class))).thenReturn(Collections.emptySet());
        when(mysqlClient.loadAll()).thenReturn(Collections.singletonMap(1L, "v1"));

        RedisMysqlIMapStorage storage = storage(NORMAL_MAP);

        assertEquals(Collections.singletonMap(1L, "v1"), storage.loadAll());
    }

    @Test
    void testLoadPrefersRedisOverMysql() {
        String value = "redis-value";
        byte[] keyBytes = serialize("k");
        RedisIMapData envelope =
                RedisIMapData.builder()
                        .key(keyBytes)
                        .keyClassName(String.class.getName())
                        .value(serialize(value))
                        .valueClassName(String.class.getName())
                        .timestamp(0L)
                        .build();

        when(redisClient.getCluster()).thenReturn(jedisCluster);
        when(redisClient.getStateKey()).thenReturn("state".getBytes(StandardCharsets.UTF_8));
        when(jedisCluster.hget(any(byte[].class), eq(keyBytes))).thenReturn(serialize(envelope));

        assertEquals(value, storage(NORMAL_MAP).load("k"));
        verifyNoInteractions(mysqlClient);
    }

    /**
     * Redis 无值 + key 在 dirty 集合里 → Redis 是权威的，说明确实被删了，必须返回 null 而 <b>不能</b>回落 MySQL（否则会把已删除的数据复活）。
     */
    @Test
    void testLoadReturnsNullForDirtyButAbsentKey() {
        byte[] keyBytes = serialize("k");
        RedisIMapData marker =
                RedisIMapData.builder()
                        .key(keyBytes)
                        .keyClassName(String.class.getName())
                        .timestamp(0L)
                        .build();

        when(redisClient.getCluster()).thenReturn(jedisCluster);
        when(redisClient.getStateKey()).thenReturn("state".getBytes(StandardCharsets.UTF_8));
        when(redisClient.getDirtyKey()).thenReturn("dirty".getBytes(StandardCharsets.UTF_8));
        when(jedisCluster.hget(any(byte[].class), eq(keyBytes))).thenReturn(null);
        when(jedisCluster.smembers(any(byte[].class)))
                .thenReturn(Collections.singleton(serialize(marker)));

        assertNull(storage(NORMAL_MAP).load("k"));
        verifyNoInteractions(mysqlClient);
    }

    /** Redis 无值且不在 dirty 集合 → 回落 MySQL（覆盖 Redis 曾丢数据的情况）。 */
    @Test
    void testLoadFallsBackToMysqlWhenNotDirty() {
        byte[] keyBytes = serialize("k");

        when(redisClient.getCluster()).thenReturn(jedisCluster);
        when(redisClient.getStateKey()).thenReturn("state".getBytes(StandardCharsets.UTF_8));
        when(redisClient.getDirtyKey()).thenReturn("dirty".getBytes(StandardCharsets.UTF_8));
        when(jedisCluster.hget(any(byte[].class), eq(keyBytes))).thenReturn(null);
        when(jedisCluster.smembers(any(byte[].class))).thenReturn(Collections.emptySet());
        when(mysqlClient.load(keyBytes)).thenReturn("mysql-value");

        assertEquals("mysql-value", storage(NORMAL_MAP).load("k"));
    }
}

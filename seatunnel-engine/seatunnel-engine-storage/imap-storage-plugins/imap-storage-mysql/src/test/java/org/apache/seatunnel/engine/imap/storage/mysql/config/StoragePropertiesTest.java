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

package org.apache.seatunnel.engine.imap.storage.mysql.config;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StoragePropertiesTest {

    private static Map<String, Object> baseConfig(String businessName) {
        Map<String, Object> config = new HashMap<>();
        config.put(StorageProperties.BUSINESS_KEY, businessName);
        config.put(StorageProperties.CLUSTER_NAME, "test-cluster");
        config.put(StorageProperties.REDIS_HOST, "localhost");
        config.put(StorageProperties.REDIS_PASSWORD, "pwd");
        config.put(StorageProperties.MYSQL_URL, "jdbc:mysql://localhost:3306/test");
        config.put(StorageProperties.MYSQL_USERNAME, "root");
        config.put(StorageProperties.MYSQL_PASSWORD, "pwd");
        return config;
    }

    @Test
    void testDefaults() {
        StorageProperties props = StorageProperties.from(baseConfig("engine_runningJobState"));

        assertEquals(StorageProperties.DEFAULT_HISTORY_QUERY_LIMIT, props.getHistoryQueryLimit());
        assertTrue(props.getHistoryMaps().contains("engine_finishedJobState"));
    }

    @Test
    void testDefaultHistoryMapsAreNotHistoryForRunningMaps() {
        // 最关键的一条：running / checkpoint 类 map 绝不能落入白名单，
        // 否则主节点故障恢复会丢作业、checkpoint 断点续传会断。
        assertFalse(StorageProperties.from(baseConfig("engine_runningJobState")).isHistoryMap());
        assertFalse(StorageProperties.from(baseConfig("engine_runningJobInfo")).isHistoryMap());
        assertFalse(StorageProperties.from(baseConfig("engine_checkpoint-id-map")).isHistoryMap());
        assertFalse(StorageProperties.from(baseConfig("engine_checkpoint_monitor")).isHistoryMap());
        assertFalse(StorageProperties.from(baseConfig("engine_stateTimestamps")).isHistoryMap());
    }

    @Test
    void testDefaultHistoryMapsAreHistoryMap() {
        assertTrue(StorageProperties.from(baseConfig("engine_finishedJobState")).isHistoryMap());
        assertTrue(StorageProperties.from(baseConfig("engine_finishedJobMetrics")).isHistoryMap());
        assertTrue(
                StorageProperties.from(baseConfig("engine_finishedJobVertexInfo")).isHistoryMap());
    }

    @Test
    void testHistoryMapMatchingIsExact() {
        // 精确匹配，不做前缀/子串匹配
        assertFalse(StorageProperties.from(baseConfig("engine_finishedJobStateV2")).isHistoryMap());
        assertFalse(
                StorageProperties.from(baseConfig("my_engine_finishedJobState")).isHistoryMap());
    }

    @Test
    void testHistoryMapsParsingTrimsAndIgnoresBlanks() {
        Map<String, Object> config = baseConfig("engine_finishedJobState");
        config.put(
                StorageProperties.HISTORY_MAPS,
                " engine_finishedJobState , , engine_finishedJobMetrics ");

        StorageProperties props = StorageProperties.from(config);

        assertEquals(2, props.getHistoryMaps().size());
        assertTrue(props.isHistoryMap());
    }

    @Test
    void testBlankHistoryMapsDisablesSkipping() {
        // 显式配空串 -> 空集 -> 关闭“跳过加载”，可作为回退开关
        Map<String, Object> config = baseConfig("engine_finishedJobState");
        config.put(StorageProperties.HISTORY_MAPS, "");

        StorageProperties props = StorageProperties.from(config);

        assertTrue(props.getHistoryMaps().isEmpty());
        assertFalse(props.isHistoryMap());
    }

    @Test
    void testHistoryQueryLimitOverride() {
        Map<String, Object> config = baseConfig("engine_finishedJobState");
        config.put(StorageProperties.HISTORY_QUERY_LIMIT, "50");

        assertEquals(50, StorageProperties.from(config).getHistoryQueryLimit());
    }
}

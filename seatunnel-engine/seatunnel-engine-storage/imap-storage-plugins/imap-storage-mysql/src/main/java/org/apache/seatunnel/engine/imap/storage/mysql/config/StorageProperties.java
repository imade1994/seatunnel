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

package org.apache.seatunnel.engine.imap.storage.mysql.config;

import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;

import lombok.Data;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** Configuration of the Redis + MySQL backed IMap storage plugin. */
@Data
public class StorageProperties {

    /** required: map name, filled by FileMapStoreFactory */
    public static final String BUSINESS_KEY = "businessName";

    /** required: cluster name */
    public static final String CLUSTER_NAME = "clusterName";

    /** Redis */
    public static final String REDIS_HOST = "redis.host";

    public static final String REDIS_PORT = "redis.port";
    public static final String REDIS_PASSWORD = "redis.password";
    public static final String REDIS_DB = "redis.db";
    public static final String REDIS_PREFIX = "redis.prefix";
    public static final String REDIS_TIMEOUT_MS = "redis.timeout.ms";

    /** MySQL */
    public static final String MYSQL_URL = "mysql.url";

    public static final String MYSQL_USERNAME = "mysql.username";
    public static final String MYSQL_PASSWORD = "mysql.password";
    public static final String MYSQL_TABLE = "mysql.table";

    /** flush scheduler */
    public static final String FLUSH_INTERVAL_MS = "flush.interval.ms";

    public static final String FLUSH_BATCH_SIZE = "flush.batch.size";

    /** 单条 upsert SQL 的总字节上限（value 之和），防止超过 MySQL max_allowed_packet */
    public static final String FLUSH_BATCH_BYTES = "flush.batch.bytes";

    /**
     * 逗号分隔的「历史 map」白名单。名单内的 map 视为纯历史数据：启动时跳过全量加载，改由 单 key 读穿按需回源（见 {@code
     * IMapStorage#load}）。仍正常写入持久化，不做任何删除。
     */
    public static final String HISTORY_MAPS = "history.maps";

    /**
     * 列表类接口直查存储时返回的最大条数。全量反序列化历史（每条含 {@code JobDAGInfo} 的 envOptions 与 vertexInfoMap）在 REST 进程里是几十
     * MB 级开销 + 秒级延迟，故默认只取最近若干条。
     */
    public static final String HISTORY_QUERY_LIMIT = "history.query.limit";

    public static final String DEFAULT_REDIS_PREFIX = "seatunnel-imap";
    public static final String DEFAULT_MYSQL_TABLE = "seatunnel_imap";
    public static final String DEFAULT_REDIS_DB = "0";
    public static final int DEFAULT_REDIS_TIMEOUT_MS = 3000;
    public static final long DEFAULT_FLUSH_INTERVAL_MS = 5000L;
    public static final int DEFAULT_FLUSH_BATCH_SIZE = 1000;
    public static final long DEFAULT_FLUSH_BATCH_BYTES = 64L * 1024 * 1024;
    public static final int DEFAULT_HISTORY_QUERY_LIMIT = 1000;

    /**
     * 默认历史 map：作业完成后只写不读的三个 map，名字取自 {@code org.apache.seatunnel.engine.common.Constant}。插件不依赖
     * engine-common，故此处写字面量。
     */
    public static final String DEFAULT_HISTORY_MAPS =
            "engine_finishedJobState,engine_finishedJobMetrics,engine_finishedJobVertexInfo";

    private String businessName;
    private String clusterName;

    private String redisHost;
    private int redisPort;
    private String redisPassword;
    private int redisDb;
    private String redisPrefix;
    private int redisTimeoutMs;

    private String mysqlUrl;
    private String mysqlUsername;
    private String mysqlPassword;
    private String mysqlTable;

    private long flushIntervalMs;
    private int flushBatchSize;
    private long flushBatchBytes;

    /** 解析后的历史 map 白名单，见 {@link #HISTORY_MAPS}。 */
    private Set<String> historyMaps = Collections.emptySet();

    /** 列表类接口直查存储的条数上限，见 {@link #HISTORY_QUERY_LIMIT}。 */
    private int historyQueryLimit = DEFAULT_HISTORY_QUERY_LIMIT;

    /**
     * 当前 map 是否属于历史 map（精确匹配，不支持前缀/通配）。为 true 时启动不加载，靠读穿按需回源。
     *
     * <p>注意 {@code engine*} 通配会命中 {@code engine_checkpoint-id-map}，而它必须全量加载（否则丢 checkpoint
     * 断点续传）；{@code engine_running*} 也必须全量加载（主节点故障恢复依赖它）。二者都 绝不能进白名单。
     */
    public boolean isHistoryMap() {
        return businessName != null && historyMaps.contains(businessName);
    }

    private static Set<String> parseHistoryMaps(String raw) {
        Set<String> result = new LinkedHashSet<>();
        for (String item : raw.split(",")) {
            String trimmed = item.trim();
            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }
        return Collections.unmodifiableSet(result);
    }

    public static StorageProperties from(Map<String, Object> configuration) {
        StorageProperties props = new StorageProperties();
        props.businessName = get(configuration, BUSINESS_KEY);
        props.clusterName = get(configuration, CLUSTER_NAME);
        props.redisHost = get(configuration, REDIS_HOST);
        props.redisPassword = get(configuration, REDIS_PASSWORD);
        props.redisPrefix = getOrDefault(configuration, REDIS_PREFIX, DEFAULT_REDIS_PREFIX);
        props.redisDb = Integer.parseInt(getOrDefault(configuration, REDIS_DB, DEFAULT_REDIS_DB));
        props.redisTimeoutMs =
                Integer.parseInt(
                        getOrDefault(
                                configuration,
                                REDIS_TIMEOUT_MS,
                                String.valueOf(DEFAULT_REDIS_TIMEOUT_MS)));
        props.redisPort =
                Integer.parseInt(getOrDefault(configuration, REDIS_PORT, String.valueOf(6379)));
        props.mysqlUrl = get(configuration, MYSQL_URL);
        props.mysqlUsername = get(configuration, MYSQL_USERNAME);
        props.mysqlPassword = get(configuration, MYSQL_PASSWORD);
        props.mysqlTable = getOrDefault(configuration, MYSQL_TABLE, DEFAULT_MYSQL_TABLE);
        props.flushIntervalMs =
                Long.parseLong(
                        getOrDefault(
                                configuration,
                                FLUSH_INTERVAL_MS,
                                String.valueOf(DEFAULT_FLUSH_INTERVAL_MS)));
        props.flushBatchSize =
                Integer.parseInt(
                        getOrDefault(
                                configuration,
                                FLUSH_BATCH_SIZE,
                                String.valueOf(DEFAULT_FLUSH_BATCH_SIZE)));
        props.flushBatchBytes =
                Long.parseLong(
                        getOrDefault(
                                configuration,
                                FLUSH_BATCH_BYTES,
                                String.valueOf(DEFAULT_FLUSH_BATCH_BYTES)));
        // 缺省用默认白名单；显式配成空串则解析为空集，等于关闭跳过加载（可作为回退开关）
        Object rawHistoryMaps = configuration.get(HISTORY_MAPS);
        props.historyMaps =
                parseHistoryMaps(
                        rawHistoryMaps == null ? DEFAULT_HISTORY_MAPS : rawHistoryMaps.toString());
        props.historyQueryLimit =
                Integer.parseInt(
                        getOrDefault(
                                configuration,
                                HISTORY_QUERY_LIMIT,
                                String.valueOf(DEFAULT_HISTORY_QUERY_LIMIT)));
        return props;
    }

    private static String get(Map<String, Object> configuration, String key) {
        Object value = configuration.get(key);
        if (value == null || value.toString().trim().isEmpty()) {
            throw new IMapStorageException("Missing required configuration key: " + key);
        }
        return value.toString();
    }

    private static String getOrDefault(
            Map<String, Object> configuration, String key, String defaultValue) {
        Object value = configuration.get(key);
        return value == null || value.toString().trim().isEmpty() ? defaultValue : value.toString();
    }
}

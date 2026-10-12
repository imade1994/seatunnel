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

package org.apache.seatunnel.engine.imap.storage.mysql.redis;

import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;
import org.apache.seatunnel.engine.imap.storage.mysql.config.StorageProperties;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.Connection;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisCluster;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/**
 * Redis cluster client wrapper. Redis holds the write buffer (state hash + dirty set).
 *
 * <p>Uses {@link JedisCluster} to handle MOVED/ASK redirections, and hash tags in the key layout so
 * the state and dirty keys of the same business always land on the same slot.
 */
public class RedisClientManager implements AutoCloseable {

    private final JedisCluster jedisCluster;

    private final byte[] stateKey;
    private final byte[] dirtyKey;

    public RedisClientManager(StorageProperties props) {
        GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(16);
        poolConfig.setMaxIdle(8);
        poolConfig.setMinIdle(1);

        Set<HostAndPort> nodes = new HashSet<>();
        nodes.add(new HostAndPort(props.getRedisHost(), props.getRedisPort()));

        DefaultJedisClientConfig.Builder configBuilder =
                DefaultJedisClientConfig.builder()
                        .connectionTimeoutMillis(props.getRedisTimeoutMs())
                        .socketTimeoutMillis(props.getRedisTimeoutMs());
        if (props.getRedisPassword() != null && !props.getRedisPassword().isEmpty()) {
            configBuilder.password(props.getRedisPassword());
        }
        JedisClientConfig clientConfig = configBuilder.build();

        try {
            this.jedisCluster = new JedisCluster(nodes, clientConfig, 3, poolConfig);
        } catch (Exception e) {
            throw new IMapStorageException("Failed to init redis cluster client", e);
        }

        // hash tag so state + dirty of the same business map to the same slot
        String prefix =
                "{"
                        + props.getRedisPrefix()
                        + ":"
                        + props.getClusterName()
                        + ":"
                        + props.getBusinessName()
                        + "}";
        this.stateKey = (prefix + ":state").getBytes(StandardCharsets.UTF_8);
        this.dirtyKey = (prefix + ":dirty").getBytes(StandardCharsets.UTF_8);
    }

    public JedisCluster getCluster() {
        return jedisCluster;
    }

    public byte[] getStateKey() {
        return stateKey;
    }

    public byte[] getDirtyKey() {
        return dirtyKey;
    }

    @Override
    public void close() {
        jedisCluster.close();
    }
}

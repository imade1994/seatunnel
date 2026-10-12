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
import org.apache.seatunnel.engine.imap.storage.api.IMapStorageFactory;
import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;

import com.google.auto.service.AutoService;

import java.util.Map;

/**
 * Redis + MySQL backed IMap storage. Configure it in hazelcast.yaml:
 *
 * <pre>
 * map:
 *   engine*:
 *     map-store:
 *       enabled: true
 *       factory-class-name: org.apache.seatunnel.engine.server.persistence.FileMapStoreFactory
 *       properties:
 *         type: mysql
 *         clusterName: seatunnel
 *         redis.host: ...
 *         redis.port: 6379
 *         mysql.url: jdbc:mysql://...
 *         mysql.username: ...
 *         mysql.password: ...
 *         flush.interval.ms: 5000
 * </pre>
 */
@AutoService(IMapStorageFactory.class)
public class RedisMysqlIMapStorageFactory implements IMapStorageFactory {

    @Override
    public String factoryIdentifier() {
        return "mysql";
    }

    @Override
    public IMapStorage create(Map<String, Object> configuration) throws IMapStorageException {
        return new RedisMysqlIMapStorage(configuration);
    }
}

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

package org.apache.seatunnel.engine.imap.storage.mysql.bean;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

/**
 * Serialized representation of one IMap key-value entry. Carries the class names so the original
 * key/value objects can be reconstructed when reading from Redis/MySQL.
 */
@Data
@Builder
public class RedisIMapData implements Serializable {

    private static final long serialVersionUID = 1L;

    /** serialized key */
    private byte[] key;

    /** original class name of the key */
    private String keyClassName;

    /** serialized value */
    private byte[] value;

    /** original class name of the value */
    private String valueClassName;

    /** write timestamp, used for ordering if needed */
    private long timestamp;
}

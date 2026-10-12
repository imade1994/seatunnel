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

package org.apache.seatunnel.engine.imap.storage.mysql.util;

import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;
import org.apache.seatunnel.engine.serializer.api.Serializer;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import java.io.IOException;

/** Shared protostuff serializer used for IMap key/value payloads. */
public final class SerializationUtils {

    private static final Serializer SERIALIZER = new ProtoStuffSerializer();

    private SerializationUtils() {}

    public static byte[] serialize(Object obj) {
        try {
            return SERIALIZER.serialize(obj);
        } catch (IOException e) {
            throw new IMapStorageException("Failed to serialize object", e);
        }
    }

    public static <T> T deserialize(byte[] data, Class<T> clazz) {
        try {
            return SERIALIZER.deserialize(data, clazz);
        } catch (IOException e) {
            throw new IMapStorageException("Failed to deserialize object", e);
        }
    }
}

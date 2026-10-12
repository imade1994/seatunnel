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

import org.apache.seatunnel.engine.imap.storage.api.IMapStorage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FileMapStoreTest {

    @Mock private IMapStorage mapStorage;

    @InjectMocks private FileMapStore fileMapStore;

    /**
     * 回归：{@code MapLoader#loadAll} 的结果不得含 null value（契约要求），否则 Hazelcast 的 {@code
     * DefaultRecordStore#checkKeyAndValue} 会抛 NPE 并让整批 PutFromLoadAllOperation 失败。 场景：loadAllKeys
     * 返回了某 key，但随后 loadAll 时该行已消失（两次独立查询）。
     */
    @Test
    void testLoadAllDropsKeysMissingFromStorage() throws Exception {
        Object present = 1L;
        Object missing = 2L;
        when(mapStorage.loadAll()).thenReturn(Collections.singletonMap(present, "v1"));

        Map<Object, Object> result = fileMapStore.loadAll(Arrays.asList(present, missing));

        assertEquals(1, result.size());
        assertEquals("v1", result.get(present));
        assertFalse(result.containsKey(missing));
        assertNull(result.get(missing));
    }

    @Test
    void testLoadAllReturnsEmptyWhenStorageHasNothing() throws Exception {
        when(mapStorage.loadAll()).thenReturn(Collections.emptyMap());

        Map<Object, Object> result = fileMapStore.loadAll(Collections.singletonList(1L));

        assertTrue(result.isEmpty());
    }

    /** 读穿：map 启动时被跳过加载后，get 未命中内存就靠这条路径把单个 entry 取回来。 */
    @Test
    void testLoadDelegatesToStorage() {
        when(mapStorage.load(7L)).thenReturn("v7");

        assertEquals("v7", fileMapStore.load(7L));
        verify(mapStorage).load(7L);
    }

    /** 不实现读穿的插件（默认实现）返回 null，行为与引入 load 之前一致。 */
    @Test
    void testLoadReturnsNullWhenStorageDoesNotSupportReadThrough() {
        when(mapStorage.load(7L)).thenReturn(null);

        assertNull(fileMapStore.load(7L));
    }

    @Test
    void testLoadAllKeysDelegatesToStorage() throws Exception {
        Set<Object> keys = new HashSet<>(Collections.singletonList(1L));
        when(mapStorage.loadAllKeys()).thenReturn(keys);

        assertEquals(keys, fileMapStore.loadAllKeys());
        verify(mapStorage, never()).loadAll();
    }
}

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

package org.apache.seatunnel.connectors.seatunnel.cdc.tdengine.source.offset;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * TDengine TMQ offset, keyed by vgroupId and position.
 *
 * <p>The offset is stored as a map: {"vgroupId": "vgroup1", "offset": "12345"}
 */
public class TdengineOffset extends Offset {

    private static final long serialVersionUID = 1L;

    public static final String VGROUP_ID_KEY = "vgroupId";
    public static final String OFFSET_KEY = "offset";

    public static final TdengineOffset INITIAL_OFFSET = new TdengineOffset("", 0L);
    public static final TdengineOffset NO_STOPPING_OFFSET = new TdengineOffset("", Long.MAX_VALUE);

    public TdengineOffset(Map<String, String> offset) {
        this.offset = offset;
    }

    public TdengineOffset(String vgroupId, long position) {
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(VGROUP_ID_KEY, vgroupId);
        offsetMap.put(OFFSET_KEY, String.valueOf(position));
        this.offset = offsetMap;
    }

    public String getVgroupId() {
        return offset.getOrDefault(VGROUP_ID_KEY, "");
    }

    public long getPosition() {
        return Long.parseLong(offset.getOrDefault(OFFSET_KEY, "0"));
    }

    @Override
    public int compareTo(Offset o) {
        TdengineOffset that = (TdengineOffset) o;
        int vgroupCompare = this.getVgroupId().compareTo(that.getVgroupId());
        if (vgroupCompare != 0) {
            return vgroupCompare;
        }
        return Long.compare(this.getPosition(), that.getPosition());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TdengineOffset)) {
            return false;
        }
        TdengineOffset that = (TdengineOffset) o;
        return Objects.equals(offset, that.offset);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(offset);
    }
}

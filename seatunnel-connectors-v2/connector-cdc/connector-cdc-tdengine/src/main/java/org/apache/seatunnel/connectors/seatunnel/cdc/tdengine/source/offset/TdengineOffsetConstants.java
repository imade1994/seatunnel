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

/** Constants for TDengine CDC offset and record fields. */
public final class TdengineOffsetConstants {

    private TdengineOffsetConstants() {}

    /** Field name for the operation type in CDC records. */
    public static final String OP_FIELD = "op";

    /** Operation type: insert / create. */
    public static final String OP_INSERT = "c";

    /** Operation type: update. */
    public static final String OP_UPDATE = "u";

    /** Operation type: delete. */
    public static final String OP_DELETE = "d";

    /** Field name for the table name in CDC records. */
    public static final String TABLE_NAME_FIELD = "table_name";

    /** Field name for the database name. */
    public static final String DB_NAME_FIELD = "db_name";

    /** Field name for the super table name. */
    public static final String STABLE_NAME_FIELD = "stable_name";

    /** Field name for the fetch timestamp. */
    public static final String TS_MS_FIELD = "ts_ms";

    /** Field name for the source info. */
    public static final String SOURCE_FIELD = "source";

    /** Schema name for TDengine connector records. */
    public static final String SCHEMA_NAME = "tdengine";

    /** The dialect name. */
    public static final String DIALECT_NAME = "TDengine";
}

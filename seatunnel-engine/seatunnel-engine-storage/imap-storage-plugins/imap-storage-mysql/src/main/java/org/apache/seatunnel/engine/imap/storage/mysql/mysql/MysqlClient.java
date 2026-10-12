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

package org.apache.seatunnel.engine.imap.storage.mysql.mysql;

import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;
import org.apache.seatunnel.engine.imap.storage.mysql.config.StorageProperties;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.seatunnel.engine.imap.storage.mysql.util.SerializationUtils.deserialize;

/**
 * Minimal JDBC client for the durable IMAP state table. MySQL is only touched by the flush
 * scheduler and loadAll, both low frequency, so a fresh connection per batch operation is enough.
 */
@Slf4j
public class MysqlClient implements AutoCloseable {

    /** Keys per statement in {@link #loadBatch}; bounds statement size for large BLOB values. */
    private static final int MAX_KEYS_PER_QUERY = 200;

    private final String url;
    private final String username;
    private final String password;
    private final String table;
    private final String businessName;
    private final String clusterName;

    public MysqlClient(StorageProperties props) {
        this.url = props.getMysqlUrl();
        this.username = props.getMysqlUsername();
        this.password = props.getMysqlPassword();
        this.table = props.getMysqlTable();
        this.businessName = props.getBusinessName();
        this.clusterName = props.getClusterName();
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new IMapStorageException("MySQL driver not found on classpath", e);
        }
    }

    public void ensureTable() {
        String sql =
                "CREATE TABLE IF NOT EXISTS `"
                        + table
                        + "` ("
                        + "`business` VARCHAR(128) NOT NULL,"
                        + "`cluster` VARCHAR(128) NOT NULL,"
                        + "`map_key` VARBINARY(1024) NOT NULL,"
                        + "`key_class` VARCHAR(255) NOT NULL,"
                        + "`value` LONGBLOB NOT NULL,"
                        + "`value_class` VARCHAR(255) NOT NULL,"
                        + "`update_time` DATETIME(3) NOT NULL,"
                        + "PRIMARY KEY (`business`,`cluster`,`map_key`)"
                        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        try (Connection conn = getConnection();
                Statement st = conn.createStatement()) {
            st.execute(sql);
        } catch (SQLException e) {
            throw new IMapStorageException("Failed to create IMAP table " + table, e);
        }
    }

    /** Load the current state from MySQL, keyed by deserialized key object. */
    public Map<Object, Object> loadAll() {
        Map<Object, Object> result = new HashMap<>();
        String sql =
                "SELECT `map_key`,`key_class`,`value`,`value_class` FROM `"
                        + table
                        + "` WHERE `business`=? AND `cluster`=?";
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, businessName);
            ps.setString(2, clusterName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    byte[] keyBytes = rs.getBytes("map_key");
                    String keyClass = rs.getString("key_class");
                    byte[] valueBytes = rs.getBytes("value");
                    String valueClass = rs.getString("value_class");
                    result.put(
                            deserialize(keyBytes, loadClass(keyClass)),
                            deserialize(valueBytes, loadClass(valueClass)));
                }
            }
            return result;
        } catch (Exception e) {
            throw new IMapStorageException("Failed to load IMAP data from mysql", e);
        }
    }

    /**
     * Load a single entry by serialized key. Backs the read-through path used when a map was
     * skipped at startup and a {@code get()} misses the in-memory IMap.
     *
     * @return the stored value, or null if the row does not exist
     */
    public Object load(byte[] keyBytes) {
        String sql =
                "SELECT `value`,`value_class` FROM `"
                        + table
                        + "` WHERE `business`=? AND `cluster`=? AND `map_key`=?";
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, businessName);
            ps.setString(2, clusterName);
            ps.setBytes(3, keyBytes);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return deserialize(rs.getBytes("value"), loadClass(rs.getString("value_class")));
            }
        } catch (Exception e) {
            throw new IMapStorageException("Failed to load IMAP entry from mysql", e);
        }
    }

    /**
     * Load the given keys, chunked so a single statement cannot blow past {@code
     * max_allowed_packet} when the values are large. Keys absent from MySQL are simply missing from
     * the returned map.
     */
    public Map<Object, Object> loadBatch(List<byte[]> keys) {
        Map<Object, Object> result = new LinkedHashMap<>();
        if (keys == null || keys.isEmpty()) {
            return result;
        }
        for (int start = 0; start < keys.size(); start += MAX_KEYS_PER_QUERY) {
            List<byte[]> chunk =
                    keys.subList(start, Math.min(start + MAX_KEYS_PER_QUERY, keys.size()));
            StringBuilder sql =
                    new StringBuilder(
                            "SELECT `map_key`,`key_class`,`value`,`value_class` FROM `"
                                    + table
                                    + "` WHERE `business`=? AND `cluster`=? AND `map_key` IN (");
            for (int i = 0; i < chunk.size(); i++) {
                if (i > 0) {
                    sql.append(",");
                }
                sql.append("?");
            }
            sql.append(")");
            try (Connection conn = getConnection();
                    PreparedStatement ps = conn.prepareStatement(sql.toString())) {
                int idx = 1;
                ps.setString(idx++, businessName);
                ps.setString(idx++, clusterName);
                for (byte[] key : chunk) {
                    ps.setBytes(idx++, key);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        result.put(
                                deserialize(
                                        rs.getBytes("map_key"),
                                        loadClass(rs.getString("key_class"))),
                                deserialize(
                                        rs.getBytes("value"),
                                        loadClass(rs.getString("value_class"))));
                    }
                }
            } catch (Exception e) {
                throw new IMapStorageException("Failed to batch load IMAP data from mysql", e);
            }
        }
        return result;
    }

    /**
     * Load the most recently written entries, newest first (by {@code update_time}). Used by the
     * REST job-list endpoints, which must read history straight from storage because Hazelcast's
     * {@code IMap#values()} does not read through.
     */
    public Map<Object, Object> loadLatest(int limit) {
        Map<Object, Object> result = new LinkedHashMap<>();
        if (limit <= 0) {
            return result;
        }
        String sql =
                "SELECT `map_key`,`key_class`,`value`,`value_class` FROM `"
                        + table
                        + "` WHERE `business`=? AND `cluster`=? ORDER BY `update_time` DESC LIMIT ?";
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, businessName);
            ps.setString(2, clusterName);
            ps.setInt(3, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.put(
                            deserialize(
                                    rs.getBytes("map_key"), loadClass(rs.getString("key_class"))),
                            deserialize(
                                    rs.getBytes("value"), loadClass(rs.getString("value_class"))));
                }
            }
            return result;
        } catch (Exception e) {
            throw new IMapStorageException("Failed to load latest IMAP data from mysql", e);
        }
    }

    /** Load only the keys currently in MySQL. */
    public Set<Object> loadAllKeys() {
        java.util.HashSet<Object> keys = new java.util.HashSet<>();
        String sql =
                "SELECT `map_key`,`key_class` FROM `"
                        + table
                        + "` WHERE `business`=? AND `cluster`=?";
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, businessName);
            ps.setString(2, clusterName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    keys.add(
                            deserialize(
                                    rs.getBytes("map_key"), loadClass(rs.getString("key_class"))));
                }
            }
            return keys;
        } catch (Exception e) {
            throw new IMapStorageException("Failed to load IMAP keys from mysql", e);
        }
    }

    /** Batch upsert rows. Each row is [keyBytes, keyClass, valueBytes, valueClass]. */
    public void batchUpsert(List<Object[]> rows) {
        if (rows == null || rows.isEmpty()) {
            return;
        }
        StringBuilder sql =
                new StringBuilder(
                        "INSERT INTO `"
                                + table
                                + "`(`business`,`cluster`,`map_key`,`key_class`,`value`,`value_class`,`update_time`) VALUES ");
        for (int i = 0; i < rows.size(); i++) {
            if (i > 0) {
                sql.append(",");
            }
            sql.append("(?,?,?,?,?,?,NOW(3))");
        }
        sql.append(
                " ON DUPLICATE KEY UPDATE `key_class`=VALUES(`key_class`),`value`=VALUES(`value`),`value_class`=VALUES(`value_class`),`update_time`=NOW(3)");
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int idx = 1;
            for (Object[] row : rows) {
                ps.setString(idx++, businessName);
                ps.setString(idx++, clusterName);
                ps.setBytes(idx++, (byte[]) row[0]);
                ps.setString(idx++, (String) row[1]);
                ps.setBytes(idx++, (byte[]) row[2]);
                ps.setString(idx++, (String) row[3]);
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IMapStorageException("Failed to batch upsert IMAP data", e);
        }
    }

    /** Batch delete rows by serialized key bytes. */
    public void batchDelete(List<byte[]> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        StringBuilder sql =
                new StringBuilder(
                        "DELETE FROM `"
                                + table
                                + "` WHERE `business`=? AND `cluster`=? AND `map_key` IN (");
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) {
                sql.append(",");
            }
            sql.append("?");
        }
        sql.append(")");
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            int idx = 1;
            ps.setString(idx++, businessName);
            ps.setString(idx++, clusterName);
            for (byte[] key : keys) {
                ps.setBytes(idx++, key);
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IMapStorageException("Failed to batch delete IMAP data", e);
        }
    }

    public void deleteAll() {
        String sql = "DELETE FROM `" + table + "` WHERE `business`=? AND `cluster`=?";
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, businessName);
            ps.setString(2, clusterName);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IMapStorageException("Failed to delete all IMAP data", e);
        }
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }

    private static Class<?> loadClass(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IMapStorageException("Class not found: " + className, e);
        }
    }

    @Override
    public void close() {
        // no persistent resources; a fresh connection is acquired per batch operation
    }
}

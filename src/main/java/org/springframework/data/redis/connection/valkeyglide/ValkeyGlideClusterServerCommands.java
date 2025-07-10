/*
 * Copyright 2011-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection.valkeyglide;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterServerCommands;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisServerCommands.FlushOption;
import org.springframework.data.redis.connection.RedisServerCommands.MigrateOption;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.util.Assert;

/**
 * Implementation of {@link RedisClusterServerCommands} for Valkey-Glide.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideClusterServerCommands implements RedisClusterServerCommands {

    private final ValkeyGlideClusterConnection connection;

    /**
     * Create a new {@link ValkeyGlideClusterServerCommands}.
     *
     * @param connection must not be {@literal null}.
     */
    public ValkeyGlideClusterServerCommands(ValkeyGlideClusterConnection connection) {
        Assert.notNull(connection, "Connection must not be null!");
        this.connection = connection;
    }

    @Override
    public void bgReWriteAof(RedisClusterNode node) {
        connection.bgReWriteAof(node);
    }

    @Override
    public void bgSave(RedisClusterNode node) {
        connection.bgSave(node);
    }

    @Override
    public Long lastSave(RedisClusterNode node) {
        return connection.lastSave(node);
    }

    @Override
    public void save(RedisClusterNode node) {
        connection.save(node);
    }

    @Override
    public Long dbSize(RedisClusterNode node) {
        return connection.dbSize(node);
    }

    @Override
    public void flushDb(RedisClusterNode node) {
        connection.flushDb(node);
    }

    @Override
    public void flushDb(RedisClusterNode node, FlushOption option) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void flushAll(RedisClusterNode node) {
        connection.flushAll(node);
    }

    @Override
    public void flushAll(RedisClusterNode node, FlushOption option) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Properties info(RedisClusterNode node) {
        return connection.info(node);
    }

    @Override
    public Properties info(RedisClusterNode node, String section) {
        return connection.info(node, section);
    }

    @Override
    public void shutdown(RedisClusterNode node) {
        connection.shutdown(node);
    }

    @Override
    public Properties getConfig(RedisClusterNode node, String pattern) {
        return connection.getConfig(node, pattern);
    }

    @Override
    public void setConfig(RedisClusterNode node, String param, String value) {
        connection.setConfig(node, param, value);
    }

    @Override
    public void resetConfigStats(RedisClusterNode node) {
        connection.resetConfigStats(node);
    }

    @Override
    public void rewriteConfig(RedisClusterNode node) {
        connection.rewriteConfig(node);
    }

    @Override
    public Long time(RedisClusterNode node, TimeUnit timeUnit) {
        return connection.time(node);
    }

    @Override
    public List<RedisClientInfo> getClientList(RedisClusterNode node) {
        return connection.getClientList(node);
    }

    // Base server commands (non-cluster specific)

    @Override
    public void bgReWriteAof() {
        throw new UnsupportedOperationException("Use bgReWriteAof(RedisClusterNode) in cluster mode");
    }

    @Override
    public void bgSave() {
        throw new UnsupportedOperationException("Use bgSave(RedisClusterNode) in cluster mode");
    }

    @Override
    public Long lastSave() {
        throw new UnsupportedOperationException("Use lastSave(RedisClusterNode) in cluster mode");
    }

    @Override
    public void save() {
        throw new UnsupportedOperationException("Use save(RedisClusterNode) in cluster mode");
    }

    @Override
    public Long dbSize() {
        throw new UnsupportedOperationException("Use dbSize(RedisClusterNode) in cluster mode");
    }

    @Override
    public void flushDb() {
        throw new UnsupportedOperationException("Use flushDb(RedisClusterNode) in cluster mode");
    }

    @Override
    public void flushDb(FlushOption option) {
        throw new UnsupportedOperationException("Use flushDb(RedisClusterNode, FlushOption) in cluster mode");
    }

    @Override
    public void flushAll() {
        throw new UnsupportedOperationException("Use flushAll(RedisClusterNode) in cluster mode");
    }

    @Override
    public void flushAll(FlushOption option) {
        throw new UnsupportedOperationException("Use flushAll(RedisClusterNode, FlushOption) in cluster mode");
    }

    @Override
    public Properties info() {
        throw new UnsupportedOperationException("Use info(RedisClusterNode) in cluster mode");
    }

    @Override
    public Properties info(String section) {
        throw new UnsupportedOperationException("Use info(RedisClusterNode, String) in cluster mode");
    }

    @Override
    public void shutdown() {
        throw new UnsupportedOperationException("Use shutdown(RedisClusterNode) in cluster mode");
    }

    @Override
    public void shutdown(ShutdownOption option) {
        throw new UnsupportedOperationException("Use shutdown(RedisClusterNode) in cluster mode");
    }

    @Override
    public Properties getConfig(String pattern) {
        throw new UnsupportedOperationException("Use getConfig(RedisClusterNode, String) in cluster mode");
    }

    @Override
    public void setConfig(String param, String value) {
        throw new UnsupportedOperationException("Use setConfig(RedisClusterNode, String, String) in cluster mode");
    }

    @Override
    public void resetConfigStats() {
        throw new UnsupportedOperationException("Use resetConfigStats(RedisClusterNode) in cluster mode");
    }

    @Override
    public void rewriteConfig() {
        throw new UnsupportedOperationException("Use rewriteConfig(RedisClusterNode) in cluster mode");
    }

    @Override
    public Long time(TimeUnit timeUnit) {
        throw new UnsupportedOperationException("Use time(RedisClusterNode, TimeUnit) in cluster mode");
    }

    @Override
    public List<RedisClientInfo> getClientList() {
        throw new UnsupportedOperationException("Use getClientList(RedisClusterNode) in cluster mode");
    }
    
    @Override
    public void killClient(String host, int port) {
        throw new UnsupportedOperationException("Not supported in cluster mode");
    }

    @Override
    public void setClientName(byte[] name) {
        throw new UnsupportedOperationException("Not supported in cluster mode");
    }

    @Override
    public String getClientName() {
        throw new UnsupportedOperationException("Not supported in cluster mode");
    }

    public Long getClientId() {
        throw new UnsupportedOperationException("Not supported in cluster mode");
    }

    public void slaveOf(String host, int port) {
        throw new UnsupportedOperationException("Not supported in cluster mode");
    }

    public void replicaOf(String host, int port) {
        throw new UnsupportedOperationException("Not supported in cluster mode");
    }

    public void slaveOfNoOne() {
        throw new UnsupportedOperationException("Not supported in cluster mode");
    }

    public void replicaOfNoOne() {
        throw new UnsupportedOperationException("Not supported in cluster mode");
    }
    
    @Override
    public void migrate(byte[] key, RedisNode target, int dbIndex, MigrateOption option) {
        throw new UnsupportedOperationException("Not supported in cluster mode");
    }

    @Override
    public void migrate(byte[] key, RedisNode target, int dbIndex, MigrateOption option, long timeout) {
        throw new UnsupportedOperationException("Not supported in cluster mode");
    }
}

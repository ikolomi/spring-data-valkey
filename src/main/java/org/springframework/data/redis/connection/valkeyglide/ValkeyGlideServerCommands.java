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

import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisServerCommands;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Stub implementation of {@link RedisServerCommands} for Valkey-Glide.
 * All methods throw UnsupportedOperationException as placeholder.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideServerCommands implements RedisServerCommands {

    private final ValkeyGlideConnection connection;

    /**
     * Creates a new {@link ValkeyGlideServerCommands}.
     *
     * @param connection must not be {@literal null}.
     */
    public ValkeyGlideServerCommands(ValkeyGlideConnection connection) {
        Assert.notNull(connection, "Connection must not be null!");
        this.connection = connection;
    }

    @Override
    public void bgReWriteAof() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void bgSave() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long lastSave() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void save() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long dbSize() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void flushDb() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void flushDb(FlushOption option) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void flushAll() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void flushAll(FlushOption option) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Properties info() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Properties info(String section) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void shutdown() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void shutdown(ShutdownOption option) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Properties getConfig(String pattern) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void setConfig(String param, String value) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void resetConfigStats() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void rewriteConfig() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long time(TimeUnit timeUnit) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void killClient(String host, int port) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void setClientName(byte[] name) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public String getClientName() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<RedisClientInfo> getClientList() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void replicaOf(String host, int port) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void replicaOfNoOne() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void migrate(byte[] key, RedisNode target, int dbIndex, @Nullable MigrateOption option) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void migrate(byte[] key, RedisNode target, int dbIndex, @Nullable MigrateOption option, long timeout) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}

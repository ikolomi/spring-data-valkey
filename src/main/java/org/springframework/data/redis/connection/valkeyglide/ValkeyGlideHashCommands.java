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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.ExpirationOptions;
import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Stub implementation of {@link RedisHashCommands} for Valkey-Glide.
 * All methods throw UnsupportedOperationException as placeholder.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideHashCommands implements RedisHashCommands {

    private final ValkeyGlideConnection connection;

    /**
     * Creates a new {@link ValkeyGlideHashCommands}.
     *
     * @param connection must not be {@literal null}.
     */
    public ValkeyGlideHashCommands(ValkeyGlideConnection connection) {
        Assert.notNull(connection, "Connection must not be null!");
        this.connection = connection;
    }

    @Override
    @Nullable
    public Boolean hSet(byte[] key, byte[] field, byte[] value) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public byte[] hGet(byte[] key, byte[] field) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<byte[]> hMGet(byte[] key, byte[]... fields) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void hMSet(byte[] key, Map<byte[], byte[]> hashes) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long hIncrBy(byte[] key, byte[] field, long delta) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Double hIncrBy(byte[] key, byte[] field, double delta) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Boolean hExists(byte[] key, byte[] field) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long hDel(byte[] key, byte[]... fields) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long hLen(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<byte[]> hKeys(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<byte[]> hVals(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Map<byte[], byte[]> hGetAll(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public byte[] hRandField(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Map.Entry<byte[], byte[]> hRandFieldWithValues(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<byte[]> hRandField(byte[] key, long count) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<Map.Entry<byte[], byte[]>> hRandFieldWithValues(byte[] key, long count) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Cursor<Map.Entry<byte[], byte[]>> hScan(byte[] key, ScanOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long hStrLen(byte[] key, byte[] field) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<Long> hExpire(byte[] key, long seconds, ExpirationOptions.Condition condition, byte[]... fields) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<Long> hpExpire(byte[] key, long millis, ExpirationOptions.Condition condition, byte[]... fields) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<Long> hExpireAt(byte[] key, long unixTime, ExpirationOptions.Condition condition, byte[]... fields) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<Long> hpExpireAt(byte[] key, long unixTimeInMillis, ExpirationOptions.Condition condition, byte[]... fields) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<Long> hPersist(byte[] key, byte[]... fields) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<Long> hTtl(byte[] key, byte[]... fields) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<Long> hTtl(byte[] key, TimeUnit timeUnit, byte[]... fields) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<Long> hpTtl(byte[] key, byte[]... fields) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}

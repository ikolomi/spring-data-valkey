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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.ExpirationOptions;
import org.springframework.data.redis.connection.RedisKeyCommands;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.ValueEncoding;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.KeyScanOptions;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Stub implementation of {@link RedisKeyCommands} for Valkey-Glide.
 * All methods throw UnsupportedOperationException as placeholder.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideKeyCommands implements RedisKeyCommands {

    private final ValkeyGlideConnection connection;

    /**
     * Creates a new {@link ValkeyGlideKeyCommands}.
     *
     * @param connection must not be {@literal null}.
     */
    public ValkeyGlideKeyCommands(ValkeyGlideConnection connection) {
        Assert.notNull(connection, "Connection must not be null!");
        this.connection = connection;
    }

    @Override
    @Nullable
    public Boolean copy(byte[] sourceKey, byte[] targetKey, boolean replace) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long exists(byte[]... keys) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long del(byte[]... keys) {
        Assert.notEmpty(keys, "Keys must not be empty");
        
        try {
            Object result = connection.execute("DEL", (Object[]) keys);
            return result instanceof Number ? ((Number) result).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long unlink(byte[]... keys) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public DataType type(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long touch(byte[]... keys) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<byte[]> keys(byte[] pattern) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Cursor<byte[]> scan(ScanOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public byte[] randomKey() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void rename(byte[] oldKey, byte[] newKey) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Boolean renameNX(byte[] oldKey, byte[] newKey) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Boolean expire(byte[] key, long seconds, ExpirationOptions.Condition condition) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Boolean pExpire(byte[] key, long millis, ExpirationOptions.Condition condition) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Boolean expireAt(byte[] key, long unixTime, ExpirationOptions.Condition condition) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Boolean pExpireAt(byte[] key, long unixTimeInMillis, ExpirationOptions.Condition condition) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Boolean persist(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Boolean move(byte[] key, int dbIndex) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long ttl(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long ttl(byte[] key, TimeUnit timeUnit) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long pTtl(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long pTtl(byte[] key, TimeUnit timeUnit) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<byte[]> sort(byte[] key, SortParameters params) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long sort(byte[] key, SortParameters params, byte[] storeKey) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public byte[] dump(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void restore(byte[] key, long ttlInMillis, byte[] serializedValue, boolean replace) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public ValueEncoding encodingOf(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Duration idletime(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long refcount(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}

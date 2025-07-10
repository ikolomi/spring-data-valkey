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

import org.springframework.data.redis.connection.RedisListCommands;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Stub implementation of {@link RedisListCommands} for Valkey-Glide.
 * All methods throw UnsupportedOperationException as placeholder.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideListCommands implements RedisListCommands {

    private final ValkeyGlideConnection connection;

    /**
     * Creates a new {@link ValkeyGlideListCommands}.
     *
     * @param connection must not be {@literal null}.
     */
    public ValkeyGlideListCommands(ValkeyGlideConnection connection) {
        Assert.notNull(connection, "Connection must not be null!");
        this.connection = connection;
    }

    @Override
    @Nullable
    public Long rPush(byte[] key, byte[]... values) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<Long> lPos(byte[] key, byte[] element, @Nullable Integer rank, @Nullable Integer count) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long lPush(byte[] key, byte[]... values) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long rPushX(byte[] key, byte[] value) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long lPushX(byte[] key, byte[] value) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long lLen(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<byte[]> lRange(byte[] key, long start, long end) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void lTrim(byte[] key, long start, long end) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public byte[] lIndex(byte[] key, long index) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public byte[] lMove(byte[] sourceKey, byte[] destinationKey, Direction from, Direction to) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public byte[] bLMove(byte[] sourceKey, byte[] destinationKey, Direction from, Direction to, double timeout) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void lSet(byte[] key, long index, byte[] value) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long lRem(byte[] key, long count, byte[] value) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public byte[] lPop(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<byte[]> lPop(byte[] key, long count) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public byte[] rPop(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<byte[]> rPop(byte[] key, long count) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<byte[]> bLPop(int timeout, byte[]... keys) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<byte[]> bRPop(int timeout, byte[]... keys) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}

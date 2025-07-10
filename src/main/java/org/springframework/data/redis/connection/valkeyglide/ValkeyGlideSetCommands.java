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
import java.util.Set;

import org.springframework.data.redis.connection.RedisSetCommands;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Stub implementation of {@link RedisSetCommands} for Valkey-Glide.
 * All methods throw UnsupportedOperationException as placeholder.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideSetCommands implements RedisSetCommands {

    private final ValkeyGlideConnection connection;

    /**
     * Creates a new {@link ValkeyGlideSetCommands}.
     *
     * @param connection must not be {@literal null}.
     */
    public ValkeyGlideSetCommands(ValkeyGlideConnection connection) {
        Assert.notNull(connection, "Connection must not be null!");
        this.connection = connection;
    }

    @Override
    @Nullable
    public Long sAdd(byte[] key, byte[]... values) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long sRem(byte[] key, byte[]... values) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public byte[] sPop(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<byte[]> sPop(byte[] key, long count) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long sCard(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Boolean sIsMember(byte[] key, byte[] value) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<Boolean> sMIsMember(byte[] key, byte[]... values) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<byte[]> sDiff(byte[]... keys) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long sDiffStore(byte[] destKey, byte[]... keys) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<byte[]> sInter(byte[]... keys) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long sInterStore(byte[] destKey, byte[]... keys) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<byte[]> sUnion(byte[]... keys) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long sUnionStore(byte[] destKey, byte[]... keys) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<byte[]> sMembers(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public byte[] sRandMember(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<byte[]> sRandMember(byte[] key, long count) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Cursor<byte[]> sScan(byte[] key, ScanOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}

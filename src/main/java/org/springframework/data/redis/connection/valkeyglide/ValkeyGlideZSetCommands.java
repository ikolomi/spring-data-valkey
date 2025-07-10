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
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.connection.zset.Weights;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Stub implementation of {@link RedisZSetCommands} for Valkey-Glide.
 * All methods throw UnsupportedOperationException as placeholder.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideZSetCommands implements RedisZSetCommands {

    private final ValkeyGlideConnection connection;

    /**
     * Creates a new {@link ValkeyGlideZSetCommands}.
     *
     * @param connection must not be {@literal null}.
     */
    public ValkeyGlideZSetCommands(ValkeyGlideConnection connection) {
        Assert.notNull(connection, "Connection must not be null!");
        this.connection = connection;
    }

    @Override
    @Nullable
    public Boolean zAdd(byte[] key, double score, byte[] value, ZAddArgs args) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long zAdd(byte[] key, Set<Tuple> tuples, ZAddArgs args) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long zRem(byte[] key, byte[]... values) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Double zIncrBy(byte[] key, double increment, byte[] value) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public byte[] zRandMember(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<byte[]> zRandMember(byte[] key, long count) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Tuple zRandMemberWithScore(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<Tuple> zRandMemberWithScore(byte[] key, long count) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long zRank(byte[] key, byte[] value) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long zRevRank(byte[] key, byte[] value) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<byte[]> zRange(byte[] key, long start, long end) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<Tuple> zRangeByScoreWithScores(byte[] key, org.springframework.data.domain.Range<? extends Number> range,
            org.springframework.data.redis.connection.Limit limit) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<byte[]> zRevRange(byte[] key, long start, long end) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<byte[]> zRevRangeByScore(byte[] key, org.springframework.data.domain.Range<? extends Number> range,
            org.springframework.data.redis.connection.Limit limit) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, org.springframework.data.domain.Range<? extends Number> range,
            org.springframework.data.redis.connection.Limit limit) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long zCount(byte[] key, org.springframework.data.domain.Range<? extends Number> range) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long zLexCount(byte[] key, org.springframework.data.domain.Range<byte[]> range) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Tuple zPopMin(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<Tuple> zPopMin(byte[] key, long count) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Tuple bZPopMin(byte[] key, long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Tuple zPopMax(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<Tuple> zPopMax(byte[] key, long count) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Tuple bZPopMax(byte[] key, long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long zCard(byte[] key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Double zScore(byte[] key, byte[] value) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public List<Double> zMScore(byte[] key, byte[]... values) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long zRemRange(byte[] key, long start, long end) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long zRemRangeByLex(byte[] key, org.springframework.data.domain.Range<byte[]> range) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long zRemRangeByScore(byte[] key, org.springframework.data.domain.Range<? extends Number> range) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<byte[]> zDiff(byte[]... sets) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<Tuple> zDiffWithScores(byte[]... sets) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long zDiffStore(byte[] destKey, byte[]... sets) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<byte[]> zInter(byte[]... sets) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<Tuple> zInterWithScores(byte[]... sets) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<Tuple> zInterWithScores(Aggregate aggregate, Weights weights, byte[]... sets) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long zInterStore(byte[] destKey, byte[]... sets) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long zInterStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<byte[]> zUnion(byte[]... sets) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<Tuple> zUnionWithScores(byte[]... sets) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<Tuple> zUnionWithScores(Aggregate aggregate, Weights weights, byte[]... sets) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long zUnionStore(byte[] destKey, byte[]... sets) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long zUnionStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Cursor<Tuple> zScan(byte[] key, ScanOptions options) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<byte[]> zRangeByScore(byte[] key, String min, String max, long offset, long count) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<byte[]> zRangeByScore(byte[] key, org.springframework.data.domain.Range<? extends Number> range,
            org.springframework.data.redis.connection.Limit limit) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<byte[]> zRangeByLex(byte[] key, org.springframework.data.domain.Range<byte[]> range,
            org.springframework.data.redis.connection.Limit limit) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Set<byte[]> zRevRangeByLex(byte[] key, org.springframework.data.domain.Range<byte[]> range,
            org.springframework.data.redis.connection.Limit limit) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long zRangeStoreByLex(byte[] dstKey, byte[] srcKey, org.springframework.data.domain.Range<byte[]> range,
            org.springframework.data.redis.connection.Limit limit) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long zRangeStoreRevByLex(byte[] dstKey, byte[] srcKey, org.springframework.data.domain.Range<byte[]> range,
            org.springframework.data.redis.connection.Limit limit) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long zRangeStoreByScore(byte[] dstKey, byte[] srcKey, org.springframework.data.domain.Range<? extends Number> range,
            org.springframework.data.redis.connection.Limit limit) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    @Nullable
    public Long zRangeStoreRevByScore(byte[] dstKey, byte[] srcKey, org.springframework.data.domain.Range<? extends Number> range,
            org.springframework.data.redis.connection.Limit limit) {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}

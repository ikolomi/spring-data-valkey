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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.zset.Aggregate;
import org.springframework.data.redis.connection.zset.DefaultTuple;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.connection.zset.Weights;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Implementation of {@link RedisZSetCommands} for Valkey-Glide.
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
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(value, "Value must not be null");
        Assert.notNull(args, "ZAddArgs must not be null");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(key);
            
            // Add arguments based on ZAddArgs
            if (args.contains(ZAddArgs.Flag.NX)) {
                commandArgs.add("NX");
            }
            if (args.contains(ZAddArgs.Flag.XX)) {
                commandArgs.add("XX");
            }
            if (args.contains(ZAddArgs.Flag.GT)) {
                commandArgs.add("GT");
            }
            if (args.contains(ZAddArgs.Flag.LT)) {
                commandArgs.add("LT");
            }
            if (args.contains(ZAddArgs.Flag.CH)) {
                commandArgs.add("CH");
            }
            
            commandArgs.add(score);
            commandArgs.add(value);
            
            Object result = connection.execute("ZADD", commandArgs.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted instanceof Number) {
                long longValue = ((Number) converted).longValue();
                // Simple boolean conversion: return true if result > 0, false otherwise
                // This matches the Jedis implementation approach
                return longValue > 0;
            } else {
                return false;
            }
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long zAdd(byte[] key, Set<Tuple> tuples, ZAddArgs args) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(tuples, "Tuples must not be null");
        Assert.notNull(args, "ZAddArgs must not be null");
        
        if (tuples.isEmpty()) {
            return 0L;
        }
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(key);
            
            // Add arguments based on ZAddArgs
            if (args.contains(ZAddArgs.Flag.NX)) {
                commandArgs.add("NX");
            }
            if (args.contains(ZAddArgs.Flag.XX)) {
                commandArgs.add("XX");
            }
            if (args.contains(ZAddArgs.Flag.GT)) {
                commandArgs.add("GT");
            }
            if (args.contains(ZAddArgs.Flag.LT)) {
                commandArgs.add("LT");
            }
            if (args.contains(ZAddArgs.Flag.CH)) {
                commandArgs.add("CH");
            }
            
            // Add score-value pairs
            for (Tuple tuple : tuples) {
                commandArgs.add(tuple.getScore());
                commandArgs.add(tuple.getValue());
            }
            
            Object result = connection.execute("ZADD", commandArgs.toArray());
            return ((Number) ValkeyGlideConverters.defaultFromGlideResult(result)).longValue();
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long zRem(byte[] key, byte[]... values) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(values, "Values must not be null");
        Assert.noNullElements(values, "Values must not contain null elements");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(key);
            for (byte[] value : values) {
                commandArgs.add(value);
            }
            
            Object result = connection.execute("ZREM", commandArgs.toArray());
            return ((Number) ValkeyGlideConverters.defaultFromGlideResult(result)).longValue();
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Double zIncrBy(byte[] key, double increment, byte[] value) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            Object result = connection.execute("ZINCRBY", key, increment, value);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted instanceof Number) {
                return ((Number) converted).doubleValue();
            }
            if (converted instanceof String) {
                return Double.parseDouble((String) converted);
            }
            if (converted instanceof byte[]) {
                return Double.parseDouble(new String((byte[]) converted));
            }
            
            return null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public byte[] zRandMember(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("ZRANDMEMBER", key);
            return (byte[]) ValkeyGlideConverters.defaultFromGlideResult(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public List<byte[]> zRandMember(byte[] key, long count) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("ZRANDMEMBER", key, count);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted == null) {
                return null;
            }
            
            List<Object> list = convertToList(converted);
            List<byte[]> resultList = new ArrayList<>(list.size());
            for (Object item : list) {
                resultList.add((byte[]) ValkeyGlideConverters.defaultFromGlideResult(item));
            }
            return resultList;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Tuple zRandMemberWithScore(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("ZRANDMEMBER", key, 1, "WITHSCORES");
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted == null) {
                return null;
            }
            
            List<Object> list = convertToList(converted);
            
            // Handle nested structure: the result is a list containing one element which is itself a [value, score] pair
            if (list.size() >= 1) {
                Object firstElement = list.get(0);
                
                // Check if the first element is a nested array/list containing [value, score]
                if (firstElement instanceof List) {
                    List<?> nestedList = (List<?>) firstElement;
                    
                    if (nestedList.size() >= 2) {
                        // The elements in the nested list are already in their final form
                        Object valueObj = nestedList.get(0);
                        Object scoreObj = nestedList.get(1);
                        
                        // Handle the value using the proper conversion method
                        byte[] value = convertToByteArray(valueObj);
                        
                        // Handle the score
                        Double score = parseScore(scoreObj);
                        
                        return new DefaultTuple(value, score);
                    }
                }
            }
            
            return null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public List<Tuple> zRandMemberWithScore(byte[] key, long count) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("ZRANDMEMBER", key, count, "WITHSCORES");
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted == null) {
                return null;
            }
            
            List<Object> list = convertToList(converted);
            List<Tuple> resultList = new ArrayList<>();
            
            // Check if we have nested arrays/lists (valkey-glide format)
            if (!list.isEmpty() && list.get(0) instanceof List) {
                // Format: [[member1, score1], [member2, score2], ...]
                for (Object item : list) {
                    if (item instanceof List) {
                        List<?> tupleList = (List<?>) item;
                        if (tupleList.size() >= 2) {
                            Object valueObj = tupleList.get(0);
                            Object scoreObj = tupleList.get(1);
                            
                            byte[] value = convertToByteArray(valueObj);
                            Double score = parseScore(scoreObj);
                            resultList.add(new DefaultTuple(value, score));
                        }
                    }
                }
            } else {
                // Standard flat format: [member, score, member, score, ...]
                for (int i = 0; i < list.size(); i += 2) {
                    if (i + 1 < list.size()) {
                        Object valueObj = ValkeyGlideConverters.defaultFromGlideResult(list.get(i));
                        Object scoreObj = ValkeyGlideConverters.defaultFromGlideResult(list.get(i + 1));
                        
                        byte[] value = convertToByteArray(valueObj);
                        Double score = parseScore(scoreObj);
                        resultList.add(new DefaultTuple(value, score));
                    }
                }
            }
            
            return resultList;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long zRank(byte[] key, byte[] value) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            Object result = connection.execute("ZRANK", key, value);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted instanceof Number) {
                return ((Number) converted).longValue();
            }
            
            return null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long zRevRank(byte[] key, byte[] value) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            Object result = connection.execute("ZREVRANK", key, value);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted instanceof Number) {
                return ((Number) converted).longValue();
            }
            
            return null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<byte[]> zRange(byte[] key, long start, long end) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("ZRANGE", key, start, end);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted == null) {
                return new LinkedHashSet<>();
            }
            
            List<Object> list = convertToList(converted);
            Set<byte[]> resultSet = new LinkedHashSet<>(list.size());
            for (Object item : list) {
                resultSet.add((byte[]) ValkeyGlideConverters.defaultFromGlideResult(item));
            }
            return resultSet;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<Tuple> zRangeWithScores(byte[] key, long start, long end) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("ZRANGE", key, start, end, "WITHSCORES");
            return convertToTupleSet(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<byte[]> zRangeByScore(byte[] key, org.springframework.data.domain.Range<? extends Number> range,
            org.springframework.data.redis.connection.Limit limit) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(range, "Range must not be null");
        Assert.notNull(limit, "Limit must not be null");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(key);
            
            // Add range bounds
            commandArgs.add(formatRangeBound(range.getLowerBound(), true));
            commandArgs.add(formatRangeBound(range.getUpperBound(), false));
            
            // Add limit if specified
            if (limit.isLimited()) {
                commandArgs.add("LIMIT");
                commandArgs.add(limit.getOffset());
                commandArgs.add(limit.getCount());
            }
            
            Object result = connection.execute("ZRANGEBYSCORE", commandArgs.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted == null) {
                return new LinkedHashSet<>();
            }
            
            List<Object> list = convertToList(converted);
            Set<byte[]> resultSet = new LinkedHashSet<>(list.size());
            for (Object item : list) {
                resultSet.add((byte[]) ValkeyGlideConverters.defaultFromGlideResult(item));
            }
            return resultSet;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<byte[]> zRangeByScore(byte[] key, String min, String max, long offset, long count) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(min, "Min must not be null");
        Assert.notNull(max, "Max must not be null");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(key);
            commandArgs.add(min);
            commandArgs.add(max);
            commandArgs.add("LIMIT");
            commandArgs.add(offset);
            commandArgs.add(count);
            
            Object result = connection.execute("ZRANGEBYSCORE", commandArgs.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted == null) {
                return new LinkedHashSet<>();
            }
            
            List<Object> list = convertToList(converted);
            Set<byte[]> resultSet = new LinkedHashSet<>(list.size());
            for (Object item : list) {
                resultSet.add((byte[]) ValkeyGlideConverters.defaultFromGlideResult(item));
            }
            return resultSet;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<Tuple> zRangeByScoreWithScores(byte[] key, org.springframework.data.domain.Range<? extends Number> range,
            org.springframework.data.redis.connection.Limit limit) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(range, "Range must not be null");
        Assert.notNull(limit, "Limit must not be null");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(key);
            
            // Add range bounds
            commandArgs.add(formatRangeBound(range.getLowerBound(), true));
            commandArgs.add(formatRangeBound(range.getUpperBound(), false));
            
            commandArgs.add("WITHSCORES");
            
            // Add limit if specified
            if (limit.isLimited()) {
                commandArgs.add("LIMIT");
                commandArgs.add(limit.getOffset());
                commandArgs.add(limit.getCount());
            }
            
            Object result = connection.execute("ZRANGEBYSCORE", commandArgs.toArray());
            return convertToTupleSet(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<byte[]> zRevRange(byte[] key, long start, long end) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("ZREVRANGE", key, start, end);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted == null) {
                return new LinkedHashSet<>();
            }
            
            List<Object> list = convertToList(converted);
            Set<byte[]> resultSet = new LinkedHashSet<>(list.size());
            for (Object item : list) {
                resultSet.add((byte[]) ValkeyGlideConverters.defaultFromGlideResult(item));
            }
            return resultSet;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<Tuple> zRevRangeWithScores(byte[] key, long start, long end) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("ZREVRANGE", key, start, end, "WITHSCORES");
            return convertToTupleSet(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<byte[]> zRevRangeByScore(byte[] key, org.springframework.data.domain.Range<? extends Number> range,
            org.springframework.data.redis.connection.Limit limit) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(range, "Range must not be null");
        Assert.notNull(limit, "Limit must not be null");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(key);
            
            // Add range bounds (reversed for ZREVRANGEBYSCORE)
            commandArgs.add(formatRangeBound(range.getUpperBound(), true));
            commandArgs.add(formatRangeBound(range.getLowerBound(), false));
            
            // Add limit if specified
            if (limit.isLimited()) {
                commandArgs.add("LIMIT");
                commandArgs.add(limit.getOffset());
                commandArgs.add(limit.getCount());
            }
            
            Object result = connection.execute("ZREVRANGEBYSCORE", commandArgs.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted == null) {
                return new LinkedHashSet<>();
            }
            
            List<Object> list = convertToList(converted);
            Set<byte[]> resultSet = new LinkedHashSet<>(list.size());
            for (Object item : list) {
                resultSet.add((byte[]) ValkeyGlideConverters.defaultFromGlideResult(item));
            }
            return resultSet;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<Tuple> zRevRangeByScoreWithScores(byte[] key, org.springframework.data.domain.Range<? extends Number> range,
            org.springframework.data.redis.connection.Limit limit) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(range, "Range must not be null");
        Assert.notNull(limit, "Limit must not be null");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(key);
            
            // Add range bounds (reversed for ZREVRANGEBYSCORE)
            commandArgs.add(formatRangeBound(range.getUpperBound(), true));
            commandArgs.add(formatRangeBound(range.getLowerBound(), false));
            
            commandArgs.add("WITHSCORES");
            
            // Add limit if specified
            if (limit.isLimited()) {
                commandArgs.add("LIMIT");
                commandArgs.add(limit.getOffset());
                commandArgs.add(limit.getCount());
            }
            
            Object result = connection.execute("ZREVRANGEBYSCORE", commandArgs.toArray());
            return convertToTupleSet(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long zCount(byte[] key, org.springframework.data.domain.Range<? extends Number> range) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(range, "Range must not be null");
        
        try {
            Object result = connection.execute("ZCOUNT", key, 
                formatRangeBound(range.getLowerBound(), true), 
                formatRangeBound(range.getUpperBound(), false));
            
            return ((Number) ValkeyGlideConverters.defaultFromGlideResult(result)).longValue();
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long zLexCount(byte[] key, org.springframework.data.domain.Range<byte[]> range) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(range, "Range must not be null");
        
        try {
            Object result = connection.execute("ZLEXCOUNT", key, 
                formatLexRangeBound(range.getLowerBound(), true), 
                formatLexRangeBound(range.getUpperBound(), false));
            
            return ((Number) ValkeyGlideConverters.defaultFromGlideResult(result)).longValue();
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Tuple zPopMin(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("ZPOPMIN", key);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted == null) {
                return null;
            }
            
            List<Object> list = convertToList(converted);
            if (list.size() >= 2) {
                byte[] value = (byte[]) ValkeyGlideConverters.defaultFromGlideResult(list.get(0));
                Double score = parseScore(ValkeyGlideConverters.defaultFromGlideResult(list.get(1)));
                return new DefaultTuple(value, score);
            }
            
            return null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<Tuple> zPopMin(byte[] key, long count) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("ZPOPMIN", key, count);
            return convertToTupleSetForPop(result, true);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Tuple bZPopMin(byte[] key, long timeout, TimeUnit unit) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(unit, "TimeUnit must not be null");
        
        try {
            long timeoutInSeconds = unit.toSeconds(timeout);
            Object result = connection.execute("BZPOPMIN", key, timeoutInSeconds);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted == null) {
                return null;
            }
            
            List<Object> list = convertToList(converted);
            if (list.size() >= 3) {
                // Result format: [key, value, score]
                byte[] value = (byte[]) ValkeyGlideConverters.defaultFromGlideResult(list.get(1));
                Double score = parseScore(ValkeyGlideConverters.defaultFromGlideResult(list.get(2)));
                return new DefaultTuple(value, score);
            }
            
            return null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Tuple zPopMax(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("ZPOPMAX", key);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted == null) {
                return null;
            }
            
            List<Object> list = convertToList(converted);
            if (list.size() >= 2) {
                byte[] value = (byte[]) ValkeyGlideConverters.defaultFromGlideResult(list.get(0));
                Double score = parseScore(ValkeyGlideConverters.defaultFromGlideResult(list.get(1)));
                return new DefaultTuple(value, score);
            }
            
            return null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<Tuple> zPopMax(byte[] key, long count) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("ZPOPMAX", key, count);
            return convertToTupleSetForPop(result, false);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Tuple bZPopMax(byte[] key, long timeout, TimeUnit unit) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(unit, "TimeUnit must not be null");
        
        try {
            long timeoutInSeconds = unit.toSeconds(timeout);
            Object result = connection.execute("BZPOPMAX", key, timeoutInSeconds);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted == null) {
                return null;
            }
            
            List<Object> list = convertToList(converted);
            if (list.size() >= 3) {
                // Result format: [key, value, score]
                byte[] value = (byte[]) ValkeyGlideConverters.defaultFromGlideResult(list.get(1));
                Double score = parseScore(ValkeyGlideConverters.defaultFromGlideResult(list.get(2)));
                return new DefaultTuple(value, score);
            }
            
            return null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long zCard(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("ZCARD", key);
            return ((Number) ValkeyGlideConverters.defaultFromGlideResult(result)).longValue();
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Double zScore(byte[] key, byte[] value) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            Object result = connection.execute("ZSCORE", key, value);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted == null) {
                return null;
            }
            
            return parseScore(converted);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public List<Double> zMScore(byte[] key, byte[]... values) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(values, "Values must not be null");
        Assert.noNullElements(values, "Values must not contain null elements");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(key);
            for (byte[] value : values) {
                commandArgs.add(value);
            }
            
            Object result = connection.execute("ZMSCORE", commandArgs.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted == null) {
                return null;
            }
            
            List<Object> list = convertToList(converted);
            List<Double> resultList = new ArrayList<>(list.size());
            for (Object item : list) {
                Object convertedItem = ValkeyGlideConverters.defaultFromGlideResult(item);
                if (convertedItem == null) {
                    resultList.add(null);
                } else {
                    resultList.add(parseScore(convertedItem));
                }
            }
            return resultList;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long zRemRange(byte[] key, long start, long end) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("ZREMRANGEBYRANK", key, start, end);
            return ((Number) ValkeyGlideConverters.defaultFromGlideResult(result)).longValue();
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long zRemRangeByLex(byte[] key, org.springframework.data.domain.Range<byte[]> range) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(range, "Range must not be null");
        
        try {
            Object result = connection.execute("ZREMRANGEBYLEX", key, 
                formatLexRangeBound(range.getLowerBound(), true), 
                formatLexRangeBound(range.getUpperBound(), false));
            
            return ((Number) ValkeyGlideConverters.defaultFromGlideResult(result)).longValue();
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long zRemRangeByScore(byte[] key, org.springframework.data.domain.Range<? extends Number> range) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(range, "Range must not be null");
        
        try {
            Object result = connection.execute("ZREMRANGEBYSCORE", key, 
                formatRangeBound(range.getLowerBound(), true), 
                formatRangeBound(range.getUpperBound(), false));
            
            return ((Number) ValkeyGlideConverters.defaultFromGlideResult(result)).longValue();
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<byte[]> zDiff(byte[]... sets) {
        Assert.notNull(sets, "Sets must not be null");
        Assert.noNullElements(sets, "Sets must not contain null elements");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(sets.length);
            for (byte[] set : sets) {
                commandArgs.add(set);
            }
            
            Object result = connection.execute("ZDIFF", commandArgs.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted == null) {
                return new LinkedHashSet<>();
            }
            
            List<Object> list = convertToList(converted);
            Set<byte[]> resultSet = new LinkedHashSet<>(list.size());
            for (Object item : list) {
                resultSet.add((byte[]) ValkeyGlideConverters.defaultFromGlideResult(item));
            }
            return resultSet;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<Tuple> zDiffWithScores(byte[]... sets) {
        Assert.notNull(sets, "Sets must not be null");
        Assert.noNullElements(sets, "Sets must not contain null elements");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(sets.length);
            for (byte[] set : sets) {
                commandArgs.add(set);
            }
            commandArgs.add("WITHSCORES");
            
            Object result = connection.execute("ZDIFF", commandArgs.toArray());
            return convertToTupleSet(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long zDiffStore(byte[] destKey, byte[]... sets) {
        Assert.notNull(destKey, "Destination key must not be null");
        Assert.notNull(sets, "Sets must not be null");
        Assert.noNullElements(sets, "Sets must not contain null elements");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(destKey);
            commandArgs.add(sets.length);
            for (byte[] set : sets) {
                commandArgs.add(set);
            }
            
            Object result = connection.execute("ZDIFFSTORE", commandArgs.toArray());
            return ((Number) ValkeyGlideConverters.defaultFromGlideResult(result)).longValue();
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<byte[]> zInter(byte[]... sets) {
        Assert.notNull(sets, "Sets must not be null");
        Assert.noNullElements(sets, "Sets must not contain null elements");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(sets.length);
            for (byte[] set : sets) {
                commandArgs.add(set);
            }
            
            Object result = connection.execute("ZINTER", commandArgs.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted == null) {
                return new LinkedHashSet<>();
            }
            
            List<Object> list = convertToList(converted);
            Set<byte[]> resultSet = new LinkedHashSet<>(list.size());
            for (Object item : list) {
                resultSet.add((byte[]) ValkeyGlideConverters.defaultFromGlideResult(item));
            }
            return resultSet;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<Tuple> zInterWithScores(byte[]... sets) {
        Assert.notNull(sets, "Sets must not be null");
        Assert.noNullElements(sets, "Sets must not contain null elements");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(sets.length);
            for (byte[] set : sets) {
                commandArgs.add(set);
            }
            commandArgs.add("WITHSCORES");
            
            Object result = connection.execute("ZINTER", commandArgs.toArray());
            return convertToTupleSet(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<Tuple> zInterWithScores(Aggregate aggregate, Weights weights, byte[]... sets) {
        Assert.notNull(aggregate, "Aggregate must not be null");
        Assert.notNull(weights, "Weights must not be null");
        Assert.notNull(sets, "Sets must not be null");
        Assert.noNullElements(sets, "Sets must not contain null elements");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(sets.length);
            for (byte[] set : sets) {
                commandArgs.add(set);
            }
            
            // Add WEIGHTS
            commandArgs.add("WEIGHTS");
            for (double weight : weights.toArray()) {
                commandArgs.add(weight);
            }
            
            // Add AGGREGATE
            commandArgs.add("AGGREGATE");
            commandArgs.add(aggregate.name());
            
            commandArgs.add("WITHSCORES");
            
            Object result = connection.execute("ZINTER", commandArgs.toArray());
            return convertToTupleSet(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long zInterStore(byte[] destKey, byte[]... sets) {
        Assert.notNull(destKey, "Destination key must not be null");
        Assert.notNull(sets, "Sets must not be null");
        Assert.noNullElements(sets, "Sets must not contain null elements");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(destKey);
            commandArgs.add(sets.length);
            for (byte[] set : sets) {
                commandArgs.add(set);
            }
            
            Object result = connection.execute("ZINTERSTORE", commandArgs.toArray());
            return ((Number) ValkeyGlideConverters.defaultFromGlideResult(result)).longValue();
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long zInterStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets) {
        Assert.notNull(destKey, "Destination key must not be null");
        Assert.notNull(aggregate, "Aggregate must not be null");
        Assert.notNull(weights, "Weights must not be null");
        Assert.notNull(sets, "Sets must not be null");
        Assert.noNullElements(sets, "Sets must not contain null elements");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(destKey);
            commandArgs.add(sets.length);
            for (byte[] set : sets) {
                commandArgs.add(set);
            }
            
            // Add WEIGHTS
            commandArgs.add("WEIGHTS");
            for (double weight : weights.toArray()) {
                commandArgs.add(weight);
            }
            
            // Add AGGREGATE
            commandArgs.add("AGGREGATE");
            commandArgs.add(aggregate.name());
            
            Object result = connection.execute("ZINTERSTORE", commandArgs.toArray());
            return ((Number) ValkeyGlideConverters.defaultFromGlideResult(result)).longValue();
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<byte[]> zUnion(byte[]... sets) {
        Assert.notNull(sets, "Sets must not be null");
        Assert.noNullElements(sets, "Sets must not contain null elements");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(sets.length);
            for (byte[] set : sets) {
                commandArgs.add(set);
            }
            
            Object result = connection.execute("ZUNION", commandArgs.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted == null) {
                return new LinkedHashSet<>();
            }
            
            List<Object> list = convertToList(converted);
            Set<byte[]> resultSet = new LinkedHashSet<>(list.size());
            for (Object item : list) {
                resultSet.add((byte[]) ValkeyGlideConverters.defaultFromGlideResult(item));
            }
            return resultSet;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<Tuple> zUnionWithScores(byte[]... sets) {
        Assert.notNull(sets, "Sets must not be null");
        Assert.noNullElements(sets, "Sets must not contain null elements");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(sets.length);
            for (byte[] set : sets) {
                commandArgs.add(set);
            }
            commandArgs.add("WITHSCORES");
            
            Object result = connection.execute("ZUNION", commandArgs.toArray());
            return convertToTupleSet(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<Tuple> zUnionWithScores(Aggregate aggregate, Weights weights, byte[]... sets) {
        Assert.notNull(aggregate, "Aggregate must not be null");
        Assert.notNull(weights, "Weights must not be null");
        Assert.notNull(sets, "Sets must not be null");
        Assert.noNullElements(sets, "Sets must not contain null elements");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(sets.length);
            for (byte[] set : sets) {
                commandArgs.add(set);
            }
            
            // Add WEIGHTS
            commandArgs.add("WEIGHTS");
            for (double weight : weights.toArray()) {
                commandArgs.add(weight);
            }
            
            // Add AGGREGATE
            commandArgs.add("AGGREGATE");
            commandArgs.add(aggregate.name());
            
            commandArgs.add("WITHSCORES");
            
            Object result = connection.execute("ZUNION", commandArgs.toArray());
            return convertToTupleSet(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long zUnionStore(byte[] destKey, byte[]... sets) {
        Assert.notNull(destKey, "Destination key must not be null");
        Assert.notNull(sets, "Sets must not be null");
        Assert.noNullElements(sets, "Sets must not contain null elements");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(destKey);
            commandArgs.add(sets.length);
            for (byte[] set : sets) {
                commandArgs.add(set);
            }
            
            Object result = connection.execute("ZUNIONSTORE", commandArgs.toArray());
            return ((Number) ValkeyGlideConverters.defaultFromGlideResult(result)).longValue();
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long zUnionStore(byte[] destKey, Aggregate aggregate, Weights weights, byte[]... sets) {
        Assert.notNull(destKey, "Destination key must not be null");
        Assert.notNull(aggregate, "Aggregate must not be null");
        Assert.notNull(weights, "Weights must not be null");
        Assert.notNull(sets, "Sets must not be null");
        Assert.noNullElements(sets, "Sets must not contain null elements");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(destKey);
            commandArgs.add(sets.length);
            for (byte[] set : sets) {
                commandArgs.add(set);
            }
            
            // Add WEIGHTS
            commandArgs.add("WEIGHTS");
            for (double weight : weights.toArray()) {
                commandArgs.add(weight);
            }
            
            // Add AGGREGATE
            commandArgs.add("AGGREGATE");
            commandArgs.add(aggregate.name());
            
            Object result = connection.execute("ZUNIONSTORE", commandArgs.toArray());
            return ((Number) ValkeyGlideConverters.defaultFromGlideResult(result)).longValue();
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<byte[]> zRangeByLex(byte[] key, org.springframework.data.domain.Range<byte[]> range,
            org.springframework.data.redis.connection.Limit limit) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(range, "Range must not be null");
        Assert.notNull(limit, "Limit must not be null");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(key);
            commandArgs.add(formatLexRangeBound(range.getLowerBound(), true));
            commandArgs.add(formatLexRangeBound(range.getUpperBound(), false));
            
            if (limit.isLimited()) {
                commandArgs.add("LIMIT");
                commandArgs.add(limit.getOffset());
                commandArgs.add(limit.getCount());
            }
            
            Object result = connection.execute("ZRANGEBYLEX", commandArgs.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted == null) {
                return new LinkedHashSet<>();
            }
            
            List<Object> list = convertToList(converted);
            Set<byte[]> resultSet = new LinkedHashSet<>(list.size());
            for (Object item : list) {
                resultSet.add((byte[]) ValkeyGlideConverters.defaultFromGlideResult(item));
            }
            return resultSet;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<byte[]> zRevRangeByLex(byte[] key, org.springframework.data.domain.Range<byte[]> range,
            org.springframework.data.redis.connection.Limit limit) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(range, "Range must not be null");
        Assert.notNull(limit, "Limit must not be null");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(key);
            commandArgs.add(formatLexRangeBound(range.getUpperBound(), false));
            commandArgs.add(formatLexRangeBound(range.getLowerBound(), true));
            
            if (limit.isLimited()) {
                commandArgs.add("LIMIT");
                commandArgs.add(limit.getOffset());
                commandArgs.add(limit.getCount());
            }
            
            Object result = connection.execute("ZREVRANGEBYLEX", commandArgs.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
            if (converted == null) {
                return new LinkedHashSet<>();
            }
            
            List<Object> list = convertToList(converted);
            Set<byte[]> resultSet = new LinkedHashSet<>(list.size());
            for (Object item : list) {
                resultSet.add((byte[]) ValkeyGlideConverters.defaultFromGlideResult(item));
            }
            return resultSet;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long zRangeStoreByLex(byte[] dstKey, byte[] srcKey, org.springframework.data.domain.Range<byte[]> range,
            org.springframework.data.redis.connection.Limit limit) {
        Assert.notNull(dstKey, "Destination key must not be null");
        Assert.notNull(srcKey, "Source key must not be null");
        Assert.notNull(range, "Range must not be null");
        Assert.notNull(limit, "Limit must not be null");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(dstKey);
            commandArgs.add(srcKey);
            commandArgs.add(formatLexRangeBound(range.getLowerBound(), true));
            commandArgs.add(formatLexRangeBound(range.getUpperBound(), false));
            commandArgs.add("BYLEX");
            
            if (limit.isLimited()) {
                commandArgs.add("LIMIT");
                commandArgs.add(limit.getOffset());
                commandArgs.add(limit.getCount());
            }
            
            Object result = connection.execute("ZRANGESTORE", commandArgs.toArray());
            return ((Number) ValkeyGlideConverters.defaultFromGlideResult(result)).longValue();
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long zRangeStoreRevByLex(byte[] dstKey, byte[] srcKey, org.springframework.data.domain.Range<byte[]> range,
            org.springframework.data.redis.connection.Limit limit) {
        Assert.notNull(dstKey, "Destination key must not be null");
        Assert.notNull(srcKey, "Source key must not be null");
        Assert.notNull(range, "Range must not be null");
        Assert.notNull(limit, "Limit must not be null");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(dstKey);
            commandArgs.add(srcKey);
            commandArgs.add(formatLexRangeBound(range.getUpperBound(), false));
            commandArgs.add(formatLexRangeBound(range.getLowerBound(), true));
            commandArgs.add("BYLEX");
            commandArgs.add("REV");
            
            if (limit.isLimited()) {
                commandArgs.add("LIMIT");
                commandArgs.add(limit.getOffset());
                commandArgs.add(limit.getCount());
            }
            
            Object result = connection.execute("ZRANGESTORE", commandArgs.toArray());
            return ((Number) ValkeyGlideConverters.defaultFromGlideResult(result)).longValue();
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long zRangeStoreByScore(byte[] dstKey, byte[] srcKey, 
            org.springframework.data.domain.Range<? extends Number> range,
            org.springframework.data.redis.connection.Limit limit) {
        Assert.notNull(dstKey, "Destination key must not be null");
        Assert.notNull(srcKey, "Source key must not be null");
        Assert.notNull(range, "Range must not be null");
        Assert.notNull(limit, "Limit must not be null");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(dstKey);
            commandArgs.add(srcKey);
            commandArgs.add(formatRangeBound(range.getLowerBound(), true));
            commandArgs.add(formatRangeBound(range.getUpperBound(), false));
            commandArgs.add("BYSCORE");
            
            if (limit.isLimited()) {
                commandArgs.add("LIMIT");
                commandArgs.add(limit.getOffset());
                commandArgs.add(limit.getCount());
            }
            
            Object result = connection.execute("ZRANGESTORE", commandArgs.toArray());
            return ((Number) ValkeyGlideConverters.defaultFromGlideResult(result)).longValue();
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long zRangeStoreRevByScore(byte[] dstKey, byte[] srcKey, 
            org.springframework.data.domain.Range<? extends Number> range,
            org.springframework.data.redis.connection.Limit limit) {
        Assert.notNull(dstKey, "Destination key must not be null");
        Assert.notNull(srcKey, "Source key must not be null");
        Assert.notNull(range, "Range must not be null");
        Assert.notNull(limit, "Limit must not be null");
        
        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(dstKey);
            commandArgs.add(srcKey);
            commandArgs.add(formatRangeBound(range.getUpperBound(), true));
            commandArgs.add(formatRangeBound(range.getLowerBound(), false));
            commandArgs.add("BYSCORE");
            commandArgs.add("REV");
            
            if (limit.isLimited()) {
                commandArgs.add("LIMIT");
                commandArgs.add(limit.getOffset());
                commandArgs.add(limit.getCount());
            }
            
            Object result = connection.execute("ZRANGESTORE", commandArgs.toArray());
            return ((Number) ValkeyGlideConverters.defaultFromGlideResult(result)).longValue();
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public Cursor<Tuple> zScan(byte[] key, ScanOptions options) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(options, "ScanOptions must not be null");
        
        return new ValkeyGlideZSetScanCursor(key, options, connection);
    }

    // ==================== Helper Methods ====================

    private List<Object> convertToList(Object obj) {
        if (obj instanceof List) {
            return (List<Object>) obj;
        } else if (obj instanceof Object[]) {
            Object[] array = (Object[]) obj;
            List<Object> list = new ArrayList<>(array.length);
            for (Object item : array) {
                list.add(item);
            }
            return list;
        } else if (obj instanceof Map) {
            // Handle Map objects from valkey-glide (e.g., ZPOPMIN, ZRANGEWITHSCORES)
            Map<?, ?> map = (Map<?, ?>) obj;
            List<Object> list = new ArrayList<>(map.size() * 2);
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                list.add(entry.getKey());
                list.add(entry.getValue());
            }
            return list;
        } else {
            throw new IllegalArgumentException("Cannot convert " + obj.getClass() + " to List");
        }
    }

    private Double parseScore(Object obj) {
        if (obj instanceof Number) {
            return ((Number) obj).doubleValue();
        } else if (obj instanceof String) {
            return Double.parseDouble((String) obj);
        } else if (obj instanceof byte[]) {
            return Double.parseDouble(new String((byte[]) obj));
        } else if (obj instanceof List) {
            // Handle ArrayList scores by taking the first element
            List<?> list = (List<?>) obj;
            if (!list.isEmpty()) {
                return parseScore(list.get(0));
            }
            return 0.0;
        } else {
            throw new IllegalArgumentException("Cannot parse score from " + obj.getClass());
        }
    }

    private Set<Tuple> convertToTupleSet(Object result) {
        Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
        
        if (converted == null) {
            return new LinkedHashSet<>();
        }
        
        List<Object> list = convertToList(converted);
        Set<Tuple> resultSet = new LinkedHashSet<>();
        
        // Check if we have nested arrays/lists (valkey-glide format)
        if (!list.isEmpty() && list.get(0) instanceof List) {
            // Format: [[member1, score1], [member2, score2], ...]
            for (Object item : list) {
                if (item instanceof List) {
                    List<?> tupleList = (List<?>) item;
                    if (tupleList.size() >= 2) {
                        Object valueObj = ValkeyGlideConverters.defaultFromGlideResult(tupleList.get(0));
                        Object scoreObj = ValkeyGlideConverters.defaultFromGlideResult(tupleList.get(1));
                        
                        byte[] value = convertToByteArray(valueObj);
                        Double score = parseScore(scoreObj);
                        resultSet.add(new DefaultTuple(value, score));
                    }
                }
            }
        } else {
            // Standard flat format: [member, score, member, score, ...]
            // Process in pairs, maintaining the order returned by Redis
            for (int i = 0; i < list.size(); i += 2) {
                if (i + 1 < list.size()) {
                    Object valueObj = ValkeyGlideConverters.defaultFromGlideResult(list.get(i));
                    Object scoreObj = ValkeyGlideConverters.defaultFromGlideResult(list.get(i + 1));
                    
                    byte[] value = convertToByteArray(valueObj);
                    Double score = parseScore(scoreObj);
                    resultSet.add(new DefaultTuple(value, score));
                }
            }
        }
        
        return resultSet;
    }

    /**
     * Unified method to convert pop operation results to tuple sets with configurable sort order.
     * 
     * @param result the result from ZPOPMIN/ZPOPMAX operation
     * @param ascending true for ascending order (lowest first), false for descending order (highest first)
     * @return set of tuples in the specified order
     */
    private Set<Tuple> convertToTupleSetForPop(Object result, boolean ascending) {
        Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
        
        if (converted == null) {
            return new LinkedHashSet<>();
        }
        
        List<Tuple> tuples = new ArrayList<>();
        
        // Handle HashMap format returned by Valkey-Glide for pop operations
        Map<?, ?> map = (Map<?, ?>) converted;
        
        // Convert map entries to tuples
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            byte[] value = convertToByteArray(entry.getKey());
            Double score = parseScore(entry.getValue());
            tuples.add(new DefaultTuple(value, score));
        }
        
        // Sort tuples based on the specified order
        tuples.sort(ascending ? 
            (t1, t2) -> Double.compare(t1.getScore(), t2.getScore()) :  // ascending: lowest first
            (t1, t2) -> Double.compare(t2.getScore(), t1.getScore()));   // descending: highest first
        
        // Convert to LinkedHashSet to maintain order
        Set<Tuple> resultSet = new LinkedHashSet<>();
        resultSet.addAll(tuples);
        
        return resultSet;
    }

    private String formatRangeBound(org.springframework.data.domain.Range.Bound<? extends Number> bound, boolean isLowerBound) {
        if (bound == null || !bound.getValue().isPresent()) {
            return isLowerBound ? "-inf" : "+inf";
        }
        
        String value = bound.getValue().get().toString();
        return bound.isInclusive() ? value : "(" + value;
    }

    private String formatLexRangeBound(org.springframework.data.domain.Range.Bound<byte[]> bound, boolean isLowerBound) {
        if (bound == null || !bound.getValue().isPresent()) {
            return isLowerBound ? "-" : "+";
        }
        
        String value = new String(bound.getValue().get());
        return bound.isInclusive() ? "[" + value : "(" + value;
    }

    private byte[] convertToByteArray(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof byte[]) {
            return (byte[]) obj;
        }
        if (obj instanceof String) {
            return ((String) obj).getBytes();
        }
        if (obj instanceof List) {
            // Convert List to byte[] if needed
            List<?> list = (List<?>) obj;
            if (!list.isEmpty()) {
                Object firstElement = list.get(0);
                if (firstElement instanceof Number) {
                    // List of numbers - convert to byte array
                    byte[] result = new byte[list.size()];
                    for (int i = 0; i < list.size(); i++) {
                        result[i] = ((Number) list.get(i)).byteValue();
                    }
                    return result;
                } else if (firstElement instanceof Byte) {
                    // List of bytes - convert to byte array
                    byte[] result = new byte[list.size()];
                    for (int i = 0; i < list.size(); i++) {
                        result[i] = (Byte) list.get(i);
                    }
                    return result;
                } else {
                    // List of other objects - convert to string representation
                    StringBuilder sb = new StringBuilder();
                    for (Object item : list) {
                        if (item instanceof Byte) {
                            sb.append((char) ((Byte) item).byteValue());
                        } else {
                            sb.append(item.toString());
                        }
                    }
                    return sb.toString().getBytes();
                }
            } else {
                // Empty list - return empty byte array
                return new byte[0];
            }
        }
        return obj.toString().getBytes();
    }

    /**
     * Simple implementation of Cursor for ZSCAN operation.
     */
    private static class ValkeyGlideZSetScanCursor implements Cursor<Tuple> {
        
        private final byte[] key;
        private final ScanOptions options;
        private final ValkeyGlideConnection connection;
        private long cursor = 0;
        private List<Tuple> tuples = new ArrayList<>();
        private int currentIndex = 0;
        private boolean finished = false;
        
        public ValkeyGlideZSetScanCursor(byte[] key, ScanOptions options, ValkeyGlideConnection connection) {
            this.key = key;
            this.options = options;
            this.connection = connection;
            scanNext();
        }
        
        @Override
        public boolean hasNext() {
            if (currentIndex < tuples.size()) {
                return true;
            }
            if (finished) {
                return false;
            }
            scanNext();
            return currentIndex < tuples.size();
        }
        
        @Override
        public Tuple next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            return tuples.get(currentIndex++);
        }
        
        private void scanNext() {
            try {
                List<Object> args = new ArrayList<>();
                args.add(key);
                args.add(String.valueOf(cursor));
                
                if (options.getPattern() != null) {
                    args.add("MATCH");
                    args.add(options.getPattern());
                }
                
                if (options.getCount() != null) {
                    args.add("COUNT");
                    args.add(options.getCount());
                }
                
                Object result = connection.execute("ZSCAN", args.toArray());
                Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
                
                if (converted == null) {
                    finished = true;
                    return;
                }
                
                List<Object> scanResult = convertToList(converted);
                if (scanResult.size() >= 2) {
                    // First element is the cursor, second is the result array
                    Object cursorObj = ValkeyGlideConverters.defaultFromGlideResult(scanResult.get(0));
                    if (cursorObj instanceof String) {
                        cursor = Long.parseLong((String) cursorObj);
                    } else if (cursorObj instanceof Number) {
                        cursor = ((Number) cursorObj).longValue();
                    } else if (cursorObj instanceof byte[]) {
                        cursor = Long.parseLong(new String((byte[]) cursorObj));
                    } else {
                        cursor = Long.parseLong(cursorObj.toString());
                    }
                    
                    if (cursor == 0) {
                        finished = true;
                    }
                    
                    Object resultArray = scanResult.get(1);
                    if (resultArray != null) {
                        List<Object> tupleList = convertToList(resultArray);
                        tuples.clear();
                        currentIndex = 0;
                        
                        for (int i = 0; i < tupleList.size(); i += 2) {
                            if (i + 1 < tupleList.size()) {
                                byte[] value = (byte[]) ValkeyGlideConverters.defaultFromGlideResult(tupleList.get(i));
                                Double score = parseScore(ValkeyGlideConverters.defaultFromGlideResult(tupleList.get(i + 1)));
                                tuples.add(new DefaultTuple(value, score));
                            }
                        }
                    }
                }
            } catch (Exception ex) {
                finished = true;
                throw new ValkeyGlideExceptionConverter().convert(ex);
            }
        }
        
        @Override
        public void close() {
            finished = true;
        }
        
        @Override
        public boolean isClosed() {
            return finished;
        }
        
        @Override
        public long getCursorId() {
            return cursor;
        }
        
        @Override
        public long getPosition() {
            return currentIndex;
        }
        
        @Override
        public Cursor.CursorId getId() {
            return Cursor.CursorId.of(cursor);
        }
        
        // Helper methods for convertToList and parseScore
        private List<Object> convertToList(Object obj) {
            if (obj instanceof List) {
                return (List<Object>) obj;
            } else if (obj instanceof Object[]) {
                Object[] array = (Object[]) obj;
                List<Object> list = new ArrayList<>(array.length);
                for (Object item : array) {
                    list.add(item);
                }
                return list;
            } else {
                throw new IllegalArgumentException("Cannot convert " + obj.getClass() + " to List");
            }
        }
        
        private Double parseScore(Object obj) {
            if (obj instanceof Number) {
                return ((Number) obj).doubleValue();
            } else if (obj instanceof String) {
                return Double.parseDouble((String) obj);
            } else if (obj instanceof byte[]) {
                return Double.parseDouble(new String((byte[]) obj));
            } else {
                throw new IllegalArgumentException("Cannot parse score from " + obj.getClass());
            }
        }
    }
}

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
import java.util.List;

import org.springframework.data.redis.connection.RedisListCommands;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Implementation of {@link RedisListCommands} for Valkey-Glide.
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

    /**
     * Helper method to convert various object types to byte arrays.
     */
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
        if (obj instanceof Object[]) {
            Object[] array = (Object[]) obj;
            if (array.length > 0 && array[0] instanceof Byte) {
                // Convert Byte[] to byte[]
                byte[] result = new byte[array.length];
                for (int i = 0; i < array.length; i++) {
                    result[i] = (Byte) array[i];
                }
                return result;
            }
        }
        
        // Try to convert using standard conversion first
        try {
            Object converted = ValkeyGlideConverters.fromGlideResult(obj);
            if (converted instanceof byte[]) {
                return (byte[]) converted;
            }
            if (converted instanceof String) {
                return ((String) converted).getBytes();
            }
            // Handle nested arrays after conversion
            if (converted instanceof Object[]) {
                Object[] convertedArray = (Object[]) converted;
                if (convertedArray.length > 0 && convertedArray[0] instanceof Byte) {
                    byte[] result = new byte[convertedArray.length];
                    for (int i = 0; i < convertedArray.length; i++) {
                        result[i] = (Byte) convertedArray[i];
                    }
                    return result;
                }
            }
        } catch (Exception e) {
            // If conversion fails, try direct cast as fallback
        }
        
        // As a last resort, try to handle as raw bytes
        if (obj instanceof Object[]) {
            Object[] array = (Object[]) obj;
            try {
                byte[] result = new byte[array.length];
                for (int i = 0; i < array.length; i++) {
                    if (array[i] instanceof Number) {
                        result[i] = ((Number) array[i]).byteValue();
                    } else {
                        return null; // Cannot convert this type
                    }
                }
                return result;
            } catch (Exception e) {
                return null;
            }
        }
        
        return null;
    }

    @Override
    @Nullable
    public Long rPush(byte[] key, byte[]... values) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(values, "Values must not be null");
        Assert.noNullElements(values, "Values must not contain null elements");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            for (byte[] value : values) {
                args.add(value);
            }
            
            Object result = connection.execute("RPUSH", args.toArray());
            if (result instanceof Number) {
                return ((Number) result).longValue();
            }
            return null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public List<Long> lPos(byte[] key, byte[] element, @Nullable Integer rank, @Nullable Integer count) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(element, "Element must not be null");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            args.add(element);
            
            if (rank != null) {
                args.add("RANK");
                args.add(rank);
            }
            
            if (count != null) {
                args.add("COUNT");
                args.add(count);
            }
            
            Object result = connection.execute("LPOS", args.toArray());
            Object converted = ValkeyGlideConverters.fromGlideResult(result);
            if (converted == null) {
                return null;
            }
            
            if (converted instanceof Number) {
                // Single result case
                List<Long> resultList = new ArrayList<>();
                resultList.add(((Number) converted).longValue());
                return resultList;
            }
            
            List<Object> list;
            if (converted instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> castList = (List<Object>) converted;
                list = castList;
            } else if (converted instanceof Object[]) {
                Object[] array = (Object[]) converted;
                list = new ArrayList<>(array.length);
                for (Object item : array) {
                    list.add(item);
                }
            } else {
                throw new IllegalStateException("Unexpected result type from LPOS: " + converted.getClass());
            }
            
            List<Long> resultList = new ArrayList<>(list.size());
            for (Object item : list) {
                Object convertedItem = ValkeyGlideConverters.fromGlideResult(item);
                if (convertedItem instanceof Number) {
                    resultList.add(((Number) convertedItem).longValue());
                } else {
                    resultList.add(null);
                }
            }
            return resultList;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long lPush(byte[] key, byte[]... values) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(values, "Values must not be null");
        Assert.noNullElements(values, "Values must not contain null elements");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            for (byte[] value : values) {
                args.add(value);
            }
            
            Object result = connection.execute("LPUSH", args.toArray());
            if (result instanceof Number) {
                return ((Number) result).longValue();
            }
            return null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long rPushX(byte[] key, byte[] value) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            Object result = connection.execute("RPUSHX", key, value);
            if (result instanceof Number) {
                return ((Number) result).longValue();
            }
            return null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long lPushX(byte[] key, byte[] value) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            Object result = connection.execute("LPUSHX", key, value);
            if (result instanceof Number) {
                return ((Number) result).longValue();
            }
            return null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long lLen(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("LLEN", key);
            if (result instanceof Number) {
                return ((Number) result).longValue();
            }
            return null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public List<byte[]> lRange(byte[] key, long start, long end) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("LRANGE", key, start, end);
            Object converted = ValkeyGlideConverters.fromGlideResult(result);
            if (converted == null) {
                return new ArrayList<>();
            }
            
            List<Object> list;
            if (converted instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> castList = (List<Object>) converted;
                list = castList;
            } else if (converted instanceof Object[]) {
                Object[] array = (Object[]) converted;
                list = new ArrayList<>(array.length);
                for (Object item : array) {
                    list.add(item);
                }
            } else {
                throw new IllegalStateException("Unexpected result type from LRANGE: " + converted.getClass());
            }
            
            List<byte[]> resultList = new ArrayList<>(list.size());
            for (Object item : list) {
                Object convertedItem = ValkeyGlideConverters.fromGlideResult(item);
                resultList.add((byte[]) convertedItem);
            }
            return resultList;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public void lTrim(byte[] key, long start, long end) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            connection.execute("LTRIM", key, start, end);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public byte[] lIndex(byte[] key, long index) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("LINDEX", key, index);
            return convertToByteArray(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long lInsert(byte[] key, Position where, byte[] pivot, byte[] value) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(where, "Position must not be null");
        Assert.notNull(pivot, "Pivot must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            String position = (where == Position.BEFORE) ? "BEFORE" : "AFTER";
            Object result = connection.execute("LINSERT", key, position, pivot, value);
            if (result instanceof Number) {
                return ((Number) result).longValue();
            }
            return null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public byte[] lMove(byte[] sourceKey, byte[] destinationKey, Direction from, Direction to) {
        Assert.notNull(sourceKey, "Source key must not be null");
        Assert.notNull(destinationKey, "Destination key must not be null");
        Assert.notNull(from, "From direction must not be null");
        Assert.notNull(to, "To direction must not be null");
        
        try {
            String fromStr = (from == Direction.LEFT) ? "LEFT" : "RIGHT";
            String toStr = (to == Direction.LEFT) ? "LEFT" : "RIGHT";
            Object result = connection.execute("LMOVE", sourceKey, destinationKey, fromStr, toStr);
            return convertToByteArray(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public byte[] bLMove(byte[] sourceKey, byte[] destinationKey, Direction from, Direction to, double timeout) {
        Assert.notNull(sourceKey, "Source key must not be null");
        Assert.notNull(destinationKey, "Destination key must not be null");
        Assert.notNull(from, "From direction must not be null");
        Assert.notNull(to, "To direction must not be null");
        
        try {
            String fromStr = (from == Direction.LEFT) ? "LEFT" : "RIGHT";
            String toStr = (to == Direction.LEFT) ? "LEFT" : "RIGHT";
            Object result = connection.execute("BLMOVE", sourceKey, destinationKey, fromStr, toStr, timeout);
            return convertToByteArray(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public void lSet(byte[] key, long index, byte[] value) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            connection.execute("LSET", key, index, value);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long lRem(byte[] key, long count, byte[] value) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            Object result = connection.execute("LREM", key, count, value);
            if (result instanceof Number) {
                return ((Number) result).longValue();
            }
            return null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public byte[] lPop(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("LPOP", key);
            return convertToByteArray(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public List<byte[]> lPop(byte[] key, long count) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("LPOP", key, count);
            Object converted = ValkeyGlideConverters.fromGlideResult(result);
            if (converted == null) {
                return null;
            }
            
            List<Object> list;
            if (converted instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> castList = (List<Object>) converted;
                list = castList;
            } else if (converted instanceof Object[]) {
                Object[] array = (Object[]) converted;
                list = new ArrayList<>(array.length);
                for (Object item : array) {
                    list.add(item);
                }
            } else {
                throw new IllegalStateException("Unexpected result type from LPOP: " + converted.getClass());
            }
            
            List<byte[]> resultList = new ArrayList<>(list.size());
            for (Object item : list) {
                Object convertedItem = ValkeyGlideConverters.fromGlideResult(item);
                resultList.add((byte[]) convertedItem);
            }
            return resultList;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public byte[] rPop(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("RPOP", key);
            return convertToByteArray(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public List<byte[]> rPop(byte[] key, long count) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("RPOP", key, count);
            Object converted = ValkeyGlideConverters.fromGlideResult(result);
            if (converted == null) {
                return null;
            }
            
            List<Object> list;
            if (converted instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> castList = (List<Object>) converted;
                list = castList;
            } else if (converted instanceof Object[]) {
                Object[] array = (Object[]) converted;
                list = new ArrayList<>(array.length);
                for (Object item : array) {
                    list.add(item);
                }
            } else {
                throw new IllegalStateException("Unexpected result type from RPOP: " + converted.getClass());
            }
            
            List<byte[]> resultList = new ArrayList<>(list.size());
            for (Object item : list) {
                Object convertedItem = ValkeyGlideConverters.fromGlideResult(item);
                resultList.add((byte[]) convertedItem);
            }
            return resultList;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public List<byte[]> bLPop(int timeout, byte[]... keys) {
        Assert.notNull(keys, "Keys must not be null");
        Assert.noNullElements(keys, "Keys must not contain null elements");
        
        try {
            List<Object> args = new ArrayList<>();
            for (byte[] key : keys) {
                args.add(key);
            }
            args.add(timeout);
            
            Object result = connection.execute("BLPOP", args.toArray());
            Object converted = ValkeyGlideConverters.fromGlideResult(result);
            if (converted == null) {
                return new ArrayList<>();
            }
            
            List<Object> list;
            if (converted instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> castList = (List<Object>) converted;
                list = castList;
            } else if (converted instanceof Object[]) {
                Object[] array = (Object[]) converted;
                list = new ArrayList<>(array.length);
                for (Object item : array) {
                    list.add(item);
                }
            } else {
                throw new IllegalStateException("Unexpected result type from BLPOP: " + converted.getClass());
            }
            
            List<byte[]> resultList = new ArrayList<>(list.size());
            for (Object item : list) {
                Object convertedItem = ValkeyGlideConverters.fromGlideResult(item);
                resultList.add((byte[]) convertedItem);
            }
            return resultList;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public List<byte[]> bRPop(int timeout, byte[]... keys) {
        Assert.notNull(keys, "Keys must not be null");
        Assert.noNullElements(keys, "Keys must not contain null elements");
        
        try {
            List<Object> args = new ArrayList<>();
            for (byte[] key : keys) {
                args.add(key);
            }
            args.add(timeout);
            
            Object result = connection.execute("BRPOP", args.toArray());
            Object converted = ValkeyGlideConverters.fromGlideResult(result);
            if (converted == null) {
                return new ArrayList<>();
            }
            
            List<Object> list;
            if (converted instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> castList = (List<Object>) converted;
                list = castList;
            } else if (converted instanceof Object[]) {
                Object[] array = (Object[]) converted;
                list = new ArrayList<>(array.length);
                for (Object item : array) {
                    list.add(item);
                }
            } else {
                throw new IllegalStateException("Unexpected result type from BRPOP: " + converted.getClass());
            }
            
            List<byte[]> resultList = new ArrayList<>(list.size());
            for (Object item : list) {
                Object convertedItem = ValkeyGlideConverters.fromGlideResult(item);
                resultList.add((byte[]) convertedItem);
            }
            return resultList;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public byte[] rPopLPush(byte[] srcKey, byte[] dstKey) {
        Assert.notNull(srcKey, "Source key must not be null");
        Assert.notNull(dstKey, "Destination key must not be null");
        
        try {
            Object result = connection.execute("RPOPLPUSH", srcKey, dstKey);
            return convertToByteArray(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public byte[] bRPopLPush(int timeout, byte[] srcKey, byte[] dstKey) {
        Assert.notNull(srcKey, "Source key must not be null");
        Assert.notNull(dstKey, "Destination key must not be null");
        
        try {
            Object result = connection.execute("BRPOPLPUSH", srcKey, dstKey, timeout);
            return convertToByteArray(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }
}

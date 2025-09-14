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
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Stub implementation of {@link RedisStringCommands} for Valkey-Glide.
 * All methods throw UnsupportedOperationException as placeholder.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideStringCommands implements RedisStringCommands {

    private final ValkeyGlideConnection connection;

    /**
     * Creates a new {@link ValkeyGlideStringCommands}.
     *
     * @param connection must not be {@literal null}.
     */
    public ValkeyGlideStringCommands(ValkeyGlideConnection connection) {
        Assert.notNull(connection, "Connection must not be null!");
        this.connection = connection;
    }

    @Override
    @Nullable
    public byte[] get(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            // delegate to connection.execute which handles pipelining/transactions
            Object result = connection.execute("GET", key);
            return result != null ? (byte[]) result : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Boolean setNX(byte[] key, byte[] value) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            Object result = connection.execute("SETNX", key, value);
            return result != null ? ((Number) result).longValue() != 0 : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public byte[] getDel(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("GETDEL", key);
            return result != null ? (byte[]) result : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public byte[] getEx(byte[] key, Expiration expiration) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(expiration, "Expiration must not be null");
        
        try {
            Object result;
            if (expiration.isKeepTtl()) {
                result = connection.execute("GETEX", key, "PERSIST");
            } else if (expiration.isUnixTimestamp()) {
                if (expiration.getTimeUnit() == TimeUnit.SECONDS) {
                    result = connection.execute("GETEX", key, "EXAT", expiration.getExpirationTime());
                } else {
                    result = connection.execute("GETEX", key, "PXAT", expiration.getExpirationTime());
                }
            } else {
                if (expiration.getTimeUnit() == TimeUnit.SECONDS) {
                    result = connection.execute("GETEX", key, "EX", expiration.getExpirationTime());
                } else {
                    result = connection.execute("GETEX", key, "PX", expiration.getExpirationTime());
                }
            }
            return result != null ? (byte[]) result : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public byte[] getSet(byte[] key, byte[] value) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            Object result = connection.execute("GETSET", key, value);
            return result != null ? (byte[]) result : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public List<byte[]> mGet(byte[]... keys) {
        Assert.notNull(keys, "Keys must not be null");
        Assert.noNullElements(keys, "Keys must not contain null elements");
        
        try {
            Object result = connection.execute("MGET", (Object[]) keys);
            if (result == null) {
                return null;
            }
            
            List<Object> list;
            if (result instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> castList = (List<Object>) result;
                list = castList;
            } else if (result instanceof Object[]) {
                // Handle case where Glide returns Object[] instead of List
                Object[] array = (Object[]) result;
                list = new ArrayList<>(array.length);
                for (Object item : array) {
                    list.add(item);
                }
            } else {
                throw new IllegalStateException("Unexpected result type from MGET: " + result.getClass());
            }
            
            List<byte[]> resultList = new ArrayList<>(list.size());
            for (Object item : list) {
                resultList.add((byte[]) item);
            }
            return resultList;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Boolean set(byte[] key, byte[] value) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            Object result = connection.execute("SET", key, value);
            return result != null ? (Boolean) result : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Boolean set(byte[] key, byte[] value, Expiration expiration, SetOption option) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            args.add(value);
            
            if (expiration != null && !expiration.isPersistent()) {
                if (expiration.isUnixTimestamp()) {
                    if (expiration.getTimeUnit() == TimeUnit.SECONDS) {
                        args.add("EXAT");
                    } else {
                        args.add("PXAT");
                    }
                } else {
                    if (expiration.getTimeUnit() == TimeUnit.SECONDS) {
                        args.add("EX");
                    } else {
                        args.add("PX");
                    }
                }
                args.add(expiration.getExpirationTime());
            }
            
            boolean hasConditionalOption = false;
            if (option != null) {
                switch (option) {
                    case SET_IF_ABSENT:
                        args.add("NX");
                        hasConditionalOption = true;
                        break;
                    case SET_IF_PRESENT:
                        args.add("XX");
                        hasConditionalOption = true;
                        break;
                    case UPSERT:
                        // No additional argument needed
                        break;
                }
            }
            
            Object result = connection.execute("SET", args.toArray());
            
            // Handle null result based on connection state
            if (result == null) {
                // If in pipeline/transaction mode, null means command was deferred
                if (connection.isPipelined() || connection.isQueueing()) {
                    return null;
                }
                // If not in pipeline/transaction mode and has conditional option,
                // null means the condition failed (key exists for NX or doesn't exist for XX)
                if (hasConditionalOption) {
                    return Boolean.FALSE;
                }
                // For non-conditional SET, null result is unexpected - return null
                return null;
            }
            
            // Handle different result types from SET command
            if (result instanceof Boolean) {
                return (Boolean) result;
            }
            
            if (result instanceof String) {
                String strResult = (String) result;
                if ("OK".equals(strResult)) {
                    return Boolean.TRUE;
                }
                // Unexpected string result
                return null;
            }
            
            // Try to convert other types to Boolean
            return result != null ? (Boolean) result : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public byte[] setGet(byte[] key, byte[] value, Expiration expiration, SetOption option) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            args.add(value);
            args.add("GET");
            
            if (expiration != null && !expiration.isPersistent()) {
                if (expiration.isUnixTimestamp()) {
                    if (expiration.getTimeUnit() == TimeUnit.SECONDS) {
                        args.add("EXAT");
                    } else {
                        args.add("PXAT");
                    }
                } else {
                    if (expiration.getTimeUnit() == TimeUnit.SECONDS) {
                        args.add("EX");
                    } else {
                        args.add("PX");
                    }
                }
                args.add(expiration.getExpirationTime());
            }
            
            if (option != null) {
                switch (option) {
                    case SET_IF_ABSENT:
                        args.add("NX");
                        break;
                    case SET_IF_PRESENT:
                        args.add("XX");
                        break;
                    case UPSERT:
                        // No additional argument needed
                        break;
                }
            }
            
            Object result = connection.execute("SET", args.toArray());
            return result != null ? (byte[]) result : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Boolean setEx(byte[] key, long seconds, byte[] value) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            Object result = connection.execute("SETEX", key, seconds, value);
            return result != null ? (Boolean) result : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Boolean pSetEx(byte[] key, long milliseconds, byte[] value) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            Object result = connection.execute("PSETEX", key, milliseconds, value);
            return result != null ? (Boolean) result : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Boolean mSet(Map<byte[], byte[]> tuple) {
        Assert.notNull(tuple, "Tuple must not be null");
        
        try {
            List<Object> args = new ArrayList<>();
            for (Map.Entry<byte[], byte[]> entry : tuple.entrySet()) {
                args.add(entry.getKey());
                args.add(entry.getValue());
            }
            
            Object result = connection.execute("MSET", args.toArray());
            return result != null ? (Boolean) result : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Boolean mSetNX(Map<byte[], byte[]> tuple) {
        Assert.notNull(tuple, "Tuple must not be null");
        
        try {
            List<Object> args = new ArrayList<>();
            for (Map.Entry<byte[], byte[]> entry : tuple.entrySet()) {
                args.add(entry.getKey());
                args.add(entry.getValue());
            }
            
            Object result = connection.execute("MSETNX", args.toArray());
            return result != null ? (Boolean) result : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long incr(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("INCR", key);
            return result != null ? ((Number) result).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long incrBy(byte[] key, long value) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("INCRBY", key, value);
            return result != null ? ((Number) result).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Double incrBy(byte[] key, double value) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("INCRBYFLOAT", key, value);
            if (result == null) {
                return null;
            }
            if (result instanceof String) {
                return Double.parseDouble((String) result);
            } else {
                return ((Number) result).doubleValue();
            }
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long decr(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("DECR", key);
            return result != null ? ((Number) result).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long decrBy(byte[] key, long value) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("DECRBY", key, value);
            return result != null ? ((Number) result).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long append(byte[] key, byte[] value) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            Object result = connection.execute("APPEND", key, value);
            return result != null ? ((Number) result).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public byte[] getRange(byte[] key, long start, long end) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("GETRANGE", key, start, end);
            return result != null ? (byte[]) result : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public void setRange(byte[] key, byte[] value, long offset) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            connection.execute("SETRANGE", key, offset, value);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Boolean getBit(byte[] key, long offset) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("GETBIT", key, offset);
            return result != null ? ((Number) result).longValue() != 0 : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Boolean setBit(byte[] key, long offset, boolean value) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("SETBIT", key, offset, value ? 1 : 0);
            return result != null ? ((Number) result).longValue() != 0 : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long bitCount(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("BITCOUNT", key);
            return result != null ? ((Number) result).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long bitCount(byte[] key, long start, long end) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("BITCOUNT", key, start, end);
            return result != null ? ((Number) result).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public List<Long> bitField(byte[] key, BitFieldSubCommands subCommands) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(subCommands, "SubCommands must not be null");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            
            for (BitFieldSubCommands.BitFieldSubCommand subCommand : subCommands.getSubCommands()) {
                if (subCommand instanceof BitFieldSubCommands.BitFieldGet) {
                    BitFieldSubCommands.BitFieldGet get = (BitFieldSubCommands.BitFieldGet) subCommand;
                    args.add("GET");
                    args.add(get.getType().asString());
                    args.add(get.getOffset().getValue());
                } else if (subCommand instanceof BitFieldSubCommands.BitFieldSet) {
                    BitFieldSubCommands.BitFieldSet set = (BitFieldSubCommands.BitFieldSet) subCommand;
                    args.add("SET");
                    args.add(set.getType().asString());
                    args.add(set.getOffset().getValue());
                    args.add(set.getValue());
                } else if (subCommand instanceof BitFieldSubCommands.BitFieldIncrBy) {
                    BitFieldSubCommands.BitFieldIncrBy incrBy = (BitFieldSubCommands.BitFieldIncrBy) subCommand;
                    args.add("INCRBY");
                    args.add(incrBy.getType().asString());
                    args.add(incrBy.getOffset().getValue());
                    args.add(incrBy.getValue());
                }
            }
            
            Object result = connection.execute("BITFIELD", args.toArray());
            if (result == null) {
                return null;
            }
            
            List<Object> list;
            if (result instanceof List) {
                @SuppressWarnings("unchecked")
                List<Object> castList = (List<Object>) result;
                list = castList;
            } else if (result instanceof Object[]) {
                // Handle case where Glide returns Object[] instead of List
                Object[] array = (Object[]) result;
                list = new ArrayList<>(array.length);
                for (Object item : array) {
                    list.add(item);
                }
            } else {
                throw new IllegalStateException("Unexpected result type from BITFIELD: " + result.getClass());
            }
            
            List<Long> resultList = new ArrayList<>(list.size());
            for (Object item : list) {
                if (item instanceof Number) {
                    resultList.add(((Number) item).longValue());
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
    public Long bitOp(BitOperation op, byte[] destination, byte[]... keys) {
        Assert.notNull(op, "BitOperation must not be null");
        Assert.notNull(destination, "Destination key must not be null");
        Assert.notNull(keys, "Keys must not be null");
        Assert.noNullElements(keys, "Keys must not contain null elements");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(op.name());
            args.add(destination);
            for (byte[] key : keys) {
                args.add(key);
            }
            
            Object result = connection.execute("BITOP", args.toArray());
            return result != null ? ((Number) result).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long bitPos(byte[] key, boolean bit, Range<Long> range) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            args.add(bit ? 1 : 0);
            
            if (range != null) {
                if (range.getLowerBound().isBounded()) {
                    args.add(range.getLowerBound().getValue().get());
                }
                if (range.getUpperBound().isBounded()) {
                    args.add(range.getUpperBound().getValue().get());
                }
            }
            
            Object result = connection.execute("BITPOS", args.toArray());
            return result != null ? ((Number) result).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long strLen(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("STRLEN", key);
            return result != null ? ((Number) result).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }
}

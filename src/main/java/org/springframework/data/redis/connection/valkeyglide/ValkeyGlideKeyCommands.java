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

import java.nio.charset.StandardCharsets;
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

import glide.api.models.GlideString;

/**
 * Implementation of {@link RedisKeyCommands} for Valkey-Glide.
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
        Assert.notNull(sourceKey, "Source key must not be null");
        Assert.notNull(targetKey, "Target key must not be null");
        
        try {
            String[] args = replace 
                ? new String[]{"COPY", new String(sourceKey), new String(targetKey), "REPLACE"}
                : new String[]{"COPY", new String(sourceKey), new String(targetKey)};
            Object result = connection.execute(args[0], (Object[]) java.util.Arrays.copyOfRange(args, 1, args.length));
            return result instanceof Boolean ? (Boolean) result : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long exists(byte[]... keys) {
        Assert.notEmpty(keys, "Keys must not be empty");
        Assert.noNullElements(keys, "Keys must not contain null elements");
        for (byte[] key : keys) {
            if (key.length == 0) {
                throw new IllegalArgumentException("Keys must not contain empty elements");
            }
        }
        
        try {
            Object result = connection.execute("EXISTS", (Object[]) keys);
            return result instanceof Number ? ((Number) result).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long del(byte[]... keys) {
        Assert.notEmpty(keys, "Keys must not be empty");
        Assert.noNullElements(keys, "Keys must not contain null elements");
        for (byte[] key : keys) {
            if (key.length == 0) {
                throw new IllegalArgumentException("Keys must not contain empty elements");
            }
        }
        
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
        Assert.notEmpty(keys, "Keys must not be empty");
        Assert.noNullElements(keys, "Keys must not contain null elements");
        for (byte[] key : keys) {
            if (key.length == 0) {
                throw new IllegalArgumentException("Keys must not contain empty elements");
            }
        }
        
        try {
            Object result = connection.execute("UNLINK", (Object[]) keys);
            return result instanceof Number ? ((Number) result).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public DataType type(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("TYPE", key);
            return ValkeyGlideConverters.toDataType((byte[]) result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long touch(byte[]... keys) {
        Assert.notEmpty(keys, "Keys must not be empty");
        Assert.noNullElements(keys, "Keys must not contain null elements");
        for (byte[] key : keys) {
            if (key.length == 0) {
                throw new IllegalArgumentException("Keys must not contain empty elements");
            }
        }
        
        try {
            Object result = connection.execute("TOUCH", (Object[]) keys);
            return result instanceof Number ? ((Number) result).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<byte[]> keys(byte[] pattern) {
        Assert.notNull(pattern, "Pattern must not be null");
        
        try {
            Object result = connection.execute("KEYS", pattern);
            return ValkeyGlideConverters.toBytesSet(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public Cursor<byte[]> scan(ScanOptions options) {
        return new ValkeyGlideKeyScanCursor(connection, options != null ? options : ScanOptions.NONE);
    }

    /**
     * Simple implementation of a scan cursor for keys.
     */
    private static class ValkeyGlideKeyScanCursor implements Cursor<byte[]> {
        
        private final ValkeyGlideConnection connection;
        private final ScanOptions scanOptions;
        private String cursor = "0";
        private java.util.Iterator<byte[]> currentBatch;
        private boolean finished = false;

        public ValkeyGlideKeyScanCursor(ValkeyGlideConnection connection, ScanOptions scanOptions) {
            this.connection = connection;
            this.scanOptions = scanOptions;
        }

        @Override
        public long getCursorId() {
            try {
                return Long.parseLong(cursor);
            } catch (NumberFormatException e) {
                return 0;
            }
        }

        @Override
        public Cursor.CursorId getId() {
            return Cursor.CursorId.of(cursor);
        }

        @Override
        public boolean hasNext() {
            // First check if we have items in current batch
            if (currentBatch != null && currentBatch.hasNext()) {
                return true;
            }
            
            // If we're finished and no more items in current batch, return false
            if (finished) {
                return false;
            }
            
            // Try to load next batch
            loadNextBatch();
            
            // Check if we have items after loading next batch
            return currentBatch != null && currentBatch.hasNext();
        }

        @Override
        public byte[] next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            return currentBatch.next();
        }

        @Override
        public void close() {
            finished = true;
            currentBatch = null;
        }

        @Override
        public long getPosition() {
            return getCursorId();
        }

        @Override
        public boolean isClosed() {
            return finished;
        }

        private void loadNextBatch() {
            try {
                java.util.List<Object> args = new java.util.ArrayList<>();
                args.add(cursor);
                
                if (scanOptions.getPattern() != null) {
                    args.add("MATCH");
                    args.add(scanOptions.getPattern());
                }
                
                if (scanOptions.getCount() != null) {
                    args.add("COUNT");
                    args.add(scanOptions.getCount());
                }
                
                Object rawResult = connection.execute("SCAN", args.toArray());
                
                // Convert from Glide format to Spring Data Redis format
                Object result = ValkeyGlideConverters.defaultFromGlideResult(rawResult);
                
                if (result instanceof java.util.List) {
                    java.util.List<?> scanResult = (java.util.List<?>) result;
                    if (scanResult.size() >= 2) {
                        Object nextCursor = scanResult.get(0);
                        Object keysResult = scanResult.get(1);
                        
                        // Ensure cursor is properly converted to string
                        if (nextCursor instanceof byte[]) {
                            cursor = new String((byte[]) nextCursor, StandardCharsets.UTF_8);
                        } else {
                            cursor = nextCursor.toString();
                        }
                        
                        if ("0".equals(cursor)) {
                            finished = true;
                        }
                        
                        // Convert keys result using the existing converter
                        java.util.List<byte[]> keys = ValkeyGlideConverters.toBytesList(keysResult);
                        currentBatch = keys.iterator();
                    }
                } else if (result instanceof Object[]) {
                    // Handle array result format
                    Object[] scanResult = (Object[]) result;
                    if (scanResult.length >= 2) {
                        Object nextCursor = scanResult[0];
                        Object keysResult = scanResult[1];
                        
                        // Ensure cursor is properly converted to string
                        if (nextCursor instanceof byte[]) {
                            cursor = new String((byte[]) nextCursor, StandardCharsets.UTF_8);
                        } else {
                            cursor = nextCursor.toString();
                        }
                        
                        if ("0".equals(cursor)) {
                            finished = true;
                        }
                        
                        // Convert keys result using the existing converter
                        java.util.List<byte[]> keys = ValkeyGlideConverters.toBytesList(keysResult);
                        currentBatch = keys.iterator();
                    }
                }
            } catch (Exception ex) {
                throw new ValkeyGlideExceptionConverter().convert(ex);
            }
        }
    }

    @Override
    @Nullable
    public byte[] randomKey() {
        try {
            Object result = connection.execute("RANDOMKEY");
            return result instanceof byte[] ? (byte[]) result : 
                   result instanceof String ? ((String) result).getBytes() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public void rename(byte[] oldKey, byte[] newKey) {
        Assert.notNull(oldKey, "Old key must not be null");
        Assert.notNull(newKey, "New key must not be null");
        
        try {
            connection.execute("RENAME", oldKey, newKey);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Boolean renameNX(byte[] oldKey, byte[] newKey) {
        Assert.notNull(oldKey, "Old key must not be null");
        Assert.notNull(newKey, "New key must not be null");
        
        try {
            Object result = connection.execute("RENAMENX", oldKey, newKey);
            return result instanceof Boolean ? (Boolean) result : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Boolean expire(byte[] key, long seconds, ExpirationOptions.Condition condition) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result;
            if (condition == ExpirationOptions.Condition.ALWAYS) {
                result = connection.execute("EXPIRE", key, seconds);
            } else {
                String conditionStr = condition == ExpirationOptions.Condition.XX ? "XX" : "NX";
                result = connection.execute("EXPIRE", key, seconds, conditionStr);
            }
            return result instanceof Boolean ? (Boolean) result : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Boolean pExpire(byte[] key, long millis, ExpirationOptions.Condition condition) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result;
            if (condition == ExpirationOptions.Condition.ALWAYS) {
                result = connection.execute("PEXPIRE", key, millis);
            } else {
                String conditionStr = condition == ExpirationOptions.Condition.XX ? "XX" : "NX";
                result = connection.execute("PEXPIRE", key, millis, conditionStr);
            }
            return result instanceof Boolean ? (Boolean) result : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Boolean expireAt(byte[] key, long unixTime, ExpirationOptions.Condition condition) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result;
            if (condition == ExpirationOptions.Condition.ALWAYS) {
                result = connection.execute("EXPIREAT", key, unixTime);
            } else {
                String conditionStr = condition == ExpirationOptions.Condition.XX ? "XX" : "NX";
                result = connection.execute("EXPIREAT", key, unixTime, conditionStr);
            }
            return result instanceof Boolean ? (Boolean) result : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Boolean pExpireAt(byte[] key, long unixTimeInMillis, ExpirationOptions.Condition condition) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result;
            if (condition == ExpirationOptions.Condition.ALWAYS) {
                result = connection.execute("PEXPIREAT", key, unixTimeInMillis);
            } else {
                String conditionStr = condition == ExpirationOptions.Condition.XX ? "XX" : "NX";
                result = connection.execute("PEXPIREAT", key, unixTimeInMillis, conditionStr);
            }
            return result instanceof Boolean ? (Boolean) result : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Boolean persist(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("PERSIST", key);
            return result instanceof Boolean ? (Boolean) result : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Boolean move(byte[] key, int dbIndex) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("MOVE", key, dbIndex);
            return result instanceof Boolean ? (Boolean) result : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long ttl(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("TTL", key);
            return result instanceof Number ? ((Number) result).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long ttl(byte[] key, TimeUnit timeUnit) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(timeUnit, "TimeUnit must not be null");
        
        Long ttlSeconds = ttl(key);
        if (ttlSeconds == null) {
            return null;
        }
        
        return timeUnit.convert(ttlSeconds, TimeUnit.SECONDS);
    }

    @Override
    @Nullable
    public Long pTtl(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("PTTL", key);
            return result instanceof Number ? ((Number) result).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long pTtl(byte[] key, TimeUnit timeUnit) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(timeUnit, "TimeUnit must not be null");
        
        Long pTtlMillis = pTtl(key);
        if (pTtlMillis == null) {
            return null;
        }
        
        return timeUnit.convert(pTtlMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    @Nullable
    public List<byte[]> sort(byte[] key, SortParameters params) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            List<Object> args = new java.util.ArrayList<>();
            args.add(key);
            
            if (params != null) {
                ValkeyGlideConverters.appendSortParameters(args, params);
            }
            
            Object result = connection.execute("SORT", args.toArray());
            return ValkeyGlideConverters.toBytesList(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long sort(byte[] key, SortParameters params, byte[] storeKey) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(storeKey, "Store key must not be null");
        
        try {
            List<Object> args = new java.util.ArrayList<>();
            args.add(key);
            
            if (params != null) {
                ValkeyGlideConverters.appendSortParameters(args, params);
            }
            
            args.add("STORE");
            args.add(storeKey);
            
            Object result = connection.execute("SORT", args.toArray());
            return result instanceof Number ? ((Number) result).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public byte[] dump(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("DUMP", key);
            return result instanceof byte[] ? (byte[]) result : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public void restore(byte[] key, long ttlInMillis, byte[] serializedValue, boolean replace) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(serializedValue, "Serialized value must not be null");
        
        try {
            if (replace) {
                connection.execute("RESTORE", key, ttlInMillis, serializedValue, "REPLACE");
            } else {
                connection.execute("RESTORE", key, ttlInMillis, serializedValue);
            }
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public ValueEncoding encodingOf(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("OBJECT", "ENCODING", key);
            return ValkeyGlideConverters.toValueEncoding(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Duration idletime(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("OBJECT", "IDLETIME", key);
            if (result instanceof Number) {
                return Duration.ofSeconds(((Number) result).longValue());
            }
            return null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long refcount(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("OBJECT", "REFCOUNT", key);
            return result instanceof Number ? ((Number) result).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }
}

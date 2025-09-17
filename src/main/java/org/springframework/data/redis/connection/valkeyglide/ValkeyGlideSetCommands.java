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
import java.util.List;
import java.util.Set;

import org.springframework.data.redis.connection.RedisSetCommands;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Implementation of {@link RedisSetCommands} for Valkey-Glide.
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

    /**
     * Helper method to convert various object types to byte arrays.
     */
    private static byte[] convertToByteArray(Object obj) {
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
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(obj);
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
    public Long sAdd(byte[] key, byte[]... values) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(values, "Values must not be null");
        Assert.noNullElements(values, "Values must not contain null elements");
        
        if (key.length == 0) {
            throw new IllegalArgumentException("Key must not be empty");
        }
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            for (byte[] value : values) {
                args.add(value);
            }
            
            Object result = connection.execute("SADD", args.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return converted instanceof Number ? ((Number) converted).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long sRem(byte[] key, byte[]... values) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(values, "Values must not be null");
        Assert.noNullElements(values, "Values must not contain null elements");
        
        if (key.length == 0) {
            throw new IllegalArgumentException("Key must not be empty");
        }
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            for (byte[] value : values) {
                args.add(value);
            }
            
            Object result = connection.execute("SREM", args.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return converted instanceof Number ? ((Number) converted).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public byte[] sPop(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("SPOP", key);
            return (byte[]) ValkeyGlideConverters.defaultFromGlideResult(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public List<byte[]> sPop(byte[] key, long count) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("SPOP", key, count);
            @SuppressWarnings("unchecked")
            Set<Object> setResult = (Set<Object>) ValkeyGlideConverters.defaultFromGlideResult(result);
            
            List<byte[]> resultList = new ArrayList<>(setResult.size());
            for (Object item : setResult) {
                resultList.add((byte[]) ValkeyGlideConverters.defaultFromGlideResult(item));
            }
            return resultList;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Boolean sMove(byte[] srcKey, byte[] destKey, byte[] value) {
        Assert.notNull(srcKey, "Source key must not be null");
        Assert.notNull(destKey, "Destination key must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            Object result = connection.execute("SMOVE", srcKey, destKey, value);
            return (Boolean) ValkeyGlideConverters.defaultFromGlideResult(result);
            
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long sCard(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        if (key.length == 0) {
            throw new IllegalArgumentException("Key must not be empty");
        }
        
        try {
            Object result = connection.execute("SCARD", key);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return converted instanceof Number ? ((Number) converted).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Boolean sIsMember(byte[] key, byte[] value) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            Object result = connection.execute("SISMEMBER", key, value);
            return (Boolean) ValkeyGlideConverters.defaultFromGlideResult(result);
            
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public List<Boolean> sMIsMember(byte[] key, byte[]... values) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(values, "Values must not be null");
        Assert.noNullElements(values, "Values must not contain null elements");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            for (byte[] value : values) {
                args.add(value);
            }
            
            Object result = connection.execute("SMISMEMBER", args.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            
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
                throw new IllegalStateException("Unexpected result type from SMISMEMBER: " + converted.getClass());
            }
            
            List<Boolean> resultList = new ArrayList<>(list.size());
            for (Object item : list) {
                resultList.add((Boolean) ValkeyGlideConverters.defaultFromGlideResult(item));
            }
            return resultList;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<byte[]> sDiff(byte[]... keys) {
        Assert.notNull(keys, "Keys must not be null");
        Assert.noNullElements(keys, "Keys must not contain null elements");
        
        try {
            List<Object> args = new ArrayList<>();
            for (byte[] key : keys) {
                args.add(key);
            }
            
            Object result = connection.execute("SDIFF", args.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
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
            } else if (converted instanceof Set) {
                @SuppressWarnings("unchecked")
                Set<Object> castSet = (Set<Object>) converted;
                list = new ArrayList<>(castSet);
            } else {
                throw new IllegalStateException("Unexpected result type from SDIFF: " + converted.getClass());
            }
            
            Set<byte[]> resultSet = new HashSet<>(list.size());
            for (Object item : list) {
                Object convertedItem = ValkeyGlideConverters.defaultFromGlideResult(item);
                resultSet.add((byte[]) convertedItem);
            }
            return resultSet;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long sDiffStore(byte[] destKey, byte[]... keys) {
        Assert.notNull(destKey, "Destination key must not be null");
        Assert.notNull(keys, "Keys must not be null");
        Assert.noNullElements(keys, "Keys must not contain null elements");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(destKey);
            for (byte[] key : keys) {
                args.add(key);
            }
            
            Object result = connection.execute("SDIFFSTORE", args.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return converted instanceof Number ? ((Number) converted).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<byte[]> sInter(byte[]... keys) {
        Assert.notNull(keys, "Keys must not be null");
        Assert.noNullElements(keys, "Keys must not contain null elements");
        
        try {
            List<Object> args = new ArrayList<>();
            for (byte[] key : keys) {
                args.add(key);
            }
            
            Object result = connection.execute("SINTER", args.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
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
            } else if (converted instanceof Set) {
                @SuppressWarnings("unchecked")
                Set<Object> castSet = (Set<Object>) converted;
                list = new ArrayList<>(castSet);
            } else {
                throw new IllegalStateException("Unexpected result type from SINTER: " + converted.getClass());
            }
            
            Set<byte[]> resultSet = new HashSet<>(list.size());
            for (Object item : list) {
                Object convertedItem = ValkeyGlideConverters.defaultFromGlideResult(item);
                resultSet.add((byte[]) convertedItem);
            }
            return resultSet;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long sInterStore(byte[] destKey, byte[]... keys) {
        Assert.notNull(destKey, "Destination key must not be null");
        Assert.notNull(keys, "Keys must not be null");
        Assert.noNullElements(keys, "Keys must not contain null elements");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(destKey);
            for (byte[] key : keys) {
                args.add(key);
            }
            
            Object result = connection.execute("SINTERSTORE", args.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return converted instanceof Number ? ((Number) converted).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<byte[]> sUnion(byte[]... keys) {
        Assert.notNull(keys, "Keys must not be null");
        Assert.noNullElements(keys, "Keys must not contain null elements");
        
        try {
            List<Object> args = new ArrayList<>();
            for (byte[] key : keys) {
                args.add(key);
            }
            
            Object result = connection.execute("SUNION", args.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
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
            } else if (converted instanceof Set) {
                @SuppressWarnings("unchecked")
                Set<Object> castSet = (Set<Object>) converted;
                list = new ArrayList<>(castSet);
            } else {
                throw new IllegalStateException("Unexpected result type from SUNION: " + converted.getClass());
            }
            
            Set<byte[]> resultSet = new HashSet<>(list.size());
            for (Object item : list) {
                Object convertedItem = ValkeyGlideConverters.defaultFromGlideResult(item);
                resultSet.add((byte[]) convertedItem);
            }
            return resultSet;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long sUnionStore(byte[] destKey, byte[]... keys) {
        Assert.notNull(destKey, "Destination key must not be null");
        Assert.notNull(keys, "Keys must not be null");
        Assert.noNullElements(keys, "Keys must not contain null elements");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(destKey);
            for (byte[] key : keys) {
                args.add(key);
            }
            
            Object result = connection.execute("SUNIONSTORE", args.toArray());
            return ((Number) ValkeyGlideConverters.defaultFromGlideResult(result)).longValue();
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Set<byte[]> sMembers(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("SMEMBERS", key);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
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
            } else if (converted instanceof Set) {
                @SuppressWarnings("unchecked")
                Set<Object> castSet = (Set<Object>) converted;
                list = new ArrayList<>(castSet);
            } else {
                throw new IllegalStateException("Unexpected result type from SMEMBERS: " + converted.getClass());
            }
            
            Set<byte[]> resultSet = new HashSet<>(list.size());
            for (Object item : list) {
                Object convertedItem = ValkeyGlideConverters.defaultFromGlideResult(item);
                resultSet.add((byte[]) convertedItem);
            }
            return resultSet;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public byte[] sRandMember(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("SRANDMEMBER", key);
            return (byte[]) ValkeyGlideConverters.defaultFromGlideResult(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public List<byte[]> sRandMember(byte[] key, long count) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("SRANDMEMBER", key, count);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
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
                throw new IllegalStateException("Unexpected result type from SRANDMEMBER: " + converted.getClass());
            }
            
            List<byte[]> resultList = new ArrayList<>(list.size());
            for (Object item : list) {
                Object convertedItem = ValkeyGlideConverters.defaultFromGlideResult(item);
                resultList.add((byte[]) convertedItem);
            }
            return resultList;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public Cursor<byte[]> sScan(byte[] key, ScanOptions options) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(options, "ScanOptions must not be null");
        
        return new ValkeyGlideSetScanCursor(key, options, connection);
    }
    
    /**
     * Simple implementation of Cursor for SSCAN operation.
     */
    private static class ValkeyGlideSetScanCursor implements Cursor<byte[]> {
        
        private final byte[] key;
        private final ScanOptions options;
        private final ValkeyGlideConnection connection;
        private long cursor = 0;
        private List<byte[]> members = new ArrayList<>();
        private int currentIndex = 0;
        private boolean finished = false;
        
        public ValkeyGlideSetScanCursor(byte[] key, ScanOptions options, ValkeyGlideConnection connection) {
            this.key = key;
            this.options = options;
            this.connection = connection;
            scanNext();
        }
        
        @Override
        public boolean hasNext() {
            if (currentIndex < members.size()) {
                return true;
            }
            if (finished) {
                return false;
            }
            scanNext();
            return currentIndex < members.size();
        }
        
        @Override
        public byte[] next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            return members.get(currentIndex++);
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
                
                Object result = connection.execute("SSCAN", args.toArray());
                Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
                
                List<Object> scanResultList;
                if (converted instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<Object> castList = (List<Object>) converted;
                    scanResultList = castList;
                } else if (converted instanceof Object[]) {
                    Object[] array = (Object[]) converted;
                    scanResultList = new ArrayList<>(array.length);
                    for (Object item : array) {
                        scanResultList.add(item);
                    }
                } else {
                    throw new IllegalStateException("Unexpected SSCAN result type: " + (converted != null ? converted.getClass() : "null"));
                }
                
                if (scanResultList.size() >= 2) {
                    // First element is the new cursor
                    Object cursorObj = ValkeyGlideConverters.defaultFromGlideResult(scanResultList.get(0));
                    
                    if (cursorObj instanceof String) {
                        cursor = Long.parseLong((String) cursorObj);
                    } else if (cursorObj instanceof Number) {
                        cursor = ((Number) cursorObj).longValue();
                    } else if (cursorObj instanceof byte[]) {
                        cursor = Long.parseLong(new String((byte[]) cursorObj));
                    }
                    
                    if (cursor == 0) {
                        finished = true;
                    }
                    
                    // Second element is the array/list of members
                    Object membersObj = ValkeyGlideConverters.defaultFromGlideResult(scanResultList.get(1));
                    
                    // Reset members for this batch
                    members.clear();
                    currentIndex = 0;
                    
                    if (membersObj instanceof Object[]) {
                        Object[] memberArray = (Object[]) membersObj;
                        // Process members
                        for (Object memberObj : memberArray) {
                            Object convertedMember = ValkeyGlideConverters.defaultFromGlideResult(memberObj);
                            if (convertedMember instanceof byte[]) {
                                members.add((byte[]) convertedMember);
                            } else {
                                // Handle other types that can be converted to byte[]
                                byte[] memberBytes = convertToByteArray(convertedMember);
                                if (memberBytes != null) {
                                    members.add(memberBytes);
                                }
                            }
                        }
                    } else if (membersObj instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<Object> memberList = (List<Object>) membersObj;
                        // Process members
                        for (Object memberObj : memberList) {
                            Object convertedMember = ValkeyGlideConverters.defaultFromGlideResult(memberObj);
                            if (convertedMember instanceof byte[]) {
                                members.add((byte[]) convertedMember);
                            } else {
                                // Handle other types that can be converted to byte[]
                                byte[] memberBytes = convertToByteArray(convertedMember);
                                if (memberBytes != null) {
                                    members.add(memberBytes);
                                }
                            }
                        }
                    }
                }
            } catch (Exception ex) {
                throw new ValkeyGlideExceptionConverter().convert(ex);
            }
        }
        
        @Override
        public void close() {
            // No resources to close for this implementation
        }
        
        @Override
        public boolean isClosed() {
            return finished && currentIndex >= members.size();
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
        public CursorId getId() {
            return CursorId.of(cursor);
        }
    }
}

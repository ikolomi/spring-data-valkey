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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
 * Implementation of {@link RedisHashCommands} for Valkey-Glide.
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
    public Boolean hSet(byte[] key, byte[] field, byte[] value) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(field, "Field must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            Object result = connection.execute("HSET", key, field, value);
            if (result instanceof Number) {
                return ((Number) result).longValue() == 1;
            }
            if (result instanceof Boolean) {
                return (Boolean) result;
            }
            return null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Boolean hSetNX(byte[] key, byte[] field, byte[] value) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(field, "Field must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            Object result = connection.execute("HSETNX", key, field, value);
            if (result instanceof Number) {
                return ((Number) result).longValue() == 1;
            }
            if (result instanceof Boolean) {
                return (Boolean) result;
            }
            return null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public byte[] hGet(byte[] key, byte[] field) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(field, "Field must not be null");
        
        try {
            Object result = connection.execute("HGET", key, field);
            return convertToByteArray(result);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public List<byte[]> hMGet(byte[] key, byte[]... fields) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(fields, "Fields must not be null");
        Assert.noNullElements(fields, "Fields must not contain null elements");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            for (byte[] field : fields) {
                args.add(field);
            }
            
            Object result = connection.execute("HMGET", args.toArray());
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
                throw new IllegalStateException("Unexpected result type from HMGET: " + converted.getClass());
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
    public void hMSet(byte[] key, Map<byte[], byte[]> hashes) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(hashes, "Hashes must not be null");
        Assert.notEmpty(hashes, "Hashes must not be empty");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            for (Map.Entry<byte[], byte[]> entry : hashes.entrySet()) {
                args.add(entry.getKey());
                args.add(entry.getValue());
            }
            
            connection.execute("HMSET", args.toArray());
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long hIncrBy(byte[] key, byte[] field, long delta) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(field, "Field must not be null");
        
        try {
            Object result = connection.execute("HINCRBY", key, field, delta);
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
    public Double hIncrBy(byte[] key, byte[] field, double delta) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(field, "Field must not be null");
        
        try {
            Object result = connection.execute("HINCRBYFLOAT", key, field, delta);
            if (result instanceof String) {
                return Double.parseDouble((String) result);
            } else if (result instanceof Number) {
                return ((Number) result).doubleValue();
            }
            return null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Boolean hExists(byte[] key, byte[] field) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(field, "Field must not be null");
        
        try {
            Object result = connection.execute("HEXISTS", key, field);
            if (result instanceof Number) {
                return ((Number) result).longValue() == 1;
            }
            if (result instanceof Boolean) {
                return (Boolean) result;
            }
            return null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long hDel(byte[] key, byte[]... fields) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(fields, "Fields must not be null");
        Assert.noNullElements(fields, "Fields must not contain null elements");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            for (byte[] field : fields) {
                args.add(field);
            }
            
            Object result = connection.execute("HDEL", args.toArray());
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
    public Long hLen(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("HLEN", key);
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
    public Set<byte[]> hKeys(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("HKEYS", key);
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
                throw new IllegalStateException("Unexpected result type from HKEYS: " + converted.getClass());
            }
            
            Set<byte[]> resultSet = new HashSet<>(list.size());
            for (Object item : list) {
                Object convertedItem = ValkeyGlideConverters.fromGlideResult(item);
                resultSet.add((byte[]) convertedItem);
            }
            return resultSet;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public List<byte[]> hVals(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("HVALS", key);
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
                throw new IllegalStateException("Unexpected result type from HVALS: " + converted.getClass());
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
    public Map<byte[], byte[]> hGetAll(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("HGETALL", key);
            Object converted = ValkeyGlideConverters.fromGlideResult(result);
            if (converted == null) {
                return null;
            }
            
            // Handle case where Glide returns a Map directly
            if (converted instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<Object, Object> sourceMap = (Map<Object, Object>) converted;
                Map<byte[], byte[]> resultMap = new LinkedHashMap<>();
                for (Map.Entry<Object, Object> entry : sourceMap.entrySet()) {
                    Object keyItem = ValkeyGlideConverters.fromGlideResult(entry.getKey());
                    Object valueItem = ValkeyGlideConverters.fromGlideResult(entry.getValue());
                    resultMap.put((byte[]) keyItem, (byte[]) valueItem);
                }
                return resultMap;
            }
            
            // Handle case where result is a list/array of key-value pairs
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
                throw new IllegalStateException("Unexpected result type from HGETALL: " + converted.getClass());
            }
            
            Map<byte[], byte[]> resultMap = new LinkedHashMap<>();
            for (int i = 0; i < list.size(); i += 2) {
                Object keyItem = ValkeyGlideConverters.fromGlideResult(list.get(i));
                Object valueItem = ValkeyGlideConverters.fromGlideResult(list.get(i + 1));
                resultMap.put((byte[]) keyItem, (byte[]) valueItem);
            }
            return resultMap;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public byte[] hRandField(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("HRANDFIELD", key);
            return (byte[]) result;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Map.Entry<byte[], byte[]> hRandFieldWithValues(byte[] key) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("HRANDFIELD", key, 1, "WITHVALUES");
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
                throw new IllegalStateException("Unexpected result type from HRANDFIELD: " + converted.getClass());
            }
            
            // Handle different possible result structures from HRANDFIELD with WITHVALUES
            if (list.isEmpty()) {
                return null;
            }
            
            // Check if the result structure is flat (field, value) or nested ([[field, value]])
            Object firstElement = list.get(0);
            Object firstConverted = ValkeyGlideConverters.fromGlideResult(firstElement);
            
            if (firstConverted instanceof Object[] || firstConverted instanceof List) {
                // Result structure is nested pair: [[field, value]]
                Object convertedItem = ValkeyGlideConverters.fromGlideResult(firstElement);
                
                if (convertedItem instanceof Object[]) {
                    Object[] pair = (Object[]) convertedItem;
                    if (pair.length >= 2) {
                        byte[] fieldBytes = convertToByteArray(ValkeyGlideConverters.fromGlideResult(pair[0]));
                        byte[] valueBytes = convertToByteArray(ValkeyGlideConverters.fromGlideResult(pair[1]));
                        if (fieldBytes != null && valueBytes != null) {
                            return new HashMap.SimpleEntry<>(fieldBytes, valueBytes);
                        }
                    }
                } else if (convertedItem instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<Object> pair = (List<Object>) convertedItem;
                    if (pair.size() >= 2) {
                        byte[] fieldBytes = convertToByteArray(ValkeyGlideConverters.fromGlideResult(pair.get(0)));
                        byte[] valueBytes = convertToByteArray(ValkeyGlideConverters.fromGlideResult(pair.get(1)));
                        if (fieldBytes != null && valueBytes != null) {
                            return new HashMap.SimpleEntry<>(fieldBytes, valueBytes);
                        }
                    }
                }
            } else {
                // Result structure is flat: [field, value]
                if (list.size() >= 2) {
                    Object keyItem = ValkeyGlideConverters.fromGlideResult(list.get(0));
                    Object valueItem = ValkeyGlideConverters.fromGlideResult(list.get(1));
                    
                    byte[] fieldBytes = convertToByteArray(keyItem);
                    byte[] valueBytes = convertToByteArray(valueItem);
                    
                    if (fieldBytes != null && valueBytes != null) {
                        return new HashMap.SimpleEntry<>(fieldBytes, valueBytes);
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
    public List<byte[]> hRandField(byte[] key, long count) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("HRANDFIELD", key, count);
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
                throw new IllegalStateException("Unexpected result type from HRANDFIELD: " + converted.getClass());
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
    public List<Map.Entry<byte[], byte[]>> hRandFieldWithValues(byte[] key, long count) {
        Assert.notNull(key, "Key must not be null");
        
        try {
            Object result = connection.execute("HRANDFIELD", key, count, "WITHVALUES");
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
                throw new IllegalStateException("Unexpected result type from HRANDFIELD: " + converted.getClass());
            }
            
            List<Map.Entry<byte[], byte[]>> resultList = new ArrayList<>();
            
            // Handle different possible result structures from HRANDFIELD with WITHVALUES
            if (list.isEmpty()) {
                return resultList;
            }
            
            // Check if the result structure is flat (field1, value1, field2, value2, ...)
            // or nested pairs ([[field1, value1], [field2, value2], ...])
            Object firstElement = list.get(0);
            Object firstConverted = ValkeyGlideConverters.fromGlideResult(firstElement);
            
            if (firstConverted instanceof Object[] || firstConverted instanceof List) {
                // Result structure is nested pairs: [[field1, value1], [field2, value2], ...]
                for (Object item : list) {
                    Object convertedItem = ValkeyGlideConverters.fromGlideResult(item);
                    
                    if (convertedItem instanceof Object[]) {
                        Object[] pair = (Object[]) convertedItem;
                        if (pair.length >= 2) {
                            byte[] fieldBytes = convertToByteArray(ValkeyGlideConverters.fromGlideResult(pair[0]));
                            byte[] valueBytes = convertToByteArray(ValkeyGlideConverters.fromGlideResult(pair[1]));
                            if (fieldBytes != null && valueBytes != null) {
                                resultList.add(new HashMap.SimpleEntry<>(fieldBytes, valueBytes));
                            }
                        }
                    } else if (convertedItem instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<Object> pair = (List<Object>) convertedItem;
                        if (pair.size() >= 2) {
                            byte[] fieldBytes = convertToByteArray(ValkeyGlideConverters.fromGlideResult(pair.get(0)));
                            byte[] valueBytes = convertToByteArray(ValkeyGlideConverters.fromGlideResult(pair.get(1)));
                            if (fieldBytes != null && valueBytes != null) {
                                resultList.add(new HashMap.SimpleEntry<>(fieldBytes, valueBytes));
                            }
                        }
                    }
                }
            } else {
                // Result structure is flat: [field1, value1, field2, value2, ...]
                for (int i = 0; i < list.size(); i += 2) {
                    if (i + 1 < list.size()) {
                        Object keyItem = ValkeyGlideConverters.fromGlideResult(list.get(i));
                        Object valueItem = ValkeyGlideConverters.fromGlideResult(list.get(i + 1));
                        
                        byte[] fieldBytes = convertToByteArray(keyItem);
                        byte[] valueBytes = convertToByteArray(valueItem);
                        
                        if (fieldBytes != null && valueBytes != null) {
                            resultList.add(new HashMap.SimpleEntry<>(fieldBytes, valueBytes));
                        }
                    }
                }
            }
            
            return resultList;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public Cursor<Map.Entry<byte[], byte[]>> hScan(byte[] key, ScanOptions options) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(options, "ScanOptions must not be null");
        
        return new ValkeyGlideHashScanCursor(key, options, connection);
    }
    
    /**
     * Simple implementation of Cursor for HSCAN operation.
     */
    private static class ValkeyGlideHashScanCursor implements Cursor<Map.Entry<byte[], byte[]>> {
        
        private final byte[] key;
        private final ScanOptions options;
        private final ValkeyGlideConnection connection;
        private long cursor = 0;
        private List<Map.Entry<byte[], byte[]>> entries = new ArrayList<>();
        private int currentIndex = 0;
        private boolean finished = false;
        
        public ValkeyGlideHashScanCursor(byte[] key, ScanOptions options, ValkeyGlideConnection connection) {
            this.key = key;
            this.options = options;
            this.connection = connection;
            scanNext();
        }
        
        @Override
        public boolean hasNext() {
            if (currentIndex < entries.size()) {
                return true;
            }
            if (finished) {
                return false;
            }
            scanNext();
            return currentIndex < entries.size();
        }
        
        @Override
        public Map.Entry<byte[], byte[]> next() {
            if (!hasNext()) {
                throw new java.util.NoSuchElementException();
            }
            return entries.get(currentIndex++);
        }
        
        private void scanNext() {
            try {
                List<Object> args = new ArrayList<>();
                args.add(key);
                args.add(String.valueOf(cursor));
                
                if (options.getCount() != null) {
                    args.add("COUNT");
                    args.add(options.getCount());
                }
                
                if (options.getPattern() != null) {
                    args.add("MATCH");
                    args.add(options.getPattern());
                }
                
                Object result = connection.execute("HSCAN", args.toArray());
                Object converted = ValkeyGlideConverters.fromGlideResult(result);
                
                // Handle both Object[] and List since fromGlideResult might convert Object[] to List
                List<Object> scanResult;
                if (converted instanceof List) {
                    @SuppressWarnings("unchecked")
                    List<Object> castList = (List<Object>) converted;
                    scanResult = castList;
                } else if (converted instanceof Object[]) {
                    Object[] array = (Object[]) converted;
                    scanResult = new ArrayList<>(array.length);
                    for (Object item : array) {
                        scanResult.add(item);
                    }
                } else {
                    return;
                }
                
                if (scanResult.size() >= 2) {
                    // First element is the new cursor
                    Object cursorObj = ValkeyGlideConverters.fromGlideResult(scanResult.get(0));
                    
                    if (cursorObj instanceof String) {
                        cursor = Long.parseLong((String) cursorObj);
                    } else if (cursorObj instanceof Number) {
                        cursor = ((Number) cursorObj).longValue();
                    }
                    
                    if (cursor == 0) {
                        finished = true;
                    }
                    
                    // Second element is the array of field-value pairs
                    Object fieldsObj = ValkeyGlideConverters.fromGlideResult(scanResult.get(1));
                    
                    // Reset entries for this batch
                    entries.clear();
                    currentIndex = 0;
                    
                    // Handle different possible structures for field-value pairs
                    List<Object> fieldsList;
                    if (fieldsObj instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<Object> castList = (List<Object>) fieldsObj;
                        fieldsList = castList;
                    } else if (fieldsObj instanceof Object[]) {
                        Object[] fieldsArray = (Object[]) fieldsObj;
                        fieldsList = new ArrayList<>(fieldsArray.length);
                        for (Object item : fieldsArray) {
                            fieldsList.add(item);
                        }
                    } else {
                        // If fieldsObj is null or unexpected type, skip processing
                        fieldsList = new ArrayList<>();
                    }
                    
                    // Process field-value pairs
                    for (int i = 0; i < fieldsList.size(); i += 2) {
                        if (i + 1 < fieldsList.size()) {
                            Object fieldObj = ValkeyGlideConverters.fromGlideResult(fieldsList.get(i));
                            Object valueObj = ValkeyGlideConverters.fromGlideResult(fieldsList.get(i + 1));
                            
                            byte[] fieldBytes = (byte[]) fieldObj;
                            byte[] valueBytes = (byte[]) valueObj;
                            
                            entries.add(new HashMap.SimpleEntry<>(fieldBytes, valueBytes));
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
            return finished && currentIndex >= entries.size();
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

    @Override
    @Nullable
    public Long hStrLen(byte[] key, byte[] field) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(field, "Field must not be null");
        
        try {
            Object result = connection.execute("HSTRLEN", key, field);
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
    public List<Long> hExpire(byte[] key, long seconds, ExpirationOptions.Condition condition, byte[]... fields) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(fields, "Fields must not be null");
        Assert.noNullElements(fields, "Fields must not contain null elements");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            args.add(seconds);
            
            // Add condition if not ALWAYS
            if (condition != ExpirationOptions.Condition.ALWAYS) {
                args.add(condition.name());
            }
            
            // Add FIELDS keyword and numfields
            args.add("FIELDS");
            args.add(fields.length);
            
            // Add fields
            for (byte[] field : fields) {
                args.add(field);
            }
            
            Object result = connection.execute("HEXPIRE", args.toArray());
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
                throw new IllegalStateException("Unexpected result type from HEXPIRE: " + converted.getClass());
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
    public List<Long> hpExpire(byte[] key, long millis, ExpirationOptions.Condition condition, byte[]... fields) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(fields, "Fields must not be null");
        Assert.noNullElements(fields, "Fields must not contain null elements");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            args.add(millis);
            
            // Add condition if not ALWAYS
            if (condition != ExpirationOptions.Condition.ALWAYS) {
                args.add(condition.name());
            }
            
            // Add FIELDS keyword and numfields
            args.add("FIELDS");
            args.add(fields.length);
            
            // Add fields
            for (byte[] field : fields) {
                args.add(field);
            }
            
            Object result = connection.execute("HPEXPIRE", args.toArray());
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
                throw new IllegalStateException("Unexpected result type from HPEXPIRE: " + converted.getClass());
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
    public List<Long> hExpireAt(byte[] key, long unixTime, ExpirationOptions.Condition condition, byte[]... fields) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(fields, "Fields must not be null");
        Assert.noNullElements(fields, "Fields must not contain null elements");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            args.add(unixTime);
            
            // Add condition if not ALWAYS
            if (condition != ExpirationOptions.Condition.ALWAYS) {
                args.add(condition.name());
            }
            
            // Add FIELDS keyword and numfields
            args.add("FIELDS");
            args.add(fields.length);
            
            // Add fields
            for (byte[] field : fields) {
                args.add(field);
            }
            
            Object result = connection.execute("HEXPIREAT", args.toArray());
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
                throw new IllegalStateException("Unexpected result type from HEXPIREAT: " + converted.getClass());
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
    public List<Long> hpExpireAt(byte[] key, long unixTimeInMillis, ExpirationOptions.Condition condition, byte[]... fields) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(fields, "Fields must not be null");
        Assert.noNullElements(fields, "Fields must not contain null elements");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            args.add(unixTimeInMillis);
            
            // Add condition if not ALWAYS
            if (condition != ExpirationOptions.Condition.ALWAYS) {
                args.add(condition.name());
            }
            
            // Add FIELDS keyword and numfields
            args.add("FIELDS");
            args.add(fields.length);
            
            // Add fields
            for (byte[] field : fields) {
                args.add(field);
            }
            
            Object result = connection.execute("HPEXPIREAT", args.toArray());
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
                throw new IllegalStateException("Unexpected result type from HPEXPIREAT: " + converted.getClass());
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
    public List<Long> hTtl(byte[] key, byte[]... fields) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(fields, "Fields must not be null");
        Assert.noNullElements(fields, "Fields must not contain null elements");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            args.add("FIELDS");
            args.add(fields.length);
            for (byte[] field : fields) {
                args.add(field);
            }
            
            Object result = connection.execute("HTTL", args.toArray());
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
                throw new IllegalStateException("Unexpected result type from HTTL: " + converted.getClass());
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
    public List<Long> hTtl(byte[] key, TimeUnit timeUnit, byte[]... fields) {
        List<Long> ttls = hTtl(key, fields);
        if (ttls == null) {
            return null;
        }
        
        List<Long> converted = new ArrayList<>(ttls.size());
        for (Long ttl : ttls) {
            if (ttl == null) {
                converted.add(null);
            } else {
                converted.add(timeUnit.convert(ttl, TimeUnit.SECONDS));
            }
        }
        return converted;
    }

    @Override
    @Nullable
    public List<Long> hpTtl(byte[] key, byte[]... fields) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(fields, "Fields must not be null");
        Assert.noNullElements(fields, "Fields must not contain null elements");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            args.add("FIELDS");
            args.add(fields.length);
            for (byte[] field : fields) {
                args.add(field);
            }
            
            Object result = connection.execute("HPTTL", args.toArray());
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
                throw new IllegalStateException("Unexpected result type from HPTTL: " + converted.getClass());
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
    public List<Long> hPersist(byte[] key, byte[]... fields) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(fields, "Fields must not be null");
        Assert.noNullElements(fields, "Fields must not contain null elements");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            args.add("FIELDS");
            args.add(fields.length);
            for (byte[] field : fields) {
                args.add(field);
            }
            
            Object result = connection.execute("HPERSIST", args.toArray());
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
                throw new IllegalStateException("Unexpected result type from HPERSIST: " + converted.getClass());
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
}

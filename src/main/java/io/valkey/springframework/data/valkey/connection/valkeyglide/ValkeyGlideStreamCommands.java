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
package io.valkey.springframework.data.valkey.connection.valkeyglide;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Range;
import io.valkey.springframework.data.valkey.connection.Limit;
import io.valkey.springframework.data.valkey.connection.ValkeyStreamCommands;
import io.valkey.springframework.data.valkey.connection.stream.*;
import io.valkey.springframework.data.valkey.connection.stream.StreamInfo.XInfoConsumers;
import io.valkey.springframework.data.valkey.connection.stream.StreamInfo.XInfoGroups;
import io.valkey.springframework.data.valkey.connection.stream.StreamInfo.XInfoStream;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Implementation of {@link ValkeyStreamCommands} for Valkey-Glide.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideStreamCommands implements ValkeyStreamCommands {

    private final ValkeyGlideConnection connection;

    /**
     * Creates a new {@link ValkeyGlideStreamCommands}.
     *
     * @param connection must not be {@literal null}.
     */
    public ValkeyGlideStreamCommands(ValkeyGlideConnection connection) {
        Assert.notNull(connection, "Connection must not be null!");
        this.connection = connection;
    }

    @Override
    public Long xAck(byte[] key, String group, RecordId... recordIds) {
        Assert.notNull(key, "Key must not be null!");
        Assert.notNull(group, "Group must not be null!");
        Assert.notNull(recordIds, "Record IDs must not be null!");

        if (recordIds.length == 0) {
            return 0L;
        }

        try {
            Object[] params = new Object[recordIds.length + 2];
            params[0] = key;
            params[1] = group;

            for (int i = 0; i < recordIds.length; i++) {
                params[i + 2] = recordIds[i].getValue();
            }

            Object result = connection.execute("XACK", params);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return ((Number) converted).longValue();
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public RecordId xAdd(MapRecord<byte[], byte[], byte[]> record) {
        return xAdd(record, XAddOptions.none());
    }

    @Override
    public RecordId xAdd(MapRecord<byte[], byte[], byte[]> record, XAddOptions options) {
        Assert.notNull(record, "Record must not be null!");
        Assert.notNull(options, "Options must not be null!");

        byte[] key = record.getStream();
        RecordId recordId = record.getId();
        Map<byte[], byte[]> body = record.getValue();

        Assert.notNull(key, "Key must not be null!");
        Assert.notEmpty(body, "Body must not be empty!");

        try {
            List<Object> params = new ArrayList<>(2 + body.size() * 2 + 4);
            params.add(key);

            if (options.isNoMkStream()) {
                params.add("NOMKSTREAM");
            }

            if (options.hasMaxlen()) {
                params.add("MAXLEN");
                if (options.isApproximateTrimming()) {
                    params.add("~");
                }
                params.add(String.valueOf(options.getMaxlen()));
            }

            if (options.hasMinId()) {
                params.add("MINID");
                if (options.isApproximateTrimming()) {
                    params.add("~");
                }
                params.add(options.getMinId().getValue());
            }

            if (recordId == null || recordId.equals(RecordId.autoGenerate())) {
                params.add("*");
            } else {
                params.add(recordId.getValue());
            }

            for (Map.Entry<byte[], byte[]> entry : body.entrySet()) {
                params.add(entry.getKey());
                params.add(entry.getValue());
            }

            Object result = connection.execute("XADD", params.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            String response = convertResultToString(converted);
            return RecordId.of(response);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public List<RecordId> xClaimJustId(byte[] key, String group, String newOwner, XClaimOptions options) {
        Assert.notNull(key, "Key must not be null!");
        Assert.hasText(group, "Group must not be null or empty!");
        Assert.hasText(newOwner, "New owner must not be null or empty!");
        Assert.notNull(options, "Options must not be null!");

        try {
            List<Object> args = buildXClaimArgs(key, group, newOwner, options);
            args.add("JUSTID");
            
            Object result = connection.execute("XCLAIM", args.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return parseRecordIds(converted);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public List<ByteRecord> xClaim(byte[] key, String group, String newOwner, XClaimOptions options) {
        Assert.notNull(key, "Key must not be null!");
        Assert.hasText(group, "Group must not be null or empty!");
        Assert.hasText(newOwner, "New owner must not be null or empty!");
        Assert.notNull(options, "Options must not be null!");

        try {
            List<Object> args = buildXClaimArgs(key, group, newOwner, options);
            
            Object result = connection.execute("XCLAIM", args.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return parseByteRecords(converted, key, false);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public Long xDel(byte[] key, RecordId... recordIds) {
        Assert.notNull(key, "Key must not be null!");
        Assert.notNull(recordIds, "Record IDs must not be null!");
        
        if (recordIds.length == 0) {
            return 0L;
        }
        
        try {
            Object[] params = new Object[recordIds.length + 1];
            params[0] = key;
            
            for (int i = 0; i < recordIds.length; i++) {
                params[i + 1] = recordIds[i].getValue();
            }
            
            Object result = connection.execute("XDEL", params);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return ((Number) converted).longValue();
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public String xGroupCreate(byte[] key, String groupName, ReadOffset readOffset) {
        return xGroupCreate(key, groupName, readOffset, false);
    }

    @Override
    public String xGroupCreate(byte[] key, String groupName, ReadOffset readOffset, boolean makeStream) {
        Assert.notNull(key, "Key must not be null!");
        Assert.hasText(groupName, "Group name must not be null or empty!");
        Assert.notNull(readOffset, "ReadOffset must not be null!");

        try {
            List<Object> args = new ArrayList<>(6);
            args.add("CREATE");
            args.add(key);
            args.add(groupName);
            args.add(readOffset.getOffset());
            
            if (makeStream) {
                args.add("MKSTREAM");
            }

            Object result = connection.execute("XGROUP", args.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return convertResultToString(converted);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }
    
    @Override
    public Boolean xGroupDelConsumer(byte[] key, Consumer consumer) {
        Assert.notNull(key, "Key must not be null!");
        Assert.notNull(consumer, "Consumer must not be null!");

        try {
            Object result = connection.execute("XGROUP", "DELCONSUMER", key, consumer.getGroup(), consumer.getName());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return ((Number) converted).longValue() > 0;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public Boolean xGroupDestroy(byte[] key, String groupName) {
        Assert.notNull(key, "Key must not be null!");
        Assert.hasText(groupName, "Group name must not be null or empty!");

        try {
            Object result = connection.execute("XGROUP", "DESTROY", key, groupName);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return (Boolean) converted;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public XInfoStream xInfo(byte[] key) {
        Assert.notNull(key, "Key must not be null!");

        try {
            Object result = connection.execute("XINFO", "STREAM", key);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return parseXInfoStream(converted);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public XInfoGroups xInfoGroups(byte[] key) {
        Assert.notNull(key, "Key must not be null!");

        try {
            Object result = connection.execute("XINFO", "GROUPS", key);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return parseXInfoGroups(converted);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public XInfoConsumers xInfoConsumers(byte[] key, String groupName) {
        Assert.notNull(key, "Key must not be null!");
        Assert.hasText(groupName, "Group name must not be null or empty!");

        try {
            Object result = connection.execute("XINFO", "CONSUMERS", key, groupName);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return parseXInfoConsumers(converted);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public Long xLen(byte[] key) {
        Assert.notNull(key, "Key must not be null!");
        
        try {
            Object result = connection.execute("XLEN", key);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            if (converted == null) {
                return 0L;
            }
            return ((Number) converted).longValue();
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public PendingMessagesSummary xPending(byte[] key, String groupName) {
        Assert.notNull(key, "Key must not be null!");
        Assert.hasText(groupName, "Group name must not be null or empty!");

        try {
            Object result = connection.execute("XPENDING", key, groupName);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return parsePendingMessagesSummary(converted);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public PendingMessages xPending(byte[] key, String groupName, XPendingOptions options) {
        Assert.notNull(key, "Key must not be null!");
        Assert.hasText(groupName, "Group name must not be null or empty!");
        Assert.notNull(options, "Options must not be null!");

        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            args.add(groupName);

            if (options.isLimited()) {
                Range<?> range = options.getRange();
                Object lowerBound = range.getLowerBound().getValue().map(Object.class::cast).orElse("-");
                Object upperBound = range.getUpperBound().getValue().map(Object.class::cast).orElse("+");
                
                args.add(lowerBound);
                args.add(upperBound);
                args.add(options.getCount());

                if (options.hasConsumer()) {
                    args.add(options.getConsumerName());
                }
            }

            Object result = connection.execute("XPENDING", args.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return parsePendingMessages(converted, groupName);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    // Convenience method for getting pending messages for a specific consumer
    public PendingMessages xPending(byte[] key, String groupName, String consumerName) {
        Assert.notNull(key, "Key must not be null!");
        Assert.hasText(groupName, "Group name must not be null or empty!");
        Assert.hasText(consumerName, "Consumer name must not be null or empty!");

        ValkeyStreamCommands.XPendingOptions options = ValkeyStreamCommands.XPendingOptions
                .range(Range.unbounded(), 100L)
                .consumer(consumerName);
        
        return xPending(key, groupName, options);
    }

    @Override
    public List<ByteRecord> xRange(byte[] key, Range<String> range, Limit limit) {
        Assert.notNull(key, "Key must not be null!");
        Assert.notNull(range, "Range must not be null!");
        Assert.notNull(limit, "Limit must not be null!");

        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            
            Object lowerBound = range.getLowerBound().getValue().orElse("-");
            Object upperBound = range.getUpperBound().getValue().orElse("+");
            args.add(lowerBound);
            args.add(upperBound);

            if (limit.isLimited()) {
                args.add("COUNT");
                args.add(limit.getCount());
            }

            Object result = connection.execute("XRANGE", args.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return parseByteRecords(converted, key, false);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public List<ByteRecord> xRead(StreamReadOptions readOptions, StreamOffset<byte[]>... streams) {
        Assert.notNull(readOptions, "Read options must not be null!");
        Assert.notNull(streams, "Streams must not be null!");
        Assert.notEmpty(streams, "Streams must not be empty!");

        try {
            List<Object> args = new ArrayList<>();

            if (readOptions.getCount() != null && readOptions.getCount() > 0) {
                args.add("COUNT");
                args.add(readOptions.getCount());
            }

            if (readOptions.getBlock() != null && readOptions.getBlock() >= 0) {
                args.add("BLOCK");
                args.add(readOptions.getBlock());
            }

            args.add("STREAMS");

            for (StreamOffset<byte[]> stream : streams) {
                args.add(stream.getKey());
            }

            for (StreamOffset<byte[]> stream : streams) {
                args.add(stream.getOffset().getOffset());
            }

            Object result = connection.execute("XREAD", args.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return parseMultiStreamByteRecords(converted);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public List<ByteRecord> xReadGroup(Consumer consumer, StreamReadOptions readOptions, StreamOffset<byte[]>... streams) {
        Assert.notNull(consumer, "Consumer must not be null!");
        Assert.notNull(readOptions, "Read options must not be null!");
        Assert.notNull(streams, "Streams must not be null!");
        Assert.notEmpty(streams, "Streams must not be empty!");

        try {
            List<Object> args = new ArrayList<>();
            args.add("GROUP");
            args.add(consumer.getGroup());
            args.add(consumer.getName());

            if (readOptions.getCount() != null && readOptions.getCount() > 0) {
                args.add("COUNT");
                args.add(readOptions.getCount());
            }

            if (readOptions.getBlock() != null && readOptions.getBlock() >= 0) {
                args.add("BLOCK");
                args.add(readOptions.getBlock());
            }

            if (readOptions.isNoack()) {
                args.add("NOACK");
            }

            args.add("STREAMS");

            for (StreamOffset<byte[]> stream : streams) {
                args.add(stream.getKey());
            }

            for (StreamOffset<byte[]> stream : streams) {
                args.add(stream.getOffset().getOffset());
            }

            Object result = connection.execute("XREADGROUP", args.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return parseMultiStreamByteRecords(converted);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public List<ByteRecord> xRevRange(byte[] key, Range<String> range, Limit limit) {
        Assert.notNull(key, "Key must not be null!");
        Assert.notNull(range, "Range must not be null!");
        Assert.notNull(limit, "Limit must not be null!");

        try {
            List<Object> args = new ArrayList<>();
            args.add(key);
            
            Object upperBound = range.getUpperBound().getValue().orElse("+");
            Object lowerBound = range.getLowerBound().getValue().orElse("-");
            args.add(upperBound);
            args.add(lowerBound);

            if (limit.isLimited()) {
                args.add("COUNT");
                args.add(limit.getCount());
            }

            Object result = connection.execute("XREVRANGE", args.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return parseByteRecords(converted, key, true);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public Long xTrim(byte[] key, long count) {
        return xTrim(key, count, false);
    }

    @Override
    public Long xTrim(byte[] key, long count, boolean approximateTrimming) {
        Assert.notNull(key, "Key must not be null!");

        try {
            List<Object> args = new ArrayList<>(4);
            args.add(key);
            args.add("MAXLEN");
            
            if (approximateTrimming) {
                args.add("~");
            }
            
            args.add(String.valueOf(count));

            Object result = connection.execute("XTRIM", args.toArray());
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return ((Number) converted).longValue();
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    // ==================== Helper Methods ====================

    private String convertResultToString(Object result) {
        if (result instanceof byte[]) {
            return ValkeyGlideConverters.toString((byte[]) result);
        } else if (result instanceof String) {
            return (String) result;
        } else if (result == null) {
            return null;
        } else {
            return result.toString();
        }
    }

    private List<Object> buildXClaimArgs(byte[] key, String group, String newOwner, XClaimOptions options) {
        List<Object> args = new ArrayList<>();
        args.add(key);
        args.add(group);
        args.add(newOwner);
        args.add(options.getMinIdleTime().toMillis());
        
        if (options.getIds() != null) {
            for (RecordId recordId : options.getIds()) {
                args.add(recordId.getValue());
            }
        }
        
        if (options.getIdleTime() != null) {
            args.add("IDLE");
            args.add(options.getIdleTime().toMillis());
        }
        
        if (options.getUnixTime() != null) {
            args.add("TIME");
            args.add(options.getUnixTime().toEpochMilli());
        }
        
        if (options.getRetryCount() != null) {
            args.add("RETRYCOUNT");
            args.add(options.getRetryCount());
        }
        
        if (options.isForce()) {
            args.add("FORCE");
        }
        
        return args;
    }

    private List<RecordId> parseRecordIds(Object result) {
        if (result == null) {
            return new ArrayList<>();
        }

        List<Object> list = convertToList(result);
        List<RecordId> recordIds = new ArrayList<>(list.size());

        for (Object item : list) {
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(item);
            String recordIdString = convertResultToString(converted);
            recordIds.add(RecordId.of(recordIdString));
        }

        return recordIds;
    }

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
            Map<?, ?> map = (Map<?, ?>) obj;
            if (map.isEmpty()) {
                return new ArrayList<>();
            } else {
                List<Object> list = new ArrayList<>(map.size() * 2);
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    list.add(entry.getKey());
                    list.add(entry.getValue());
                }
                return list;
            }
        } else if (obj == null) {
            return new ArrayList<>();
        } else {
            throw new IllegalArgumentException("Cannot convert " + obj.getClass() + " to List");
        }
    }

    private Map<String, Object> convertToMap(Object obj) {
        if (obj instanceof Map) {
            return (Map<String, Object>) obj;
        } else if (obj instanceof List) {
            List<Object> list = (List<Object>) obj;
            Map<String, Object> map = new HashMap<>();
            for (int i = 0; i < list.size(); i += 2) {
                if (i + 1 < list.size()) {
                    String key = convertResultToString(ValkeyGlideConverters.defaultFromGlideResult(list.get(i)));
                    Object value = ValkeyGlideConverters.defaultFromGlideResult(list.get(i + 1));
                    map.put(key, value);
                }
            }
            return map;
        } else {
            throw new IllegalArgumentException("Cannot convert " + obj.getClass() + " to Map");
        }
    }

    private List<ByteRecord> parseByteRecords(Object result, byte[] streamKey, boolean reverseOrder) {
        if (result == null) {
            return new ArrayList<>();
        }

        // Handle HashMap format from Valkey-Glide XRANGE response
        if (result instanceof Map) {
            Map<?, ?> recordsMap = (Map<?, ?>) result;
            List<ByteRecord> byteRecords = new ArrayList<>(recordsMap.size());
            
            // Sort entries by record ID timestamp to maintain correct order
            List<Map.Entry<?, ?>> sortedEntries = new ArrayList<>(recordsMap.entrySet());
            sortedEntries.sort((e1, e2) -> {
                String id1 = ValkeyGlideConverters.toString((byte[]) e1.getKey());
                String id2 = ValkeyGlideConverters.toString((byte[]) e2.getKey());
                
                // Extract timestamp part (before the dash)
                long timestamp1 = Long.parseLong(id1.split("-")[0]);
                long timestamp2 = Long.parseLong(id2.split("-")[0]);
                
                int comparison = Long.compare(timestamp1, timestamp2);
                return reverseOrder ? -comparison : comparison;
            });
            
            for (Map.Entry<?, ?> entry : sortedEntries) {
                byte[] recordIdBytes = (byte[]) entry.getKey();
                String recordIdString = ValkeyGlideConverters.toString(recordIdBytes);
                
                Object fieldsListObj = entry.getValue();
                Map<byte[], byte[]> fields = new HashMap<>();
                
                if (fieldsListObj instanceof List) {
                    List<Object> fieldPairs = (List<Object>) fieldsListObj;
                    
                    for (int i = 0; i < fieldPairs.size(); i++) {
                        Object fieldPair = fieldPairs.get(i);
                        
                        if (fieldPair instanceof List) {
                            List<Object> pairList = (List<Object>) fieldPair;
                            if (pairList.size() >= 2) {
                                Object keyObj = ValkeyGlideConverters.defaultFromGlideResult(pairList.get(0));
                                Object valueObj = ValkeyGlideConverters.defaultFromGlideResult(pairList.get(1));
                                
                                byte[] key = keyObj instanceof byte[] ? (byte[]) keyObj : keyObj.toString().getBytes();
                                byte[] value = valueObj instanceof byte[] ? (byte[]) valueObj : valueObj.toString().getBytes();
                                
                                fields.put(key, value);
                            }
                        }
                    }
                }
                
                ByteRecord byteRecord = StreamRecords.newRecord()
                        .in(streamKey)
                        .withId(RecordId.of(recordIdString))
                        .ofBytes(fields);
                byteRecords.add(byteRecord);
            }
            
            return byteRecords;
        }

        // Fallback to original list-based parsing for other formats
        List<Object> records = convertToList(result);
        List<ByteRecord> byteRecords = new ArrayList<>(records.size());

        for (Object record : records) {
            Object convertedRecord = ValkeyGlideConverters.defaultFromGlideResult(record);
            
            if (convertedRecord instanceof List) {
                List<Object> recordData = (List<Object>) convertedRecord;
                
                if (recordData.size() >= 2) {
                    Object rawRecordId = recordData.get(0);
                    Object convertedRecordId = ValkeyGlideConverters.defaultFromGlideResult(rawRecordId);
                    String recordIdString = convertResultToString(convertedRecordId);
                    
                    Object fieldsObj = recordData.get(1);
                    Map<byte[], byte[]> fields = parseFields(fieldsObj);
                    
                    ByteRecord byteRecord = StreamRecords.newRecord()
                            .in(streamKey)
                            .withId(RecordId.of(recordIdString))
                            .ofBytes(fields);
                    byteRecords.add(byteRecord);
                }
            }
        }
        
        return byteRecords;
    }

    private List<ByteRecord> parseMultiStreamByteRecords(Object result) {
        if (result == null) {
            return new ArrayList<>();
        }

        List<ByteRecord> allRecords = new ArrayList<>();

        // Handle both List and Map formats from Valkey-Glide XREAD/XREADGROUP responses
        if (result instanceof Map) {
            // When Valkey-Glide returns Map format: {streamKey: recordsData}
            Map<?, ?> streamsMap = (Map<?, ?>) result;
            
            for (Map.Entry<?, ?> streamEntry : streamsMap.entrySet()) {
                Object streamKeyObj = streamEntry.getKey();
                Object streamRecordsObj = streamEntry.getValue();
                
                byte[] streamKey = streamKeyObj instanceof byte[] ? 
                    (byte[]) streamKeyObj : streamKeyObj.toString().getBytes();
                
                List<ByteRecord> streamRecords = parseByteRecords(streamRecordsObj, streamKey, false);
                allRecords.addAll(streamRecords);
            }
        } else if (result instanceof List) {
            List<Object> streamResults = (List<Object>) result;

            for (Object streamResult : streamResults) {
                Object convertedStreamResult = ValkeyGlideConverters.defaultFromGlideResult(streamResult);

                if (convertedStreamResult instanceof List) {
                    List<Object> streamData = (List<Object>) convertedStreamResult;

                    if (streamData.size() >= 2) {
                        Object streamKeyObj = streamData.get(0);
                        Object streamRecordsObj = streamData.get(1);

                        byte[] streamKey = ValkeyGlideConverters.defaultFromGlideResult(streamKeyObj) instanceof byte[] ?
                                (byte[]) ValkeyGlideConverters.defaultFromGlideResult(streamKeyObj) :
                                streamKeyObj.toString().getBytes();

                        List<ByteRecord> streamRecords = parseByteRecords(streamRecordsObj, streamKey, false);
                        allRecords.addAll(streamRecords);
                    }
                } else if (convertedStreamResult instanceof Map) {
                    // Handle nested Map format within List
                    Map<?, ?> streamMap = (Map<?, ?>) convertedStreamResult;
                    for (Map.Entry<?, ?> entry : streamMap.entrySet()) {
                        byte[] streamKey = entry.getKey() instanceof byte[] ? 
                            (byte[]) entry.getKey() : entry.getKey().toString().getBytes();
                        List<ByteRecord> streamRecords = parseByteRecords(entry.getValue(), streamKey, false);
                        allRecords.addAll(streamRecords);
                    }
                }
            }
        }

        return allRecords;
    }

    private Map<byte[], byte[]> parseFields(Object fieldsObj) {
        Map<byte[], byte[]> fields = new HashMap<>();
        
        if (fieldsObj instanceof List) {
            List<Object> fieldList = (List<Object>) fieldsObj;
            
            for (int i = 0; i < fieldList.size(); i += 2) {
                if (i + 1 < fieldList.size()) {
                    Object keyObj = ValkeyGlideConverters.defaultFromGlideResult(fieldList.get(i));
                    Object valueObj = ValkeyGlideConverters.defaultFromGlideResult(fieldList.get(i + 1));
                    
                    byte[] key = keyObj instanceof byte[] ? (byte[]) keyObj : 
                        (keyObj != null ? keyObj.toString().getBytes() : new byte[0]);
                    byte[] value = valueObj instanceof byte[] ? (byte[]) valueObj : 
                        (valueObj != null ? valueObj.toString().getBytes() : new byte[0]);
                    
                    fields.put(key, value);
                }
            }
        }
        
        return fields;
    }

    private XInfoStream parseXInfoStream(Object result) {
        try {
            List<Object> list = convertToList(result);
            return XInfoStream.fromList(list);
        } catch (Exception e) {
            throw new ValkeyGlideExceptionConverter().convert(e);
        }
    }

    private XInfoGroups parseXInfoGroups(Object result) {
        try {
            // XInfoGroups.fromList expects a list where each group is represented as a separate List
            // Format: [group1List, group2List, ...] where each groupList is [key1, value1, key2, value2, ...]
            List<Object> groupsList = new ArrayList<>();
            
            if (result instanceof List) {
                List<Object> resultList = (List<Object>) result;
                
                for (Object groupInfo : resultList) {
                    Object convertedGroup = ValkeyGlideConverters.defaultFromGlideResult(groupInfo);
                    
                    if (convertedGroup instanceof Map) {
                        // Convert each group Map to a separate list with key-value pairs
                        Map<?, ?> groupMap = (Map<?, ?>) convertedGroup;
                        List<Object> singleGroupList = new ArrayList<>();
                        
                        for (Map.Entry<?, ?> entry : groupMap.entrySet()) {
                            Object key = ValkeyGlideConverters.defaultFromGlideResult(entry.getKey());
                            Object value = ValkeyGlideConverters.defaultFromGlideResult(entry.getValue());
                            
                            String keyStr = convertResultToString(key);
                            singleGroupList.add(keyStr);
                            
                            // Keep numeric values as numbers, convert others to strings
                            if (("consumers".equals(keyStr) || "pending".equals(keyStr)) && value instanceof Number) {
                                singleGroupList.add(((Number) value).longValue());
                            } else {
                                singleGroupList.add(convertResultToString(value));
                            }
                        }
                        
                        groupsList.add(singleGroupList);
                    } else if (convertedGroup instanceof List) {
                        // Already in list format - preserve numeric values where appropriate
                        List<Object> groupList = (List<Object>) convertedGroup;
                        List<Object> convertedGroupList = new ArrayList<>();
                        
                        for (int i = 0; i < groupList.size(); i += 2) {
                            if (i + 1 < groupList.size()) {
                                Object keyObj = ValkeyGlideConverters.defaultFromGlideResult(groupList.get(i));
                                Object valueObj = ValkeyGlideConverters.defaultFromGlideResult(groupList.get(i + 1));
                                
                                String keyStr = convertResultToString(keyObj);
                                convertedGroupList.add(keyStr);
                                
                                // Keep numeric values as numbers, convert others to strings
                                if (("consumers".equals(keyStr) || "pending".equals(keyStr)) && valueObj instanceof Number) {
                                    convertedGroupList.add(((Number) valueObj).longValue());
                                } else {
                                    convertedGroupList.add(convertResultToString(valueObj));
                                }
                            }
                        }
                        
                        groupsList.add(convertedGroupList);
                    }
                }
            } else if (result instanceof Map) {
                // Single group in map format
                Map<?, ?> resultMap = (Map<?, ?>) result;
                List<Object> singleGroupList = new ArrayList<>();
                
                for (Map.Entry<?, ?> entry : resultMap.entrySet()) {
                    Object key = ValkeyGlideConverters.defaultFromGlideResult(entry.getKey());
                    Object value = ValkeyGlideConverters.defaultFromGlideResult(entry.getValue());
                    
                    String keyStr = convertResultToString(key);
                    singleGroupList.add(keyStr);
                    
                    // Keep numeric values as numbers, convert others to strings
                    if (("consumers".equals(keyStr) || "pending".equals(keyStr)) && value instanceof Number) {
                        singleGroupList.add(((Number) value).longValue());
                    } else {
                        singleGroupList.add(convertResultToString(value));
                    }
                }
                
                groupsList.add(singleGroupList);
            }
            
            return XInfoGroups.fromList(groupsList);
        } catch (Exception e) {
            throw new ValkeyGlideExceptionConverter().convert(e);
        }
    }

    private XInfoConsumers parseXInfoConsumers(Object result) {
        try {
            // XInfoConsumers.fromList expects a list where each consumer is represented as a separate List
            // Format: [consumer1List, consumer2List, ...] where each consumerList is [key1, value1, key2, value2, ...]
            List<Object> consumersList = new ArrayList<>();
            
            if (result instanceof List) {
                List<Object> resultList = (List<Object>) result;
                
                for (Object consumerInfo : resultList) {
                    Object convertedConsumer = ValkeyGlideConverters.defaultFromGlideResult(consumerInfo);
                    
                    if (convertedConsumer instanceof Map) {
                        // Convert each consumer Map to a separate list with key-value pairs
                        Map<?, ?> consumerMap = (Map<?, ?>) convertedConsumer;
                        List<Object> singleConsumerList = new ArrayList<>();
                        
                        for (Map.Entry<?, ?> entry : consumerMap.entrySet()) {
                            Object key = ValkeyGlideConverters.defaultFromGlideResult(entry.getKey());
                            Object value = ValkeyGlideConverters.defaultFromGlideResult(entry.getValue());
                            
                            String keyStr = convertResultToString(key);
                            singleConsumerList.add(keyStr);
                            
                            // Keep numeric values as numbers, convert others to strings
                            if (("pending".equals(keyStr) || "idle".equals(keyStr)) && value instanceof Number) {
                                singleConsumerList.add(((Number) value).longValue());
                            } else {
                                singleConsumerList.add(convertResultToString(value));
                            }
                        }
                        
                        consumersList.add(singleConsumerList);
                    } else if (convertedConsumer instanceof List) {
                        // Already in list format - preserve numeric values where appropriate
                        List<Object> consumerList = (List<Object>) convertedConsumer;
                        List<Object> convertedConsumerList = new ArrayList<>();
                        
                        for (int i = 0; i < consumerList.size(); i += 2) {
                            if (i + 1 < consumerList.size()) {
                                Object keyObj = ValkeyGlideConverters.defaultFromGlideResult(consumerList.get(i));
                                Object valueObj = ValkeyGlideConverters.defaultFromGlideResult(consumerList.get(i + 1));
                                
                                String keyStr = convertResultToString(keyObj);
                                convertedConsumerList.add(keyStr);
                                
                                // Keep numeric values as numbers, convert others to strings
                                if (("pending".equals(keyStr) || "idle".equals(keyStr)) && valueObj instanceof Number) {
                                    convertedConsumerList.add(((Number) valueObj).longValue());
                                } else {
                                    convertedConsumerList.add(convertResultToString(valueObj));
                                }
                            }
                        }
                        
                        consumersList.add(convertedConsumerList);
                    }
                }
            } else if (result instanceof Map) {
                // Single consumer in map format
                Map<?, ?> resultMap = (Map<?, ?>) result;
                List<Object> singleConsumerList = new ArrayList<>();
                
                for (Map.Entry<?, ?> entry : resultMap.entrySet()) {
                    Object key = ValkeyGlideConverters.defaultFromGlideResult(entry.getKey());
                    Object value = ValkeyGlideConverters.defaultFromGlideResult(entry.getValue());
                    
                    String keyStr = convertResultToString(key);
                    singleConsumerList.add(keyStr);
                    
                    // Keep numeric values as numbers, convert others to strings
                    if (("pending".equals(keyStr) || "idle".equals(keyStr)) && value instanceof Number) {
                        singleConsumerList.add(((Number) value).longValue());
                    } else {
                        singleConsumerList.add(convertResultToString(value));
                    }
                }
                
                consumersList.add(singleConsumerList);
            }
            
            return XInfoConsumers.fromList("", consumersList);
        } catch (Exception e) {
            throw new ValkeyGlideExceptionConverter().convert(e);
        }
    }

    private PendingMessagesSummary parsePendingMessagesSummary(Object result) {
        if (result == null) {
            return new PendingMessagesSummary("", 0L, Range.closed("", ""), Collections.emptyMap());
        }

        try {
            List<Object> list = convertToList(result);
            
            if (list.size() >= 4) {
                Object totalObj = ValkeyGlideConverters.defaultFromGlideResult(list.get(0));
                Long totalPendingMessages = 0L;
                if (totalObj instanceof Number) {
                    totalPendingMessages = ((Number) totalObj).longValue();
                } else if (totalObj instanceof List) {
                    // Handle case where result is nested - extract the actual number
                    List<?> nested = (List<?>) totalObj;
                    if (!nested.isEmpty() && nested.get(0) instanceof Number) {
                        totalPendingMessages = ((Number) nested.get(0)).longValue();
                    }
                }
                
                String lowestId = convertResultToString(ValkeyGlideConverters.defaultFromGlideResult(list.get(1)));
                String highestId = convertResultToString(ValkeyGlideConverters.defaultFromGlideResult(list.get(2)));
                Range<String> range = Range.closed(lowestId, highestId);
                
                Map<String, Long> consumerMessageCount = new HashMap<>();
                Object consumersObj = list.get(3);
                
                if (consumersObj instanceof List) {
                    List<Object> consumers = (List<Object>) consumersObj;
                    
                    // Check if consumers list contains pairs or nested lists
                    if (!consumers.isEmpty() && consumers.get(0) instanceof List) {
                        // Handle case where each consumer is a separate list: [[consumerName, count], [consumerName, count], ...]
                        for (Object consumerPair : consumers) {
                            if (consumerPair instanceof List) {
                                List<Object> pair = (List<Object>) consumerPair;
                                if (pair.size() >= 2) {
                                    Object consumerNameObj = ValkeyGlideConverters.defaultFromGlideResult(pair.get(0));
                                    Object countObj = ValkeyGlideConverters.defaultFromGlideResult(pair.get(1));
                                    
                                    String consumerName = convertResultToString(consumerNameObj);
                                    
                                    Long messageCount = 0L;
                                    if (countObj instanceof Number) {
                                        messageCount = ((Number) countObj).longValue();
                                    } else if (countObj instanceof byte[]) {
                                        String countStr = new String((byte[]) countObj);
                                        messageCount = Long.parseLong(countStr);
                                    } else if (countObj != null) {
                                        String countStr = countObj.toString();
                                        messageCount = Long.parseLong(countStr);
                                    }
                                    
                                    consumerMessageCount.put(consumerName, messageCount);
                                }
                            }
                        }
                    } else {
                        // Handle flat list format: [consumerName1, count1, consumerName2, count2, ...]
                        for (int i = 0; i < consumers.size(); i += 2) {
                            if (i + 1 < consumers.size()) {
                                Object consumerNameObj = ValkeyGlideConverters.defaultFromGlideResult(consumers.get(i));
                                Object countObj = ValkeyGlideConverters.defaultFromGlideResult(consumers.get(i + 1));
                                
                                // Enhanced byte array to string conversion with nested array handling
                                String consumerName;
                                if (consumerNameObj instanceof byte[]) {
                                    consumerName = new String((byte[]) consumerNameObj);
                                } else if (consumerNameObj instanceof byte[][]) {
                                    // Handle byte[][] case - take first element
                                    byte[][] nestedArrays = (byte[][]) consumerNameObj;
                                    if (nestedArrays.length > 0 && nestedArrays[0] != null) {
                                        consumerName = new String(nestedArrays[0]);
                                    } else {
                                        consumerName = convertResultToString(consumerNameObj);
                                    }
                                } else if (consumerNameObj instanceof List) {
                                    // Handle nested list case - extract the byte[] and convert
                                    List<?> nestedList = (List<?>) consumerNameObj;
                                    if (!nestedList.isEmpty()) {
                                        Object firstElement = nestedList.get(0);
                                        if (firstElement instanceof byte[]) {
                                            consumerName = new String((byte[]) firstElement);
                                        } else if (firstElement instanceof byte[][]) {
                                            byte[][] nestedArrays = (byte[][]) firstElement;
                                            if (nestedArrays.length > 0 && nestedArrays[0] != null) {
                                                consumerName = new String(nestedArrays[0]);
                                            } else {
                                                consumerName = convertResultToString(consumerNameObj);
                                            }
                                        } else {
                                            consumerName = convertResultToString(firstElement);
                                        }
                                    } else {
                                        consumerName = convertResultToString(consumerNameObj);
                                    }
                                } else {
                                    consumerName = convertResultToString(consumerNameObj);
                                }
                                
                                Long messageCount = 0L;
                                if (countObj instanceof Number) {
                                    messageCount = ((Number) countObj).longValue();
                                }
                                consumerMessageCount.put(consumerName, messageCount);
                            }
                        }
                    }
                }
                
                return new PendingMessagesSummary("", totalPendingMessages, range, consumerMessageCount);
            }
            
            return new PendingMessagesSummary("", 0L, Range.closed("", ""), Collections.emptyMap());
        } catch (Exception e) {
            throw new ValkeyGlideExceptionConverter().convert(e);
        }
    }

    private PendingMessages parsePendingMessages(Object result, String groupName) {
        if (result == null) {
            return new PendingMessages(groupName, Collections.emptyList());
        }

        try {
            List<Object> list = convertToList(result);
            List<PendingMessage> pendingMessages = new ArrayList<>();

            for (Object item : list) {
                Object convertedItem = ValkeyGlideConverters.defaultFromGlideResult(item);
                if (convertedItem instanceof List) {
                    List<Object> messageData = (List<Object>) convertedItem;
                    if (messageData.size() >= 4) {
                        String recordId = convertResultToString(ValkeyGlideConverters.defaultFromGlideResult(messageData.get(0)));
                        String consumerName = convertResultToString(ValkeyGlideConverters.defaultFromGlideResult(messageData.get(1)));
                        Long idleTime = ((Number) ValkeyGlideConverters.defaultFromGlideResult(messageData.get(2))).longValue();
                        Long deliveryCount = ((Number) ValkeyGlideConverters.defaultFromGlideResult(messageData.get(3))).longValue();
                        
                        PendingMessage pendingMessage = new PendingMessage(
                            RecordId.of(recordId), 
                            Consumer.from(groupName, consumerName), 
                            Duration.ofMillis(idleTime), 
                            deliveryCount
                        );
                        pendingMessages.add(pendingMessage);
                    }
                }
            }

            return new PendingMessages(groupName, pendingMessages);
        } catch (Exception e) {
            throw new ValkeyGlideExceptionConverter().convert(e);
        }
    }
}

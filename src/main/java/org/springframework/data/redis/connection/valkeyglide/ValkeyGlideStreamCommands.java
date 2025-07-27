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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.util.Assert;

/**
 * Implementation of {@link RedisStreamCommands} for Valkey-Glide.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideStreamCommands implements RedisStreamCommands {

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

        Object[] params = new Object[recordIds.length + 2];
        params[0] = key;
        params[1] = group;

        for (int i = 0; i < recordIds.length; i++) {
            params[i + 2] = recordIds[i].getValue();
        }

        return (Long) connection.execute("XACK", params);
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

        List<Object> params = new ArrayList<>(2 + body.size() * 2 + 4);
        params.add(key);

        if (options.hasMaxlen()) {
            if (options.isApproximateTrimming()) {
                params.add("MAXLEN");
                params.add("~");
            } else {
                params.add("MAXLEN");
            }
            params.add(String.valueOf(options.getMaxlen()));
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

        byte[] responseBytes = (byte[]) connection.execute("XADD", params.toArray());
        String response = new String(responseBytes);
        return RecordId.of(response);
    }

    @Override
    public Long xDel(byte[] key, RecordId... recordIds) {
        Assert.notNull(key, "Key must not be null!");
        Assert.notNull(recordIds, "Record IDs must not be null!");
        
        if (recordIds.length == 0) {
            return 0L;
        }
        
        Object[] params = new Object[recordIds.length + 1];
        params[0] = key;
        
        for (int i = 0; i < recordIds.length; i++) {
            params[i + 1] = recordIds[i].getValue();
        }
        
        return (Long) connection.execute("XDEL", params);
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

        List<Object> args = new ArrayList<>(6);
        args.add("CREATE");
        args.add(key);
        args.add(groupName);
        args.add(readOffset.getOffset());
        
        if (makeStream) {
            args.add("MKSTREAM");
        }

        return new String((byte[]) connection.execute("XGROUP", args.toArray()));
    }
    
    @Override
    public Boolean xGroupDelConsumer(byte[] key, Consumer consumer) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Boolean xGroupDestroy(byte[] key, String groupName) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public StreamInfo.XInfoStream xInfo(byte[] key) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public StreamInfo.XInfoGroups xInfoGroups(byte[] key) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public StreamInfo.XInfoConsumers xInfoConsumers(byte[] key, String groupName) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Long xLen(byte[] key) {
        Assert.notNull(key, "Key must not be null!");
        
        return (Long) connection.execute("XLEN", key);
    }

    @Override
    public PendingMessagesSummary xPending(byte[] key, String groupName) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public PendingMessages xPending(byte[] key, String groupName, XPendingOptions options) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public List<ByteRecord> xRange(byte[] key, Range<String> range, Limit limit) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public List<ByteRecord> xRead(StreamReadOptions readOptions, StreamOffset<byte[]>... streams) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public List<ByteRecord> xReadGroup(Consumer consumer, StreamReadOptions readOptions, StreamOffset<byte[]>... streams) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public List<ByteRecord> xRevRange(byte[] key, Range<String> range, Limit limit) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Long xTrim(byte[] key, long count) {
        return xTrim(key, count, false);
    }

    @Override
    public Long xTrim(byte[] key, long count, boolean approximateTrimming) {
        Assert.notNull(key, "Key must not be null!");

        List<Object> args = new ArrayList<>(4);
        args.add(key);
        
        if (approximateTrimming) {
            args.add("MAXLEN");
            args.add("~");
        } else {
            args.add("MAXLEN");
        }
        
        args.add(String.valueOf(count));

        return (Long) connection.execute("XTRIM", args.toArray());
    }

    public Long xClaim(byte[] key, String group, String newOwner, long minIdleTime, XClaimOptions options, RecordId... recordIds) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public List<ByteRecord> xClaim(byte[] key, String group, String newOwner, XClaimOptions options) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
    
    @Override
    public List<RecordId> xClaimJustId(byte[] key, String group, String newOwner, XClaimOptions options) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}

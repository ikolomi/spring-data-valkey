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

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisHyperLogLogCommands;
import org.springframework.util.Assert;

/**
 * Implementation of {@link RedisHyperLogLogCommands} for Valkey-Glide.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideHyperLogLogCommands implements RedisHyperLogLogCommands {

    private final ValkeyGlideConnection connection;

    /**
     * Creates a new {@link ValkeyGlideHyperLogLogCommands}.
     *
     * @param connection must not be {@literal null}.
     */
    public ValkeyGlideHyperLogLogCommands(ValkeyGlideConnection connection) {
        Assert.notNull(connection, "Connection must not be null!");
        this.connection = connection;
    }

    @Override
    public Long pfAdd(byte[] key, byte[]... values) {
        Assert.notNull(key, "Key must not be null!");
        Assert.notNull(values, "Values must not be null!");
        Assert.noNullElements(values, "Values must not contain null elements!");

        if (values.length == 0) {
            return 0L;
        }

        Object[] args = new Object[1 + values.length];
        args[0] = key;
        System.arraycopy(values, 0, args, 1, values.length);

        Object result = connection.execute("PFADD", args);
        Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
        
        if (converted instanceof Boolean) {
            return ((Boolean) converted) ? 1L : 0L;
        } else if (converted instanceof Number) {
            return ((Number) converted).longValue();
        } else {
            return 0L;
        }
    }

    @Override
    public Long pfCount(byte[]... keys) {
        Assert.notNull(keys, "Keys must not be null!");
        Assert.noNullElements(keys, "Keys must not contain null elements!");

        if (keys.length == 0) {
            throw new IllegalArgumentException("At least one key is required for PFCOUNT");
        }

        Object result = connection.execute("PFCOUNT", keys);
        Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
        
        if (converted instanceof Boolean) {
            return ((Boolean) converted) ? 1L : 0L;
        } else if (converted instanceof Number) {
            return ((Number) converted).longValue();
        } else {
            return 0L;
        }
    }

    @Override
    public void pfMerge(byte[] destinationKey, byte[]... sourceKeys) {
        Assert.notNull(destinationKey, "Destination key must not be null!");
        Assert.notNull(sourceKeys, "Source keys must not be null!");
        Assert.noNullElements(sourceKeys, "Source keys must not contain null elements!");

        if (sourceKeys.length == 0) {
            throw new IllegalArgumentException("At least one source key is required for PFMERGE");
        }

        Object[] args = new Object[1 + sourceKeys.length];
        args[0] = destinationKey;
        System.arraycopy(sourceKeys, 0, args, 1, sourceKeys.length);

        Object result = connection.execute("PFMERGE", args);
        // PFMERGE typically returns OK or null, we don't need to return anything
        ValkeyGlideConverters.defaultFromGlideResult(result);
    }
}

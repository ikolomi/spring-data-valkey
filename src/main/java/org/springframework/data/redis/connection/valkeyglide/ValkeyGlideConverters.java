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

import org.springframework.lang.Nullable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Collection;

import glide.api.models.GlideString;

/**
 * Converter utilities for mapping between Spring Data Redis types and Valkey-Glide types.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public abstract class ValkeyGlideConverters {

    /**
     * Convert a Spring Data Redis command argument to the corresponding Valkey-Glide format.
     *
     * @param arg The command argument to convert
     * @return The converted argument
     */
    public static Object toGlideArgument(Object arg) {
        if (arg == null) {
            return null;
        }
        
        if (arg instanceof byte[]) {
            // Convert byte array to ByteBuffer as Glide might expect ByteBuffer
            return ByteBuffer.wrap((byte[]) arg);
        } else if (arg instanceof Map) {
            // Handle Map conversions if needed
            return arg;
        } else if (arg instanceof Collection) {
            // Handle Collection conversions if needed
            List<Object> result = new ArrayList<>();
            for (Object item : (Collection<?>) arg) {
                result.add(toGlideArgument(item));
            }
            return result;
        } else if (arg instanceof Number || arg instanceof Boolean || arg instanceof String) {
            // Simple types can be passed as-is
            return arg;
        }
        
        // Default: return as is
        return arg;
    }

    /**
     * Convert a Valkey-Glide result to the Spring Data Redis format.
     *
     * @param result The Glide result to convert
     * @return The converted result
     */
    @Nullable
    public static Object fromGlideResult(@Nullable Object result) {
        if (result == null) {
            return null;
        }
        
        if (result instanceof GlideString) {
            // Convert GlideString back to byte[]
            return ((GlideString) result).getBytes();
        } else if (result instanceof ByteBuffer) {
            // Convert ByteBuffer back to byte[]
            ByteBuffer buffer = (ByteBuffer) result;
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        } else if (result instanceof List) {
            // Convert list elements
            List<?> list = (List<?>) result;
            List<Object> converted = new ArrayList<>(list.size());
            for (Object item : list) {
                converted.add(fromGlideResult(item));
            }
            return converted;
        } else if (result instanceof Set) {
            // Convert set elements
            Set<?> set = (Set<?>) result;
            List<Object> converted = new ArrayList<>(set.size());
            for (Object item : set) {
                converted.add(fromGlideResult(item));
            }
            return set;
        } else if (result instanceof Map) {
            // Convert map entries
            return result;
        } else if (result instanceof Number || result instanceof Boolean || result instanceof String) {
            // Simple types can be passed as-is
            return result;
        }
        
        // Default: return as is
        return result;
    }
    
    /**
     * Convert a byte array to a string using the UTF-8 charset.
     *
     * @param bytes The bytes to convert
     * @return The resulting string or null if input is null
     */
    @Nullable
    public static String toString(@Nullable byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Convert a string to a byte array using the UTF-8 charset.
     *
     * @param string The string to convert
     * @return The resulting byte array or null if input is null
     */
    @Nullable
    public static byte[] toBytes(@Nullable String string) {
        return string == null ? null : string.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Convert a Redis command result to a Boolean value.
     * Redis SET command returns "OK" on success, null on failure with conditions.
     *
     * @param result The command result
     * @return Boolean representation of the result
     */
    @Nullable
    public static Boolean stringToBoolean(@Nullable Object result) {
        if (result == null) {
            return false;
        }
        if (result instanceof String) {
            return "OK".equals(result);
        }
        if (result instanceof Boolean) {
            return (Boolean) result;
        }
        if (result instanceof Number) {
            return ((Number) result).longValue() != 0;
        }
        return result != null;
    }
}

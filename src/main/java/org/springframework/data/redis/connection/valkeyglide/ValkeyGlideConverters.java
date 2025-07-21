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
import java.util.HashSet;
import java.util.Collection;

import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.ValueEncoding;

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
            //return ((GlideString) result).toString();
        } else if (result instanceof ByteBuffer) {
            // Convert ByteBuffer back to byte[]
            ByteBuffer buffer = (ByteBuffer) result;
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        } else if (result instanceof List) {
            // Convert list elements recursively
            List<?> list = (List<?>) result;
            List<Object> converted = new ArrayList<>(list.size());
            for (Object item : list) {
                converted.add(fromGlideResult(item));
            }
            return converted;
        } else if (result instanceof Set) {
            // Convert set elements recursively, but return as Set
            Set<?> set = (Set<?>) result;
            Set<Object> converted = new HashSet<>(set.size());
            for (Object item : set) {
                converted.add(fromGlideResult(item));
            }
            return converted;
        } else if (result instanceof Object[]) {
            // Convert array elements recursively
            Object[] array = (Object[]) result;
            List<Object> converted = new ArrayList<>(array.length);
            for (Object item : array) {
                converted.add(fromGlideResult(item));
            }
            return converted;
        } else if (result instanceof Map) {
            // Convert map entries recursively
            Map<?, ?> map = (Map<?, ?>) result;
            Map<Object, Object> converted = new java.util.HashMap<>(map.size());
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                converted.put(fromGlideResult(entry.getKey()), fromGlideResult(entry.getValue()));
            }
            return converted;
        } else if (result instanceof Number || result instanceof Boolean || result instanceof String) {
            // Simple types can be passed as-is
            return result;
        } else if (result instanceof byte[]) {
            // byte[] should be passed as-is (already in Spring format)
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

    /**
     * Convert Redis TYPE command result to DataType enum.
     *
     * @param result The TYPE command result (GlideString is converted to byte[])
     * @return The corresponding DataType
     */
    public static DataType toDataType(@Nullable byte[] result) {
        if (result == null) {
            return DataType.NONE;
        }
        

        // Convert byte[] to String using UTF-8 encoding
        String resultStr = new String(result, StandardCharsets.UTF_8);
        switch (resultStr.toLowerCase()) {
            case "string":
                return DataType.STRING;
            case "list":
                return DataType.LIST;
            case "set":
                return DataType.SET;
            case "zset":
                return DataType.ZSET;
            case "hash":
                return DataType.HASH;
            case "stream":
                return DataType.STREAM;
            case "none":
            default:
                return DataType.NONE;
        }
    }

    /**
     * Convert result to Set<byte[]>.
     *
     * @param result The command result
     * @return Set of byte arrays
     */
    @Nullable
    public static Set<byte[]> toBytesSet(@Nullable Object result) {
        if (result == null) {
            return new HashSet<>();
        }
        
        if (result instanceof Collection) {
            Set<byte[]> set = new HashSet<>();
            for (Object item : (Collection<?>) result) {
                if (item instanceof byte[]) {
                    set.add((byte[]) item);
                } else if (item instanceof String) {
                    set.add(((String) item).getBytes(StandardCharsets.UTF_8));
                } else if (item != null) {
                    set.add(item.toString().getBytes(StandardCharsets.UTF_8));
                }
            }
            return set;
        }
        
        // Handle array types (Object[] and specific array types)
        if (result instanceof Object[]) {
            Set<byte[]> set = new HashSet<>();
            for (Object item : (Object[]) result) {
                if (item instanceof byte[]) {
                    set.add((byte[]) item);
                } else if (item instanceof String) {
                    set.add(((String) item).getBytes(StandardCharsets.UTF_8));
                } else if (item != null) {
                    set.add(item.toString().getBytes(StandardCharsets.UTF_8));
                }
            }
            return set;
        }
        
        // Handle byte[][] specifically
        if (result instanceof byte[][]) {
            Set<byte[]> set = new HashSet<>();
            for (byte[] item : (byte[][]) result) {
                if (item != null) {
                    set.add(item);
                }
            }
            return set;
        }
        
        // Handle String[] specifically
        if (result instanceof String[]) {
            Set<byte[]> set = new HashSet<>();
            for (String item : (String[]) result) {
                if (item != null) {
                    set.add(item.getBytes(StandardCharsets.UTF_8));
                }
            }
            return set;
        }
        
        return new HashSet<>();
    }

    /**
     * Convert result to List<byte[]>.
     *
     * @param result The command result
     * @return List of byte arrays
     */
    @Nullable
    public static List<byte[]> toBytesList(@Nullable Object result) {
        if (result == null) {
            return new ArrayList<>();
        }
        
        if (result instanceof Collection) {
            List<byte[]> list = new ArrayList<>();
            for (Object item : (Collection<?>) result) {
                if (item instanceof byte[]) {
                    list.add((byte[]) item);
                } else if (item instanceof String) {
                    list.add(((String) item).getBytes(StandardCharsets.UTF_8));
                } else if (item != null) {
                    list.add(item.toString().getBytes(StandardCharsets.UTF_8));
                }
            }
            return list;
        }
        
        // Handle array types (Object[] and specific array types)
        if (result instanceof Object[]) {
            List<byte[]> list = new ArrayList<>();
            for (Object item : (Object[]) result) {
                if (item instanceof byte[]) {
                    list.add((byte[]) item);
                } else if (item instanceof String) {
                    list.add(((String) item).getBytes(StandardCharsets.UTF_8));
                } else if (item != null) {
                    list.add(item.toString().getBytes(StandardCharsets.UTF_8));
                }
            }
            return list;
        }
        
        // Handle byte[][] specifically
        if (result instanceof byte[][]) {
            List<byte[]> list = new ArrayList<>();
            for (byte[] item : (byte[][]) result) {
                if (item != null) {
                    list.add(item);
                }
            }
            return list;
        }
        
        // Handle String[] specifically
        if (result instanceof String[]) {
            List<byte[]> list = new ArrayList<>();
            for (String item : (String[]) result) {
                if (item != null) {
                    list.add(item.getBytes(StandardCharsets.UTF_8));
                }
            }
            return list;
        }
        
        return new ArrayList<>();
    }

    /**
     * Append sort parameters to the command arguments.
     *
     * @param args The command arguments list
     * @param params The sort parameters
     */
    public static void appendSortParameters(List<Object> args, SortParameters params) {
        if (params == null) {
            return;
        }
        
        // Add BY pattern if specified
        if (params.getByPattern() != null) {
            args.add("BY");
            args.add(params.getByPattern());
        }
        
        // Add LIMIT if specified
        if (params.getLimit() != null) {
            args.add("LIMIT");
            args.add(params.getLimit().getStart());
            args.add(params.getLimit().getCount());
        }
        
        // Add GET patterns if specified
        if (params.getGetPattern() != null) {
            for (byte[] pattern : params.getGetPattern()) {
                args.add("GET");
                args.add(pattern);
            }
        }
        
        // Add ORDER if specified
        if (params.getOrder() != null) {
            args.add(params.getOrder().name());
        }
        
        // Add ALPHA if specified
        if (params.isAlphabetic()) {
            args.add("ALPHA");
        }
    }

    /**
     * Convert OBJECT ENCODING result to ValueEncoding.
     *
     * @param result The OBJECT ENCODING command result
     * @return The corresponding ValueEncoding
     */
    @Nullable
    public static ValueEncoding toValueEncoding(@Nullable Object result) {
        if (result == null) {
            return ValueEncoding.RedisValueEncoding.VACANT;
        }
        
        if (result instanceof String) {
            String encoding = (String) result;
            switch (encoding.toLowerCase()) {
                case "raw":
                    return ValueEncoding.RedisValueEncoding.RAW;
                case "int":
                    return ValueEncoding.RedisValueEncoding.INT;
                case "hashtable":
                    return ValueEncoding.RedisValueEncoding.HASHTABLE;
                case "zipmap":
                case "linkedlist":
                    return ValueEncoding.RedisValueEncoding.LINKEDLIST;
                case "ziplist":
                case "listpack":
                    return ValueEncoding.RedisValueEncoding.ZIPLIST;
                case "intset":
                    return ValueEncoding.RedisValueEncoding.INTSET;
                case "skiplist":
                    return ValueEncoding.RedisValueEncoding.SKIPLIST;
                case "embstr":
                case "quicklist":
                case "stream":
                    return ValueEncoding.RedisValueEncoding.RAW;
                default:
                    return ValueEncoding.RedisValueEncoding.VACANT;
            }
        }
        
        return ValueEncoding.RedisValueEncoding.VACANT;
    }
}

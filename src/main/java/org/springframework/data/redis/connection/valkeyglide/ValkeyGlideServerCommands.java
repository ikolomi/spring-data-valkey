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
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisServerCommands;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;


/**
 * Implementation of {@link RedisServerCommands} for Valkey-Glide.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideServerCommands implements RedisServerCommands {

    private final ValkeyGlideConnection connection;

    /**
     * Creates a new {@link ValkeyGlideServerCommands}.
     *
     * @param connection must not be {@literal null}.
     */
    public ValkeyGlideServerCommands(ValkeyGlideConnection connection) {
        Assert.notNull(connection, "Connection must not be null!");
        this.connection = connection;
    }

    @Override
    public void bgReWriteAof() {
        try {
            connection.execute("BGREWRITEAOF");
            // Command returns "Background AOF rewrite started" on success
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public void bgSave() {
        try {
            connection.execute("BGSAVE");
            // Command returns "Background saving started" on success
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long lastSave() {
        try {
            Object result = connection.execute("LASTSAVE");
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return converted instanceof Number ? ((Number) converted).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public void save() {
        try {
            connection.execute("SAVE");
            // Command returns "OK" on success
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long dbSize() {
        try {
            Object result = connection.execute("DBSIZE");
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return converted instanceof Number ? ((Number) converted).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public void flushDb() {
        try {
            connection.execute("FLUSHDB");
            // Command returns "OK" on success
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public void flushDb(FlushOption option) {
        Assert.notNull(option, "FlushOption must not be null");
        
        try {
            connection.execute("FLUSHDB", option.name());
            // Command returns "OK" on success
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public void flushAll() {
        try {
            connection.execute("FLUSHALL");
            // Command returns "OK" on success
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public void flushAll(FlushOption option) {
        Assert.notNull(option, "FlushOption must not be null");
        
        try {
            connection.execute("FLUSHALL", option.name());
            // Command returns "OK" on success
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Properties info() {
        try {
            Object result = connection.execute("INFO");
            String infoResponse = convertResultToString(result);
            return parseInfoResponse(infoResponse);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Properties info(String section) {
        Assert.notNull(section, "Section must not be null");
        
        try {
            Object result = connection.execute("INFO", section);
            String infoResponse = convertResultToString(result);
            return parseInfoResponse(infoResponse);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    /**
     * Converts the command result to a String, handling both byte[] and String types.
     * 
     * @param result the result from the command execution
     * @return String representation of the result
     */
    private String convertResultToString(Object result) {
        Object convertedResult = ValkeyGlideConverters.defaultFromGlideResult(result);
        if (convertedResult instanceof byte[]) {
            return ValkeyGlideConverters.toString((byte[]) convertedResult);
        } else if (convertedResult instanceof String) {
            return (String) convertedResult;
        } else if (convertedResult == null) {
            return null;
        } else {
            return convertedResult.toString();
        }
    }

    /**
     * Parses the INFO command response string into Properties.
     * 
     * @param infoResponse the response from the INFO command
     * @return Properties containing the parsed key-value pairs
     */
    private Properties parseInfoResponse(String infoResponse) {
        Properties properties = new Properties();
        
        if (infoResponse == null) {
            return properties;
        }
        
        String[] lines = infoResponse.split("\r?\n");
        
        for (String line : lines) {
            line = line.trim();
            
            // Skip empty lines and comments (lines starting with #)
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            
            // Parse key:value pairs
            int colonIndex = line.indexOf(':');
            if (colonIndex > 0 && colonIndex < line.length() - 1) {
                String key = line.substring(0, colonIndex).trim();
                String value = line.substring(colonIndex + 1).trim();
                properties.setProperty(key, value);
            }
        }
        
        return properties;
    }

    /**
     * Parses the CONFIG GET command response into Properties.
     * 
     * @param result the result from the CONFIG GET command
     * @return Properties containing the parsed key-value pairs
     */
    private Properties parseConfigResponse(Object result) {
        Properties properties = new Properties();
        
        if (result == null) {
            return properties;
        }
        
        // CONFIG GET can return either a Map or a List depending on the implementation
        if (result instanceof java.util.Map) {
            java.util.Map<?, ?> map = (java.util.Map<?, ?>) result;
            for (java.util.Map.Entry<?, ?> entry : map.entrySet()) {
                String key = convertResultToString(ValkeyGlideConverters.defaultFromGlideResult(entry.getKey()));
                String value = convertResultToString(ValkeyGlideConverters.defaultFromGlideResult(entry.getValue()));
                if (key != null && value != null) {
                    properties.setProperty(key, value);
                }
            }
        } else {
            List<Object> list = convertToList(result);
            
            // CONFIG GET returns key-value pairs as a flat list
            for (int i = 0; i < list.size(); i += 2) {
                if (i + 1 < list.size()) {
                    String key = convertResultToString(ValkeyGlideConverters.defaultFromGlideResult(list.get(i)));
                    String value = convertResultToString(ValkeyGlideConverters.defaultFromGlideResult(list.get(i + 1)));
                    if (key != null && value != null) {
                        properties.setProperty(key, value);
                    }
                }
            }
        }
        
        return properties;
    }

    /**
     * Parses the TIME command response into a Long value in the specified TimeUnit.
     * 
     * @param result the result from the TIME command
     * @param timeUnit the desired time unit
     * @return the time in the specified unit
     */
    private Long parseTimeResponse(Object result, TimeUnit timeUnit) {
        if (result == null) {
            return null;
        }
        
        List<Object> list = convertToList(result);
        
        if (list.size() >= 2) {
            // TIME returns [seconds, microseconds]
            Object secondsObj = ValkeyGlideConverters.defaultFromGlideResult(list.get(0));
            Object microsecondsObj = ValkeyGlideConverters.defaultFromGlideResult(list.get(1));
            
            long seconds = parseNumber(secondsObj).longValue();
            long microseconds = parseNumber(microsecondsObj).longValue();
            
            // Convert to milliseconds first
            long milliseconds = seconds * 1000 + microseconds / 1000;
            
            return timeUnit.convert(milliseconds, TimeUnit.MILLISECONDS);
        }
        
        return null;
    }

    /**
     * Converts an object to a List.
     * 
     * @param obj the object to convert
     * @return the converted list
     */
    private List<Object> convertToList(Object obj) {
        if (obj instanceof List) {
            return (List<Object>) obj;
        } else if (obj instanceof Object[]) {
            Object[] array = (Object[]) obj;
            List<Object> list = new java.util.ArrayList<>(array.length);
            for (Object item : array) {
                list.add(item);
            }
            return list;
        } else {
            throw new IllegalArgumentException("Cannot convert " + obj.getClass() + " to List");
        }
    }

    /**
     * Parses a number from an object.
     * 
     * @param obj the object to parse
     * @return the parsed number
     */
    private Number parseNumber(Object obj) {
        if (obj instanceof Number) {
            return (Number) obj;
        } else if (obj instanceof String) {
            return Long.parseLong((String) obj);
        } else if (obj instanceof byte[]) {
            return Long.parseLong(new String((byte[]) obj));
        } else {
            throw new IllegalArgumentException("Cannot parse number from " + obj.getClass());
        }
    }

    @Override
    public void shutdown() {
        try {
            connection.execute("SHUTDOWN");
            // Command does not return a response as server shuts down
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public void shutdown(ShutdownOption option) {
        Assert.notNull(option, "ShutdownOption must not be null");
        
        try {
            connection.execute("SHUTDOWN", option.name());
            // Command does not return a response as server shuts down
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Properties getConfig(String pattern) {
        Assert.notNull(pattern, "Pattern must not be null");
        
        try {
            Object result = connection.execute("CONFIG", "GET", pattern);
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return parseConfigResponse(converted);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public void setConfig(String param, String value) {
        Assert.notNull(param, "Parameter must not be null");
        Assert.notNull(value, "Value must not be null");
        
        try {
            connection.execute("CONFIG", "SET", param, value);
            // Command returns "OK" on success
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public void resetConfigStats() {
        try {
            connection.execute("CONFIG", "RESETSTAT");
            // Command returns "OK" on success
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public void rewriteConfig() {
        try {
            connection.execute("CONFIG", "REWRITE");
            // Command returns "OK" on success
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long time(TimeUnit timeUnit) {
        Assert.notNull(timeUnit, "TimeUnit must not be null");
        
        try {
            Object result = connection.execute("TIME");
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            return parseTimeResponse(converted, timeUnit);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public void killClient(String host, int port) {
        Assert.notNull(host, "Host must not be null");
        
        try {
            connection.execute("CLIENT", "KILL", host + ":" + port);
            // Command returns "OK" on success
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public void setClientName(byte[] name) {
        Assert.notNull(name, "Name must not be null");
        
        try {
            connection.execute("CLIENT", "SETNAME", name);
            // Command returns "OK" on success
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public String getClientName() {
        try {
            Object result = connection.execute("CLIENT", "GETNAME");
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            if (converted == null) {
                return null;
            }
            
            return convertResultToString(converted);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public List<RedisClientInfo> getClientList() {
        try {
            Object result = connection.execute("CLIENT", "LIST");
            Object converted = ValkeyGlideConverters.defaultFromGlideResult(result);
            String clientListResponse = convertResultToString(converted);
            return parseClientListResponse(clientListResponse);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public void replicaOf(String host, int port) {
        Assert.notNull(host, "Host must not be null");
        
        try {
            connection.execute("REPLICAOF", host, String.valueOf(port));
            // Command returns "OK" on success
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public void replicaOfNoOne() {
        try {
            connection.execute("REPLICAOF", "NO", "ONE");
            // Command returns "OK" on success
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public void migrate(byte[] key, RedisNode target, int dbIndex, @Nullable MigrateOption option) {
        migrate(key, target, dbIndex, option, 0);
    }

    @Override
    public void migrate(byte[] key, RedisNode target, int dbIndex, @Nullable MigrateOption option, long timeout) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(target, "Target must not be null");
        
        try {
            List<Object> args = new ArrayList<>();
            args.add("MIGRATE");
            args.add(target.getHost());
            args.add(String.valueOf(target.getPort()));
            args.add(key);
            args.add(String.valueOf(dbIndex));
            args.add(String.valueOf(timeout));
            
            if (option != null) {
                args.add(option.name());
            }
            
            connection.execute("MIGRATE", args.subList(1, args.size()).toArray());
            // Command returns "OK" on success
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    /**
     * Parses the CLIENT LIST command response into a list of RedisClientInfo objects.
     * 
     * @param clientListResponse the response from the CLIENT LIST command
     * @return List of RedisClientInfo objects
     */
    private List<RedisClientInfo> parseClientListResponse(String clientListResponse) {
        List<RedisClientInfo> clientInfos = new ArrayList<>();
        
        if (clientListResponse == null || clientListResponse.isEmpty()) {
            return clientInfos;
        }
        
        String[] lines = clientListResponse.split("\r?\n");
        
        for (String line : lines) {
            line = line.trim();
            
            if (line.isEmpty()) {
                continue;
            }
            
            // Parse client info line into a RedisClientInfo object
            RedisClientInfo clientInfo = parseClientInfoLine(line);
            if (clientInfo != null) {
                clientInfos.add(clientInfo);
            }
        }
        
        return clientInfos;
    }

    /**
     * Parses a single client info line into a RedisClientInfo object.
     * 
     * @param line the client info line
     * @return RedisClientInfo object or null if parsing fails
     */
    private RedisClientInfo parseClientInfoLine(String line) {
        Properties properties = new Properties();
        
        // Parse space-separated key=value pairs
        String[] parts = line.split("\\s+");
        
        for (String part : parts) {
            int equalsIndex = part.indexOf('=');
            if (equalsIndex > 0 && equalsIndex < part.length() - 1) {
                String key = part.substring(0, equalsIndex).trim();
                String value = part.substring(equalsIndex + 1).trim();
                properties.setProperty(key, value);
            }
        }
        
        return new RedisClientInfo(properties);
    }
}

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
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.AbstractRedisConnection;
import org.springframework.data.redis.connection.RedisCommands;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.connection.RedisHyperLogLogCommands;
import org.springframework.data.redis.connection.RedisKeyCommands;
import org.springframework.data.redis.connection.RedisListCommands;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.data.redis.connection.RedisScriptingCommands;
import org.springframework.data.redis.connection.RedisServerCommands;
import org.springframework.data.redis.connection.RedisSetCommands;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.connection.RedisSubscribedConnectionException;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.connection.valkeyglide.ValkeyGlideConverters.ResultMapper;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.fasterxml.jackson.annotation.JsonTypeInfo.As;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

// Imports from valkey-glide library
import glide.api.GlideClient;
import glide.api.models.GlideString;
import glide.api.models.Batch;

/**
 * Connection to a Redis server using Valkey-Glide client. The connection
 * adapts Valkey-Glide's asynchronous API to Spring Data Redis's synchronous API.
 *
 * @author Ilya Kolomin
 */
public class ValkeyGlideConnection extends AbstractRedisConnection {

    // // Commands that return 1/0 but Spring Data Redis expects boolean true/false
    // private static final Set<String> NUMERIC_TO_BOOLEAN_COMMANDS = Set.of(
    //     "SETNX", "MSETNX", "HSETNX", "SMOVE", "SISMEMBER", "EXPIRE", "EXPIREAT", 
    //     "PEXPIRE", "PEXPIREAT", "PERSIST", "MOVE", "RENAMENX", "EXISTS", "HSET"
    // );
    
    // // Geo commands that need special result processing
    // private static final Set<String> GEO_COMMANDS = Set.of(
    //     "GEOPOS", "GEOHASH", "GEODIST", "GEORADIUS", "GEORADIUSBYMEMBER", "GEOSEARCH"
    // );

    private final GlideClient client;
    private final long timeout;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final boolean isShared;
    private final ValkeyGlideConnectionProvider connectionProvider;

    private @Nullable Batch currentBatch;
    private final List<ResultMapper<?, ?>> batchCommandsConverters = new ArrayList<>();
    private final Set<byte[]> watchedKeys = new HashSet<>();
    private @Nullable Subscription subscription;

    // Command interfaces
    private final ValkeyGlideKeyCommands keyCommands;
    private final ValkeyGlideStringCommands stringCommands;
    private final ValkeyGlideListCommands listCommands;
    private final ValkeyGlideSetCommands setCommands;
    private final ValkeyGlideZSetCommands zSetCommands;
    private final ValkeyGlideHashCommands hashCommands;
    private final ValkeyGlideGeoCommands geoCommands;
    private final ValkeyGlideHyperLogLogCommands hyperLogLogCommands;
    private final ValkeyGlideScriptingCommands scriptingCommands;
    private final ValkeyGlideServerCommands serverCommands;
    private final ValkeyGlideStreamCommands streamCommands;

    /**
     * Creates a new {@link ValkeyGlideConnection} with a dedicated client.
     *
     * @param client valkey-glide client
     * @param timeout command timeout in milliseconds
     * @param connectionProvider the connection provider
     */
    public ValkeyGlideConnection(GlideClient client, long timeout, ValkeyGlideConnectionProvider connectionProvider) {
        this(client, timeout, connectionProvider, false);
    }

    /**
     * Creates a new {@link ValkeyGlideConnection} with a client (generic Object type for compatibility).
     *
     * @param client valkey-glide client as Object
     * @param timeout command timeout in milliseconds
     * @param connectionProvider the connection provider
     */
    public ValkeyGlideConnection(Object client, long timeout, ValkeyGlideConnectionProvider connectionProvider) {
        this((GlideClient) client, timeout, connectionProvider, false);
    }

    /**
     * Creates a new {@link ValkeyGlideConnection}.
     *
     * @param client valkey-glide client
     * @param timeout command timeout in milliseconds
     * @param connectionProvider the connection provider
     * @param isShared flag indicating whether the client is shared or dedicated to this connection
     */
    public ValkeyGlideConnection(GlideClient client, long timeout, ValkeyGlideConnectionProvider connectionProvider, boolean isShared) {
        Assert.notNull(client, "Client must not be null");
        Assert.notNull(connectionProvider, "ConnectionProvider must not be null");
        
        this.client = client;
        this.timeout = timeout;
        this.isShared = isShared;
        this.connectionProvider = connectionProvider;
        
        // Initialize command interfaces
        this.keyCommands = new ValkeyGlideKeyCommands(this);
        this.stringCommands = new ValkeyGlideStringCommands(this);
        this.listCommands = new ValkeyGlideListCommands(this);
        this.setCommands = new ValkeyGlideSetCommands(this);
        this.zSetCommands = new ValkeyGlideZSetCommands(this);
        this.hashCommands = new ValkeyGlideHashCommands(this);
        this.geoCommands = new ValkeyGlideGeoCommands(this);
        this.hyperLogLogCommands = new ValkeyGlideHyperLogLogCommands(this);
        this.scriptingCommands = new ValkeyGlideScriptingCommands(this);
        this.serverCommands = new ValkeyGlideServerCommands(this);
        this.streamCommands = new ValkeyGlideStreamCommands(this);
    }

    /**
     * Returns the native client used.
     *
     * @return the native client instance
     */
    public GlideClient getNativeClient() {
        verifyConnectionOpen();
        return client;
    }

    /**
     * Synchronously executes a Redis command and returns the future result.
     *
     * @param <T> The type of the command result
     * @param future The future containing the result
     * @return The command result
     */
    protected <T> T execute(CompletableFuture<T> future) {
        return ValkeyGlideFutureUtils.get(future, timeout, new ValkeyGlideExceptionConverter());
    }

    /**
     * Verifies that the connection is open.
     *
     * @throws InvalidDataAccessApiUsageException if the connection is closed
     */
    protected void verifyConnectionOpen() {
        if (isClosed()) {
            throw new InvalidDataAccessApiUsageException("Connection is closed");
        }
    }

    @Override
    public RedisGeoCommands geoCommands() {
        return this.geoCommands;
    }

    @Override
    public RedisHashCommands hashCommands() {
        return this.hashCommands;
    }

    @Override
    public RedisHyperLogLogCommands hyperLogLogCommands() {
        return this.hyperLogLogCommands;
    }

    @Override
    public RedisKeyCommands keyCommands() {
        return this.keyCommands;
    }

    @Override
    public RedisListCommands listCommands() {
        return this.listCommands;
    }

    @Override
    public RedisSetCommands setCommands() {
        return this.setCommands;
    }

    @Override
    public RedisScriptingCommands scriptingCommands() {
        return this.scriptingCommands;
    }

    @Override
    public RedisServerCommands serverCommands() {
        return this.serverCommands;
    }

    @Override
    public RedisStreamCommands streamCommands() {
        return this.streamCommands;
    }

    @Override
    public RedisStringCommands stringCommands() {
        return this.stringCommands;
    }

    @Override
    public RedisZSetCommands zSetCommands() {
        return this.zSetCommands;
    }

    @Override
    public RedisCommands commands() {
        return this;
    }

    @Override
    public void close() throws DataAccessException {
        if (closed.compareAndSet(false, true)) {
            if (!isShared) {
                try {
                    connectionProvider.release(client);
                } catch (Exception ex) {
                    throw new DataAccessException("Error closing Valkey-Glide connection", ex) {};
                }
            }
            
            if (subscription != null) {
                subscription.close();
                subscription = null;
            }
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public Object getNativeConnection() {
        verifyConnectionOpen();
        return client;
    }

    @Override
    public boolean isQueueing() {
        return (currentBatch != null && currentBatch.getProtobufBatch().getIsAtomic());
    }

    @Override
    public boolean isPipelined() {
        return (currentBatch != null && !currentBatch.getProtobufBatch().getIsAtomic());
    }

    @Override
    public void openPipeline() {
        if (isQueueing()) {
			throw new InvalidDataAccessApiUsageException("Cannot use pipelining while a transaction is active");
		}
        if (!isPipelined()) {
            currentBatch = new Batch(false).withBinaryOutput();
        }
    }

    @Override
    public List<Object> closePipeline() {
        if (!isPipelined()) {
            return new ArrayList<>();
        }

        try {
            if (currentBatch.getProtobufBatch().getCommandsCount() == 0) {
                return new ArrayList<>();
            }

            Object[] results = ValkeyGlideFutureUtils.get(
                client.exec(currentBatch, false), 
                timeout, 
                new ValkeyGlideExceptionConverter()
            );
            // We have to clear the currentBatch before processing results to allow
            // mappers to diffrentiate between normal and pipeline/transaction mode.
            // e.g. LUA scripts return null for FALSE, requiring special handling in the mapper
            currentBatch = null;
            List<Object> resultList = new ArrayList<>(results.length);
            for (int i = 0; i < results.length; i++) {
                Object item = results[i];
                if (item instanceof Exception) {
                    // Convert exceptions in pipeline results
                    resultList.add(new ValkeyGlideExceptionConverter().convert((Exception) item));
                    continue;
                }
                @SuppressWarnings("unchecked")
                ResultMapper<Object, ?> mapper = (ResultMapper<Object, ?>) batchCommandsConverters.get(i);
                resultList.add(mapper.map(item));
            }
            return resultList;
        } catch (Exception ex) {
            throw new RedisPipelineException(ex);
        } finally {
            currentBatch = null;
            batchCommandsConverters.clear();
        }
    }

    @Override
    public void multi() {
        if (isPipelined()) {
			throw new InvalidDataAccessApiUsageException("Cannot use transaction while a pipeline is open");
		}
        
        if (!isQueueing()) {
            // Create atomic batch (transaction)
            currentBatch = new Batch(true).withBinaryOutput();
        }
    }

    @Override
    public void discard() {
        if (!isQueueing()) { 
            throw new InvalidDataAccessApiUsageException("No ongoing transaction; Did you forget to call multi");
        }
        
        // Clear the current batch and reset transaction state
        currentBatch = null;
        batchCommandsConverters.clear();
    }

    @Override
    public List<Object> exec() {
        if (!isQueueing()) {
		    throw new InvalidDataAccessApiUsageException("No ongoing transaction; Did you forget to call multi");
        }
		
        try {
            if (currentBatch.getProtobufBatch().getCommandsCount() == 0) {
                return new ArrayList<>();
            }

            Object[] results = ValkeyGlideFutureUtils.get(
                client.exec(currentBatch, false), 
                timeout, 
                new ValkeyGlideExceptionConverter()
            );
            
            // Handle transaction abort cases - valkey-glide returns null for WATCH conflicts
            if (results == null) {
                // Check if we're being called from RedisTemplate context
                if (isCalledFromRedisTemplate()) {
                    // For RedisTemplate compatibility, throw exception that can be caught
                    throw new ValkeyGlideWatchConflictException("Transaction aborted due to WATCH conflict");
                } else {
                    // For direct connection usage, return null as per Redis specification
                    return null;
                }
            }
            
            // We have to clear the currentBatch before processing results to allow
            // mappers to diffrentiate between normal and pipeline/transaction mode.
            // e.g. LUA scripts return null for FALSE, requiring special handling in the mapper
            currentBatch = null;
            List<Object> resultList = new ArrayList<>(results.length);
            for (int i = 0; i < results.length; i++) {
                Object item = results[i];
                if (item instanceof Exception) {
                    // Convert exceptions in pipeline results
                    resultList.add(new ValkeyGlideExceptionConverter().convert((Exception) item));
                    continue;
                }
                @SuppressWarnings("unchecked")
                ResultMapper<Object, ?> mapper = (ResultMapper<Object, ?>) batchCommandsConverters.get(i);
                resultList.add(mapper.map(item));
            }
            return resultList;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        } finally {
            // Clean up transaction state
            currentBatch = null;
            batchCommandsConverters.clear();
            // Watches are automatically cleared after EXEC
            watchedKeys.clear();
        }
    }
    
    /**
     * Checks if this method is being called from RedisTemplate context.
     * This helps us adapt behavior for template vs direct connection usage.
     */
    private boolean isCalledFromRedisTemplate() {
        StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        for (StackTraceElement element : stackTrace) {
            if (element.getClassName().contains("RedisTemplate") &&
                (element.getMethodName().equals("execRaw") || 
                 element.getMethodName().equals("exec"))) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Convert geo command results to proper Spring Data Redis types for pipeline/transaction mode.
     */
    private Object convertGeoResult(String commandName, Object result) {
        if (result == null) {
            return null;
        }
        
        switch (commandName) {
            case "GEOPOS":
                return convertGeoPosResult(result);
            case "GEOHASH":
                return convertGeoHashResult(result);
            case "GEODIST":
                // GEODIST already returns Double, just return as-is
                return result;
            case "GEORADIUS":
            case "GEORADIUSBYMEMBER":
            case "GEOSEARCH":
                return convertGeoSearchResult(result);
            default:
                return result;
        }
    }
    
    /**
     * Convert GEOPOS raw result to List<Point>.
     */
    private Object convertGeoPosResult(Object result) {
        if (result == null) {
            return new ArrayList<>();
        }
        
        if (result instanceof List) {
            List<?> list = (List<?>) result;
            List<org.springframework.data.geo.Point> pointList = new ArrayList<>(list.size());
            for (Object item : list) {
                if (item == null) {
                    pointList.add(null);
                } else if (item instanceof List) {
                    List<?> coordinates = (List<?>) item;
                    if (coordinates.size() >= 2) {
                        double x = parseGeoDouble(coordinates.get(0));
                        double y = parseGeoDouble(coordinates.get(1));
                        pointList.add(new org.springframework.data.geo.Point(x, y));
                    } else {
                        pointList.add(null);
                    }
                } else {
                    pointList.add(null);
                }
            }
            return pointList;
        }
        
        return result;
    }
    
    /**
     * Convert GEOHASH raw result to List<String>.
     */
    private Object convertGeoHashResult(Object result) {
        if (result == null) {
            return new ArrayList<>();
        }
        
        if (result instanceof List) {
            List<?> list = (List<?>) result;
            List<String> hashList = new ArrayList<>(list.size());
            for (Object item : list) {
                if (item == null) {
                    hashList.add(null);
                } else if (item instanceof String) {
                    hashList.add((String) item);
                } else if (item instanceof byte[]) {
                    hashList.add(new String((byte[]) item));
                } else {
                    hashList.add(item.toString());
                }
            }
            return hashList;
        }
        
        return result;
    }
    
    /**
     * Convert geo search results (GEORADIUS, GEORADIUSBYMEMBER, GEOSEARCH) to GeoResults.
     */
    private Object convertGeoSearchResult(Object result) {
        if (result == null) {
            return new org.springframework.data.geo.GeoResults<>(new ArrayList<>());
        }
        
        if (result instanceof List) {
            List<?> list = (List<?>) result;
            List<org.springframework.data.geo.GeoResult<org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation<byte[]>>> geoResults = new ArrayList<>(list.size());
            
            for (Object item : list) {
                if (item instanceof List) {
                    // Complex result with member name and possibly distance/coordinates
                    List<?> itemList = (List<?>) item;
                    if (!itemList.isEmpty()) {
                        // First element is always the member name
                        byte[] memberName = convertToBytes(itemList.get(0));
                        org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation<byte[]> location = 
                            new org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation<>(memberName, null);
                        
                        // Default distance - we don't have access to the original metric here
                        org.springframework.data.geo.Distance distance = new org.springframework.data.geo.Distance(0.0, 
                            org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit.METERS);
                        
                        geoResults.add(new org.springframework.data.geo.GeoResult<>(location, distance));
                    }
                } else {
                    // Simple result - just member name
                    byte[] memberName = convertToBytes(item);
                    org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation<byte[]> location = 
                        new org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation<>(memberName, null);
                    org.springframework.data.geo.Distance distance = new org.springframework.data.geo.Distance(0.0, 
                        org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit.METERS);
                    geoResults.add(new org.springframework.data.geo.GeoResult<>(location, distance));
                }
            }
            
            return new org.springframework.data.geo.GeoResults<>(geoResults);
        }
        
        return result;
    }
    
    /**
     * Convert various types to byte array.
     */
    private byte[] convertToBytes(Object obj) {
        if (obj instanceof byte[]) {
            return (byte[]) obj;
        } else if (obj instanceof String) {
            return ((String) obj).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        } else if (obj instanceof List) {
            // Handle case where bytes come as List<Integer>
            List<?> byteList = (List<?>) obj;
            byte[] bytes = new byte[byteList.size()];
            for (int i = 0; i < byteList.size(); i++) {
                Object byteVal = byteList.get(i);
                if (byteVal instanceof Number) {
                    bytes[i] = ((Number) byteVal).byteValue();
                } else {
                    bytes[i] = 0;
                }
            }
            return bytes;
        } else {
            return obj.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
    }
    
    /**
     * Parse a double value from various geo result formats.
     */
    private double parseGeoDouble(Object obj) {
        if (obj instanceof Number) {
            return ((Number) obj).doubleValue();
        } else if (obj instanceof String) {
            return Double.parseDouble((String) obj);
        } else if (obj instanceof byte[]) {
            return Double.parseDouble(new String((byte[]) obj));
        } else {
            throw new IllegalArgumentException("Cannot parse double from " + obj.getClass());
        }
    }

    @Override
    public void select(int dbIndex) {
        Assert.isTrue(dbIndex >= 0, "DB index must be non-negative");
        try {
            execute("SELECT", dbIndex);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public void unwatch() {
        try {
            if (watchedKeys.isEmpty()) {
                return; // No keys to unwatch
            }
            execute("UNWATCH");
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        } finally {
            watchedKeys.clear();
        }
    }

    @Override
    public void watch(byte[]... keys) {
        Assert.notNull(keys, "Keys must not be null");
        Assert.noNullElements(keys, "Keys must not contain null elements");

        if (isQueueing()) {
            throw new InvalidDataAccessApiUsageException("WATCH is not allowed during MULTI");
        }

        try {
            // Execute WATCH immediately to set up key monitoring at connection level
            execute("WATCH", keys);
            
            // Track watched keys for cleanup
            Collections.addAll(watchedKeys, keys);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public Long publish(byte[] channel, byte[] message) {
        Assert.notNull(channel, "Channel must not be null");
        Assert.notNull(message, "Message must not be null");

        try {
            Object result = execute("PUBLISH", channel, message);
            return (Long) result;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public void subscribe(MessageListener listener, byte[]... channels) {
        Assert.notNull(listener, "MessageListener must not be null");
        Assert.notNull(channels, "Channels must not be null");
        Assert.noNullElements(channels, "Channels must not contain null elements");

		if (isSubscribed()) {
			throw new RedisSubscribedConnectionException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}

		if (isQueueing() || isPipelined()) {
			throw new InvalidDataAccessApiUsageException("Cannot subscribe in pipeline / transaction mode");
		}

        // TODO: Implement dynamic subscription management when supported by valkey-glide
        throw new UnsupportedOperationException("Dynamic subscriptions not yet implemented");
    }

    @Override
    public void pSubscribe(MessageListener listener, byte[]... patterns) {
        Assert.notNull(listener, "MessageListener must not be null");
        Assert.notNull(patterns, "Patterns must not be null");
        Assert.noNullElements(patterns, "Patterns must not contain null elements");

		if (isSubscribed()) {
			throw new RedisSubscribedConnectionException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}

		if (isQueueing() || isPipelined()) {
			throw new InvalidDataAccessApiUsageException("Cannot subscribe in pipeline / transaction mode");
		}

        // TODO: Implement dynamic subscription management when supported by valkey-glide
        throw new UnsupportedOperationException("Dynamic subscriptions not yet implemented");
    }

    @Override
    public Subscription getSubscription() {
        return subscription;
    }

    @Override
    public boolean isSubscribed() {
        return subscription != null && subscription.isAlive();
    }

    /**
     * Execute a Redis command using string arguments.
     * 
     * @param command the command to execute
     * @param args the command arguments
     * @return the command result
     */

    /**
     * Executes a Redis command with arguments and converts the raw driver result
     * into a strongly typed value using the provided {@link ResultMapper}.
     *
     * <p>Behavior depends on whether pipelining/transaction is enabled:
     * <ul>
     *   <li><b>Immediate mode</b> – the command is sent directly to the driver,
     *       and the raw result is synchronously converted via {@code mapper.map(raw)}.</li>
     *   <li><b>Pipeline/Transaction mode</b> – the command and mapper are queued for later execution.
     *       In this case, the method returns {@code null}. When
     *       {@link #closePipeline()}/{@link #exec()} are called, all queued commands are flushed,
     *       raw results are collected, and each queued {@code ResultMapper}
     *       is applied in order.</li>
     * </ul>
     *
     * <p>The caller (API layer) is responsible for providing the appropriate
     * {@link ResultMapper} for the Redis command being executed. This allows each
     * high-level API method to encapsulate its own decoding logic.
     *
     * @param command The Redis command name (e.g. "GET", "SMEMBERS").
     * @param mapper  A function that knows how to convert the raw driver result
     *                into a strongly typed value of type {@code R}.
     * @param args    The command arguments, already encoded into driver-acceptable
     *                representations (e.g. {@code byte[]} or primitives).
     * @param <R>     The expected return type after mapping the driver result.
     * @return        The mapped result in immediate mode, or {@code null} if
     *                pipelining/transaction is active (result will be available after
     *                {@link #closePipeline()} or {@link #exec()}).
     */
    @SuppressWarnings("unchecked")
    @Nullable
    public <I, R> R execute(String command, ResultMapper<I, R> mapper, Object... args) {
        verifyConnectionOpen();

        // Convert arguments to appropriate format for Glide
        GlideString[] glideArgs = new GlideString[args.length + 1];
        glideArgs[0] = GlideString.of(command);
        for (int i = 0; i < args.length; i++) {
            if (args[i] == null) {
                glideArgs[i + 1] = null;
            } else if (args[i] instanceof byte[]) {
                glideArgs[i + 1] = GlideString.of((byte[]) args[i]);
            } else if (args[i] instanceof String) {
                glideArgs[i + 1] = GlideString.of((String) args[i]);
            } else {
                glideArgs[i + 1] = GlideString.of(args[i].toString());
            }
        }
        // Handle pipeline/transaction mode - add command to batch instead of executing
        if (isQueueing() || isPipelined()) {
            // Store converter for later conversion
            batchCommandsConverters.add(mapper);
            // Add command to the current batch
            currentBatch.customCommand(glideArgs);
            return null; // Return null for queued commands in transaction
        }
        
        I result = (I) execute(client.customCommand(glideArgs));
        return mapper.map(result);
    }
    
    public Object execute(String command, Object... args) {
        return execute(command, rawResult -> {
            return ValkeyGlideConverters.defaultFromGlideResult(rawResult);
        },
        args);
    }

    @Override
    public Object execute(String command, byte[]... args) {
        Assert.notNull(command, "Command must not be null");
        Assert.notNull(args, "Arguments must not be null");
        Assert.noNullElements(args, "Arguments must not contain null elements");
        try {
            // Delegate to the generic execute method
            return execute(command, rawResult -> {
                return ValkeyGlideConverters.defaultFromGlideResult(rawResult);
            },
            (Object[]) args);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    /**
     * Returns the command timeout.
     * 
     * @return the command timeout in milliseconds
     */
    public long getTimeout() {
        return timeout;
    }

    @Override
    protected boolean isActive(RedisNode node) {
        Assert.notNull(node, "RedisNode must not be null");
        // TODO: Create new valkey-glide GlideClient instance to test connection to the node
        // connection params should be clonned from the current client except host/port
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    protected RedisSentinelConnection getSentinelConnection(RedisNode sentinel) {
        // TODO: Uncomment when sentinel support is added to valkey-glide
        // and implement sentinel connection using a dedicated GlideClient instance
        // Assert.notNull(sentinel, "Sentinel RedisNode must not be null");
        throw new UnsupportedOperationException("Sentinel is not supported by this client.");
    }

    @Override
    public byte[] echo(byte[] message) {
        Assert.notNull(message, "Message must not be null");
        try {
            Object result = execute("ECHO", message);
            return result != null ? (byte[]) result : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    public String ping() {
        try {
            Object result = execute("PING");
            return result != null ? new String((byte[]) result, StandardCharsets.UTF_8) : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }
}

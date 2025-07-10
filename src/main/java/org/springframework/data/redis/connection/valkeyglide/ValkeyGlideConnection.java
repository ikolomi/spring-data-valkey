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
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

// Imports from valkey-glide library
import glide.api.GlideClient;
import glide.api.models.GlideString;

/**
 * Connection to a Redis server using Valkey-Glide client. The connection
 * adapts Valkey-Glide's asynchronous API to Spring Data Redis's synchronous API.
 *
 * @author Ilya Kolomin
 */
public class ValkeyGlideConnection extends AbstractRedisConnection {

    private final GlideClient client;
    private final long timeout;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final boolean isShared;
    private final ValkeyGlideConnectionProvider connectionProvider;

    private boolean pipelined = false;
    private @Nullable List<Object> pipelinedResults;
    private boolean multi = false;
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
    public <T> T execute(CompletableFuture<T> future) {
        return ValkeyGlideFutureUtils.get(future, timeout, new ValkeyGlideExceptionConverter());
    }

    /**
     * Verifies that the connection is open.
     *
     * @throws DataAccessException if the connection is closed
     */
    protected void verifyConnectionOpen() {
        if (isClosed()) {
            throw new IllegalStateException("Connection is closed");
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
        return multi;
    }

    @Override
    public boolean isPipelined() {
        return pipelined;
    }

    @Override
    public void openPipeline() {
        if (!pipelined) {
            pipelined = true;
            pipelinedResults = new ArrayList<>();
        }
    }

    @Override
    public List<Object> closePipeline() {
        if (!pipelined) {
            return new ArrayList<>();
        }
        
        try {
            List<Object> results = pipelinedResults;
            pipelinedResults = null;
            pipelined = false;
            return results != null ? results : new ArrayList<>();
        } catch (Exception ex) {
            throw new RedisPipelineException(ex);
        }
    }

    @Override
    public void multi() {
        if (pipelined) {
            // Add MULTI command to pipeline
            pipeline(execute("MULTI"));
            return;
        }
        
        if (!isQueueing()) {
            execute("MULTI");
            multi = true;
        }
    }

    @Override
    public void discard() {
        if (pipelined) {
            pipeline(execute("DISCARD"));
            return;
        }
        
        if (isQueueing()) {
            execute("DISCARD");
            multi = false;
        }
    }

    @Override
    public List<Object> exec() {
        if (pipelined) {
            pipeline(execute("EXEC"));
            return null;
        }
        
        if (isQueueing()) {
            try {
                Object result = execute("EXEC");
                @SuppressWarnings("unchecked")
                List<Object> results = (result instanceof List) ? (List<Object>) result : null;
                return results;
            } finally {
                multi = false;
            }
        }
        
        return null;
    }

    @Override
    public void select(int dbIndex) {
        if (pipelined) {
            pipeline(execute(client.select(dbIndex)));
            return;
        }
        
        if (isQueueing()) {
            execute(client.select(dbIndex));
            return;
        }
        
        execute(client.select(dbIndex));
    }

    @Override
    public void unwatch() {
        if (pipelined) {
            pipeline(execute(client.unwatch()));
            return;
        }
        
        if (isQueueing()) {
            execute(client.unwatch());
            return;
        }
        
        execute(client.unwatch());
    }

    @Override
    public void watch(byte[]... keys) {
        if (isQueueing()) {
            throw new IllegalStateException("WATCH is not allowed during MULTI");
        }
        
        GlideString[] glideKeys = new GlideString[keys.length];
        for (int i = 0; i < keys.length; i++) {
            glideKeys[i] = GlideString.of(keys[i]);
        }
        
        if (pipelined) {
            pipeline(execute(client.watch(glideKeys)));
            return;
        }
        
        execute(client.watch(glideKeys));
    }

    @Override
    public Long publish(byte[] channel, byte[] message) {
        if (pipelined) {
            pipeline(execute(client.publish(GlideString.of(message), GlideString.of(channel))));
            return null;
        }
        
        if (isQueueing()) {
            execute(client.publish(GlideString.of(message), GlideString.of(channel)));
            return null;
        }
        
        // Note: GlideClient.publish returns String, but we need Long for compatibility
        execute(client.publish(GlideString.of(message), GlideString.of(channel)));
        return 1L; // Simplified - actual implementation would parse the result
    }

    @Override
    public void subscribe(MessageListener listener, byte[]... channels) {
        if (isPipelined() || isQueueing()) {
            throw new UnsupportedOperationException("Subscribe not supported in pipelined or multi mode");
        }
        
        if (isSubscribed()) {
            throw new IllegalStateException("Already subscribed");
        }
        
        // Create subscription using ValkeyGlideSubscription
        subscription = new ValkeyGlideSubscription(client, listener);
        subscription.subscribe(channels);
    }

    @Override
    public void pSubscribe(MessageListener listener, byte[]... patterns) {
        if (isPipelined() || isQueueing()) {
            throw new UnsupportedOperationException("PSubscribe not supported in pipelined or multi mode");
        }
        
        if (isSubscribed()) {
            throw new IllegalStateException("Already subscribed");
        }
        
        // Create subscription using ValkeyGlideSubscription
        subscription = new ValkeyGlideSubscription(client, listener);
        subscription.pSubscribe(patterns);
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
     * Adds a result to the pipeline.
     *
     * @param result the result to add
     */
    public void pipeline(Object result) {
        if (pipelined && pipelinedResults != null) {
            pipelinedResults.add(result);
        }
    }

    /**
     * Execute a Redis command using string arguments.
     * 
     * @param command the command to execute
     * @param args the command arguments
     * @return the command result
     */
    public Object execute(String command, Object... args) {
        verifyConnectionOpen();

        try {
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
            
            // Use the client's customCommand method to execute arbitrary commands
            return execute(client.customCommand(glideArgs));
            
        } catch (Exception e) {
            throw new DataAccessException("Error executing Redis command: " + command, e) {};
        }
    }
    
    /**
     * Helper method to concatenate command with arguments.
     */
    private String[] concatenateCommandAndArgs(String command, String[] args) {
        String[] result = new String[args.length + 1];
        result[0] = command;
        System.arraycopy(args, 0, result, 1, args.length);
        return result;
    }

    @Override
    public Object execute(String command, byte[]... args) {
        verifyConnectionOpen();
        
        if (isPipelined()) {
            pipeline(execute(command, (Object[]) args));
            return null;
        }
        
        if (isQueueing()) {
            execute(command, (Object[]) args);
            return null;
        }
        
        return execute(command, (Object[]) args);
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
        // In a real implementation, we would connect to the node and check if it's alive
        return false;
    }

    @Override
    protected RedisSentinelConnection getSentinelConnection(RedisNode sentinel) {
        throw new UnsupportedOperationException("Sentinel is not supported by this client.");
    }

    @Override
    public byte[] echo(byte[] message) {
        if (pipelined) {
            pipeline(execute(client.echo(GlideString.of(message))));
            return null;
        }
        
        if (isQueueing()) {
            execute(client.echo(GlideString.of(message)));
            return null;
        }
        
        GlideString result = execute(client.echo(GlideString.of(message)));
        return result != null ? result.getBytes() : null;
    }

    @Override
    public String ping() {
        if (pipelined) {
            pipeline(execute(client.ping()));
            return null;
        }
        
        if (isQueueing()) {
            execute(client.ping());
            return null;
        }
        
        return execute(client.ping());
    }
}

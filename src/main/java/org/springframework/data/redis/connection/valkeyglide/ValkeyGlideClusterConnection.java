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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;

import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.ClusterInfo;
import org.springframework.data.redis.connection.ClusterSlotHashUtil;
import org.springframework.data.redis.connection.ClusterTopology;
import org.springframework.data.redis.connection.ClusterTopologyProvider;
import org.springframework.data.redis.connection.DefaultedRedisClusterConnection;
import org.springframework.data.redis.connection.RedisClusterCommands;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisClusterServerCommands;
import org.springframework.data.redis.connection.RedisCommands;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.RedisHashCommands;
import org.springframework.data.redis.connection.RedisHyperLogLogCommands;
import org.springframework.data.redis.connection.RedisKeyCommands;
import org.springframework.data.redis.connection.RedisListCommands;
import org.springframework.data.redis.connection.RedisPipelineException;
import org.springframework.data.redis.connection.RedisScriptingCommands;
import org.springframework.data.redis.connection.RedisServerCommands;
import org.springframework.data.redis.connection.RedisSetCommands;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.data.redis.connection.Subscription;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * {@link RedisClusterConnection} implementation for Valkey-Glide.
 * 
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideClusterConnection implements RedisClusterConnection, RedisClusterCommands, DefaultedRedisClusterConnection {

    private final Object client; // This would be a GlideClusterClient in the actual implementation
    private final long timeout;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ClusterTopologyProvider topologyProvider;
    private final ValkeyGlideClusterNodeResourceProvider nodeResourceProvider;
    private boolean pipelined = false;
    private @Nullable List<Object> pipelinedResults;

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
    private final ValkeyGlideClusterServerCommands serverCommands;
    private final ValkeyGlideStreamCommands streamCommands;

    /**
     * Creates a new {@link ValkeyGlideClusterConnection}.
     *
     * @param client must not be {@literal null}.
     * @param timeout The connection timeout (in milliseconds)
     * @param topologyProvider must not be {@literal null}.
     */
    public ValkeyGlideClusterConnection(Object client, long timeout, ClusterTopologyProvider topologyProvider) {
        this(client, timeout, topologyProvider, null);
    }
    
    /**
     * Creates a new {@link ValkeyGlideClusterConnection}.
     *
     * @param client must not be {@literal null}.
     * @param timeout The connection timeout (in milliseconds)
     * @param topologyProvider must not be {@literal null}.
     * @param nodeResourceProvider can be {@literal null}.
     */
    public ValkeyGlideClusterConnection(Object client, long timeout, ClusterTopologyProvider topologyProvider, 
                                       @Nullable ValkeyGlideClusterNodeResourceProvider nodeResourceProvider) {
        Assert.notNull(client, "Client must not be null!");
        Assert.notNull(topologyProvider, "TopologyProvider must not be null!");
        
        this.client = client;
        this.timeout = timeout;
        this.topologyProvider = topologyProvider;
        this.nodeResourceProvider = nodeResourceProvider;
        
        this.keyCommands = new ValkeyGlideKeyCommands(this.adaptToConnection());
        this.stringCommands = new ValkeyGlideStringCommands(this.adaptToConnection());
        this.listCommands = new ValkeyGlideListCommands(this.adaptToConnection());
        this.setCommands = new ValkeyGlideSetCommands(this.adaptToConnection());
        this.zSetCommands = new ValkeyGlideZSetCommands(this.adaptToConnection());
        this.hashCommands = new ValkeyGlideHashCommands(this.adaptToConnection());
        this.geoCommands = new ValkeyGlideGeoCommands(this.adaptToConnection());
        this.hyperLogLogCommands = new ValkeyGlideHyperLogLogCommands(this.adaptToConnection());
        this.scriptingCommands = new ValkeyGlideScriptingCommands(this.adaptToConnection());
        this.serverCommands = new ValkeyGlideClusterServerCommands(this);
        this.streamCommands = new ValkeyGlideStreamCommands(this.adaptToConnection());
    }

    /**
     * Creates a {@link ValkeyGlideConnection} adapter that delegates to this cluster connection but doesn't expose cluster
     * operations.
     *
     * @return non-null
     */
    private ValkeyGlideConnection adaptToConnection() {
        // Note: This is a stub implementation. In a real implementation, we would create a proper
        // ValkeyGlideConnection adapter that delegates operations to this cluster connection.
        // For now, we create a dummy connection provider that doesn't manage the client lifecycle
        ValkeyGlideConnectionProvider dummyProvider = new ValkeyGlideConnectionProvider() {
            @Override
            public ValkeyGlideConnection getConnection(Object client) {
                // For cluster connections, we don't create new connections
                throw new UnsupportedOperationException("Cluster connections don't support getConnection");
            }
            
            @Override
            public void release(Object connection) {
                // No-op for cluster connections since the cluster client manages its own lifecycle
            }
            
            @Override
            public Object getConnectionClient() {
                return client;
            }
        };
        return new ValkeyGlideConnection((glide.api.GlideClient)client, timeout, dummyProvider);
    }

    /**
     * Synchronously executes a Redis command and returns the future result.
     *
     * @param <T> The type of the command result
     * @param future The future containing the result
     * @return The command result
     */
    public <T> T execute(CompletableFuture<T> future) {
        ValkeyGlideExceptionConverter exceptionConverter = new ValkeyGlideExceptionConverter();
        return ValkeyGlideFutureUtils.get(future, timeout, exceptionConverter);
    }

    /**
     * Execute a Redis command on the client.
     * 
     * @param command the command to execute
     * @param args the command arguments
     * @return the command result
     */
    public Object execute(String command, Object... args) {
        // In the actual implementation, we would use the Valkey-Glide client to execute the command
        return null;
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
    public RedisClusterServerCommands serverCommands() {
        return this.serverCommands;
    }
    
    @Override
    public org.springframework.data.redis.connection.RedisClusterCommands clusterCommands() {
        return this;
    }

    @Override
    public RedisStreamCommands streamCommands() {
        return this.streamCommands;
    }
    
    @Override
    public ClusterInfo clusterGetClusterInfo() {
        return clusterGetInfo();
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

    @Override
    public Object getNativeConnection() {
        verifyConnectionOpen();
        return client;
    }

    @Override
    public void close() throws DataAccessException {
        if (closed.compareAndSet(false, true)) {
            // In the actual implementation, we would release resources
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    public ClusterInfo clusterGetInfo() {
        // In the actual implementation, we would use the Valkey-Glide client to get cluster info
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Iterable<RedisClusterNode> clusterGetNodes() {
        return getClusterTopology().getNodes();
    }

    @Override
    public Collection<RedisClusterNode> clusterGetReplicas(RedisClusterNode master) {
        Assert.notNull(master, "Master must not be null!");
        
        // In the actual implementation, we would use the Valkey-Glide client to get replicas
        return Collections.emptyList();
    }

    @Override
    public Map<RedisClusterNode, Collection<RedisClusterNode>> clusterGetMasterReplicaMap() {
        // In the actual implementation, we would use the Valkey-Glide client to get master-replica map
        return Collections.emptyMap();
    }

    @Override
    public Integer clusterGetSlotForKey(byte[] key) {
        Assert.notNull(key, "Key must not be null!");
        return ClusterSlotHashUtil.calculateSlot(key);
    }

    @Override
    public RedisClusterNode clusterGetNodeForSlot(int slot) {
        // In the actual implementation, we would use the Valkey-Glide client to get node for slot
        return null;
    }

    @Override
    public RedisClusterNode clusterGetNodeForKey(byte[] key) {
        Assert.notNull(key, "Key must not be null!");
        return clusterGetNodeForSlot(clusterGetSlotForKey(key));
    }

    public ClusterTopology getClusterTopology() {
        return topologyProvider.getTopology();
    }

    @Override
    public void clusterAddSlots(RedisClusterNode node, int... slots) {
        // In the actual implementation, we would use the Valkey-Glide client to add slots
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void clusterAddSlots(RedisClusterNode node, RedisClusterNode.SlotRange range) {
        // In the actual implementation, we would use the Valkey-Glide client to add slots
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Long clusterCountKeysInSlot(int slot) {
        // In the actual implementation, we would use the Valkey-Glide client to count keys
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void clusterDeleteSlots(RedisClusterNode node, int... slots) {
        // In the actual implementation, we would use the Valkey-Glide client to delete slots
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void clusterDeleteSlotsInRange(RedisClusterNode node, RedisClusterNode.SlotRange range) {
        // In the actual implementation, we would use the Valkey-Glide client to delete slots
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void clusterForget(RedisClusterNode node) {
        // In the actual implementation, we would use the Valkey-Glide client to forget node
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void clusterMeet(RedisClusterNode node) {
        // In the actual implementation, we would use the Valkey-Glide client to meet node
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void clusterSetSlot(RedisClusterNode node, int slot, AddSlots mode) {
        // In the actual implementation, we would use the Valkey-Glide client to set slot
        throw new UnsupportedOperationException("Not yet implemented");
    }
    
    @Override
    public List<byte[]> clusterGetKeysInSlot(int slot, Integer count) {
        // In the actual implementation, we would use the Valkey-Glide client to get keys
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void clusterReplicate(RedisClusterNode master, RedisClusterNode replica) {
        // In the actual implementation, we would use the Valkey-Glide client to replicate
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void bgReWriteAof(RedisClusterNode node) {
        // In the actual implementation, we would use the Valkey-Glide client to rewrite AOF
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void bgSave(RedisClusterNode node) {
        // In the actual implementation, we would use the Valkey-Glide client to save in background
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Long lastSave(RedisClusterNode node) {
        // In the actual implementation, we would use the Valkey-Glide client to get last save time
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void save(RedisClusterNode node) {
        // In the actual implementation, we would use the Valkey-Glide client to save
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Long dbSize(RedisClusterNode node) {
        // In the actual implementation, we would use the Valkey-Glide client to get DB size
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void flushDb(RedisClusterNode node) {
        // In the actual implementation, we would use the Valkey-Glide client to flush DB
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void flushAll(RedisClusterNode node) {
        // In the actual implementation, we would use the Valkey-Glide client to flush all
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Properties info(RedisClusterNode node) {
        // In the actual implementation, we would use the Valkey-Glide client to get info
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Properties info(RedisClusterNode node, String section) {
        // In the actual implementation, we would use the Valkey-Glide client to get info
        throw new UnsupportedOperationException("Not yet implemented");
    }
    
    @Override
    public Cursor<byte[]> scan(RedisClusterNode node, ScanOptions options) {
        // In the actual implementation, we would use the Valkey-Glide client to scan
        throw new UnsupportedOperationException("Not yet implemented");
    }
    
    @Override
    public String ping(RedisClusterNode node) {
        // In the actual implementation, we would use the Valkey-Glide client to ping
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Set<byte[]> keys(RedisClusterNode node, byte[] pattern) {
        // In the actual implementation, we would use the Valkey-Glide client to get keys
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public byte[] randomKey(RedisClusterNode node) {
        // In the actual implementation, we would use the Valkey-Glide client to get random key
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void shutdown(RedisClusterNode node) {
        // In the actual implementation, we would use the Valkey-Glide client to shutdown
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Properties getConfig(RedisClusterNode node, String pattern) {
        // In the actual implementation, we would use the Valkey-Glide client to get config
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void setConfig(RedisClusterNode node, String param, String value) {
        // In the actual implementation, we would use the Valkey-Glide client to set config
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void resetConfigStats(RedisClusterNode node) {
        // In the actual implementation, we would use the Valkey-Glide client to reset config stats
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void rewriteConfig(RedisClusterNode node) {
        // In the actual implementation, we would use the Valkey-Glide client to rewrite config
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Long time(RedisClusterNode node) {
        // In the actual implementation, we would use the Valkey-Glide client to get time
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public List<RedisClientInfo> getClientList(RedisClusterNode node) {
        // In the actual implementation, we would use the Valkey-Glide client to get client list
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void multi() {
        throw new UnsupportedOperationException("MULTI is currently not supported in cluster mode!");
    }

    @Override
    public void discard() {
        throw new UnsupportedOperationException("DISCARD is currently not supported in cluster mode!");
    }

    @Override
    public void watch(byte[]... keys) {
        throw new UnsupportedOperationException("WATCH is currently not supported in cluster mode!");
    }

    @Override
    public void unwatch() {
        throw new UnsupportedOperationException("UNWATCH is currently not supported in cluster mode!");
    }

    @Override
    public List<Object> exec() {
        throw new UnsupportedOperationException("EXEC is currently not supported in cluster mode!");
    }

    @Override
    public boolean isQueueing() {
        return false;
    }

    @Override
    public boolean isPipelined() {
        return pipelined;
    }

    @Override
    public void openPipeline() {
        if (!pipelined) {
            pipelined = true;
            pipelinedResults = Collections.synchronizedList(new ArrayList<>());
        }
    }

    @Override
    public List<Object> closePipeline() {
        if (!pipelined) {
            return Collections.emptyList();
        }
        
        try {
            return Collections.unmodifiableList(pipelinedResults);
        } catch (Exception ex) {
            throw new RedisPipelineException(ex);
        } finally {
            pipelined = false;
            pipelinedResults = null;
        }
    }

    @Override
    public void select(int dbIndex) {
        if (dbIndex != 0) {
            throw new InvalidDataAccessApiUsageException("Cannot SELECT non zero index in cluster mode!");
        }
    }
    
    @Override
    public RedisSentinelConnection getSentinelConnection() {
        throw new UnsupportedOperationException("Sentinel not supported in cluster mode!");
    }

    @Override
    public byte[] echo(byte[] message) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public String ping() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Long publish(byte[] channel, byte[] message) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void subscribe(MessageListener listener, byte[]... channels) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void pSubscribe(MessageListener listener, byte[]... patterns) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public Subscription getSubscription() {
        return null;
    }

    @Override
    public boolean isSubscribed() {
        return false;
    }

    public void executeCommand(Object command) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
    
    @Override
    public Object execute(String command, byte[]... args) {
        verifyConnectionOpen();
        
        if (isPipelined()) {
            pipeline(execute(command, (Object[]) args));
            return null;
        }
        
        return execute(command, (Object[]) args);
    }
}

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
package org.springframework.data.valkey.connection.valkeyglide;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.data.valkey.connection.ClusterTopology;
import org.springframework.data.valkey.connection.ClusterTopologyProvider;
import org.springframework.data.valkey.connection.ValkeyClusterNode;
import org.springframework.data.valkey.connection.ValkeyClusterNode.LinkState;
import org.springframework.data.valkey.connection.ValkeyClusterNode.SlotRange;
import org.springframework.data.valkey.connection.ValkeyNode.NodeType;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Topology provider for Valkey Cluster using Valkey-Glide.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideClusterTopologyProvider implements ClusterTopologyProvider {

    private final Object clusterClient;
    private final ValkeyGlideExceptionConverter exceptionConverter;
    private final long timeout;
    
    // Cache of cluster nodes
    private final Map<String, ValkeyClusterNode> knownNodes = new ConcurrentHashMap<>();

    /**
     * Creates a new {@link ValkeyGlideClusterTopologyProvider}.
     *
     * @param clusterClient The Valkey-Glide cluster client, must not be {@literal null}.
     * @param timeout Command timeout in milliseconds.
     */
    public ValkeyGlideClusterTopologyProvider(Object clusterClient, long timeout) {
        this(clusterClient, timeout, new ValkeyGlideExceptionConverter());
    }

    /**
     * Creates a new {@link ValkeyGlideClusterTopologyProvider}.
     *
     * @param clusterClient The Valkey-Glide cluster client, must not be {@literal null}.
     * @param timeout Command timeout in milliseconds.
     * @param exceptionConverter Exception converter, must not be {@literal null}.
     */
    public ValkeyGlideClusterTopologyProvider(Object clusterClient, long timeout, ValkeyGlideExceptionConverter exceptionConverter) {
        Assert.notNull(clusterClient, "ClusterClient must not be null!");
        Assert.notNull(exceptionConverter, "ExceptionConverter must not be null!");
        
        this.clusterClient = clusterClient;
        this.timeout = timeout;
        this.exceptionConverter = exceptionConverter;
    }

    /**
     * Get the cluster topology map.
     * 
     * @return A map of node ID to cluster node.
     */
    public Map<String, ValkeyClusterNode> getTopologyMap() {
        try {
            // In the actual implementation, we would call the Valkey-Glide client to get cluster nodes
            // For now, just return a sample cluster topology
            CompletableFuture<Object> future = CompletableFuture.completedFuture(createSampleClusterTopology());
            
            // Process cluster nodes response
            Object result = ValkeyGlideFutureUtils.get(future, timeout, exceptionConverter);
            Map<String, ValkeyClusterNode> nodes = convertClusterNodesResponse(result);
            
            if (nodes == null || nodes.isEmpty()) {
                return Collections.emptyMap();
            }
            
            // Update known nodes
            synchronized (knownNodes) {
                knownNodes.clear();
                knownNodes.putAll(nodes);
            }
            
            return Collections.unmodifiableMap(new HashMap<>(nodes));
        } catch (Exception ex) {
            throw exceptionConverter.convert(ex);
        }
    }

    /**
     * Gets a cluster node by ID.
     * 
     * @param nodeId The node ID.
     * @return The cluster node, or {@literal null} if not found.
     */
    @Nullable
    public ValkeyClusterNode getNode(String nodeId) {
        return knownNodes.get(nodeId);
    }

    /**
     * Gets a set of all cluster nodes.
     * 
     * @return The set of all cluster nodes.
     */
    public Set<ValkeyClusterNode> getNodes() {
        return new HashSet<>(knownNodes.values());
    }

    /**
     * Gets the cluster client.
     * 
     * @return The cluster client.
     */
    public Object getClusterClient() {
        return clusterClient;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ClusterTopology getTopology() {
        return new ClusterTopology(new HashSet<>(getTopologyMap().values()));
    }

    /**
     * Converts a cluster nodes response to a map of node ID to cluster node.
     * 
     * @param response The cluster nodes response.
     * @return A map of node ID to cluster node.
     */
    private Map<String, ValkeyClusterNode> convertClusterNodesResponse(Object response) {
        // In the actual implementation, we would convert the Valkey-Glide response
        // to a map of node ID to cluster node
        // For now, just return a sample cluster topology
        return createSampleClusterTopology();
    }

    /**
     * Creates a sample cluster topology for testing purposes.
     * 
     * @return A map of node ID to cluster node.
     */
    private Map<String, ValkeyClusterNode> createSampleClusterTopology() {
        Map<String, ValkeyClusterNode> nodes = new HashMap<>();
        
        // Create a sample master node
        ValkeyClusterNode masterNode = ValkeyClusterNode.newValkeyClusterNode()
            .listeningAt("127.0.0.1", 7000)
            .withId("node-1")
            .promotedAs(NodeType.MASTER)
            .serving(new SlotRange(0, 5460))
            .linkState(LinkState.CONNECTED)
            .build();
        
        nodes.put(masterNode.getId(), masterNode);
        
        // Create a sample replica node
        ValkeyClusterNode replicaNode = ValkeyClusterNode.newValkeyClusterNode()
            .listeningAt("127.0.0.1", 7001)
            .withId("node-2")
            .promotedAs(NodeType.REPLICA)
            .replicaOf("node-1")
            .linkState(LinkState.CONNECTED)
            .build();
        
        nodes.put(replicaNode.getId(), replicaNode);
        
        return nodes;
    }
}

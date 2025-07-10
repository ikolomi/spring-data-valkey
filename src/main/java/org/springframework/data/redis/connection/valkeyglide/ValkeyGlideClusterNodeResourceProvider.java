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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.dao.DataAccessException;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Provider for Redis Cluster node resources using Valkey-Glide.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideClusterNodeResourceProvider {

    private final Object clusterClient;
    private final ValkeyGlideExceptionConverter exceptionConverter;
    private final long timeout;
    
    private final Map<String, Object> resources = new ConcurrentHashMap<>();

    /**
     * Creates a new {@link ValkeyGlideClusterNodeResourceProvider}.
     *
     * @param clusterClient The Valkey-Glide cluster client, must not be {@literal null}.
     * @param timeout Command timeout in milliseconds.
     * @param exceptionConverter Exception converter, must not be {@literal null}.
     */
    public ValkeyGlideClusterNodeResourceProvider(Object clusterClient, long timeout, ValkeyGlideExceptionConverter exceptionConverter) {
        Assert.notNull(clusterClient, "ClusterClient must not be null!");
        Assert.notNull(exceptionConverter, "ExceptionConverter must not be null!");
        
        this.clusterClient = clusterClient;
        this.timeout = timeout;
        this.exceptionConverter = exceptionConverter;
    }

    /**
     * Gets a cluster node resource by ID, or creates it if it doesn't exist.
     *
     * @param node The cluster node.
     * @param resourceFactory The factory to create the resource.
     * @return The resource.
     */
    @SuppressWarnings("unchecked")
    public <T> T getResourceForNode(RedisClusterNode node, Function<RedisClusterNode, T> resourceFactory) {
        Assert.notNull(node, "RedisClusterNode must not be null!");
        Assert.notNull(resourceFactory, "ResourceFactory must not be null!");
        
        String key = createNodeKey(node);
        
        if (resources.containsKey(key)) {
            return (T) resources.get(key);
        }
        
        synchronized (resources) {
            if (resources.containsKey(key)) {
                return (T) resources.get(key);
            }
            
            T resource = resourceFactory.apply(node);
            resources.put(key, resource);
            return resource;
        }
    }

    /**
     * Gets a cluster node resource by ID, or creates it if it doesn't exist.
     *
     * @param node The cluster node.
     * @return The resource, or {@literal null} if not found and no factory is provided.
     */
    @Nullable
    @SuppressWarnings("unchecked")
    public <T> T getResourceForNode(RedisClusterNode node) {
        Assert.notNull(node, "RedisClusterNode must not be null!");
        
        String key = createNodeKey(node);
        return (T) resources.get(key);
    }

    /**
     * Closes a resource for a node.
     *
     * @param node The cluster node.
     */
    public void closeResourceForNode(RedisClusterNode node) {
        Assert.notNull(node, "RedisClusterNode must not be null!");
        
        String key = createNodeKey(node);
        Object resource = resources.remove(key);
        
        if (resource != null) {
            try {
                // In the actual implementation, we would close the resource
                // For now, just do nothing
            } catch (Exception ex) {
                throw exceptionConverter.convert(ex);
            }
        }
    }

    /**
     * Closes all resources.
     */
    public void closeAllResources() {
        for (Object resource : resources.values()) {
            try {
                // In the actual implementation, we would close the resource
                // For now, just do nothing
            } catch (Exception ex) {
                throw exceptionConverter.convert(ex);
            }
        }
        
        resources.clear();
    }

    /**
     * Creates a key for a node.
     *
     * @param node The cluster node.
     * @return The key.
     */
    private String createNodeKey(RedisClusterNode node) {
        return node.getId();
    }

    /**
     * Gets the cluster client.
     *
     * @return The cluster client.
     */
    public Object getClusterClient() {
        return clusterClient;
    }
}

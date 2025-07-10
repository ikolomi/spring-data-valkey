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

import java.time.Duration;

import org.springframework.dao.DataAccessException;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ValkeyGlideConnectionProvider} that creates new
 * connections based on the client configuration.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class DefaultValkeyGlideConnectionProvider implements ValkeyGlideConnectionProvider {

    private final ValkeyGlideClientConfiguration clientConfig;

    /**
     * Create a new {@link DefaultValkeyGlideConnectionProvider} with the given {@link ValkeyGlideClientConfiguration}.
     *
     * @param clientConfig must not be {@literal null}.
     */
    public DefaultValkeyGlideConnectionProvider(ValkeyGlideClientConfiguration clientConfig) {
        Assert.notNull(clientConfig, "ValkeyGlideClientConfiguration must not be null!");
        this.clientConfig = clientConfig;
    }

    @Override
    public ValkeyGlideConnection getConnection(Object client) {
        Assert.notNull(client, "Client must not be null!");
        
        try {
            // In the actual implementation, we would use the client to create a connection
            // For now, create a simple connection with a timeout
            Duration timeout = getClientTimeout();
            long timeoutMillis = timeout != null ? timeout.toMillis() : 60000;
            
            // Pass 'this' as the connection provider instead of null
            return new ValkeyGlideConnection(client, timeoutMillis, this);
        } catch (Exception ex) {
            throw new DataAccessException("Failed to get Valkey-Glide connection", ex) {};
        }
    }

    @Override
    public void release(Object connection) {
        // In the actual implementation, we would close or return the connection to a pool
        // For now, this is a no-op
    }
    
    @Override
    public Object getConnectionClient() {
        // In the actual implementation, we would return a client instance
        // For now, return null as a placeholder
        return null;
    }

    /**
     * Gets the client timeout from the configuration.
     * 
     * @return the client timeout
     */
    private Duration getClientTimeout() {
        return clientConfig.getCommandTimeout();
    }
}

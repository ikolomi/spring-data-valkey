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
package io.valkey.springframework.data.valkey.connection.valkeyglide;

/**
 * Interface for providers creating and releasing {@link ValkeyGlideConnection}s.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public interface ValkeyGlideConnectionProvider {
    
    /**
     * Gets a {@link ValkeyGlideConnection} for the given client.
     * 
     * @param client the client to get a connection for
     * @return the connection
     */
    ValkeyGlideConnection getConnection(Object client);
    
    /**
     * Releases a previously acquired connection.
     * 
     * @param connection the connection to release
     */
    void release(Object connection);
    
    /**
     * Gets the connection client used by this provider.
     * 
     * @return the connection client
     */
    Object getConnectionClient();
}

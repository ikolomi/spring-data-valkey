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
import java.util.Optional;

import org.springframework.data.redis.connection.RedisConfiguration;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.lang.Nullable;

/**
 * Configuration interface for Valkey-Glide client settings.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public interface ValkeyGlideClientConfiguration {

    /**
     * Get the hostname used to connect to Redis.
     * 
     * @return the hostname.
     */
    Optional<String> getHostName();
    
    /**
     * Get the port used to connect to Redis.
     * 
     * @return the port.
     */
    Integer getPort();
    
    /**
     * Get the password used to connect to Redis.
     * 
     * @return the password.
     */
    Optional<RedisPassword> getPassword();
    
    /**
     * Get the database index.
     * 
     * @return the database index.
     */
    int getDatabase();

    /**
     * Get the command timeout for Valkey-Glide client operations.
     * 
     * @return The command timeout duration. May be {@literal null} if not set.
     */
    @Nullable
    Duration getCommandTimeout();
    
    /**
     * Check if cluster mode is enabled.
     * 
     * @return {@literal true} if cluster mode is enabled.
     */
    boolean isClusterAware();

    /**
     * Get the client name.
     * 
     * @return the client name, if set.
     */
    Optional<String> getClientName();

    /**
     * Get the shutdown timeout to use when closing resources.
     * 
     * @return the shutdown timeout.
     */
    Duration getShutdownTimeout();

    /**
     * Get the pool size to use.
     * 
     * @return the pool size, if set.
     */
    Optional<Integer> getPoolSize();
    
    /**
     * Check if SSL is enabled.
     * 
     * @return {@literal true} if SSL is enabled.
     */
    boolean isUseSsl();
}

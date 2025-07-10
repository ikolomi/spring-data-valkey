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

import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ValkeyGlideClientConfiguration}.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class DefaultValkeyGlideClientConfiguration implements ValkeyGlideClientConfiguration {

    private final RedisStandaloneConfiguration standaloneConfig;
    private final @Nullable Duration commandTimeout;
    private final boolean isClusterAware;
    private final Duration shutdownTimeout;
    private final Optional<String> clientName;
    private final Optional<Integer> poolSize;
    private final boolean useSsl;

    /**
     * Creates a new {@link DefaultValkeyGlideClientConfiguration} with default settings.
     */
    protected DefaultValkeyGlideClientConfiguration() {
        this(new RedisStandaloneConfiguration(), null, false, Duration.ofMillis(100), Optional.empty(), Optional.empty(), false);
    }

    /**
     * Creates a new {@link DefaultValkeyGlideClientConfiguration}.
     *
     * @param standaloneConfig must not be {@literal null}
     * @param commandTimeout can be {@literal null}
     * @param isClusterAware use cluster aware connections
     * @param shutdownTimeout must not be {@literal null}
     * @param clientName can be {@literal null}
     * @param poolSize can be {@literal null}
     * @param useSsl use SSL
     */
    protected DefaultValkeyGlideClientConfiguration(RedisStandaloneConfiguration standaloneConfig, @Nullable Duration commandTimeout,
            boolean isClusterAware, Duration shutdownTimeout, Optional<String> clientName, Optional<Integer> poolSize,
            boolean useSsl) {

        Assert.notNull(standaloneConfig, "RedisStandaloneConfiguration must not be null!");
        Assert.notNull(shutdownTimeout, "ShutdownTimeout must not be null!");
        Assert.notNull(clientName, "ClientName must not be null!");
        Assert.notNull(poolSize, "PoolSize must not be null!");

        this.standaloneConfig = standaloneConfig;
        this.commandTimeout = commandTimeout;
        this.isClusterAware = isClusterAware;
        this.shutdownTimeout = shutdownTimeout;
        this.clientName = clientName;
        this.poolSize = poolSize;
        this.useSsl = useSsl;
    }

    /**
     * Creates a new {@link ValkeyGlideClientConfigurationBuilder}.
     *
     * @return a new {@link ValkeyGlideClientConfigurationBuilder}.
     */
    public static ValkeyGlideClientConfigurationBuilder builder() {
        return new ValkeyGlideClientConfigurationBuilder();
    }

    /**
     * Creates a new {@link DefaultValkeyGlideClientConfiguration} with the given hostname and port.
     *
     * @param hostname the hostname
     * @param port the port
     * @return a new {@link DefaultValkeyGlideClientConfiguration}
     */
    public static DefaultValkeyGlideClientConfiguration create(String hostname, int port) {
        return builder()
                .hostName(hostname)
                .port(port)
                .build();
    }

    @Override
    public Optional<String> getHostName() {
        return Optional.of(standaloneConfig.getHostName());
    }

    @Override
    public Integer getPort() {
        return standaloneConfig.getPort();
    }

    @Override
    public Optional<RedisPassword> getPassword() {
        return Optional.of(standaloneConfig.getPassword());
    }

    @Override
    public int getDatabase() {
        return standaloneConfig.getDatabase();
    }

    @Nullable
    @Override
    public Duration getCommandTimeout() {
        return commandTimeout;
    }

    @Override
    public boolean isClusterAware() {
        return isClusterAware;
    }

    @Override
    public Optional<String> getClientName() {
        return clientName;
    }

    @Override
    public Duration getShutdownTimeout() {
        return shutdownTimeout;
    }

    @Override
    public Optional<Integer> getPoolSize() {
        return poolSize;
    }

    @Override
    public boolean isUseSsl() {
        return useSsl;
    }

    /**
     * Builder for {@link DefaultValkeyGlideClientConfiguration}.
     */
    public static class ValkeyGlideClientConfigurationBuilder {

        private RedisStandaloneConfiguration standaloneConfig = new RedisStandaloneConfiguration();
        private @Nullable Duration commandTimeout;
        private boolean isClusterAware;
        private Duration shutdownTimeout = Duration.ofMillis(100);
        private Optional<String> clientName = Optional.empty();
        private Optional<Integer> poolSize = Optional.empty();
        private boolean useSsl;

        /**
         * Sets the hostname.
         *
         * @param host the hostname
         * @return the builder
         */
        public ValkeyGlideClientConfigurationBuilder hostName(String host) {
            Assert.hasText(host, "Host must not be empty or null!");
            standaloneConfig.setHostName(host);
            return this;
        }

        /**
         * Sets the port.
         *
         * @param port the port
         * @return the builder
         */
        public ValkeyGlideClientConfigurationBuilder port(int port) {
            Assert.isTrue(port >= 0, "Port must be greater or equal to 0!");
            standaloneConfig.setPort(port);
            return this;
        }

        /**
         * Sets the password.
         *
         * @param password the password
         * @return the builder
         */
        public ValkeyGlideClientConfigurationBuilder password(String password) {
            Assert.hasText(password, "Password must not be null!");
            standaloneConfig.setPassword(RedisPassword.of(password));
            return this;
        }

        /**
         * Sets the password.
         *
         * @param password the password
         * @return the builder
         */
        public ValkeyGlideClientConfigurationBuilder password(RedisPassword password) {
            Assert.notNull(password, "Password must not be null!");
            standaloneConfig.setPassword(password);
            return this;
        }

        /**
         * Sets the database index.
         *
         * @param index the database index
         * @return the builder
         */
        public ValkeyGlideClientConfigurationBuilder database(int index) {
            Assert.isTrue(index >= 0, "Invalid database index: " + index);
            standaloneConfig.setDatabase(index);
            return this;
        }

        /**
         * Sets the command timeout.
         *
         * @param timeout the command timeout
         * @return the builder
         */
        public ValkeyGlideClientConfigurationBuilder commandTimeout(Duration timeout) {
            Assert.notNull(timeout, "Timeout must not be null!");
            this.commandTimeout = timeout;
            return this;
        }

        /**
         * Enables cluster support.
         *
         * @return the builder
         */
        public ValkeyGlideClientConfigurationBuilder useCluster() {
            this.isClusterAware = true;
            return this;
        }

        /**
         * Sets the client name.
         *
         * @param name the client name
         * @return the builder
         */
        public ValkeyGlideClientConfigurationBuilder clientName(String name) {
            Assert.hasText(name, "Client name must not be empty or null!");
            this.clientName = Optional.of(name);
            return this;
        }

        /**
         * Sets the shutdown timeout.
         *
         * @param timeout the shutdown timeout
         * @return the builder
         */
        public ValkeyGlideClientConfigurationBuilder shutdownTimeout(Duration timeout) {
            Assert.notNull(timeout, "Timeout must not be null!");
            this.shutdownTimeout = timeout;
            return this;
        }

        /**
         * Sets the connection pool size.
         *
         * @param poolSize the pool size
         * @return the builder
         */
        public ValkeyGlideClientConfigurationBuilder poolSize(int poolSize) {
            Assert.isTrue(poolSize > 0, "Pool size must be greater than 0!");
            this.poolSize = Optional.of(poolSize);
            return this;
        }

        /**
         * Enables SSL.
         *
         * @return the builder
         */
        public ValkeyGlideClientConfigurationBuilder useSsl() {
            this.useSsl = true;
            return this;
        }

        /**
         * Creates a new {@link DefaultValkeyGlideClientConfiguration}.
         *
         * @return a new {@link DefaultValkeyGlideClientConfiguration}
         */
        public DefaultValkeyGlideClientConfiguration build() {
            return new DefaultValkeyGlideClientConfiguration(standaloneConfig, commandTimeout, isClusterAware,
                    shutdownTimeout, clientName, poolSize, useSsl);
        }
    }
}

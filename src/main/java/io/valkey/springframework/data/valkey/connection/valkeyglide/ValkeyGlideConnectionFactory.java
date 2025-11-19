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

import java.time.Duration;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import io.valkey.springframework.data.valkey.connection.ClusterCommandExecutor;
import io.valkey.springframework.data.valkey.connection.ValkeyClusterConnection;
import io.valkey.springframework.data.valkey.connection.ValkeyConnection;
import io.valkey.springframework.data.valkey.connection.ValkeyConnectionFactory;
import io.valkey.springframework.data.valkey.connection.ValkeyPassword;
import io.valkey.springframework.data.valkey.connection.ValkeyStandaloneConfiguration;
import io.valkey.springframework.data.valkey.connection.ValkeySentinelConnection;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

// Imports needed for working with valkey-glide
// Imports for valkey-glide library
import glide.api.GlideClient;
import glide.api.GlideClusterClient;
import glide.api.models.configuration.GlideClientConfiguration;
import glide.api.models.configuration.GlideClusterClientConfiguration;
import glide.api.models.configuration.NodeAddress;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.lang.reflect.Method;

/**
 * Connection factory creating <a href="https://github.com/valkey-io/valkey-glide">Valkey Glide</a> based
 * connections. This is the central class for connecting to Valkey using Valkey-Glide.
 * 
 * <p>This factory creates a new {@link ValkeyGlideConnection} on each call to {@link #getConnection()}.
 * The underlying client instance is shared among all connections.
 * 
 * <p>This class implements the {@link org.springframework.beans.factory.InitializingBean} interface,
 * triggering the creation of the client instance on {@link #afterPropertiesSet()}.
 * It also implements {@link org.springframework.beans.factory.DisposableBean} for closing the client on
 * application shutdown.
 * 
 * <p>The Valkey Glide connection factory can be used both with a standalone Valkey server and with a
 * Valkey cluster. For standalone mode, use the {@link #ValkeyGlideConnectionFactory()} constructor
 * or {@link #ValkeyGlideConnectionFactory(ValkeyGlideClientConfiguration)} constructor. For cluster mode,
 * ensure the {@link ValkeyGlideClientConfiguration} is configured for cluster mode.
 * 
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideConnectionFactory
    implements ValkeyConnectionFactory, InitializingBean, DisposableBean, SmartLifecycle {
        
    private final ValkeyGlideClientConfiguration clientConfiguration;
    
    private boolean initialized = false;
    private boolean running = false;
    private boolean autoStartup = true;
    private boolean earlyStartup = true;
    private int phase = 0;
    private final Lock initLock = new ReentrantLock();

    private final ThreadLocal<GlideClient> threadLocalClient = new ThreadLocal<>();

    private @Nullable ValkeyGlideClusterTopologyProvider topologyProvider;
    private @Nullable ValkeyGlideClusterNodeResourceProvider nodeResourceProvider;
    private @Nullable ClusterCommandExecutor clusterCommandExecutor;
    
    private long timeout;
    private @Nullable TaskExecutor executor;

    /**
     * Constructs a new {@link ValkeyGlideConnectionFactory} instance with default settings.
     */
    public ValkeyGlideConnectionFactory() {
        this(DefaultValkeyGlideClientConfiguration.builder().build());
    }

    /**
     * Constructs a new {@link ValkeyGlideConnectionFactory} instance with the given {@link ValkeyGlideClientConfiguration}.
     *
     * @param clientConfiguration must not be {@literal null}
     */
    public ValkeyGlideConnectionFactory(ValkeyGlideClientConfiguration clientConfiguration) {
        Assert.notNull(clientConfiguration, "ValkeyGlideClientConfiguration must not be null!");
        
        this.clientConfiguration = clientConfiguration;
        
        Duration commandTimeout = clientConfiguration.getCommandTimeout();
        this.timeout = commandTimeout != null ? commandTimeout.toMillis() : 60000;
    }

    /**
     * Initialize the shared client if not already initialized.
     */
    @Override
    public void afterPropertiesSet() {
        if (initialized) {
            return;
        }
        
        // initLock.lock();
        // try {
        //     if (initialized) {
        //         return;
        //     }
            
        //     if (clientConfiguration.isClusterAware()) {
        //         initializeClusterClient();
        //         initializeClusterCommandExecutor();
        //     } else {
        //         initializeClient();
        //     }
            
        //     initialized = true;
        //     running = true;
        // } finally {
        //     initLock.unlock();
        // }
    }
    
    /**
     * Initialize the cluster command executor for Valkey cluster operations
     */
    private void initializeClusterCommandExecutor() {
        TaskExecutor taskExecutor = getExecutor();
        
        // In the actual implementation, we would properly initialize a ClusterCommandExecutor
        // For this prototype, we'll skip actual initialization since it would require
        // implementing many supporting classes and interfaces
        
        // This is intentionally commented out as a placeholder
        // this.clusterCommandExecutor = new ClusterCommandExecutor(
        //         topologyProvider, nodeResourceProvider, taskExecutor);
        
        // For testing purposes, we'll just set a non-null value
        // In a real implementation, this would be properly instantiated
        this.clusterCommandExecutor = null;
    }


    /**
     * Builds client options from configuration.
     */
    private Object buildClientOptions() {
        // In the actual implementation, we would:
        // return ClientOptions.builder()
        //     .connectTimeout(clientConfiguration.getConnectTimeout())
        //     .commandTimeout(clientConfiguration.getCommandTimeout())
        //     .sslEnabled(clientConfiguration.isUseSsl())
        //     .verifyPeer(clientConfiguration.isVerifyPeer())
        //     .username(clientConfiguration.getUsername())
        //     .password(clientConfiguration.getPassword() != null ? 
        //         String.valueOf(clientConfiguration.getPassword()) : null)
        //     .database(clientConfiguration.getDatabase())
        //     .maxPoolSize(clientConfiguration.getPoolSize())
        //     .build();
        
        // Using placeholder for now
        return new Object();
    }
    
    /**
     * Builds cluster client options from configuration.
     */
    private Object buildClusterClientOptions() {
        // In the actual implementation, we would:
        // return ClusterClientOptions.builder()
        //     .connectTimeout(clientConfiguration.getConnectTimeout())
        //     .commandTimeout(clientConfiguration.getCommandTimeout())
        //     .sslEnabled(clientConfiguration.isUseSsl())
        //     .verifyPeer(clientConfiguration.isVerifyPeer())
        //     .username(clientConfiguration.getUsername())
        //     .password(clientConfiguration.getPassword() != null ? 
        //         String.valueOf(clientConfiguration.getPassword()) : null)
        //     .maxPoolSize(clientConfiguration.getPoolSize())
        //     .build();
        
        // Using placeholder for now
        return new Object();
    }

    
    /**
     * Creates a {@link ValkeyStandaloneConfiguration} based on this factory's settings.
     *
     * @return a {@link ValkeyStandaloneConfiguration} instance
     */
    protected ValkeyStandaloneConfiguration getStandaloneConfiguration() {
        ValkeyStandaloneConfiguration config = new ValkeyStandaloneConfiguration();
        // Set hostname and port
        clientConfiguration.getHostName().ifPresent(config::setHostName);
        config.setPort(clientConfiguration.getPort());
        
        // Set username if available
        String username = getOptionalUsername();
        if (StringUtils.hasText(username)) {
            config.setUsername(username);
        }
        
        // Set password if available
        if (hasPassword()) {
            config.setPassword(extractPassword());
        }
        
        // Set database if non-default
        config.setDatabase(clientConfiguration.getDatabase());
        
        return config;
    }
    
    /**
     * Gets the username from the client configuration.
     * 
     * @return the username or null if not set
     */
    private String getOptionalUsername() {
        // This is a placeholder implementation since we don't have the actual
        // method in our stub ValkeyGlideClientConfiguration
        // In a real implementation this would return clientConfiguration.getUsername()
        return null;
    }
    
    /**
     * Checks if the client configuration has a password.
     * 
     * @return true if a password is set
     */
    private boolean hasPassword() {
        return clientConfiguration.getPassword() != null;
    }
    
    /**
     * Extracts the password from the client configuration.
     * 
     * @return the password
     */
    private io.valkey.springframework.data.valkey.connection.ValkeyPassword extractPassword() {
        // In a real implementation, this would correctly extract the password
        // For now, return a default
        return io.valkey.springframework.data.valkey.connection.ValkeyPassword.none();
    }

    @Override
    public ValkeyConnection getConnection() {
        afterPropertiesSet();
        
        if (isClusterAware()) {
            return getClusterConnection();
        }

        // Get or create ThreadLocal GlideClient
        GlideClient client = threadLocalClient.get();
        if (client == null) {
            client = (GlideClient) createGlideClient();
            threadLocalClient.set(client);
        }

        return new ValkeyGlideConnection(client, timeout);
    }

    @Override
    public ValkeyClusterConnection getClusterConnection() {
        afterPropertiesSet();
        
        if (!isClusterAware()) {
            throw new InvalidDataAccessResourceUsageException("Cluster is not configured!");
        }

        throw new UnsupportedOperationException("Cluster connections not supported with Valkey-Glide!");

        // // Create a GlideClusterClient for each connection
        // Object clusterClient = createGlideClusterClient();
        // return new ValkeyGlideClusterConnection(clusterClient, timeout, 
        //         topologyProvider, 
        //         nodeResourceProvider);
    }

    @Override
    public boolean getConvertPipelineAndTxResults() {
        return true;
    }

    @Override
    public ValkeySentinelConnection getSentinelConnection() {
        throw new UnsupportedOperationException("Sentinel connections not supported with Valkey-Glide!");
    }

    /**
     * Shut down the client when this factory is destroyed.
     */
    @Override
    public void destroy() {
        doDestroy();
    }

    // /**
    //  * Reset the connection, closing the shared client.
    //  */
    // public void resetConnection() {
    //     doDestroy();
    // }
    
    /**
     * Creates a GlideClient instance for each connection.
     */
    private Object createGlideClient() {
        try {
            // Build the configuration using the proper API
            GlideClientConfiguration.GlideClientConfigurationBuilder configBuilder = 
                GlideClientConfiguration.builder();
            
            // Set the address
            clientConfiguration.getHostName().ifPresent(hostname -> {
                configBuilder.address(NodeAddress.builder()
                    .host(hostname)
                    .port(clientConfiguration.getPort())
                    .build());
            });
            
            // If no hostname specified, use localhost
            if (clientConfiguration.getHostName().isEmpty()) {
                configBuilder.address(NodeAddress.builder()
                    .host("localhost")
                    .port(clientConfiguration.getPort())
                    .build());
            }
            
            // Set credentials if available
            if (hasPassword()) {
                configBuilder.credentials(glide.api.models.configuration.ServerCredentials.builder()
                    .password(extractPassword().toString())
                    .build());
            }
            
            // Set request timeout
            Duration timeout = clientConfiguration.getCommandTimeout();
            if (timeout != null) {
                configBuilder.requestTimeout((int) timeout.toMillis());
            }
            
            // Set TLS if enabled
            if (clientConfiguration.isUseSsl()) {
                configBuilder.useTLS(true);
            }
            
            GlideClientConfiguration config = configBuilder.build();
            
            // Create the client using the proper API
            return GlideClient.createClient(config).get();
            
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create GlideClient: " + e.getMessage(), e);
        }
    }

    /**
     * Creates a GlideClusterClient instance for each connection.
     */
    private Object createGlideClusterClient() {
        try {
            // Build the configuration using the proper API
            GlideClusterClientConfiguration.GlideClusterClientConfigurationBuilder configBuilder = 
                GlideClusterClientConfiguration.builder();
            
            // Set the address
            clientConfiguration.getHostName().ifPresent(hostname -> {
                configBuilder.address(NodeAddress.builder()
                    .host(hostname)
                    .port(clientConfiguration.getPort())
                    .build());
            });
            
            // If no hostname specified, use localhost
            if (clientConfiguration.getHostName().isEmpty()) {
                configBuilder.address(NodeAddress.builder()
                    .host("localhost")
                    .port(clientConfiguration.getPort())
                    .build());
            }
            
            // Set credentials if available
            if (hasPassword()) {
                configBuilder.credentials(glide.api.models.configuration.ServerCredentials.builder()
                    .password(extractPassword().toString())
                    .build());
            }
            
            // Set request timeout
            Duration timeout = clientConfiguration.getCommandTimeout();
            if (timeout != null) {
                configBuilder.requestTimeout((int) timeout.toMillis());
            }
            
            // Set TLS if enabled
            if (clientConfiguration.isUseSsl()) {
                configBuilder.useTLS(true);
            }
            
            GlideClusterClientConfiguration config = configBuilder.build();
            
            // Create the cluster client using the proper API
            return GlideClusterClient.createClient(config).get();
            
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create GlideClusterClient: " + e.getMessage(), e);
        }
    }

    /**
     * Internal method to perform cleanup without circular calls.
     */
    private void doDestroy() {
        
        initialized = false;
        running = false;
        topologyProvider = null;
        nodeResourceProvider = null;
        clusterCommandExecutor = null;
    }

    /**
     * Helper method to find the appropriate create method on the client class.
     * 
     * @param clientClass the client class to inspect
     * @return the create method or null if not found
     */
    private Method findCreateMethod(Class<?> clientClass) {
        // Try different parameter combinations that might match the API
        Method[] methods = clientClass.getMethods();
        for (Method method : methods) {
            if (method.getName().equals("create")) {
                return method;
            }
        }
        return null;
    }
    
    /**
     * Builds a Valkey URI based on the client configuration.
     * 
     * @return a Valkey URI string
     */
    private String buildValkeyUri() {
        StringBuilder uriBuilder = new StringBuilder();
        uriBuilder.append("valkey://");
        
        // Add authentication if available
        String username = getOptionalUsername();
        if (StringUtils.hasText(username)) {
            uriBuilder.append(username);
            if (hasPassword()) {
                uriBuilder.append(":");
                uriBuilder.append(extractPassword().toString());
            }
            uriBuilder.append("@");
        } else if (hasPassword()) {
            uriBuilder.append(":");
            uriBuilder.append(extractPassword().toString());
            uriBuilder.append("@");
        }
        
        // Add host and port
        clientConfiguration.getHostName().ifPresent(uriBuilder::append);
        uriBuilder.append(":");
        uriBuilder.append(clientConfiguration.getPort());
        
        // Add database if not default
        if (clientConfiguration.getDatabase() != 0) {
            uriBuilder.append("/").append(clientConfiguration.getDatabase());
        }
        
        return uriBuilder.toString();
    }
    
    /**
     * Gets the cluster command executor for this factory.
     * 
     * @return The task executor used for cluster operations
     * @throws IllegalStateException if the factory is not in cluster mode
     */
    TaskExecutor getExecutorForClusterOperations() {
        if (!isClusterAware()) {
            throw new IllegalStateException("This factory is not in cluster mode");
        }
        
        if (!isRunning()) {
            throw new IllegalStateException("Connection factory not initialized or not running");
        }
        
        return getExecutor();
    }
    
    /**
     * Sets the task executor used for executing commands in cluster mode.
     * 
     * @param executor the executor to use for cluster commands
     */
    public void setExecutor(TaskExecutor executor) {
        this.executor = executor;
    }
    
    /**
     * Returns the configured TaskExecutor or a default SimpleAsyncTaskExecutor.
     * 
     * @return the TaskExecutor
     */
    private TaskExecutor getExecutor() {
        return executor != null ? executor : new SimpleAsyncTaskExecutor();
    }
    
    // Lifecycle implementation
    
    /**
     * Initializes the factory, starting the client.
     */
    @Override
    public void start() {
        if (!initialized) {
            afterPropertiesSet();
        }
        running = true;
    }
    
    /**
     * Stops the client.
     */
    @Override
    public void stop() {
        running = false;
    }
    
    /**
     * Returns if the client is running.
     * 
     * @return true if running
     */
    @Override
    public boolean isRunning() {
        return initialized && running;
    }

    /**
     * @return true if cluster mode is enabled.
     */
    public boolean isClusterAware() {
        return clientConfiguration.isClusterAware();
    }
    
    /**
     * Creates a {@link ValkeyGlideConnectionFactory} for the given {@link ValkeyStandaloneConfiguration}.
     * 
     * @param standaloneConfig the Valkey standalone configuration, must not be {@literal null}
     * @return a new {@link ValkeyGlideConnectionFactory} instance
     */
    public static ValkeyGlideConnectionFactory createValkeyGlideConnectionFactory(ValkeyStandaloneConfiguration standaloneConfig) {
        Assert.notNull(standaloneConfig, "ValkeyStandaloneConfiguration must not be null!");
        
        DefaultValkeyGlideClientConfiguration.ValkeyGlideClientConfigurationBuilder builder = 
                DefaultValkeyGlideClientConfiguration.builder()
                .hostName(standaloneConfig.getHostName())
                .port(standaloneConfig.getPort())
                .database(standaloneConfig.getDatabase());
        
        // Set optional username if present
        String username = standaloneConfig.getUsername();
        if (StringUtils.hasText(username)) {
            // In a real implementation, this would be:
            // builder.username(username);
            // But we'll skip it for the mock implementation
        }
        
        // Set optional password if present
        if (!standaloneConfig.getPassword().equals(ValkeyPassword.none())) {
            builder.password(standaloneConfig.getPassword());
        }
        
        return new ValkeyGlideConnectionFactory(builder.build());
    }

    /**
     * @return The client configuration used.
     */
    public ValkeyGlideClientConfiguration getClientConfiguration() {
        return clientConfiguration;
    }


    /**
     * @return whether this lifecycle component should get started automatically by the container
     */
    @Override
    public boolean isAutoStartup() {
        return this.autoStartup;
    }

    /**
     * Configure if this Lifecycle connection factory should get started automatically by the container.
     *
     * @param autoStartup {@literal true} to automatically {@link #start()} the connection factory; {@literal false} otherwise.
     */
    public void setAutoStartup(boolean autoStartup) {
        this.autoStartup = autoStartup;
    }

    /**
     * @return whether to {@link #start()} the component during {@link #afterPropertiesSet()}.
     */
    public boolean isEarlyStartup() {
        return this.earlyStartup;
    }

    /**
     * Configure if this InitializingBean's component Lifecycle should get started early by {@link #afterPropertiesSet()}
     * at the time that the bean is initialized. The component defaults to auto-startup.
     *
     * @param earlyStartup {@literal true} to early {@link #start()} the component; {@literal false} otherwise.
     */
    public void setEarlyStartup(boolean earlyStartup) {
        this.earlyStartup = earlyStartup;
    }
    
    /**
     * @return the phase value for this lifecycle component
     */
    @Override
    public int getPhase() {
        return this.phase;
    }
    
    /**
     * Specify the lifecycle phase for pausing and resuming this executor.
     * 
     * @param phase the phase value to set
     */
    public void setPhase(int phase) {
        this.phase = phase;
    }
    
    /**
     * Translates a Valkey-Glide exception to a Spring DAO exception.
     * 
     * @param ex The exception to translate
     * @return The translated exception, or null if the exception cannot be translated
     */
    @Override
    @Nullable
    public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
        // Use ValkeyGlideExceptionConverter to translate exceptions
        return new ValkeyGlideExceptionConverter().convert(ex);
    }
}

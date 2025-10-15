/*
 * Copyright 2020-2025 the original author or authors.
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
package org.springframework.data.redis.connection.valkeyglide.extension;

import java.io.Closeable;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.valkeyglide.DefaultValkeyGlideClientConfiguration;
import org.springframework.data.redis.connection.valkeyglide.ValkeyGlideClientConfiguration;
import org.springframework.data.redis.connection.valkeyglide.ValkeyGlideConnectionFactory;
import org.springframework.data.redis.test.extension.RedisCluster;
import org.springframework.data.redis.test.extension.RedisSentinel;
import org.springframework.data.redis.test.extension.RedisStanalone;
import org.springframework.data.redis.test.extension.ShutdownQueue;
import org.springframework.data.util.Lazy;

/**
 * JUnit {@link ParameterResolver} providing pre-cached {@link ValkeyGlideConnectionFactory} instances. Connection factories
 * can be qualified with {@code @RedisStanalone} (default) or {@code @RedisCluster} to obtain a specific factory instance.
 * Instances are managed by this extension and will be shut down on JVM shutdown.
 * 
 * <p><strong>Note:</strong> Sentinel configurations are not supported by Valkey-Glide and will throw an 
 * {@link UnsupportedOperationException}.
 *
 * @author Ilya Kolomin
 * @see RedisStanalone
 * @see RedisCluster
 */
public class ValkeyGlideConnectionFactoryExtension implements ParameterResolver {

	private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace
			.create(ValkeyGlideConnectionFactoryExtension.class);

	private static final NewableLazy<ValkeyGlideConnectionFactory> STANDALONE = NewableLazy.of(() -> {

		RedisStandaloneConfiguration standaloneConfig = SettingsUtils.standaloneConfiguration();
		ValkeyGlideClientConfiguration configuration = DefaultValkeyGlideClientConfiguration.builder()
				.hostName(standaloneConfig.getHostName())
				.port(standaloneConfig.getPort())
				.database(standaloneConfig.getDatabase())
				.password(standaloneConfig.getPassword())
				.build();

		ManagedValkeyGlideConnectionFactory factory = new ManagedValkeyGlideConnectionFactory(configuration);

		factory.afterPropertiesSet();
		factory.start();
		ShutdownQueue.register(factory);

		return factory;
	});

	private static final NewableLazy<ValkeyGlideConnectionFactory> CLUSTER = NewableLazy.of(() -> {

		RedisClusterConfiguration clusterConfig = SettingsUtils.clusterConfiguration();
		ValkeyGlideClientConfiguration configuration = DefaultValkeyGlideClientConfiguration.builder()
				.hostName(clusterConfig.getClusterNodes().iterator().next().getHost())
				.port(clusterConfig.getClusterNodes().iterator().next().getPort())
				.useCluster()
				.password(clusterConfig.getPassword())
				.build();

		ManagedValkeyGlideConnectionFactory factory = new ManagedValkeyGlideConnectionFactory(configuration);

		factory.afterPropertiesSet();
		factory.start();
		ShutdownQueue.register(factory);

		return factory;
	});

	private static final Map<Class<?>, NewableLazy<ValkeyGlideConnectionFactory>> factories;

	static {

		factories = new HashMap<>();
		factories.put(RedisStanalone.class, STANDALONE);
		factories.put(RedisCluster.class, CLUSTER);
		// Sentinel is not supported by Valkey-Glide - omitted intentionally
	}

	/**
	 * Obtain a cached {@link ValkeyGlideConnectionFactory} described by {@code qualifier}. Instances are managed by this
	 * extension and will be shut down on JVM shutdown.
	 *
	 * @param qualifier can be any of {@link RedisStanalone}, {@link RedisCluster}. {@link RedisSentinel} is not supported.
	 * @return the managed {@link ValkeyGlideConnectionFactory}.
	 * @throws UnsupportedOperationException if {@link RedisSentinel} is requested.
	 */
	public static ValkeyGlideConnectionFactory getConnectionFactory(Class<? extends Annotation> qualifier) {
		
		if (RedisSentinel.class.equals(qualifier)) {
			throw new UnsupportedOperationException("Sentinel connections are not supported with Valkey-Glide!");
		}
		
		NewableLazy<ValkeyGlideConnectionFactory> factory = factories.get(qualifier);
		if (factory == null) {
			throw new IllegalArgumentException("Unsupported qualifier: " + qualifier);
		}
		
		return factory.getNew();
	}

	/**
	 * Obtain a new {@link ValkeyGlideConnectionFactory} described by {@code qualifier}. Instances are managed by this extension
	 * and will be shut down on JVM shutdown.
	 *
	 * @param qualifier can be any of {@link RedisStanalone}, {@link RedisCluster}. {@link RedisSentinel} is not supported.
	 * @return the managed {@link ValkeyGlideConnectionFactory}.
	 * @throws UnsupportedOperationException if {@link RedisSentinel} is requested.
	 */
	public static ValkeyGlideConnectionFactory getNewConnectionFactory(Class<? extends Annotation> qualifier) {
		
		if (RedisSentinel.class.equals(qualifier)) {
			throw new UnsupportedOperationException("Sentinel connections are not supported with Valkey-Glide!");
		}
		
		NewableLazy<ValkeyGlideConnectionFactory> factory = factories.get(qualifier);
		if (factory == null) {
			throw new IllegalArgumentException("Unsupported qualifier: " + qualifier);
		}
		
		return factory.getNew();
	}

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {
		return RedisConnectionFactory.class.isAssignableFrom(parameterContext.getParameter().getType());
	}

	@Override
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {

		ExtensionContext.Store store = extensionContext.getStore(NAMESPACE);

		Class<? extends Annotation> qualifier = getQualifier(parameterContext);

		return store.getOrComputeIfAbsent(qualifier, ValkeyGlideConnectionFactoryExtension::getConnectionFactory);
	}

	private static Class<? extends Annotation> getQualifier(ParameterContext parameterContext) {

		if (parameterContext.isAnnotated(RedisSentinel.class)) {
			// Explicitly throw exception for sentinel requests
			throw new UnsupportedOperationException("Sentinel connections are not supported with Valkey-Glide!");
		}

		if (parameterContext.isAnnotated(RedisCluster.class)) {
			return RedisCluster.class;
		}

		return RedisStanalone.class;
	}

	static class NewableLazy<T> {

		private final Lazy<? extends T> lazy;

		private NewableLazy(Supplier<? extends T> supplier) {
			this.lazy = Lazy.of(supplier);
		}

		public static <T> NewableLazy<T> of(Supplier<? extends T> supplier) {
			return new NewableLazy<>(supplier);
		}

		public T getNew() {
			return lazy.get();
		}
	}

	static class ManagedValkeyGlideConnectionFactory extends ValkeyGlideConnectionFactory
			implements ConnectionFactoryTracker.Managed, Closeable {

		private volatile boolean mayClose;

		ManagedValkeyGlideConnectionFactory(ValkeyGlideClientConfiguration clientConfiguration) {
			super(clientConfiguration);
		}

		@Override
		public void destroy() {

			if (!mayClose) {
				throw new IllegalStateException(
						"Prematurely attempted to close ManagedValkeyGlideConnectionFactory; Shutdown hook didn't run yet which means that the test run isn't finished yet; Please fix the tests so that they don't close this connection factory.");
			}

			super.destroy();
		}

		@Override
		public String toString() {

			StringBuilder builder = new StringBuilder("ValkeyGlide");

			if (isClusterAware()) {
				builder.append(" Cluster");
			} else {
				builder.append(" Standalone");
			}

			return builder.toString();
		}

		@Override
		public void close() throws IOException {
			try {
				mayClose = true;
				destroy();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}
}

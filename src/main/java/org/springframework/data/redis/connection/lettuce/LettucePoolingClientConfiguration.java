/*
 * Copyright 2017-2025 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.resource.ClientResources;

import java.time.Duration;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import org.springframework.util.Assert;

/**
 * Redis client configuration for lettuce using a driver level pooled connection by adding pooling specific
 * configuration to {@link LettuceClientConfiguration}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Yanming Zhou
 * @since 2.0
 */
public interface LettucePoolingClientConfiguration extends LettuceClientConfiguration {

	/**
	 * @return the {@link GenericObjectPoolConfig}. Never {@literal null}.
	 */
	GenericObjectPoolConfig<StatefulConnection<?, ?>> getPoolConfig();

	/**
	 * Creates a new {@link LettucePoolingClientConfigurationBuilder} to build {@link LettucePoolingClientConfiguration}
	 * to be used with the Lettuce client.
	 *
	 * @return a new {@link LettucePoolingClientConfigurationBuilder} to build {@link LettucePoolingClientConfiguration}.
	 */
	static LettucePoolingClientConfigurationBuilder builder() {
		return new LettucePoolingClientConfigurationBuilder();
	}

	/**
	 * Creates a default {@link LettucePoolingClientConfiguration} with
	 * <dl>
	 * <dt>SSL</dt>
	 * <dd>no</dd>
	 * <dt>Peer Verification</dt>
	 * <dd>yes</dd>
	 * <dt>Start TLS</dt>
	 * <dd>no</dd>
	 * <dt>Client Options</dt>
	 * <dd>{@link ClientOptions} with enabled {@link io.lettuce.core.TimeoutOptions}</dd>
	 * <dt>Client Resources</dt>
	 * <dd>none</dd>
	 * <dt>Client name</dt>
	 * <dd>none</dd>
	 * <dt>Read From</dt>
	 * <dd>none</dd>
	 * <dt>Connect Timeout</dt>
	 * <dd>60 Seconds</dd>
	 * <dt>Shutdown Timeout</dt>
	 * <dd>100 Milliseconds</dd>
	 * <dt>Shutdown Quiet Period</dt>
	 * <dd>100 Milliseconds</dd>
	 * <dt>pool config</dt>
	 * <dd>default {@link GenericObjectPoolConfig}</dd>
	 * </dl>
	 *
	 * @return a {@link LettucePoolingClientConfiguration} with defaults.
	 */
	static LettucePoolingClientConfiguration defaultConfiguration() {
		return builder().build();
	}

	/**
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 * @author Longlong Zhao
	 */
	class LettucePoolingClientConfigurationBuilder extends LettuceClientConfigurationBuilder {

		GenericObjectPoolConfig<StatefulConnection<?, ?>> poolConfig = new GenericObjectPoolConfig<>();

		LettucePoolingClientConfigurationBuilder() {
			super();
		}

		@Override
		public LettucePoolingSslClientConfigurationBuilder useSsl() {

			super.useSsl();
			return new LettucePoolingSslClientConfigurationBuilder(this);
		}

		@Override
		public LettucePoolingClientConfigurationBuilder clientResources(ClientResources clientResources) {

			super.clientResources(clientResources);
			return this;
		}

		@Override
		public LettucePoolingClientConfigurationBuilder clientOptions(ClientOptions clientOptions) {

			super.clientOptions(clientOptions);
			return this;
		}

		@Override
		public LettucePoolingClientConfigurationBuilder commandTimeout(Duration timeout) {

			super.commandTimeout(timeout);
			return this;
		}

		@Override
		public LettucePoolingClientConfigurationBuilder shutdownTimeout(Duration shutdownTimeout) {

			super.shutdownTimeout(shutdownTimeout);
			return this;
		}

		@Override
		public LettucePoolingClientConfigurationBuilder shutdownQuietPeriod(Duration shutdownQuietPeriod) {

			super.shutdownQuietPeriod(shutdownQuietPeriod);
			return this;
		}

		@Override
		public LettucePoolingClientConfigurationBuilder readFrom(ReadFrom readFrom) {
			super.readFrom(readFrom);
			return this;
		}

		@Override
		public LettucePoolingClientConfigurationBuilder redisCredentialsProviderFactory(
				RedisCredentialsProviderFactory redisCredentialsProviderFactory) {
			super.redisCredentialsProviderFactory(redisCredentialsProviderFactory);
			return this;
		}

		@Override
		public LettucePoolingClientConfigurationBuilder clientName(String clientName) {
			super.clientName(clientName);
			return this;
		}

		/**
		 * Set the {@link GenericObjectPoolConfig} used by the driver.
		 *
		 * @param poolConfig must not be {@literal null}.
		 */
		public LettucePoolingClientConfigurationBuilder poolConfig(
				GenericObjectPoolConfig<StatefulConnection<?, ?>> poolConfig) {

			Assert.notNull(poolConfig, "PoolConfig must not be null");

			this.poolConfig = poolConfig;
			return this;
		}

		@Override
		public LettucePoolingClientConfiguration build() {
			return new DefaultLettucePoolingClientConfiguration(super.build(), poolConfig);
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	class LettucePoolingSslClientConfigurationBuilder extends LettuceSslClientConfigurationBuilder {

		LettucePoolingSslClientConfigurationBuilder(LettucePoolingClientConfigurationBuilder delegate) {
			super(delegate);
		}

		@Override
		public LettucePoolingClientConfigurationBuilder and() {
			return (LettucePoolingClientConfigurationBuilder) super.and();
		}

		@Override
		public LettucePoolingSslClientConfigurationBuilder disablePeerVerification() {

			super.disablePeerVerification();
			return this;
		}

		@Override
		public LettucePoolingSslClientConfigurationBuilder startTls() {

			super.startTls();
			return this;
		}

		@Override
		public LettucePoolingClientConfiguration build() {
			return (LettucePoolingClientConfiguration) super.build();
		}
	}
}

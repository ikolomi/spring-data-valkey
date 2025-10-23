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

import static org.assertj.core.api.Assertions.*;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import io.valkey.springframework.data.valkey.connection.ValkeyPassword;
import io.valkey.springframework.data.valkey.connection.ValkeyStandaloneConfiguration;

/**
 * Unit tests for {@link ValkeyGlideConnectionFactory}.
 *
 * @author Jeremy Parr-Pearson
 */
class ValkeyGlideConnectionFactoryUnitTests {

	@Test
	void testDefaultConfiguration() {
		ValkeyGlideConnectionFactory factory = new ValkeyGlideConnectionFactory();

		assertThat(factory.getClientConfiguration()).isNotNull();
		assertThat(factory.getClientConfiguration().getHostName()).hasValue("localhost");
		assertThat(factory.getClientConfiguration().getPort()).isEqualTo(6379);
		assertThat(factory.getClientConfiguration().getDatabase()).isEqualTo(0);
	}

	@Test
	void testClientConfiguration() {
		ValkeyGlideClientConfiguration clientConfig = DefaultValkeyGlideClientConfiguration.builder()
				.hostName("testhost")
				.port(7000)
				.database(2)
				.commandTimeout(Duration.ofSeconds(5))
				.build();
		ValkeyGlideConnectionFactory factory = new ValkeyGlideConnectionFactory(clientConfig);

		assertThat(factory.getClientConfiguration()).isEqualTo(clientConfig);
		assertThat(factory.getClientConfiguration().getHostName()).hasValue("testhost");
		assertThat(factory.getClientConfiguration().getPort()).isEqualTo(7000);
		assertThat(factory.getClientConfiguration().getDatabase()).isEqualTo(2);
		assertThat(factory.getClientConfiguration().getCommandTimeout()).isEqualTo(Duration.ofSeconds(5));
	}

	@Test
	void testStandaloneConfiguration() {
		ValkeyStandaloneConfiguration config = new ValkeyStandaloneConfiguration("myhost", 1234);
		config.setDatabase(5);
		config.setPassword("secret");
		ValkeyGlideConnectionFactory factory = new ValkeyGlideConnectionFactory(config);

		assertThat(factory.getClientConfiguration().getHostName()).hasValue("myhost");
		assertThat(factory.getClientConfiguration().getPort()).isEqualTo(1234);
		assertThat(factory.getClientConfiguration().getDatabase()).isEqualTo(5);
		assertThat(factory.getClientConfiguration().getPassword()).hasValue(ValkeyPassword.of("secret"));
	}

	@Test
	void testStandaloneAndClientConfiguration() {
		ValkeyStandaloneConfiguration standaloneConfig = new ValkeyStandaloneConfiguration("prodhost", 6380);
		standaloneConfig.setDatabase(3);
		standaloneConfig.setPassword("pass123");
		ValkeyGlideClientConfiguration clientConfig = DefaultValkeyGlideClientConfiguration.builder()
				.commandTimeout(Duration.ofSeconds(10))
				.useSsl()
				.build();
		ValkeyGlideConnectionFactory factory = new ValkeyGlideConnectionFactory(standaloneConfig, clientConfig);

		// Standalone config values should be used
		assertThat(factory.getClientConfiguration().getHostName()).hasValue("prodhost");
		assertThat(factory.getClientConfiguration().getPort()).isEqualTo(6380);
		assertThat(factory.getClientConfiguration().getDatabase()).isEqualTo(3);
		assertThat(factory.getClientConfiguration().getPassword()).hasValue(ValkeyPassword.of("pass123"));

		// Client config values should be preserved
		assertThat(factory.getClientConfiguration().getCommandTimeout()).isEqualTo(Duration.ofSeconds(10));
		assertThat(factory.getClientConfiguration().isUseSsl()).isTrue();
	}

	@Test
	void testStandaloneDefaults() {
		ValkeyStandaloneConfiguration config = new ValkeyStandaloneConfiguration();
		ValkeyGlideConnectionFactory factory = new ValkeyGlideConnectionFactory(config);

		assertThat(factory.getClientConfiguration().getHostName()).hasValue("localhost");
		assertThat(factory.getClientConfiguration().getPort()).isEqualTo(6379);
		assertThat(factory.getClientConfiguration().getDatabase()).isEqualTo(0);
	}

	@Test
	void testNullClientConfiguration() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new ValkeyGlideConnectionFactory((ValkeyGlideClientConfiguration) null))
				.withMessageContaining("ValkeyGlideClientConfiguration must not be null");
	}

	@Test
	void testNullStandaloneConfiguration() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new ValkeyGlideConnectionFactory((ValkeyStandaloneConfiguration) null))
				.withMessageContaining("ValkeyStandaloneConfiguration must not be null");
	}
}

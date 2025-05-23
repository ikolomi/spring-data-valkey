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
package org.springframework.data.redis.config;

import static org.assertj.core.api.Assertions.*;

import jakarta.annotation.Resource;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Costin Leau
 * @author Mark Paluch
 * @author Vedran Pavic
 */
@SpringJUnitConfig(locations = "namespace.xml")
class NamespaceIntegrationTests {

	@Autowired private RedisMessageListenerContainer container;

	@Autowired private StringRedisTemplate template;

	@Autowired private StubErrorHandler handler;

	@Test
	void testSanityTest() throws Exception {
		assertThat(container.isRunning()).isTrue();
	}

	@Test
	void testWithMessages() {
		assertThat(template.convertAndSend("x1", "[X]test")).isGreaterThanOrEqualTo(1L);
		assertThat(template.convertAndSend("z1", "[Z]test")).isGreaterThanOrEqualTo(1L);
	}
}

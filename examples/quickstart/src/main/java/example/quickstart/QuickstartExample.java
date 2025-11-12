/*
 * Copyright 2025 the original author or authors.
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
package example.quickstart;

import io.valkey.springframework.data.valkey.connection.valkeyglide.ValkeyGlideConnectionFactory;
import io.valkey.springframework.data.valkey.core.StringValkeyTemplate;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Quickstart example demonstrating basic ValkeyTemplate usage.
 */
public class QuickstartExample {

	public static void main(String[] args) {

		ValkeyGlideConnectionFactory connectionFactory = new ValkeyGlideConnectionFactory();
		connectionFactory.afterPropertiesSet();

		try {
			// StringValkeyTemplate is a convenience class for String keys and values
			StringValkeyTemplate template = new StringValkeyTemplate(connectionFactory);

			// Basic operations
			template.opsForValue().set("message", "Hello, Valkey!");
			String value = template.opsForValue().get("message");
			System.out.println("Retrieved: " + value);

			// Set with expiration (TTL)
			template.opsForValue().set("session:123", "user-data", Duration.ofSeconds(60));
			Long ttl = template.getExpire("session:123", TimeUnit.SECONDS);
			System.out.println("\nSession key expires in " + ttl + " seconds");

			// Cleanup
			template.delete("message");
			template.delete("session:123");
		} finally {
			connectionFactory.destroy();
		}
	}
}

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
import io.valkey.springframework.data.valkey.core.ValkeyTemplate;
import io.valkey.springframework.data.valkey.serializer.StringValkeySerializer;

/**
 * Quickstart example demonstrating basic ValkeyTemplate usage.
 */
public class QuickstartExample {

	public static void main(String[] args) {

		ValkeyGlideConnectionFactory connectionFactory = new ValkeyGlideConnectionFactory();
		connectionFactory.afterPropertiesSet();

		try {
			ValkeyTemplate<String, String> template = new ValkeyTemplate<>();
			template.setConnectionFactory(connectionFactory);
			template.setDefaultSerializer(StringValkeySerializer.UTF_8);
			template.afterPropertiesSet();

			template.opsForValue().set("message", "Hello, Valkey!");
			String value = template.opsForValue().get("message");
			System.out.println("Retrieved: " + value);

			// Cleanup
			template.delete("message");
		} finally {
			connectionFactory.destroy();
		}
	}
}

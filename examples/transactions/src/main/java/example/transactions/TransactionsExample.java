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
package example.transactions;

import io.valkey.springframework.data.valkey.connection.valkeyglide.ValkeyGlideConnectionFactory;
import io.valkey.springframework.data.valkey.core.ValkeyCallback;
import io.valkey.springframework.data.valkey.core.ValkeyTemplate;
import io.valkey.springframework.data.valkey.serializer.StringValkeySerializer;

import java.util.List;

/**
 * Example demonstrating Valkey transactions with MULTI/EXEC.
 */
public class TransactionsExample {

	public static void main(String[] args) {

		ValkeyGlideConnectionFactory connectionFactory = new ValkeyGlideConnectionFactory();
		connectionFactory.afterPropertiesSet();

		try {
			ValkeyTemplate<String, String> template = new ValkeyTemplate<>();
			template.setConnectionFactory(connectionFactory);
			template.setDefaultSerializer(StringValkeySerializer.UTF_8);
			template.afterPropertiesSet();

			// Basic transaction
		System.out.println("=== Basic Transaction ===");
		List<Object> results = template.execute((ValkeyCallback<List<Object>>) connection -> {
			connection.multi();
			connection.stringCommands().set("key1".getBytes(), "value1".getBytes());
			connection.stringCommands().set("key2".getBytes(), "value2".getBytes());
			return connection.exec();
		});
		System.out.println("Transaction results: " + results);

		// Transaction with WATCH
		System.out.println("\n=== Transaction with WATCH ===");
		template.opsForValue().set("counter", "0");

		List<Object> watchResults = template.execute((ValkeyCallback<List<Object>>) connection -> {
			connection.watch("counter".getBytes());
			String value = new String(connection.stringCommands().get("counter".getBytes()));
			int counter = Integer.parseInt(value);

			connection.multi();
			connection.stringCommands().set("counter".getBytes(), String.valueOf(counter + 1).getBytes());
			return connection.exec();
		});

		if (watchResults != null && !watchResults.isEmpty()) {
			System.out.println("Transaction succeeded. Counter: " + template.opsForValue().get("counter"));
		} else {
			System.out.println("Transaction failed (key was modified)");
		}
		} finally {
			connectionFactory.destroy();
		}
	}
}

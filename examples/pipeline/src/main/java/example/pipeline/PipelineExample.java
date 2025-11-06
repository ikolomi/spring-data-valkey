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
package example.pipeline;

import io.valkey.springframework.data.valkey.connection.valkeyglide.ValkeyGlideConnectionFactory;
import io.valkey.springframework.data.valkey.core.ValkeyCallback;
import io.valkey.springframework.data.valkey.core.ValkeyTemplate;
import io.valkey.springframework.data.valkey.serializer.StringValkeySerializer;

import java.util.ArrayList;
import java.util.List;

/**
 * Example demonstrating pipelining for improved performance.
 */
public class PipelineExample {

	public static void main(String[] args) {

		ValkeyGlideConnectionFactory connectionFactory = new ValkeyGlideConnectionFactory();
		connectionFactory.afterPropertiesSet();

		try {
			ValkeyTemplate<String, String> template = new ValkeyTemplate<>();
			template.setConnectionFactory(connectionFactory);
			template.setDefaultSerializer(StringValkeySerializer.UTF_8);
			template.afterPropertiesSet();

			// Without pipeline - multiple round trips
			long start = System.currentTimeMillis();
			for (int i = 0; i < 100; i++) {
				template.opsForValue().set("key:" + i, "value:" + i);
			}
			long elapsed = System.currentTimeMillis() - start;
			System.out.println("Without pipeline: " + elapsed + "ms");

			// With pipeline - single round trip
			System.out.println();
			start = System.currentTimeMillis();
			List<Object> results = template.executePipelined((ValkeyCallback<Object>) connection -> {
				for (int i = 0; i < 100; i++) {
					byte[] key = ("key:" + i).getBytes();
					byte[] value = ("value:" + i).getBytes();
					connection.stringCommands().set(key, value);
				}
				return null;
			});
			elapsed = System.currentTimeMillis() - start;
			System.out.println("With pipeline: " + elapsed + "ms (" + results.size() + " results)");

			// Pipeline with mixed operations
			System.out.println();
			results = template.executePipelined((ValkeyCallback<Object>) connection -> {
				connection.stringCommands().set("user:1:name".getBytes(), "Alice".getBytes());
				connection.stringCommands().set("user:1:age".getBytes(), "30".getBytes());
				connection.listCommands().rPush("user:1:tags".getBytes(), "developer".getBytes());
				connection.listCommands().rPush("user:1:tags".getBytes(), "java".getBytes());
				connection.hashCommands().hSet("user:1:profile".getBytes(), "city".getBytes(), "NYC".getBytes());
				return null;
			});
			System.out.println("Mixed operations executed: " + results.size());

			// Verify results
			System.out.println();
			System.out.println("Name: " + template.opsForValue().get("user:1:name"));
			System.out.println("Age: " + template.opsForValue().get("user:1:age"));
			System.out.println("Tags: " + template.opsForList().range("user:1:tags", 0, -1));
			System.out.println("City: " + template.opsForHash().get("user:1:profile", "city"));

			// Cleanup
			List<String> keysToDelete = new ArrayList<>();
			keysToDelete.add("user:1:name");
			keysToDelete.add("user:1:age");
			keysToDelete.add("user:1:tags");
			keysToDelete.add("user:1:profile");
			for (int i = 0; i < 100; i++) {
				keysToDelete.add("key:" + i);
			}
			template.delete(keysToDelete);
		} finally {
			connectionFactory.destroy();
		}
	}
}

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
package example.operations;

import io.valkey.springframework.data.valkey.connection.valkeyglide.ValkeyGlideConnectionFactory;
import io.valkey.springframework.data.valkey.core.ValkeyTemplate;
import io.valkey.springframework.data.valkey.serializer.StringValkeySerializer;
import org.springframework.data.geo.Point;

/**
 * Example demonstrating various Valkey data structure operations.
 */
public class OperationsExample {

	public static void main(String[] args) {

		ValkeyGlideConnectionFactory connectionFactory = new ValkeyGlideConnectionFactory();
		connectionFactory.afterPropertiesSet();

		try {
			ValkeyTemplate<String, String> template = new ValkeyTemplate<>();
			template.setConnectionFactory(connectionFactory);
			template.setDefaultSerializer(StringValkeySerializer.UTF_8);
			template.afterPropertiesSet();

			// List operations
			System.out.println("=== List Operations ===");
			template.opsForList().rightPush("mylist", "one");
			template.opsForList().rightPush("mylist", "two");
			template.opsForList().rightPush("mylist", "three");
			System.out.println("List: " + template.opsForList().range("mylist", 0, -1));

			// Set operations
			System.out.println("\n=== Set Operations ===");
			template.opsForSet().add("myset", "apple", "banana", "cherry");
			System.out.println("Set members: " + template.opsForSet().members("myset"));

			// Hash operations
			System.out.println("\n=== Hash Operations ===");
			template.opsForHash().put("myhash", "field1", "value1");
			template.opsForHash().put("myhash", "field2", "value2");
			System.out.println("Hash: " + template.opsForHash().entries("myhash"));

			// Sorted Set operations
			System.out.println("\n=== Sorted Set Operations ===");
			template.opsForZSet().add("myzset", "member1", 1.0);
			template.opsForZSet().add("myzset", "member2", 2.0);
			template.opsForZSet().add("myzset", "member3", 3.0);
			System.out.println("ZSet range: " + template.opsForZSet().range("myzset", 0, -1));

			// Geo operations
			System.out.println("\n=== Geo Operations ===");
			template.opsForGeo().add("locations", new Point(-122.27652, 37.805186), "San Francisco");
			template.opsForGeo().add("locations", new Point(-118.24368, 34.05223), "Los Angeles");
			System.out.println("Locations: " + template.opsForGeo().position("locations", "San Francisco"));
		} finally {
			connectionFactory.destroy();
		}
	}
}

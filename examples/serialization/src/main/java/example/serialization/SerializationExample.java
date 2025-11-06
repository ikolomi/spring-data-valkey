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
package example.serialization;

import io.valkey.springframework.data.valkey.connection.valkeyglide.ValkeyGlideConnectionFactory;
import io.valkey.springframework.data.valkey.core.ValkeyTemplate;
import io.valkey.springframework.data.valkey.serializer.Jackson2JsonValkeySerializer;
import io.valkey.springframework.data.valkey.serializer.JdkSerializationValkeySerializer;
import io.valkey.springframework.data.valkey.serializer.StringValkeySerializer;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Example demonstrating different serialization strategies.
 */
public class SerializationExample {

	public static void main(String[] args) {

		ValkeyGlideConnectionFactory connectionFactory = new ValkeyGlideConnectionFactory();
		connectionFactory.afterPropertiesSet();

		try {
			User user = new User("alice", "alice@example.com", 25);

			// 1. JSON serialization (recommended for human-readable data)
			System.out.println("=== JSON Serialization ===");
			ValkeyTemplate<String, User> jsonTemplate = new ValkeyTemplate<>();
			jsonTemplate.setConnectionFactory(connectionFactory);
			jsonTemplate.setKeySerializer(new StringValkeySerializer());
			jsonTemplate.setValueSerializer(new Jackson2JsonValkeySerializer<>(User.class));
			jsonTemplate.afterPropertiesSet();

			jsonTemplate.opsForValue().set("user:json", user);
			User jsonRetrieved = jsonTemplate.opsForValue().get("user:json");
			System.out.println("JSON retrieved: " + jsonRetrieved);
			System.out.println("Stored as: " + new String((byte[]) jsonTemplate.getConnectionFactory().getConnection().stringCommands().get("user:json".getBytes())));

			// 2. JDK serialization (Java-specific, includes class metadata)
			System.out.println("\n=== JDK Serialization ===");
			ValkeyTemplate<String, User> jdkTemplate = new ValkeyTemplate<>();
			jdkTemplate.setConnectionFactory(connectionFactory);
			jdkTemplate.setKeySerializer(new StringValkeySerializer());
			jdkTemplate.setValueSerializer(new JdkSerializationValkeySerializer());
			jdkTemplate.afterPropertiesSet();

			jdkTemplate.opsForValue().set("user:jdk", user);
			User jdkRetrieved = jdkTemplate.opsForValue().get("user:jdk");
			System.out.println("JDK retrieved: " + jdkRetrieved);
			System.out.println("Stored as binary (not human-readable)");

			// 3. String serialization (for simple string values)
			System.out.println("\n=== String Serialization ===");
			ValkeyTemplate<String, String> stringTemplate = new ValkeyTemplate<>();
			stringTemplate.setConnectionFactory(connectionFactory);
			stringTemplate.setDefaultSerializer(StringValkeySerializer.UTF_8);
			stringTemplate.afterPropertiesSet();

			stringTemplate.opsForValue().set("message", "Hello, Valkey!");
			String message = stringTemplate.opsForValue().get("message");
			System.out.println("String retrieved: " + message);

			System.out.println("\n=== Serialization Comparison ===");
			System.out.println("JSON: Human-readable, language-agnostic, recommended for most use cases");
			System.out.println("JDK: Binary format, Java-only, includes full class metadata");
			System.out.println("String: Simplest, for plain text data");

			// Cleanup
			jsonTemplate.delete(Arrays.asList("user:json", "user:jdk", "message"));
		} finally {
			connectionFactory.destroy();
		}
	}

	static class User implements Serializable {

		private String name;
		private String email;
		private int age;

		public User() {}

		public User(String name, String email, int age) {
			this.name = name;
			this.email = email;
			this.age = age;
		}

		public String getName() { return name; }
		public void setName(String name) { this.name = name; }

		public String getEmail() { return email; }
		public void setEmail(String email) { this.email = email; }

		public int getAge() { return age; }
		public void setAge(int age) { this.age = age; }

		@Override
		public String toString() {
			return "User{name='" + name + "', email='" + email + "', age=" + age + "}";
		}
	}
}

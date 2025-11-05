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
import io.valkey.springframework.data.valkey.serializer.StringValkeySerializer;

import java.io.Serializable;

/**
 * Example demonstrating different serialization strategies.
 */
public class SerializationExample {

	public static void main(String[] args) {

		ValkeyGlideConnectionFactory connectionFactory = new ValkeyGlideConnectionFactory();
		connectionFactory.afterPropertiesSet();

		try {
			// JSON serialization
			ValkeyTemplate<String, User> jsonTemplate = new ValkeyTemplate<>();
			jsonTemplate.setConnectionFactory(connectionFactory);
			jsonTemplate.setKeySerializer(new StringValkeySerializer());
			jsonTemplate.setValueSerializer(new Jackson2JsonValkeySerializer<>(User.class));
			jsonTemplate.afterPropertiesSet();

			User user = new User("alice", "alice@example.com", 25);
			jsonTemplate.opsForValue().set("user:1", user);
			User retrieved = jsonTemplate.opsForValue().get("user:1");
			System.out.println("Retrieved user: " + retrieved);
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

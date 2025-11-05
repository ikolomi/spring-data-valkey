/*
 * Copyright 2023-2025 the original author or authors.
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
package example.cache;

import io.valkey.springframework.data.valkey.cache.ValkeyCacheConfiguration;
import io.valkey.springframework.data.valkey.cache.ValkeyCacheManager;
import io.valkey.springframework.data.valkey.connection.valkeyglide.ValkeyGlideConnectionFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * Example demonstrating Spring Cache abstraction with Valkey.
 */
public class CacheExample {

	public static void main(String[] args) {

		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(Config.class);
		UserService userService = context.getBean(UserService.class);

		System.out.println("First call (cache miss):");
		System.out.println(userService.getUserById("1"));

		System.out.println("\nSecond call (cache hit):");
		System.out.println(userService.getUserById("1"));

		context.close();
	}

	@Configuration
	@EnableCaching
	static class Config {

		@Bean
		public ValkeyGlideConnectionFactory connectionFactory() {
			return new ValkeyGlideConnectionFactory();
		}

		@Bean
		public ValkeyCacheManager cacheManager(ValkeyGlideConnectionFactory connectionFactory) {
			ValkeyCacheConfiguration config = ValkeyCacheConfiguration.defaultCacheConfig()
					.entryTtl(Duration.ofMinutes(10));
			return ValkeyCacheManager.builder(connectionFactory)
					.cacheDefaults(config)
					.build();
		}

		@Bean
		public UserService userService() {
			return new UserService();
		}
	}

	@Service
	static class UserService {

		@Cacheable("users")
		public String getUserById(String id) {
			System.out.println("Fetching user from database...");
			return "User-" + id;
		}
	}
}

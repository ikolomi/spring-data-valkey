// TODO: Temporarily commented out - will be fully implemented later
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
/*
package org.springframework.data.redis.connection.valkeyglide;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.DefaultStringRedisConnection;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Integration tests for {@link ValkeyGlideConnection} focusing on transaction functionality.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
/*
@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class ValkeyGlideConnectionTransactionIntegrationTests {

	@Autowired
	ValkeyGlideConnectionFactory connectionFactory;

	StringRedisConnection connection;

	@BeforeEach
	public void setUp() {
		connection = new DefaultStringRedisConnection(connectionFactory.getConnection());
	}

	@AfterEach
	public void tearDown() {
		connection.close();
		connection = null;
	}

	@Test
	public void testMultiExec() {
		connection.multi();
		connection.set("key", "value");
		connection.get("key");
		List<Object> results = connection.exec();
		assertThat(results).hasSize(2);
		assertThat(results).containsExactly(true, "value");
		assertThat(connection.get("key")).isEqualTo("value");
	}

	@Test
	public void testMultiDiscard() {
		connection.set("testitnow", "willdo");
		connection.multi();
		connection.set("testitnow2", "notok");
		connection.discard();
		assertThat(connection.exists("testitnow2")).isFalse();
		assertThat(connection.get("testitnow")).isEqualTo("willdo");
	}

	@Test
	public void testWatch() {
		connection.set("watchkey", "watchvalue");
		connection.watch("watchkey".getBytes());
		
		StringRedisConnection unwatchConn = new DefaultStringRedisConnection(connectionFactory.getConnection());
		unwatchConn.set("watchkey", "changed");
		unwatchConn.close();
		
		connection.multi();
		connection.set("watchkey", "will-not-change");
		List<Object> results = connection.exec();
		
		assertThat(results).isNull();
		assertThat(connection.get("watchkey")).isEqualTo("changed");
	}
	
	@Test
	public void testUnwatch() {
		connection.set("unwatchkey", "unwatchvalue");
		connection.watch("unwatchkey".getBytes());
		connection.unwatch();
		
		StringRedisConnection unwatchConn = new DefaultStringRedisConnection(connectionFactory.getConnection());
		unwatchConn.set("unwatchkey", "changed");
		unwatchConn.close();
		
		connection.multi();
		connection.set("unwatchkey", "will-change");
		List<Object> results = connection.exec();
		
		assertThat(results).isNotNull();
		assertThat(connection.get("unwatchkey")).isEqualTo("will-change");
	}
}
*/

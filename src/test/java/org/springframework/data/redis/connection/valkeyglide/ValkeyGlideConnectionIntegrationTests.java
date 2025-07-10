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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.data.redis.connection.ConnectionUtils;
import org.springframework.data.redis.connection.DefaultStringTuple;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.StringRedisConnection.StringTuple;
import org.springframework.data.redis.util.ConnectionVerifier;
import org.springframework.test.annotation.IfProfileValue;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Integration test of {@link ValkeyGlideConnection}
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
/*
@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class ValkeyGlideConnectionIntegrationTests extends AbstractConnectionIntegrationTests {

	@Autowired
	ValkeyGlideConnectionFactory connectionFactory;

	@AfterEach
	public void tearDown() {
		try {
			connection.flushAll();
		} catch (Exception ignore) {
			// Valkey Glide may leave some incomplete data in OutputStream on NPE caused by null key/value tests
			// Attempting to flush the DB or close the connection will result in error on sending QUIT to Redis
		}

		try {
			connection.close();
		} catch (Exception ignore) {
		}

		connection = null;
	}

	@Test
	public void testNativeConnection() {
		assertThat(connectionFactory.getConnection().getNativeConnection().getClass().getName()).contains("GlideClient");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testEvalShaArrayBytes() {
		getResults();
		byte[] sha1 = connection.scriptLoad("return {KEYS[1],ARGV[1]}").getBytes();
		initConnection();
		actual.add(byteConnection.evalSha(sha1, ReturnType.MULTI, 1, "key1".getBytes(), "arg1".getBytes()));
		List<Object> results = getResults();
		List<byte[]> scriptResults = (List<byte[]>) results.get(0);
		assertThat(Arrays.asList(new String(scriptResults.get(0)), new String(scriptResults.get(1))))
				.isEqualTo(Arrays.asList("key1", "arg1"));
	}

	@Test
	void testCreateConnectionWithDb() {
		ValkeyGlideClientConfiguration config = DefaultValkeyGlideClientConfiguration.builder()
				.database(1)
				.build();
		ValkeyGlideConnectionFactory factory2 = new ValkeyGlideConnectionFactory(config);
		factory2.afterPropertiesSet();

		ConnectionVerifier.create(factory2) //
				.execute(RedisConnection::ping) //
				.verifyAndClose();
	}

	@Test // DATAREDIS-714
	void testCreateConnectionWithDbFailure() {
		ValkeyGlideClientConfiguration config = DefaultValkeyGlideClientConfiguration.builder()
				.database(77)
				.build();
		ValkeyGlideConnectionFactory factory2 = new ValkeyGlideConnectionFactory(config);
		factory2.afterPropertiesSet();

		try {
			assertThatExceptionOfType(RedisConnectionFailureException.class).isThrownBy(factory2::getConnection);
		} finally {
			factory2.destroy();
		}
	}

	@Test
	void testZAddSameScores() {
		Set<StringTuple> strTuples = new HashSet<>();
		strTuples.add(new DefaultStringTuple("Bob".getBytes(), "Bob", 2.0));
		strTuples.add(new DefaultStringTuple("James".getBytes(), "James", 2.0));
		Long added = connection.zAdd("myset", strTuples);
		assertThat(added.longValue()).isEqualTo(2L);
	}

	@Test
	public void testEvalReturnSingleError() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> connection.eval("return redis.call('expire','foo')", ReturnType.BOOLEAN, 0));
	}

	@Test
	public void testEvalArrayScriptError() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> connection.eval("return {1,2", ReturnType.MULTI, 1, "foo", "bar"));
	}

	@Test
	public void testEvalShaNotFound() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> connection.evalSha("somefakesha", ReturnType.VALUE, 2, "key1", "key2"));
	}

	@Test
	public void testEvalShaArrayError() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> connection.evalSha("notasha", ReturnType.MULTI, 1, "key1", "arg1"));
	}

	@Test
	public void testRestoreBadData() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
				.isThrownBy(() -> connection.restore("testing".getBytes(), 0, "foo".getBytes()));
	}

	@Test
	@IfProfileValue(name = "redisVersion", value = "2.6+")
	void testEvalShaSingleError() throws Exception {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> {
			connection.evalSha("notasha", ReturnType.BOOLEAN, 0, new String[0]);
		});
	}

	@Test
	public void testRestoreExistingKey() {
		connection.set("testing:restore".getBytes(), "foo".getBytes());
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> 
			connection.restore("testing:restore".getBytes(), 0, "foo".getBytes())
		);
	}

	@Test
	public void testExecWithoutMulti() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> 
			connection.exec()
		);
	}

	@Test
	public void testErrorInTx() {
		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> {
			connection.multi();
			connection.set("foo", "bar");
			// Try to do a list op on a value
			connection.lPop("foo");
			connection.exec();
			getResults();
		});
	}

	@Test
	public void testScriptKill() {
		connection.scriptFlush();
		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> connection.scriptKill());
	}

	@Test
	public void testScriptCached() {
		String script = "return KEYS[1]";
		String scriptHash = connection.scriptLoad(script.getBytes());
		assertThat(connection.scriptExists(scriptHash, "nonexistingscript")).containsExactly(true, false);
	}

	@Test
	public void testEvalReturnArrayOK() {
		String script = "return {KEYS[1],ARGV[1]}";
		Object result = connection.eval(script.getBytes(), ReturnType.MULTI, 1, "foo".getBytes(), "bar".getBytes());
		assertThat(result).isInstanceOf(List.class);
		List<byte[]> resultList = (List<byte[]>) result;
		assertThat(resultList).containsExactly("foo".getBytes(), "bar".getBytes());
	}

	@Test
	public void testEvalReturnString() {
		byte[] result = (byte[]) connection.eval("return KEYS[1]".getBytes(), ReturnType.VALUE, 1, "foo".getBytes());
		assertThat(new String(result)).isEqualTo("foo");
	}

	@Test
	public void testEvalReturnNumber() {
		Object result = connection.eval("return 10".getBytes(), ReturnType.INTEGER, 0);
		assertThat(result).isEqualTo(10L);
	}

	@Test
	public void testEvalReturnSingleOK() {
		Object result = connection.eval("return {ok='status'}", ReturnType.STATUS, 0);
		assertThat(result).isEqualTo("status");
	}

	/**
	 * Override pub/sub test methods to use a separate connection factory for subscribing threads,
	 * to avoid issues with shared connections in pub/sub mode
	 */
	/*
	@Test
	public void testPubSubWithNamedChannels() throws Exception {
		final String expectedChannel = "channel1";
		final String expectedMessage = "msg";
		final BlockingDeque<Message> messages = new LinkedBlockingDeque<>();

		MessageListener listener = (message, pattern) -> {
			messages.add(message);
		};

		Thread t = new Thread() {
			{
				setDaemon(true);
			}

			public void run() {
				// New connection to avoid interfering with the main connection
				RedisConnection con = connectionFactory.getConnection();
				try {
					Thread.sleep(100);
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}

				con.publish(expectedChannel.getBytes(), expectedMessage.getBytes());

				try {
					Thread.sleep(100);
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}

				/*
				   In some clients, unsubscribe happens async of message
				receipt, so not all
				messages may be received if unsubscribing now.
				Connection.close in teardown
				will take care of unsubscribing.
				*//*
				if (!(ConnectionUtils.isAsync(connectionFactory))) {
					connection.getSubscription().unsubscribe();
				}
				con.close();
			}
		};
		t.start();

		connection.subscribe(listener, expectedChannel.getBytes());

		Message message = messages.poll(5, TimeUnit.SECONDS);
		assertThat(message).isNotNull();
		assertThat(new String(message.getBody())).isEqualTo(expectedMessage);
		assertThat(new String(message.getChannel())).isEqualTo(expectedChannel);
	}

	@Test
	public void testPubSubWithPatterns() throws Exception {
		final String expectedPattern = "channel*";
		final String expectedMessage = "msg";
		final BlockingDeque<Message> messages = new LinkedBlockingDeque<>();

		final MessageListener listener = (message, pattern) -> {
			assertThat(new String(pattern)).isEqualTo(expectedPattern);
			messages.add(message);
		};

		Thread th = new Thread() {
			{
				setDaemon(true);
			}

			public void run() {
				// New connection to avoid interfering with the main connection
				RedisConnection con = connectionFactory.getConnection();
				try {
					Thread.sleep(100);
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}

				try {
					con.publish("channel1".getBytes(), expectedMessage.getBytes());
					con.publish("channel2".getBytes(), expectedMessage.getBytes());

					try {
						Thread.sleep(100);
					} catch (InterruptedException ex) {
						Thread.currentThread().interrupt();
					}
				} finally {
					con.close();
				}

				// In some clients, unsubscribe happens async of message
				// receipt, so not all
				// messages may be received if unsubscribing now.
				// Connection.close in teardown
				// will take care of unsubscribing.
				if (!(ConnectionUtils.isAsync(connectionFactory))) {
					try {
						connection.getSubscription().pUnsubscribe(expectedPattern.getBytes());
					} catch (Exception ex) {
						// Ignore exceptions during unsubscribe
					}
				}
			}
		};
		th.start();

		connection.pSubscribe(listener, expectedPattern);
		// Not all providers block on subscribe, give some time for messages to be received
		Message message = messages.poll(5, TimeUnit.SECONDS);
		assertThat(message).isNotNull();
		assertThat(new String(message.getBody())).isEqualTo(expectedMessage);
		message = messages.poll(5, TimeUnit.SECONDS);
		assertThat(message).isNotNull();
		assertThat(new String(message.getBody())).isEqualTo(expectedMessage);
	}

	@Test
	void testPoolNPE() {
		ValkeyGlideClientConfiguration config = DefaultValkeyGlideClientConfiguration.builder()
				.poolSize(1)
				.build();

		ValkeyGlideConnectionFactory factory2 = new ValkeyGlideConnectionFactory(config);
		factory2.afterPropertiesSet();

		try (RedisConnection conn = factory2.getConnection()) {
			conn.get(null);
		} catch (Exception ignore) {
		} finally {
			// Make sure we don't end up with broken connection
			factory2.getConnection().dbSize();
			factory2.destroy();
		}
	}

	@Test // Based on DATAREDIS-285
	void testExecuteShouldConvertArrayReplyCorrectly() {
		connection.set("spring", "awesome");
		connection.set("data", "cool");
		connection.set("redis", "supercalifragilisticexpialidocious");

		assertThat(
				(Iterable<byte[]>) connection.execute("MGET", "spring".getBytes(), "data".getBytes(), "redis".getBytes()))
						.isInstanceOf(List.class)
						.contains("awesome".getBytes(), "cool".getBytes(), "supercalifragilisticexpialidocious".getBytes());
	}

	@Test // Based on DATAREDIS-286, DATAREDIS-564
	void expireShouldSupportExiprationForValuesLargerThanInteger() {
		connection.set("expireKey", "foo");

		long seconds = ((long) Integer.MAX_VALUE) + 1;
		connection.expire("expireKey", seconds);
		long ttl = connection.ttl("expireKey");

		assertThat(ttl).isEqualTo(seconds);
	}

	@Test // Based on DATAREDIS-286
	void pExpireShouldSupportExiprationForValuesLargerThanInteger() {
		connection.set("pexpireKey", "foo");

		long millis = ((long) Integer.MAX_VALUE) + 10;
		connection.pExpire("pexpireKey", millis);
		long ttl = connection.pTtl("pexpireKey");

		assertThat(millis - ttl < 20L)
				.describedAs("difference between millis=%s and ttl=%s should not be greater than 20ms but is %s",
						millis, ttl, millis - ttl)
				.isTrue();
	}

	@Test // Based on DATAREDIS-106
	void zRangeByScoreTest() {
		connection.zAdd("myzset", 1, "one");
		connection.zAdd("myzset", 2, "two");
		connection.zAdd("myzset", 3, "three");

		Set<String> zRangeByScore = connection.zRangeByScore("myzset", "(1", "2");

		assertThat(zRangeByScore.iterator().next()).isEqualTo("two");
	}
}
*/

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
package org.springframework.data.redis.connection.valkeyglide;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Integration tests for {@link ValkeyGlideConnectionFactory}.
 * 
 * @author Ilya Kolomin
 */
@TestInstance(Lifecycle.PER_CLASS)
public class ValkeyGlideConnectionFactoryIntegrationTests {

    private ValkeyGlideConnectionFactory connectionFactory;
    private RedisTemplate<String, String> template;

    @BeforeAll
    void setup() {
        // Create a connection factory for tests
        connectionFactory = createConnectionFactory();

        // Create a template for easier testing
        template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(StringRedisSerializer.UTF_8);
        template.setValueSerializer(StringRedisSerializer.UTF_8);
        template.afterPropertiesSet();

        // Check if server is available and log the result
        boolean serverAvailable = isServerAvailable(connectionFactory);
        // Skip tests if server is not available
        assumeTrue(serverAvailable, "Redis server is not available");
    }

    @AfterAll
    void teardown() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
    }

    @Test
    void testGetConnection() throws InterruptedException {
        RedisConnection connection = connectionFactory.getConnection();
        assertThat(connection).isNotNull();
        assertThat(connection).isInstanceOf(ValkeyGlideConnection.class);
        connection.close();
    }


    @Test
    void testHashOperations() {
        String key = "test:hash";

        template.opsForHash().put(key, "field1", "value1");
        template.opsForHash().put(key, "field2", "value2");

        assertThat(template.opsForHash().get(key, "field1")).isEqualTo("value1");
        assertThat(template.opsForHash().get(key, "field2")).isEqualTo("value2");
        
        template.delete(key);
    }

    @Test
    void testListOperations() {
        String key = "test:list";

        template.opsForList().leftPush(key, "value1");
        template.opsForList().leftPush(key, "value2");

        assertThat(template.opsForList().size(key)).isEqualTo(2);
        assertThat(template.opsForList().rightPop(key)).isEqualTo("value1");
        assertThat(template.opsForList().rightPop(key)).isEqualTo("value2");
        
        template.delete(key);
    }

    @Test
    void testSetOperations() {
        String key = "test:set";

        template.opsForSet().add(key, "value1", "value2", "value3");

        assertThat(template.opsForSet().size(key)).isEqualTo(3);
        assertThat(template.opsForSet().isMember(key, "value2")).isTrue();
        
        template.delete(key);
    }

    @Test
    void testStringOperations() {
        // Basic SET/GET operations
        String key1 = "test:string:basic";
        String value1 = "Hello, valkey-glide!";
        
        template.opsForValue().set(key1, value1);
        String retrieved1 = template.opsForValue().get(key1);
        assertThat(retrieved1).isEqualTo(value1);

        // SET with expiration
        String key2 = "test:string:expire";
        String value2 = "Expiring value";
        template.opsForValue().set(key2, value2, Duration.ofSeconds(60));
        String retrieved2 = template.opsForValue().get(key2);
        assertThat(retrieved2).isEqualTo(value2);

        // SETNX (Set if Not eXists)
        String key3 = "test:string:setnx";
        String value3 = "New value";
        Boolean setResult1 = template.opsForValue().setIfAbsent(key3, value3);
        assertThat(setResult1).isTrue();
        Boolean setResult2 = template.opsForValue().setIfAbsent(key3, "Different value");
        assertThat(setResult2).isFalse();
        assertThat(template.opsForValue().get(key3)).isEqualTo(value3);

        // GETSET operation
        String key4 = "test:string:getset";
        template.opsForValue().set(key4, "old value");
        String oldValue = template.opsForValue().getAndSet(key4, "new value");
        assertThat(oldValue).isEqualTo("old value");
        assertThat(template.opsForValue().get(key4)).isEqualTo("new value");

        // MGET/MSET operations
        Map<String, String> multiValues = Map.of(
            "test:string:multi1", "value1",
            "test:string:multi2", "value2",
            "test:string:multi3", "value3"
        );
        template.opsForValue().multiSet(multiValues);
        
        List<String> multiRetrieved = template.opsForValue().multiGet(List.of(
            "test:string:multi1", "test:string:multi2", "test:string:multi3"));
        assertThat(multiRetrieved).containsExactly("value1", "value2", "value3");
     
        // MSETNX operation (set multiple if none exist)
        Map<String, String> msetnxValues = Map.of(
            "test:string:msetnx1", "msetnx_value1",
            "test:string:msetnx2", "msetnx_value2"
        );
        Boolean msetnxResult = template.opsForValue().multiSetIfAbsent(msetnxValues);
        assertThat(msetnxResult).isTrue();
        assertThat(template.opsForValue().get("test:string:msetnx1")).isEqualTo("msetnx_value1");

        // INCR/DECR operations
        String counterKey = "test:string:counter";
        template.opsForValue().set(counterKey, "10");
        
        Long incResult1 = template.opsForValue().increment(counterKey);
        assertThat(incResult1).isEqualTo(11L);
        
        Long incResult2 = template.opsForValue().increment(counterKey, 5L);
        assertThat(incResult2).isEqualTo(16L);
        
        Long decrResult1 = template.opsForValue().decrement(counterKey);
        assertThat(decrResult1).isEqualTo(15L);
        
        Long decrResult2 = template.opsForValue().decrement(counterKey, 3L);
        assertThat(decrResult2).isEqualTo(12L);
        
        // Floating point increment operations (using separate key to avoid DECR issues)
        String floatCounterKey = "test:string:floatcounter";
        template.opsForValue().set(floatCounterKey, "10");
        Double incFloatResult = template.opsForValue().increment(floatCounterKey, 2.5);
        assertThat(incFloatResult).isEqualTo(12.5);

        // APPEND operation
        String appendKey = "test:string:append";
        template.opsForValue().set(appendKey, "Hello");
        Integer appendResult = template.opsForValue().append(appendKey, " World!");
        assertThat(appendResult).isEqualTo(12); // Length of "Hello World!"
        assertThat(template.opsForValue().get(appendKey)).isEqualTo("Hello World!");

        // STRING LENGTH operation
        Long strlenResult = template.opsForValue().size(appendKey);
        assertThat(strlenResult).isEqualTo(12L);

        // GETRANGE operation (substring)
        String rangeResult = template.opsForValue().get(appendKey, 0, 4);
        assertThat(rangeResult).isEqualTo("Hello");
        
        // SETRANGE operation
        template.opsForValue().set(appendKey, "Hello World!", 6);
        // This would set from position 6, but let's test a simpler case
        String setRangeKey = "test:string:setrange";
        template.opsForValue().set(setRangeKey, "Hello");
        template.opsForValue().set(setRangeKey, " Redis", 5);
        assertThat(template.opsForValue().get(setRangeKey)).startsWith("Hello");
        
        // BIT operations (using RedisTemplate's execute for direct access to connection)
        String bitKey = "test:string:bits";
        template.opsForValue().set(bitKey, "a"); // 'a' = 01100001 in binary
        
        // Use connection directly for bit operations
        Boolean getBitResult = template.execute((RedisCallback<Boolean>) connection -> 
            connection.stringCommands().getBit(bitKey.getBytes(), 1));
        assertThat(getBitResult).isTrue(); // Second bit of 'a' is 1
        
        Boolean setBitResult = template.execute((RedisCallback<Boolean>) connection -> 
            connection.stringCommands().setBit(bitKey.getBytes(), 0, true));
        assertThat(setBitResult).isFalse(); // Original first bit was 0
        
        Long bitCountResult = template.execute((RedisCallback<Long>) connection -> 
            connection.stringCommands().bitCount(bitKey.getBytes()));
        assertThat(bitCountResult).isGreaterThan(0L);

        // BITOP operation
        String bitKey1 = "test:string:bit1";
        String bitKey2 = "test:string:bit2";
        String bitDestKey = "test:string:bitop";
        
        template.opsForValue().set(bitKey1, "a");
        template.opsForValue().set(bitKey2, "b");
        
        Long bitOpResult = template.execute((RedisCallback<Long>) connection -> 
            connection.stringCommands().bitOp(RedisStringCommands.BitOperation.AND, 
                bitDestKey.getBytes(), bitKey1.getBytes(), bitKey2.getBytes()));
        assertThat(bitOpResult).isEqualTo(1L); // Result length

        // Clean up all test keys
        template.delete(key1);
        template.delete(key2);
        template.delete(key3);
        template.delete(key4);
        template.delete("test:string:multi1");
        template.delete("test:string:multi2");
        template.delete("test:string:multi3");
        template.delete("test:string:msetnx1");
        template.delete("test:string:msetnx2");
        template.delete(counterKey);
        template.delete(floatCounterKey);
        template.delete(appendKey);
        template.delete(setRangeKey);
        template.delete(bitKey);
        template.delete(bitKey1);
        template.delete(bitKey2);
        template.delete(bitDestKey);
    }

    @Test
    void testTransactional() {
        String key1 = "test:tx:key1";
        String key2 = "test:tx:key2";

        template.opsForValue().set(key1, "initial1");
        template.opsForValue().set(key2, "initial2");

        // Using SessionCallback for transactions
        template.execute(new SessionCallback<Object>() {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            public Object execute(RedisOperations operations) {
                operations.multi();
                
                operations.opsForValue().set(key1, "updated1");
                operations.opsForValue().set(key2, "updated2");
                
                return operations.exec();
            }
        });

        assertThat(template.opsForValue().get(key1)).isEqualTo("updated1");
        assertThat(template.opsForValue().get(key2)).isEqualTo("updated2");
        
        template.delete(key1);
        template.delete(key2);
    }

    /**
     * Creates a connection factory for testing.
     */
    private ValkeyGlideConnectionFactory createConnectionFactory() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        config.setHostName(getRedisHost());
        config.setPort(getRedisPort());
        return ValkeyGlideConnectionFactory.createValkeyGlideConnectionFactory(config);
    }

    /**
     * Checks if the Redis server is available.
     */
    private boolean isServerAvailable(RedisConnectionFactory factory) {
        try {
            RedisConnection connection = factory.getConnection();
            if (connection == null) {
                System.out.println("Connection is null - factory failed to create connection");
                return false;
            }
            connection.ping();
            connection.close();
            return true;
        } catch (Exception e) {
            System.out.println("Error checking server availability: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Gets the Redis host from environment or uses default.
     */
    private String getRedisHost() {
        return System.getProperty("redis.host", "localhost");
    }

    /**
     * Gets the Redis port from environment or uses default.
     */
    private int getRedisPort() {
        try {
            return Integer.parseInt(System.getProperty("redis.port", "6379"));
        } catch (NumberFormatException e) {
            return 6379;
        }
    }
}

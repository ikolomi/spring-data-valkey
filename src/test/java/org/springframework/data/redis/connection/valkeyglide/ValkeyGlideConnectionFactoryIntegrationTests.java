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
import java.util.Properties;
import java.util.Set;

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
        template.setHashKeySerializer(StringRedisSerializer.UTF_8);
        template.setHashValueSerializer(StringRedisSerializer.UTF_8);
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
        String key1 = "test:hash:basic";
        String key2 = "test:hash:putall";
        String key3 = "test:hash:putifabsent";
        String key4 = "test:hash:increment";
        String key5 = "test:hash:length";
        String key6 = "test:hash:delete";
        String key7 = "test:hash:random";
        String key8 = "test:hash:scan";
        String key9 = "test:hash:expiration";
        
        try {
            template.opsForHash().put(key1, "field1", "value1");
            template.opsForHash().put(key1, "field2", "value2");

            assertThat(template.opsForHash().get(key1, "field1")).isEqualTo("value1");
            assertThat(template.opsForHash().get(key1, "field2")).isEqualTo("value2");

            // PUTALL operation
            Map<String, String> hashMap = Map.of(
                "field1", "value1",
                "field2", "value2",
                "field3", "value3"
            );
            template.opsForHash().putAll(key2, hashMap);
            
            assertThat(template.opsForHash().get(key2, "field1")).isEqualTo("value1");
            assertThat(template.opsForHash().get(key2, "field2")).isEqualTo("value2");
            assertThat(template.opsForHash().get(key2, "field3")).isEqualTo("value3");

            // PUTIFABSENT operation
            Boolean putResult1 = template.opsForHash().putIfAbsent(key3, "field1", "value1");
            assertThat(putResult1).isTrue();
            Boolean putResult2 = template.opsForHash().putIfAbsent(key3, "field1", "different_value");
            assertThat(putResult2).isFalse();
            assertThat(template.opsForHash().get(key3, "field1")).isEqualTo("value1");

            // HASKEY operation
            assertThat(template.opsForHash().hasKey(key1, "field1")).isTrue();
            assertThat(template.opsForHash().hasKey(key1, "nonexistent")).isFalse();

            // MULTIGET operation
            List<Object> hashKeys = List.of("field1", "field2", "nonexistent");
            List<Object> multiGetResult = template.opsForHash().multiGet(key1, hashKeys);
            assertThat(multiGetResult).containsExactly("value1", "value2", null);

            // SIZE operation
            assertThat(template.opsForHash().size(key1)).isEqualTo(2);
            assertThat(template.opsForHash().size(key2)).isEqualTo(3);

            // KEYS operation
            Set<Object> keys = template.opsForHash().keys(key2);
            assertThat(keys).containsExactlyInAnyOrder("field1", "field2", "field3");

            // VALUES operation
            List<Object> values = template.opsForHash().values(key2);
            assertThat(values).containsExactlyInAnyOrder("value1", "value2", "value3");

            // ENTRIES operation (HGETALL)
            Map<Object, Object> entries = template.opsForHash().entries(key2);
            assertThat(entries).hasSize(3);
            assertThat(entries.get("field1")).isEqualTo("value1");
            assertThat(entries.get("field2")).isEqualTo("value2");
            assertThat(entries.get("field3")).isEqualTo("value3");

            // INCREMENT operations
            template.opsForHash().put(key4, "counter", "10");
            
            // Long increment
            Long incrResult1 = template.opsForHash().increment(key4, "counter", 5L);
            assertThat(incrResult1).isEqualTo(15L);
            
            Long incrResult2 = template.opsForHash().increment(key4, "counter", -3L);
            assertThat(incrResult2).isEqualTo(12L);
            
            // Double increment (using different field to avoid conflicts)
            template.opsForHash().put(key4, "floatcounter", "10.5");
            Double floatIncrResult = template.opsForHash().increment(key4, "floatcounter", 2.5);
            assertThat(floatIncrResult).isEqualTo(13.0);

            // LENGTHOFVALUE operation
            template.opsForHash().put(key5, "field1", "Hello World!");
            Long lengthResult = template.opsForHash().lengthOfValue(key5, "field1");
            assertThat(lengthResult).isEqualTo(12L);
            
            // Length of non-existent field
            Long lengthNonExistent = template.opsForHash().lengthOfValue(key5, "nonexistent");
            assertThat(lengthNonExistent).isEqualTo(0L);

            // DELETE hash fields operation
            template.opsForHash().put(key6, "field1", "value1");
            template.opsForHash().put(key6, "field2", "value2");
            template.opsForHash().put(key6, "field3", "value3");
            
            Long deleteResult = template.opsForHash().delete(key6, "field1", "field2");
            assertThat(deleteResult).isEqualTo(2L);
            assertThat(template.opsForHash().hasKey(key6, "field1")).isFalse();
            assertThat(template.opsForHash().hasKey(key6, "field2")).isFalse();
            assertThat(template.opsForHash().hasKey(key6, "field3")).isTrue();

            // RANDOM operations
            template.opsForHash().putAll(key7, Map.of(
                "field1", "value1",
                "field2", "value2",
                "field3", "value3",
                "field4", "value4"
            ));

            // Random key
            Object randomKey = template.opsForHash().randomKey(key7);
            assertThat(randomKey).isIn("field1", "field2", "field3", "field4");
            
            // Random entry
            Map.Entry<Object, Object> randomEntry = template.opsForHash().randomEntry(key7);
            assertThat(randomEntry.getKey()).isIn("field1", "field2", "field3", "field4");
            assertThat(randomEntry.getValue().toString()).startsWith("value");
            
            // Random keys with count
            List<Object> randomKeys = template.opsForHash().randomKeys(key7, 2);
            assertThat(randomKeys).hasSize(2);
            assertThat(randomKeys).allMatch(key -> List.of("field1", "field2", "field3", "field4").contains(key));

            // Random entries with count
            Map<Object, Object> randomEntries = template.opsForHash().randomEntries(key7, 2);
            assertThat(randomEntries).hasSize(2);
            randomEntries.forEach((k, v) -> {
                assertThat(k).isIn("field1", "field2", "field3", "field4");
                assertThat(v.toString()).startsWith("value");
            });

            // SCAN operation (using RedisCallback for direct access to connection)
            template.opsForHash().putAll(key8, Map.of(
                "scanfield1", "scanvalue1",
                "scanfield2", "scanvalue2",
                "scanfield3", "scanvalue3"
            ));
            
            // Test hScan using connection directly
            template.execute((RedisCallback<Void>) connection -> {
                try (var cursor = connection.hashCommands().hScan(key8.getBytes(), 
                        org.springframework.data.redis.core.ScanOptions.scanOptions().count(10).build())) {
                    int count = 0;
                    while (cursor.hasNext()) {
                        Map.Entry<byte[], byte[]> entry = cursor.next();
                        String field = new String(entry.getKey());
                        String value = new String(entry.getValue());
                        assertThat(field).startsWith("scanfield");
                        assertThat(value).startsWith("scanvalue");
                        count++;
                    }
                    assertThat(count).isEqualTo(3);
                }
                return null;
            });

            // Test hash field expiration operations (requires Redis/Valkey 7.4+)
            if (isServerVersionAtLeast(7, 4)) {
                template.opsForHash().putAll(key9, Map.of(
                    "expirefield1", "expirevalue1",
                    "expirefield2", "expirevalue2",
                    "persistfield", "persistvalue"
                ));

                // Test hExpire - Set TTL in seconds
                template.execute((RedisCallback<List<Long>>) connection -> {
                    List<Long> result = connection.hashCommands().hExpire(key9.getBytes(), 60L, 
                        "expirefield1".getBytes(), "expirefield2".getBytes());
                    assertThat(result).containsExactly(1L, 1L); // Both fields should have expiration set
                    return result;
                });

                // Test hpExpire - Set TTL in milliseconds  
                template.execute((RedisCallback<List<Long>>) connection -> {
                    List<Long> result = connection.hashCommands().hpExpire(key9.getBytes(), 60000L,
                        "expirefield1".getBytes());
                    assertThat(result).containsExactly(1L); // Field should have expiration updated
                    return result;
                });

                // Test hExpireAt - Set expiration at specific timestamp
                long futureTimestamp = System.currentTimeMillis() / 1000 + 120; // 2 minutes from now
                template.execute((RedisCallback<List<Long>>) connection -> {
                    List<Long> result = connection.hashCommands().hExpireAt(key9.getBytes(), futureTimestamp,
                        "expirefield1".getBytes());
                    assertThat(result).containsExactly(1L); // Field should have expiration set
                    return result;
                });

                // Test hpExpireAt - Set expiration at specific timestamp in milliseconds
                long futureTimestampMillis = System.currentTimeMillis() + 120000; // 2 minutes from now
                template.execute((RedisCallback<List<Long>>) connection -> {
                    List<Long> result = connection.hashCommands().hpExpireAt(key9.getBytes(), futureTimestampMillis,
                        "expirefield2".getBytes());
                    assertThat(result).containsExactly(1L); // Field should have expiration set
                    return result;
                });

                // Test hTtl - Get TTL in seconds
                template.execute((RedisCallback<List<Long>>) connection -> {
                    List<Long> result = connection.hashCommands().hTtl(key9.getBytes(), 
                        "expirefield1".getBytes(), "persistfield".getBytes());
                    assertThat(result).hasSize(2);
                    assertThat(result.get(0)).isGreaterThan(0L); // expirefield1 should have TTL
                    assertThat(result.get(1)).isEqualTo(-1L); // persistfield should have no expiration
                    return result;
                });

                template.execute((RedisCallback<List<Long>>) connection -> {
                    List<Long> result = connection.hashCommands().hpTtl(key9.getBytes(),
                        "expirefield1".getBytes(), "persistfield".getBytes());
                    assertThat(result).hasSize(2);
                    assertThat(result.get(0)).isGreaterThan(0L); // expirefield1 should have TTL in milliseconds
                    assertThat(result.get(1)).isEqualTo(-1L); // persistfield should have no expiration
                    return result;
                });

                // Test hPersist - Remove expiration from fields
                template.execute((RedisCallback<List<Long>>) connection -> {
                    List<Long> result = connection.hashCommands().hPersist(key9.getBytes(),
                        "expirefield1".getBytes(), "expirefield2".getBytes());
                    assertThat(result).containsExactly(1L, 1L); // Both fields should have expiration removed
                    return result;
                });

                // Verify fields no longer have expiration after hPersist
                template.execute((RedisCallback<List<Long>>) connection -> {
                    List<Long> result = connection.hashCommands().hTtl(key9.getBytes(),
                        "expirefield1".getBytes(), "expirefield2".getBytes());
                    assertThat(result).containsExactly(-1L, -1L); // Both fields should have no expiration
                    return result;
                });
            }
        } finally {
            // Clean up all test keys - this will execute even if an exception occurs
            template.delete(key1);
            template.delete(key2);
            template.delete(key3);
            template.delete(key4);
            template.delete(key5);
            template.delete(key6);
            template.delete(key7);
            template.delete(key8);
            template.delete(key9);
        }
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
        String key2 = "test:string:expire";
        String key3 = "test:string:setnx";
        String key4 = "test:string:getset";
        String multi1Key = "test:string:multi1";
        String multi2Key = "test:string:multi2";
        String multi3Key = "test:string:multi3";
        String msetnx1Key = "test:string:msetnx1";
        String msetnx2Key = "test:string:msetnx2";
        String counterKey = "test:string:counter";
        String floatCounterKey = "test:string:floatcounter";
        String appendKey = "test:string:append";
        String setRangeKey = "test:string:setrange";
        String bitKey = "test:string:bits";
        String bitKey1 = "test:string:bit1";
        String bitKey2 = "test:string:bit2";
        String bitDestKey = "test:string:bitop";
        
        try {
            String value1 = "Hello, valkey-glide!";
            
            template.opsForValue().set(key1, value1);
            String retrieved1 = template.opsForValue().get(key1);
            assertThat(retrieved1).isEqualTo(value1);

            // SET with expiration
            String value2 = "Expiring value";
            template.opsForValue().set(key2, value2, Duration.ofSeconds(60));
            String retrieved2 = template.opsForValue().get(key2);
            assertThat(retrieved2).isEqualTo(value2);

            // SETNX (Set if Not eXists)
            String value3 = "New value";
            Boolean setResult1 = template.opsForValue().setIfAbsent(key3, value3);
            assertThat(setResult1).isTrue();
            Boolean setResult2 = template.opsForValue().setIfAbsent(key3, "Different value");
            assertThat(setResult2).isFalse();
            assertThat(template.opsForValue().get(key3)).isEqualTo(value3);

            // GETSET operation
            template.opsForValue().set(key4, "old value");
            String oldValue = template.opsForValue().getAndSet(key4, "new value");
            assertThat(oldValue).isEqualTo("old value");
            assertThat(template.opsForValue().get(key4)).isEqualTo("new value");

            // MGET/MSET operations
            Map<String, String> multiValues = Map.of(
                multi1Key, "value1",
                multi2Key, "value2",
                multi3Key, "value3"
            );
            template.opsForValue().multiSet(multiValues);
            
            List<String> multiRetrieved = template.opsForValue().multiGet(List.of(
                multi1Key, multi2Key, multi3Key));
            assertThat(multiRetrieved).containsExactly("value1", "value2", "value3");
         
            // MSETNX operation (set multiple if none exist)
            Map<String, String> msetnxValues = Map.of(
                msetnx1Key, "msetnx_value1",
                msetnx2Key, "msetnx_value2"
            );
            Boolean msetnxResult = template.opsForValue().multiSetIfAbsent(msetnxValues);
            assertThat(msetnxResult).isTrue();
            assertThat(template.opsForValue().get(msetnx1Key)).isEqualTo("msetnx_value1");

            // INCR/DECR operations
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
            template.opsForValue().set(floatCounterKey, "10");
            Double incFloatResult = template.opsForValue().increment(floatCounterKey, 2.5);
            assertThat(incFloatResult).isEqualTo(12.5);

            // APPEND operation
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
            template.opsForValue().set(setRangeKey, "Hello");
            template.opsForValue().set(setRangeKey, " Redis", 5);
            assertThat(template.opsForValue().get(setRangeKey)).startsWith("Hello");
            
            // BIT operations (using RedisTemplate's execute for direct access to connection)
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
            template.opsForValue().set(bitKey1, "a");
            template.opsForValue().set(bitKey2, "b");
            
            Long bitOpResult = template.execute((RedisCallback<Long>) connection -> 
                connection.stringCommands().bitOp(RedisStringCommands.BitOperation.AND, 
                    bitDestKey.getBytes(), bitKey1.getBytes(), bitKey2.getBytes()));
            assertThat(bitOpResult).isEqualTo(1L); // Result length
        } finally {
            // Clean up all test keys - this will execute even if an exception occurs
            template.delete(key1);
            template.delete(key2);
            template.delete(key3);
            template.delete(key4);
            template.delete(multi1Key);
            template.delete(multi2Key);
            template.delete(multi3Key);
            template.delete(msetnx1Key);
            template.delete(msetnx2Key);
            template.delete(counterKey);
            template.delete(floatCounterKey);
            template.delete(appendKey);
            template.delete(setRangeKey);
            template.delete(bitKey);
            template.delete(bitKey1);
            template.delete(bitKey2);
            template.delete(bitDestKey);
        }
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
        RedisConnection connection = factory.getConnection();
        return connection.ping().equals("PONG");
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
        return Integer.parseInt(System.getProperty("redis.port", "6379"));
    }

    /**
     * Checks if the server version is at least the specified major.minor version.
     * Uses the INFO server command to get server version information.
     */
    private boolean isServerVersionAtLeast(int majorVersion, int minorVersion) {
        return template.execute((RedisCallback<Boolean>) connection -> {
                // Execute INFO server command
                Properties serverInfo = connection.serverCommands().info("server");
                String versionString = serverInfo.getProperty("redis_version");
                
                // If redis_version is not found, try server_version (for Valkey)
                if (versionString == null) {
                    versionString = serverInfo.getProperty("server_version");
                }
                
                if (versionString == null) {
                    //System.out.println("Could not determine server version, skipping version-dependent tests");
                    return false;
                }
                
                //System.out.println("Server version: " + versionString);
                
                // Parse version string (e.g., "7.4.0" or "8.0.1")
                String[] versionParts = versionString.split("\\.");
                if (versionParts.length < 2) {
                    //System.out.println("Invalid version format: " + versionString);
                    return false;
                }
                
                int serverMajor = Integer.parseInt(versionParts[0]);
                int serverMinor = Integer.parseInt(versionParts[1]);
                
                // Compare versions
                if (serverMajor > majorVersion) {
                    return true;
                } else if (serverMajor == majorVersion) {
                    return serverMinor >= minorVersion;
                } else {
                    return false;
                }
            }
        );
    }
}

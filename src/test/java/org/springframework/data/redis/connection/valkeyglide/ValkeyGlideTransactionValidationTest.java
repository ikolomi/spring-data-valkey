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

import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Comprehensive validation tests for Valkey-Glide transactional support.
 * This test suite validates that the transaction implementation works correctly
 * and follows the same patterns as other Redis drivers like Jedis.
 * 
 * @author Ilya Kolomin
 */
public class ValkeyGlideTransactionValidationTest extends AbstractValkeyGlideIntegrationTests {

    private RedisTemplate<String, String> template;

    @Override
    protected String[] getTestKeyPatterns() {
        return new String[]{
            "test:tx:basic:key1", "test:tx:basic:key2",
            "test:tx:discard:key", "test:tx:watch:key", "test:tx:watch:conflict",
            "test:tx:lowlevel:key", "test:tx:lowlevel:key2",
            "test:tx:pipeline:key1", "test:tx:pipeline:key2",
            "test:tx:nested:key"
        };
    }

    @BeforeAll
    void setupTemplate() {
        // Create a template for easier testing using the inherited connection factory
        template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(StringRedisSerializer.UTF_8);
        template.setValueSerializer(StringRedisSerializer.UTF_8);
        template.setHashKeySerializer(StringRedisSerializer.UTF_8);
        template.setHashValueSerializer(StringRedisSerializer.UTF_8);
        template.afterPropertiesSet();
    }

    @AfterAll
    void teardownTemplate() {
        // Template cleanup - connection factory is handled by abstract class
        template = null;
    }

    @Test
    void testBasicTransaction() {
        String key1 = "test:tx:basic:key1";
        String key2 = "test:tx:basic:key2";

        try {
            // Clean up any existing data
            template.delete(key1);
            template.delete(key2);

            // Execute transaction using SessionCallback
            List<Object> results = template.execute(new SessionCallback<List<Object>>() {
                @SuppressWarnings({ "unchecked", "rawtypes" })
                public List<Object> execute(RedisOperations operations) {
                    operations.multi();
                    
                    operations.opsForValue().set(key1, "value1");
                    operations.opsForValue().set(key2, "value2");
                    operations.opsForValue().get(key1);
                    operations.opsForValue().get(key2);
                    
                    return operations.exec();
                }
            });

            // Verify transaction results
            assertThat(results).isNotNull();
            assertThat(results).hasSize(4); // 2 SETs + 2 GETs
            
            // SET commands should return "OK" or null (depending on implementation)
            // GET commands should return the values
            assertThat(results.get(2)).isEqualTo("value1");
            assertThat(results.get(3)).isEqualTo("value2");

            // Verify values are actually set
            assertThat(template.opsForValue().get(key1)).isEqualTo("value1");
            assertThat(template.opsForValue().get(key2)).isEqualTo("value2");
        } finally {
            // Clean up test keys
            template.delete(key1);
            template.delete(key2);
        }
    }

    @Test
    void testTransactionDiscard() {
        String key = "test:tx:discard:key";
        
        try {
            // Clean up any existing data
            template.delete(key);
            template.opsForValue().set(key, "initial");

            // Execute transaction that will be discarded
            template.execute(new SessionCallback<Void>() {
                @SuppressWarnings({ "unchecked", "rawtypes" })
                public Void execute(RedisOperations operations) {
                    operations.multi();
                    
                    operations.opsForValue().set(key, "changed");
                    
                    // Discard instead of exec
                    operations.discard();
                    
                    return null;
                }
            });

            // Verify value was not changed
            assertThat(template.opsForValue().get(key)).isEqualTo("initial");
        } finally {
            // Clean up test key
            template.delete(key);
        }
    }

    @Test
    void testTransactionWithWatch() {
        String key = "test:tx:watch:key";
        
        try {
            // Clean up and set initial value
            template.delete(key);
            template.opsForValue().set(key, "initial");

            // Test optimistic locking with WATCH
            template.execute(new SessionCallback<List<Object>>() {
                @SuppressWarnings({ "unchecked", "rawtypes" })
                public List<Object> execute(RedisOperations operations) {
                    // Watch the key
                    operations.watch(key);
                    
                    // Start transaction
                    operations.multi();
                    operations.opsForValue().set(key, "watched_value");
                    
                    return operations.exec();
                }
            });

            // Verify the watched transaction succeeded
            assertThat(template.opsForValue().get(key)).isEqualTo("watched_value");
        } finally {
            // Clean up test key
            template.delete(key);
        }
    }

    @Test
    void testTransactionWatchConflict() {
        String key = "test:tx:watch:conflict";
        
        try {
            // Clean up and set initial value
            template.delete(key);
            template.opsForValue().set(key, "initial");

            // Simulate a conflict by modifying the watched key during transaction
            List<Object> results = template.execute(new SessionCallback<List<Object>>() {
                @SuppressWarnings({ "unchecked", "rawtypes" })
                public List<Object> execute(RedisOperations operations) {
                    // Watch the key
                    operations.watch(key);
                    
                    // Simulate external modification (this would normally be from another client)
                    // We'll use a separate connection to modify the key
                    try (RedisConnection separateConnection = connectionFactory.getConnection()) {
                        separateConnection.stringCommands().set(key.getBytes(), "modified_externally".getBytes());
                    }
                    
                    // Start transaction - this should fail due to WATCH conflict
                    operations.multi();
                    operations.opsForValue().set(key, "should_not_be_set");
                    
                    try {
                        return operations.exec();
                    } catch (ValkeyGlideWatchConflictException e) {
                        // Handle WATCH conflict by returning null
                        return null;
                    }
                }
            });

            // Transaction should have been aborted due to WATCH conflict
            // exec() should return null when transaction is aborted
            assertThat(results).isNull();
            
            // The external modification should remain
            assertThat(template.opsForValue().get(key)).isEqualTo("modified_externally");
        } finally {
            // Clean up test key
            template.delete(key);
        }
    }

    @Test
    void testLowLevelTransactionAPI() {
        String key = "test:tx:lowlevel:key";
        
        try {
            // Test using direct connection API
            template.execute((RedisCallback<Void>) connection -> {
                // Clean up
                connection.keyCommands().del(key.getBytes());
                
                // Test multi/exec directly
                connection.multi();
                
                // Verify we're in queuing mode
                assertThat(connection.isQueueing()).isTrue();
                
                // Execute commands
                connection.stringCommands().set(key.getBytes(), "direct_value".getBytes());
                connection.stringCommands().set((key + "2").getBytes(), "direct_value2".getBytes());
                
                // Execute transaction
                List<Object> results = connection.exec();
                
                // Verify we're no longer in queuing mode
                assertThat(connection.isQueueing()).isFalse();
                
                // Verify results
                assertThat(results).isNotNull();
                assertThat(results).hasSize(2);
                
                return null;
            });

            // Verify values were set
            assertThat(template.opsForValue().get(key)).isEqualTo("direct_value");
            assertThat(template.opsForValue().get(key + "2")).isEqualTo("direct_value2");
        } finally {
            // Clean up test keys
            template.delete(key);
            template.delete(key + "2");
        }
    }

    @Test
    void testTransactionWithPipelining() {
        String key1 = "test:tx:pipeline:key1";
        String key2 = "test:tx:pipeline:key2";
        
        try {
            // Clean up
            template.delete(key1);
            template.delete(key2);

            // Test transaction combined with pipelining
            List<Object> results = template.executePipelined(new SessionCallback<Object>() {
                @SuppressWarnings({ "unchecked", "rawtypes" })
                public Object execute(RedisOperations operations) {
                    operations.multi();
                    
                    operations.opsForValue().set(key1, "pipeline_value1");
                    operations.opsForValue().set(key2, "pipeline_value2");
                    
                    operations.exec();
                    
                    return null; // SessionCallback for pipelining should return null
                }
            });

            // Verify pipelined transaction worked
            assertThat(results).isNotNull();
            
            // Verify values were set
            assertThat(template.opsForValue().get(key1)).isEqualTo("pipeline_value1");
            assertThat(template.opsForValue().get(key2)).isEqualTo("pipeline_value2");
        } finally {
            // Clean up test keys
            template.delete(key1);
            template.delete(key2);
        }
    }

    @Test
    void testNestedTransactionBehavior() {
        String key = "test:tx:nested:key";
        
        try {
            // Clean up
            template.delete(key);

            // Test that nested MULTI calls are handled correctly
            template.execute((RedisCallback<Void>) connection -> {
                connection.multi();
                assertThat(connection.isQueueing()).isTrue();
                
                // Calling multi() again should not change the state
                connection.multi();
                assertThat(connection.isQueueing()).isTrue();
                
                connection.stringCommands().set(key.getBytes(), "nested_test".getBytes());
                
                List<Object> results = connection.exec();
                assertThat(connection.isQueueing()).isFalse();
                assertThat(results).hasSize(1);
                
                return null;
            });

            assertThat(template.opsForValue().get(key)).isEqualTo("nested_test");
        } finally {
            // Clean up test key
            template.delete(key);
        }
    }
}

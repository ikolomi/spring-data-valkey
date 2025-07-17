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

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;

/**
 * Comprehensive low-level integration tests for {@link ValkeyGlideConnection} 
 * transaction functionality using the RedisTxCommands interface directly.
 * 
 * These tests validate the implementation of:
 * - multi() / exec() / discard()
 * - watch() / unwatch() 
 * - isQueueing() state management
 * - Command queuing during transactions
 * - Error handling and edge cases
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
@TestInstance(Lifecycle.PER_CLASS)
public class ValkeyGlideConnectionTransactionIntegrationTests {

    private ValkeyGlideConnectionFactory connectionFactory;
    private RedisConnection connection;

    @BeforeAll
    void setUpAll() {
        // Create connection factory
        connectionFactory = createConnectionFactory();
        
        // Check if server is available
        boolean serverAvailable = isServerAvailable(connectionFactory);
        assumeTrue(serverAvailable, "Redis server is not available");
    }

    @BeforeEach
    void setUp() {
        connection = connectionFactory.getConnection();
        
        // Clean up any existing test keys
        cleanupTestKeys();
    }

    @AfterEach
    void tearDown() {
        if (connection != null && !connection.isClosed()) {
            // Clean up any remaining test keys
            cleanupTestKeys();
            
            // Reset connection state if in transaction
            if (connection.isQueueing()) {
                try {
                    connection.discard();
                } catch (Exception e) {
                    // Ignore cleanup errors
                }
            }
            
            connection.close();
        }
    }

    @AfterAll
    void tearDownAll() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
    }

    // ==================== Basic Transaction Tests ====================

    @Test
    void testBasicMultiExec() {
        String key1 = "test:tx:basic:key1";
        String key2 = "test:tx:basic:key2";
        
        try {
            // Start transaction
            connection.multi();
            assertThat(connection.isQueueing()).isTrue();
            
            // Queue commands
            connection.stringCommands().set(key1.getBytes(), "value1".getBytes());
            connection.stringCommands().set(key2.getBytes(), "value2".getBytes());
            byte[] result1 = connection.stringCommands().get(key1.getBytes());
            byte[] result2 = connection.stringCommands().get(key2.getBytes());
            
            // Commands in transaction should return null (queued)
            assertThat(result1).isNull();
            assertThat(result2).isNull();
            
            // Execute transaction
            List<Object> results = connection.exec();
            assertThat(connection.isQueueing()).isFalse();
            
            // Verify results
            assertThat(results).isNotNull();
            assertThat(results).hasSize(4); // 2 SETs + 2 GETs
            
            // Verify actual values were set
            byte[] actualValue1 = connection.stringCommands().get(key1.getBytes());
            byte[] actualValue2 = connection.stringCommands().get(key2.getBytes());
            assertThat(actualValue1).isEqualTo("value1".getBytes());
            assertThat(actualValue2).isEqualTo("value2".getBytes());
        } finally {
            // Clean up test keys
            cleanupKey(key1);
            cleanupKey(key2);
        }
    }

    @Test
    void testMultiDiscard() {
        String key = "test:tx:discard:key";
        
        try {
            // Set initial value
            connection.stringCommands().set(key.getBytes(), "initial".getBytes());
            
            // Start transaction
            connection.multi();
            assertThat(connection.isQueueing()).isTrue();
            
            // Queue a command
            connection.stringCommands().set(key.getBytes(), "should_not_be_set".getBytes());
            
            // Discard transaction
            connection.discard();
            assertThat(connection.isQueueing()).isFalse();
            
            // Verify original value is unchanged
            byte[] actualValue = connection.stringCommands().get(key.getBytes());
            assertThat(actualValue).isEqualTo("initial".getBytes());
        } finally {
            // Clean up test key
            cleanupKey(key);
        }
    }

    @Test
    void testEmptyTransaction() {
        // Start and immediately execute empty transaction
        connection.multi();
        assertThat(connection.isQueueing()).isTrue();
        
        List<Object> results = connection.exec();
        assertThat(connection.isQueueing()).isFalse();
        assertThat(results).isNotNull();
        assertThat(results).isEmpty();
    }

    @Test
    void testMultipleConsecutiveTransactions() {
        String key1 = "test:tx:consecutive:key1";
        String key2 = "test:tx:consecutive:key2";
        
        try {
            // First transaction
            connection.multi();
            connection.stringCommands().set(key1.getBytes(), "tx1_value".getBytes());
            List<Object> results1 = connection.exec();
            assertThat(results1).hasSize(1);
            
            // Second transaction
            connection.multi();
            connection.stringCommands().set(key2.getBytes(), "tx2_value".getBytes());
            List<Object> results2 = connection.exec();
            assertThat(results2).hasSize(1);
            
            // Verify both values
            assertThat(connection.stringCommands().get(key1.getBytes())).isEqualTo("tx1_value".getBytes());
            assertThat(connection.stringCommands().get(key2.getBytes())).isEqualTo("tx2_value".getBytes());
        } finally {
            // Clean up test keys
            cleanupKey(key1);
            cleanupKey(key2);
        }
    }

    // ==================== WATCH/UNWATCH Tests ====================

    @Test
    void testWatchSuccess() {
        String key = "test:tx:watch:success";
        
        try {
            // Set initial value
            connection.stringCommands().set(key.getBytes(), "initial".getBytes());
            
            // Watch the key
            connection.watch(key.getBytes());
            
            // Start transaction and modify the watched key
            connection.multi();
            connection.stringCommands().set(key.getBytes(), "modified".getBytes());
            
            List<Object> results = connection.exec();
            
            // Transaction should succeed since no external modification occurred
            assertThat(results).isNotNull();
            assertThat(results).hasSize(1);
            assertThat(connection.stringCommands().get(key.getBytes())).isEqualTo("modified".getBytes());
        } finally {
            // Clean up test key
            cleanupKey(key);
        }
    }

    @Test
    void testWatchConflict() {
        String key = "test:tx:watch:conflict";
        
        try {
            // Set initial value
            connection.stringCommands().set(key.getBytes(), "initial".getBytes());
            
            // Watch the key
            connection.watch(key.getBytes());
            
            // Simulate external modification using separate connection
            try (RedisConnection otherConnection = connectionFactory.getConnection()) {
                otherConnection.stringCommands().set(key.getBytes(), "external_change".getBytes());
            }
            
            // Start transaction
            connection.multi();
            connection.stringCommands().set(key.getBytes(), "should_not_be_set".getBytes());
            
            List<Object> results = connection.exec();
            
            // Transaction should be aborted due to WATCH conflict
            assertThat(results).isNull();
            
            // External change should remain
            assertThat(connection.stringCommands().get(key.getBytes())).isEqualTo("external_change".getBytes());
        } finally {
            // Clean up test key
            cleanupKey(key);
        }
    }

    @Test
    void testUnwatch() {
        String key = "test:tx:unwatch";
        
        try {
            // Set initial value
            connection.stringCommands().set(key.getBytes(), "initial".getBytes());
            
            // Watch then unwatch
            connection.watch(key.getBytes());
            connection.unwatch();
            
            // Modify key externally (should not affect transaction after unwatch)
            try (RedisConnection otherConnection = connectionFactory.getConnection()) {
                otherConnection.stringCommands().set(key.getBytes(), "external_change".getBytes());
            }
            
            // Transaction should succeed despite external change
            connection.multi();
            connection.stringCommands().set(key.getBytes(), "transaction_value".getBytes());
            List<Object> results = connection.exec();
            
            assertThat(results).isNotNull();
            assertThat(results).hasSize(1);
            assertThat(connection.stringCommands().get(key.getBytes())).isEqualTo("transaction_value".getBytes());
        } finally {
            // Clean up test key
            cleanupKey(key);
        }
    }

    @Test
    void testWatchMultipleKeys() {
        String key1 = "test:tx:watch:multi:key1";
        String key2 = "test:tx:watch:multi:key2";
        
        try {
            // Set initial values
            connection.stringCommands().set(key1.getBytes(), "initial1".getBytes());
            connection.stringCommands().set(key2.getBytes(), "initial2".getBytes());
            
            // Watch both keys
            connection.watch(key1.getBytes(), key2.getBytes());
            
            // Modify one watched key externally
            try (RedisConnection otherConnection = connectionFactory.getConnection()) {
                otherConnection.stringCommands().set(key1.getBytes(), "external1".getBytes());
            }
            
            // Transaction should be aborted
            connection.multi();
            connection.stringCommands().set(key1.getBytes(), "tx1".getBytes());
            connection.stringCommands().set(key2.getBytes(), "tx2".getBytes());
            List<Object> results = connection.exec();
            
            assertThat(results).isNull();
            assertThat(connection.stringCommands().get(key1.getBytes())).isEqualTo("external1".getBytes());
            assertThat(connection.stringCommands().get(key2.getBytes())).isEqualTo("initial2".getBytes());
        } finally {
            // Clean up test keys
            cleanupKey(key1);
            cleanupKey(key2);
        }
    }

    // ==================== Error Handling Tests ====================

    @Test
    void testWatchDuringMultiThrowsException() {
        String key = "test:tx:watch:error";
        
        connection.multi();
        
        assertThatThrownBy(() -> connection.watch(key.getBytes()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("WATCH is not allowed during MULTI");
        
        connection.discard();
    }

    @Test
    void testExecWithoutMulti() {
        // Execute without starting transaction
        List<Object> results = connection.exec();
        assertThat(results).isNull();
    }

    @Test
    void testDiscardWithoutMulti() {
        // Should not throw exception
        assertThatCode(() -> connection.discard()).doesNotThrowAnyException();
        assertThat(connection.isQueueing()).isFalse();
    }

    @Test
    void testNestedMultiCalls() {
        String key = "test:tx:nested";
        
        try {
            connection.multi();
            assertThat(connection.isQueueing()).isTrue();
            
            // Calling multi() again should not change state
            connection.multi();
            assertThat(connection.isQueueing()).isTrue();
            
            connection.stringCommands().set(key.getBytes(), "nested_value".getBytes());
            List<Object> results = connection.exec();
            
            assertThat(results).hasSize(1);
            assertThat(connection.stringCommands().get(key.getBytes())).isEqualTo("nested_value".getBytes());
        } finally {
            // Clean up test key
            cleanupKey(key);
        }
    }

    // ==================== Complex Scenarios ====================

    @Test
    void testTransactionWithDifferentCommandTypes() {
        String stringKey = "test:tx:complex:string";
        String listKey = "test:tx:complex:list";
        String hashKey = "test:tx:complex:hash";
        
        connection.multi();
        
        // String commands
        connection.stringCommands().set(stringKey.getBytes(), "string_value".getBytes());
        connection.stringCommands().get(stringKey.getBytes());
        
        // List commands
        connection.listCommands().lPush(listKey.getBytes(), "item1".getBytes());
        connection.listCommands().lLen(listKey.getBytes());
        
        // Hash commands
        connection.hashCommands().hSet(hashKey.getBytes(), "field1".getBytes(), "hash_value".getBytes());
        connection.hashCommands().hGet(hashKey.getBytes(), "field1".getBytes());
        
        List<Object> results = connection.exec();
        
        assertThat(results).hasSize(6);
        
        // Verify all commands were executed
        assertThat(connection.stringCommands().get(stringKey.getBytes())).isEqualTo("string_value".getBytes());
        assertThat(connection.listCommands().lLen(listKey.getBytes())).isEqualTo(1L);
        assertThat(connection.hashCommands().hGet(hashKey.getBytes(), "field1".getBytes())).isEqualTo("hash_value".getBytes());
    }

    @Test
    void testLargeTransaction() {
        String keyPrefix = "test:tx:large:";
        int commandCount = 100;
        
        connection.multi();
        
        // Queue many commands
        for (int i = 0; i < commandCount; i++) {
            String key = keyPrefix + i;
            String value = "value" + i;
            connection.stringCommands().set(key.getBytes(), value.getBytes());
        }
        
        List<Object> results = connection.exec();
        
        assertThat(results).hasSize(commandCount);
        
        // Verify a few random values
        assertThat(connection.stringCommands().get((keyPrefix + "0").getBytes())).isEqualTo("value0".getBytes());
        assertThat(connection.stringCommands().get((keyPrefix + "50").getBytes())).isEqualTo("value50".getBytes());
        assertThat(connection.stringCommands().get((keyPrefix + "99").getBytes())).isEqualTo("value99".getBytes());
    }

    @Test
    void testConcurrentTransactions() throws Exception {
        String key = "test:tx:concurrent";
        ExecutorService executor = Executors.newFixedThreadPool(5);
        
        try {
            CompletableFuture<?>[] futures = new CompletableFuture[5];
            
            for (int i = 0; i < 5; i++) {
                final int threadId = i;
                futures[i] = CompletableFuture.runAsync(() -> {
                    try (RedisConnection conn = connectionFactory.getConnection()) {
                        conn.multi();
                        conn.stringCommands().set((key + ":" + threadId).getBytes(), ("thread" + threadId).getBytes());
                        List<Object> results = conn.exec();
                        assertThat(results).hasSize(1);
                    }
                }, executor);
            }
            
            CompletableFuture.allOf(futures).get(10, TimeUnit.SECONDS);
            
            // Verify all threads succeeded
            for (int i = 0; i < 5; i++) {
                assertThat(connection.stringCommands().get((key + ":" + i).getBytes()))
                    .isEqualTo(("thread" + i).getBytes());
            }
            
        } finally {
            executor.shutdown();
        }
    }

    // ==================== State Management Tests ====================

    @Test
    void testIsQueueingState() {
        assertThat(connection.isQueueing()).isFalse();
        
        connection.multi();
        assertThat(connection.isQueueing()).isTrue();
        
        connection.exec();
        assertThat(connection.isQueueing()).isFalse();
    }

    @Test
    void testIsQueueingAfterDiscard() {
        connection.multi();
        assertThat(connection.isQueueing()).isTrue();
        
        connection.discard();
        assertThat(connection.isQueueing()).isFalse();
    }

    @Test
    void testTransactionStateAfterException() {
        connection.multi();
        assertThat(connection.isQueueing()).isTrue();
        
        try {
            connection.watch("test".getBytes()); // Should throw exception during MULTI
        } catch (IllegalStateException e) {
            // Expected exception
        }
        
        // State should remain in transaction mode
        assertThat(connection.isQueueing()).isTrue();
        
        connection.discard();
        assertThat(connection.isQueueing()).isFalse();
    }

    // ==================== Helper Methods ====================

    private ValkeyGlideConnectionFactory createConnectionFactory() {
        RedisStandaloneConfiguration config = new RedisStandaloneConfiguration();
        config.setHostName(getRedisHost());
        config.setPort(getRedisPort());
        return ValkeyGlideConnectionFactory.createValkeyGlideConnectionFactory(config);
    }

    private boolean isServerAvailable(RedisConnectionFactory factory) {
        try (RedisConnection connection = factory.getConnection()) {
            return "PONG".equals(connection.ping());
        } catch (Exception e) {
            return false;
        }
    }

    private String getRedisHost() {
        return System.getProperty("redis.host", "localhost");
    }

    private int getRedisPort() {
        return Integer.parseInt(System.getProperty("redis.port", "6379"));
    }

    private void cleanupTestKeys() {
        // Clean up specific test key patterns
        String[] testKeys = {
            "test:tx:basic:key1", "test:tx:basic:key2",
            "test:tx:discard:key", "test:tx:consecutive:key1", "test:tx:consecutive:key2",
            "test:tx:watch:success", "test:tx:watch:conflict", "test:tx:unwatch",
            "test:tx:watch:multi:key1", "test:tx:watch:multi:key2", "test:tx:watch:error",
            "test:tx:nested", "test:tx:complex:string", "test:tx:complex:list", "test:tx:complex:hash",
            "test:tx:concurrent"
        };
        
        for (String key : testKeys) {
            connection.keyCommands().del(key.getBytes());
        }
        
        // Clean up large transaction keys
        for (int i = 0; i < 100; i++) {
            connection.keyCommands().del(("test:tx:large:" + i).getBytes());
        }
        
        // Clean up concurrent test keys
        for (int i = 0; i < 5; i++) {
            connection.keyCommands().del(("test:tx:concurrent:" + i).getBytes());
        }
    }
    
    private void cleanupKey(String key) {
        connection.keyCommands().del(key.getBytes());
    }
}

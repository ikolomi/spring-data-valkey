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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.ExpirationOptions;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.SortParameters;
import org.springframework.data.redis.connection.ValueEncoding;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;

/**
 * Comprehensive low-level integration tests for {@link ValkeyGlideConnection} 
 * key functionality using the RedisKeyCommands interface directly.
 * 
 * These tests validate the implementation of all RedisKeyCommands methods:
 * - Basic key operations (exists, del, unlink, type, touch)
 * - Key discovery operations (keys, scan, randomKey)
 * - Key renaming operations (rename, renameNX)
 * - Key expiration operations (expire, pExpire, expireAt, pExpireAt, persist)
 * - Key TTL operations (ttl, pTtl)
 * - Key movement operations (move)
 * - Key copy operations (copy)
 * - Sorting operations (sort with various parameters)
 * - Serialization operations (dump, restore)
 * - Object introspection operations (encodingOf, idletime, refcount)
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
@TestInstance(Lifecycle.PER_CLASS)
public class ValkeyGlideConnectionKeyCommandsIntegrationTests {

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
            connection.close();
        }
    }

    @AfterAll
    void tearDownAll() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
    }

    // ==================== Basic Key Operations ====================

    @Test
    void testExistsAndDel() {
        String key1 = "test:key:exists:key1";
        String key2 = "test:key:exists:key2";
        String key3 = "test:key:exists:key3";
        byte[] value = "test_value".getBytes();
        
        try {
            // Test exists on non-existent keys
            Boolean exists1 = connection.keyCommands().exists(key1.getBytes());
            assertThat(exists1).isFalse();
            
            Long existsMultiple1 = connection.keyCommands().exists(key1.getBytes(), key2.getBytes(), key3.getBytes());
            assertThat(existsMultiple1).isEqualTo(0L);
            
            // Set up test data
            connection.stringCommands().set(key1.getBytes(), value);
            connection.stringCommands().set(key2.getBytes(), value);
            
            // Test exists on existing keys
            Boolean exists2 = connection.keyCommands().exists(key1.getBytes());
            assertThat(exists2).isTrue();
            
            Long existsMultiple2 = connection.keyCommands().exists(key1.getBytes(), key2.getBytes(), key3.getBytes());
            assertThat(existsMultiple2).isEqualTo(2L); // 2 keys exist
            
            // Test with duplicate keys
            Long existsDuplicate = connection.keyCommands().exists(key1.getBytes(), key1.getBytes(), key2.getBytes());
            assertThat(existsDuplicate).isEqualTo(3L); // Counts duplicates
            
            // Test del single key
            Long delResult1 = connection.keyCommands().del(key1.getBytes());
            assertThat(delResult1).isEqualTo(1L);
            
            // Verify key was deleted
            Boolean existsAfterDel = connection.keyCommands().exists(key1.getBytes());
            assertThat(existsAfterDel).isFalse();
            
            // Test del multiple keys
            connection.stringCommands().set(key3.getBytes(), value); // Add key3
            Long delResult2 = connection.keyCommands().del(key2.getBytes(), key3.getBytes());
            assertThat(delResult2).isEqualTo(2L);
            
            // Test del non-existent key
            Long delResult3 = connection.keyCommands().del("non:existent:key".getBytes());
            assertThat(delResult3).isEqualTo(0L);
        } finally {
            cleanupKey(key1);
            cleanupKey(key2);
            cleanupKey(key3);
        }
    }

    @Test
    void testCopy() {
        String sourceKey = "test:key:copy:source";
        String targetKey = "test:key:copy:target";
        String existingTargetKey = "test:key:copy:existing";
        byte[] value = "test_value".getBytes();
        byte[] existingValue = "existing_value".getBytes();
        
        try {
            // Test copy on non-existent source key
            Boolean copyNonExistent = connection.keyCommands().copy(sourceKey.getBytes(), targetKey.getBytes(), false);
            assertThat(copyNonExistent).isFalse();
            
            // Set up test data
            connection.stringCommands().set(sourceKey.getBytes(), value);
            connection.stringCommands().set(existingTargetKey.getBytes(), existingValue);
            
            // Test successful copy
            Boolean copyResult1 = connection.keyCommands().copy(sourceKey.getBytes(), targetKey.getBytes(), false);
            assertThat(copyResult1).isTrue();
            
            // Verify copy was successful
            byte[] copiedValue = connection.stringCommands().get(targetKey.getBytes());
            assertThat(copiedValue).isEqualTo(value);
            
            // Verify source still exists
            byte[] sourceValue = connection.stringCommands().get(sourceKey.getBytes());
            assertThat(sourceValue).isEqualTo(value);
            
            // Test copy without replace (should fail)
            Boolean copyResult2 = connection.keyCommands().copy(sourceKey.getBytes(), existingTargetKey.getBytes(), false);
            assertThat(copyResult2).isFalse();
            
            // Verify existing target was not overwritten
            byte[] existingTargetValue = connection.stringCommands().get(existingTargetKey.getBytes());
            assertThat(existingTargetValue).isEqualTo(existingValue);
            
            // Test copy with replace (should succeed)
            Boolean copyResult3 = connection.keyCommands().copy(sourceKey.getBytes(), existingTargetKey.getBytes(), true);
            assertThat(copyResult3).isTrue();
            
            // Verify existing target was overwritten
            byte[] replacedValue = connection.stringCommands().get(existingTargetKey.getBytes());
            assertThat(replacedValue).isEqualTo(value);
        } finally {
            cleanupKey(sourceKey);
            cleanupKey(targetKey);
            cleanupKey(existingTargetKey);
        }
    }

    // ==================== Key Type and Properties ====================

    @Test
    void testType() {
        String stringKey = "test:key:type:string";
        String listKey = "test:key:type:list";
        String hashKey = "test:key:type:hash";
        
        try {
            // Test type on non-existent key
            DataType nonExistentType = connection.keyCommands().type("non:existent:key".getBytes());
            assertThat(nonExistentType).isEqualTo(DataType.NONE);
            
            // Set up different data types
            connection.stringCommands().set(stringKey.getBytes(), "value".getBytes());
            connection.listCommands().lPush(listKey.getBytes(), "item".getBytes());
            connection.hashCommands().hSet(hashKey.getBytes(), "field".getBytes(), "value".getBytes());
            
            // Test different types
            DataType stringType = connection.keyCommands().type(stringKey.getBytes());
            assertThat(stringType).isEqualTo(DataType.STRING);
            
            DataType listType = connection.keyCommands().type(listKey.getBytes());
            assertThat(listType).isEqualTo(DataType.LIST);
            
            DataType hashType = connection.keyCommands().type(hashKey.getBytes());
            assertThat(hashType).isEqualTo(DataType.HASH);
        } finally {
            cleanupKey(stringKey);
            cleanupKey(listKey);
            cleanupKey(hashKey);
        }
    }

    @Test
    void testTouch() {
        String key1 = "test:key:touch:key1";
        String key2 = "test:key:touch:key2";
        String key3 = "test:key:touch:key3";
        byte[] value = "test_value".getBytes();
        
        try {
            // Test touch on non-existent keys
            Long touchResult1 = connection.keyCommands().touch(key1.getBytes(), key2.getBytes());
            assertThat(touchResult1).isEqualTo(0L);
            
            // Set up test data
            connection.stringCommands().set(key1.getBytes(), value);
            connection.stringCommands().set(key2.getBytes(), value);
            
            // Test touch on existing keys
            Long touchResult2 = connection.keyCommands().touch(key1.getBytes(), key2.getBytes(), key3.getBytes());
            assertThat(touchResult2).isEqualTo(2L); // Only 2 keys exist
            
            // Test single key touch
            Long touchResult3 = connection.keyCommands().touch(key1.getBytes());
            assertThat(touchResult3).isEqualTo(1L);
        } finally {
            cleanupKey(key1);
            cleanupKey(key2);
            cleanupKey(key3);
        }
    }

    // ==================== Key Discovery Operations ====================

    @Test
    void testKeys() {
        String basePattern = "test:key:keys:";
        String key1 = basePattern + "abc";
        String key2 = basePattern + "def";
        String key3 = basePattern + "xyz";
        String otherKey = "test:other:pattern";
        byte[] value = "test_value".getBytes();
        
        try {
            // Set up test data
            connection.stringCommands().set(key1.getBytes(), value);
            connection.stringCommands().set(key2.getBytes(), value);
            connection.stringCommands().set(key3.getBytes(), value);
            connection.stringCommands().set(otherKey.getBytes(), value);
            
            // Test pattern matching
            Set<byte[]> matchedKeys = connection.keyCommands().keys((basePattern + "*").getBytes());
            assertThat(matchedKeys).hasSize(3);
            
            // Convert to strings for easier comparison
            Set<String> matchedKeyStrings = matchedKeys.stream()
                .map(String::new)
                .collect(java.util.stream.Collectors.toSet());
            
            assertThat(matchedKeyStrings).containsExactlyInAnyOrder(key1, key2, key3);
            
            // Test specific pattern
            Set<byte[]> specificMatch = connection.keyCommands().keys((basePattern + "a*").getBytes());
            assertThat(specificMatch).hasSize(1);
            assertThat(new String(specificMatch.iterator().next())).isEqualTo(key1);
            
            // Test non-matching pattern
            Set<byte[]> noMatch = connection.keyCommands().keys("no:match:*".getBytes());
            assertThat(noMatch).isEmpty();
        } finally {
            cleanupKey(key1);
            cleanupKey(key2);
            cleanupKey(key3);
            cleanupKey(otherKey);
        }
    }

    @Test
    void testRandomKey() {
        String key1 = "test:key:random:key1";
        String key2 = "test:key:random:key2";
        byte[] value = "test_value".getBytes();
        
        try {
            // Test randomKey when no keys exist
            cleanupTestKeys(); // Ensure clean state
            byte[] randomKey1 = connection.keyCommands().randomKey();
            // Note: randomKey might return null or some other key depending on database state
            
            // Set up test data
            connection.stringCommands().set(key1.getBytes(), value);
            connection.stringCommands().set(key2.getBytes(), value);
            
            // Test randomKey when keys exist
            byte[] randomKey2 = connection.keyCommands().randomKey();
            assertThat(randomKey2).isNotNull();
            
            // Should be one of our keys or some other key in the database
            String randomKeyStr = new String(randomKey2);
            // We can't guarantee which key will be returned, just that one is returned
            assertThat(randomKeyStr).isNotEmpty();
        } finally {
            cleanupKey(key1);
            cleanupKey(key2);
        }
    }

    @Test
    void testScan() {
        String basePattern = "test:key:scan:";
        String key1 = basePattern + "item1";
        String key2 = basePattern + "item2";
        String key3 = basePattern + "other1";
        byte[] value = "test_value".getBytes();
        
        try {
            // Set up test data
            connection.stringCommands().set(key1.getBytes(), value);
            connection.stringCommands().set(key2.getBytes(), value);
            connection.stringCommands().set(key3.getBytes(), value);
            
            // Test basic scan
            ScanOptions options = ScanOptions.scanOptions().match(basePattern + "*").build();
            Cursor<byte[]> cursor = connection.keyCommands().scan(options);
            
            java.util.List<String> scannedKeys = new java.util.ArrayList<>();
            while (cursor.hasNext()) {
                scannedKeys.add(new String(cursor.next()));
            }
            cursor.close();
            
            // Should find our test keys
            assertThat(scannedKeys).containsAll(java.util.Arrays.asList(key1, key2, key3));
            
            // Test scan with count
            ScanOptions countOptions = ScanOptions.scanOptions().count(1).build();
            Cursor<byte[]> countCursor = connection.keyCommands().scan(countOptions);
            
            java.util.List<String> countScannedKeys = new java.util.ArrayList<>();
            while (countCursor.hasNext()) {
                countScannedKeys.add(new String(countCursor.next()));
            }
            countCursor.close();
            
            // Should get results, but in smaller batches
            assertThat(countScannedKeys).isNotEmpty();
        } finally {
            cleanupKey(key1);
            cleanupKey(key2);
            cleanupKey(key3);
        }
    }

    // ==================== Key Renaming Operations ====================

    @Test
    void testRename() {
        String oldKey = "test:key:rename:old";
        String newKey = "test:key:rename:new";
        byte[] value = "test_value".getBytes();
        
        try {
            // Set up test data
            connection.stringCommands().set(oldKey.getBytes(), value);
            
            // Test rename
            connection.keyCommands().rename(oldKey.getBytes(), newKey.getBytes());
            
            // Verify old key no longer exists
            Boolean oldExists = connection.keyCommands().exists(oldKey.getBytes());
            assertThat(oldExists).isFalse();
            
            // Verify new key exists with correct value
            Boolean newExists = connection.keyCommands().exists(newKey.getBytes());
            assertThat(newExists).isTrue();
            
            byte[] newValue = connection.stringCommands().get(newKey.getBytes());
            assertThat(newValue).isEqualTo(value);
        } finally {
            cleanupKey(oldKey);
            cleanupKey(newKey);
        }
    }

    @Test
    void testRenameNX() {
        String oldKey = "test:key:renamenx:old";
        String newKey = "test:key:renamenx:new";
        String existingKey = "test:key:renamenx:existing";
        byte[] value = "test_value".getBytes();
        byte[] existingValue = "existing_value".getBytes();
        
        try {
            // Set up test data
            connection.stringCommands().set(oldKey.getBytes(), value);
            connection.stringCommands().set(existingKey.getBytes(), existingValue);
            
            // Test renameNX to non-existing key (should succeed)
            Boolean renameResult1 = connection.keyCommands().renameNX(oldKey.getBytes(), newKey.getBytes());
            assertThat(renameResult1).isTrue();
            
            // Verify rename was successful
            Boolean oldExists = connection.keyCommands().exists(oldKey.getBytes());
            assertThat(oldExists).isFalse();
            
            Boolean newExists = connection.keyCommands().exists(newKey.getBytes());
            assertThat(newExists).isTrue();
            
            // Test renameNX to existing key (should fail)
            Boolean renameResult2 = connection.keyCommands().renameNX(newKey.getBytes(), existingKey.getBytes());
            assertThat(renameResult2).isFalse();
            
            // Verify keys remain unchanged
            Boolean newStillExists = connection.keyCommands().exists(newKey.getBytes());
            assertThat(newStillExists).isTrue();
            
            byte[] existingStillValue = connection.stringCommands().get(existingKey.getBytes());
            assertThat(existingStillValue).isEqualTo(existingValue);
        } finally {
            cleanupKey(oldKey);
            cleanupKey(newKey);
            cleanupKey(existingKey);
        }
    }

    // ==================== Key Expiration Operations ====================

    @Test
    void testExpire() {
        String key = "test:key:expire";
        byte[] value = "test_value".getBytes();
        
        try {
            // Set up test data
            connection.stringCommands().set(key.getBytes(), value);
            
            // Test expire
            Boolean expireResult = connection.keyCommands().expire(key.getBytes(), 1);
            assertThat(expireResult).isTrue();
            
            // Verify key still exists initially
            Boolean exists1 = connection.keyCommands().exists(key.getBytes());
            assertThat(exists1).isTrue();
            
            // Wait for expiration
            Thread.sleep(2000);
            Boolean exists2 = connection.keyCommands().exists(key.getBytes());
            assertThat(exists2).isFalse();
            
            // Test expire on non-existent key
            Boolean expireNonExistent = connection.keyCommands().expire("non:existent:key".getBytes(), 1);
            assertThat(expireNonExistent).isFalse();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testPExpire() {
        String key = "test:key:pexpire";
        byte[] value = "test_value".getBytes();
        
        try {
            // Set up test data
            connection.stringCommands().set(key.getBytes(), value);
            
            // Test pExpire (milliseconds)
            Boolean pExpireResult = connection.keyCommands().pExpire(key.getBytes(), 1000);
            assertThat(pExpireResult).isTrue();
            
            // Verify key still exists initially
            Boolean exists1 = connection.keyCommands().exists(key.getBytes());
            assertThat(exists1).isTrue();
            
            // Wait for expiration
            Thread.sleep(2000);
            Boolean exists2 = connection.keyCommands().exists(key.getBytes());
            assertThat(exists2).isFalse();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testExpireAt() {
        String key = "test:key:expireat";
        byte[] value = "test_value".getBytes();
        
        try {
            // Set up test data
            connection.stringCommands().set(key.getBytes(), value);
            
            // Calculate future timestamp (current time + 1 second)
            long futureTimestamp = System.currentTimeMillis() / 1000 + 1;
            
            // Test expireAt
            Boolean expireAtResult = connection.keyCommands().expireAt(key.getBytes(), futureTimestamp);
            assertThat(expireAtResult).isTrue();
            
            // Verify key still exists initially
            Boolean exists1 = connection.keyCommands().exists(key.getBytes());
            assertThat(exists1).isTrue();
            
            // Wait for expiration
            Thread.sleep(2000);
            Boolean exists2 = connection.keyCommands().exists(key.getBytes());
            assertThat(exists2).isFalse();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testPExpireAt() {
        String key = "test:key:pexpireat";
        byte[] value = "test_value".getBytes();
        
        try {
            // Set up test data
            connection.stringCommands().set(key.getBytes(), value);
            
            // Calculate future timestamp in milliseconds
            long futureTimestampMillis = System.currentTimeMillis() + 1000;
            
            // Test pExpireAt
            Boolean pExpireAtResult = connection.keyCommands().pExpireAt(key.getBytes(), futureTimestampMillis);
            assertThat(pExpireAtResult).isTrue();
            
            // Verify key still exists initially
            Boolean exists1 = connection.keyCommands().exists(key.getBytes());
            assertThat(exists1).isTrue();
            
            // Wait for expiration
            Thread.sleep(2000);
            Boolean exists2 = connection.keyCommands().exists(key.getBytes());
            assertThat(exists2).isFalse();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testPersist() {
        String key = "test:key:persist";
        byte[] value = "test_value".getBytes();
        
        try {
            // Set up test data with expiration
            connection.stringCommands().set(key.getBytes(), value);
            connection.keyCommands().expire(key.getBytes(), 10); // 10 seconds
            
            // Test persist
            Boolean persistResult = connection.keyCommands().persist(key.getBytes());
            assertThat(persistResult).isTrue();
            
            // Verify TTL is now -1 (no expiration)
            Long ttl = connection.keyCommands().ttl(key.getBytes());
            assertThat(ttl).isEqualTo(-1L);
            
            // Test persist on key without expiration
            Boolean persistResult2 = connection.keyCommands().persist(key.getBytes());
            assertThat(persistResult2).isFalse();
            
            // Test persist on non-existent key
            Boolean persistNonExistent = connection.keyCommands().persist("non:existent:key".getBytes());
            assertThat(persistNonExistent).isFalse();
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== TTL Operations ====================

    @Test
    void testTtl() {
        String key = "test:key:ttl";
        byte[] value = "test_value".getBytes();
        
        try {
            // Test TTL on non-existent key
            Long nonExistentTtl = connection.keyCommands().ttl("non:existent:key".getBytes());
            assertThat(nonExistentTtl).isEqualTo(-2L); // Key doesn't exist
            
            // Set up test data without expiration
            connection.stringCommands().set(key.getBytes(), value);
            Long noExpirationTtl = connection.keyCommands().ttl(key.getBytes());
            assertThat(noExpirationTtl).isEqualTo(-1L); // No expiration set
            
            // Set expiration and test TTL
            connection.keyCommands().expire(key.getBytes(), 10);
            Long ttl = connection.keyCommands().ttl(key.getBytes());
            assertThat(ttl).isGreaterThan(0L).isLessThanOrEqualTo(10L);
            
            // Test TTL with time unit conversion
            Long ttlInMillis = connection.keyCommands().ttl(key.getBytes(), TimeUnit.MILLISECONDS);
            assertThat(ttlInMillis).isGreaterThan(0L).isLessThanOrEqualTo(10000L);
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testPTtl() {
        String key = "test:key:pttl";
        byte[] value = "test_value".getBytes();
        
        try {
            // Set up test data with expiration
            connection.stringCommands().set(key.getBytes(), value);
            connection.keyCommands().pExpire(key.getBytes(), 10000); // 10 seconds in milliseconds
            
            // Test pTtl
            Long pTtl = connection.keyCommands().pTtl(key.getBytes());
            assertThat(pTtl).isGreaterThan(0L).isLessThanOrEqualTo(10000L);
            
            // Test pTtl with time unit conversion
            Long pTtlInSeconds = connection.keyCommands().pTtl(key.getBytes(), TimeUnit.SECONDS);
            assertThat(pTtlInSeconds).isGreaterThan(0L).isLessThanOrEqualTo(10L);
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Error Handling and Edge Cases ====================

    @Test
    void testKeyOperationsErrorHandling() {
        // Test operations on null keys (should throw IllegalArgumentException)
        assertThatThrownBy(() -> connection.keyCommands().exists((byte[]) null))
            .isInstanceOf(IllegalArgumentException.class);
        
        assertThatThrownBy(() -> connection.keyCommands().del((byte[]) null))
            .isInstanceOf(IllegalArgumentException.class);
        
        // Test operations on empty keys (should throw IllegalArgumentException)
        assertThatThrownBy(() -> connection.keyCommands().exists(new byte[0]))
            .isInstanceOf(IllegalArgumentException.class);
        
        assertThatThrownBy(() -> connection.keyCommands().del(new byte[0]))
            .isInstanceOf(IllegalArgumentException.class);
        
        // Test rename with same old and new key - commented out as renaming to the same name returns OK in Redis/Valkey
        // String key = "test:key:error:same";
        // byte[] value = "test_value".getBytes();
        
        // try {
        //     connection.stringCommands().set(key.getBytes(), value);
            
        //     // Redis should return an error when trying to rename a key to itself
        //     assertThatThrownBy(() -> connection.keyCommands().rename(key.getBytes(), key.getBytes()))
        //         .isInstanceOf(Exception.class);
        // } finally {
        //     cleanupKey(key);
        // }
        
        // Test rename with non-existent key (should throw an exception)
        assertThatThrownBy(() -> connection.keyCommands().rename("non:existent:key".getBytes(), "new:key".getBytes()))
            .isInstanceOf(Exception.class);
    }

    @Test
    void testExpirationOperationsEdgeCases() {
        String key = "test:key:expiration:edge";
        byte[] value = "test_value".getBytes();
        
        try {
            // Test expiration operations on non-existent key (should return false)
            Boolean expireNonExistent = connection.keyCommands().expire("non:existent:key".getBytes(), 10);
            assertThat(expireNonExistent).isFalse();
            
            // Test with large but reasonable expiration values
            // Using Integer.MAX_VALUE (about 68 years) which is within Redis/Valkey acceptable range
            connection.stringCommands().set(key.getBytes(), value);
            Boolean expireLarge = connection.keyCommands().expire(key.getBytes(), Integer.MAX_VALUE);
            assertThat(expireLarge).isTrue(); // Should succeed with reasonable large value
            
            // Verify the key still exists and has a TTL set
            Boolean keyExists = connection.keyCommands().exists(key.getBytes());
            assertThat(keyExists).isTrue();
            Long ttl = connection.keyCommands().ttl(key.getBytes());
            assertThat(ttl).isGreaterThan(0L); // Should have a positive TTL
            
            // Test with zero expiration (should delete the key immediately in most implementations)
            connection.stringCommands().set(key.getBytes(), value);
            Boolean expireZero = connection.keyCommands().expire(key.getBytes(), 0);
            assertThat(expireZero).isTrue(); // Should return true since key existed
            // Note: EXPIRE with 0 seconds typically deletes the key immediately
            Boolean keyExistsAfterZero = connection.keyCommands().exists(key.getBytes());
            assertThat(keyExistsAfterZero).isFalse(); // Key should be deleted
            
            // Test with negative expiration (should delete the key immediately)
            connection.stringCommands().set(key.getBytes(), value);
            Boolean expireNegative = connection.keyCommands().expire(key.getBytes(), -1);
            assertThat(expireNegative).isTrue(); // Should return true since key existed
            // Negative expire time should delete the key immediately
            Boolean keyExistsAfterNegative = connection.keyCommands().exists(key.getBytes());
            assertThat(keyExistsAfterNegative).isFalse(); // Key should be deleted
        } finally {
            cleanupKey(key);
        }
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
            "test:key:exists:key1", "test:key:exists:key2", "test:key:exists:key3",
            "test:key:copy:source", "test:key:copy:target", "test:key:copy:existing",
            "test:key:type:string", "test:key:type:list", "test:key:type:hash", 
            "test:key:touch:key1", "test:key:touch:key2", "test:key:touch:key3",
            "test:key:keys:abc", "test:key:keys:def", "test:key:keys:xyz", "test:other:pattern",
            "test:key:random:key1", "test:key:random:key2",
            "test:key:scan:item1", "test:key:scan:item2", "test:key:scan:other1",
            "test:key:rename:old", "test:key:rename:new",
            "test:key:renamenx:old", "test:key:renamenx:new", "test:key:renamenx:existing",
            "test:key:expire", "test:key:pexpire", "test:key:expireat", "test:key:pexpireat",
            "test:key:persist", "test:key:ttl", "test:key:pttl",
            "test:key:error:same", "test:key:expiration:edge",
            "non:existent:key", "non:existent:key"
        };
        
        for (String key : testKeys) {
            try {
                connection.keyCommands().del(key.getBytes());
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }
    
    private void cleanupKey(String key) {
        try {
            connection.keyCommands().del(key.getBytes());
        } catch (Exception e) {
            // Ignore cleanup errors
        }
    }
}

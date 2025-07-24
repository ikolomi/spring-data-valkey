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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.connection.ExpirationOptions;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;

/**
 * Comprehensive low-level integration tests for {@link ValkeyGlideConnection} 
 * hash functionality using the RedisHashCommands interface directly.
 * 
 * These tests validate the implementation of all RedisHashCommands methods:
 * - Basic hash operations (hSet, hSetNX, hGet, hMGet, hMSet)
 * - Increment/decrement operations (hIncrBy for long and double)
 * - Existence and deletion operations (hExists, hDel)
 * - Structural operations (hLen, hKeys, hVals, hGetAll)
 * - Random field operations (hRandField variants)
 * - Scanning operations (hScan)
 * - Field length operations (hStrLen)
 * - Field expiration operations (hExpire, hpExpire, hExpireAt, hpExpireAt, hPersist, hTtl, hpTtl)
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideConnectionHashCommandsIntegrationTests extends AbstractValkeyGlideIntegrationTests {

    @Override
    protected String[] getTestKeyPatterns() {
        return new String[]{
            "test:hash:basic", "test:hash:setnx", "test:hash:multi", "test:hash:incr",
            "test:hash:exists", "test:hash:structure", "test:hash:random", "test:hash:randomwithvals",
            "test:hash:scan", "test:hash:strlen", "test:hash:expire", "test:hash:expireat",
            "test:hash:persist", "test:hash:ttl", "test:hash:error:string", "test:hash:empty",
            "test:hash:binary", "non:existent:key"
        };
    }

    // ==================== Basic Hash Operations ====================

    @Test
    void testHSetAndHGet() {
        String key = "test:hash:basic";
        byte[] keyBytes = key.getBytes();
        byte[] field1 = "field1".getBytes();
        byte[] field2 = "field2".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        
        try {
            // Test hSet on new field
            Boolean setResult1 = connection.hashCommands().hSet(keyBytes, field1, value1);
            assertThat(setResult1).isTrue();
            
            // Test hGet
            byte[] retrievedValue1 = connection.hashCommands().hGet(keyBytes, field1);
            assertThat(retrievedValue1).isEqualTo(value1);
            
            // Test hSet on existing field (should update)
            Boolean setResult2 = connection.hashCommands().hSet(keyBytes, field1, value2);
            assertThat(setResult2).isFalse(); // Field already existed
            
            // Verify updated value
            byte[] retrievedValue2 = connection.hashCommands().hGet(keyBytes, field1);
            assertThat(retrievedValue2).isEqualTo(value2);
            
            // Test hGet on non-existent field
            byte[] nonExistentValue = connection.hashCommands().hGet(keyBytes, field2);
            assertThat(nonExistentValue).isNull();
            
            // Test hGet on non-existent key
            byte[] nonExistentKeyValue = connection.hashCommands().hGet("non:existent:key".getBytes(), field1);
            assertThat(nonExistentKeyValue).isNull();
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testHSetNX() {
        String key = "test:hash:setnx";
        byte[] keyBytes = key.getBytes();
        byte[] field = "field".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        
        try {
            // Test hSetNX on new field
            Boolean setNXResult1 = connection.hashCommands().hSetNX(keyBytes, field, value1);
            assertThat(setNXResult1).isTrue();
            
            // Verify value was set
            byte[] retrievedValue1 = connection.hashCommands().hGet(keyBytes, field);
            assertThat(retrievedValue1).isEqualTo(value1);
            
            // Test hSetNX on existing field - should fail
            Boolean setNXResult2 = connection.hashCommands().hSetNX(keyBytes, field, value2);
            assertThat(setNXResult2).isFalse();
            
            // Verify value was not changed
            byte[] retrievedValue2 = connection.hashCommands().hGet(keyBytes, field);
            assertThat(retrievedValue2).isEqualTo(value1);
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testHMGetAndHMSet() {
        String key = "test:hash:multi";
        byte[] keyBytes = key.getBytes();
        byte[] field1 = "field1".getBytes();
        byte[] field2 = "field2".getBytes();
        byte[] field3 = "field3".getBytes();
        byte[] field4 = "nonexistent".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        byte[] value3 = "value3".getBytes();
        
        try {
            // Set up test data using hMSet
            Map<byte[], byte[]> hashData = new HashMap<>();
            hashData.put(field1, value1);
            hashData.put(field2, value2);
            hashData.put(field3, value3);
            
            connection.hashCommands().hMSet(keyBytes, hashData);
            
            // Test hMGet with existing and non-existing fields
            List<byte[]> values = connection.hashCommands().hMGet(keyBytes, field1, field2, field3, field4);
            
            assertThat(values).hasSize(4);
            assertThat(values.get(0)).isEqualTo(value1);
            assertThat(values.get(1)).isEqualTo(value2);
            assertThat(values.get(2)).isEqualTo(value3);
            assertThat(values.get(3)).isNull(); // non-existent field
            
            // Test hMGet on non-existent key
            List<byte[]> nonExistentValues = connection.hashCommands().hMGet("non:existent:key".getBytes(), field1);
            assertThat(nonExistentValues).hasSize(1);
            assertThat(nonExistentValues.get(0)).isNull();
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Increment Operations ====================

    @Test
    void testHIncrBy() {
        String key = "test:hash:incr";
        byte[] keyBytes = key.getBytes();
        byte[] intField = "intField".getBytes();
        byte[] floatField = "floatField".getBytes();
        
        try {
            // Test hIncrBy with long on non-existent field (should start from 0)
            Long incrResult1 = connection.hashCommands().hIncrBy(keyBytes, intField, 5L);
            assertThat(incrResult1).isEqualTo(5L);
            
            // Test subsequent increment
            Long incrResult2 = connection.hashCommands().hIncrBy(keyBytes, intField, 10L);
            assertThat(incrResult2).isEqualTo(15L);
            
            // Test negative increment (decrement)
            Long incrResult3 = connection.hashCommands().hIncrBy(keyBytes, intField, -7L);
            assertThat(incrResult3).isEqualTo(8L);
            
            // Test hIncrBy with double on non-existent field
            Double floatIncrResult1 = connection.hashCommands().hIncrBy(keyBytes, floatField, 3.14);
            assertThat(floatIncrResult1).isEqualTo(3.14);
            
            // Test subsequent float increment
            Double floatIncrResult2 = connection.hashCommands().hIncrBy(keyBytes, floatField, 2.86);
            assertThat(floatIncrResult2).isEqualTo(6.0);
            
            // Test negative float increment
            Double floatIncrResult3 = connection.hashCommands().hIncrBy(keyBytes, floatField, -1.5);
            assertThat(floatIncrResult3).isEqualTo(4.5);
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Existence and Deletion Operations ====================

    @Test
    void testHExistsAndHDel() {
        String key = "test:hash:exists";
        byte[] keyBytes = key.getBytes();
        byte[] field1 = "field1".getBytes();
        byte[] field2 = "field2".getBytes();
        byte[] field3 = "field3".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        byte[] value3 = "value3".getBytes();
        
        try {
            // Set up test data
            connection.hashCommands().hSet(keyBytes, field1, value1);
            connection.hashCommands().hSet(keyBytes, field2, value2);
            connection.hashCommands().hSet(keyBytes, field3, value3);
            
            // Test hExists on existing fields
            Boolean exists1 = connection.hashCommands().hExists(keyBytes, field1);
            assertThat(exists1).isTrue();
            
            Boolean exists2 = connection.hashCommands().hExists(keyBytes, field2);
            assertThat(exists2).isTrue();
            
            // Test hExists on non-existent field
            Boolean existsNon = connection.hashCommands().hExists(keyBytes, "nonexistent".getBytes());
            assertThat(existsNon).isFalse();
            
            // Test hExists on non-existent key
            Boolean existsNonKey = connection.hashCommands().hExists("non:existent:key".getBytes(), field1);
            assertThat(existsNonKey).isFalse();
            
            // Test hDel with single field
            Long delResult1 = connection.hashCommands().hDel(keyBytes, field1);
            assertThat(delResult1).isEqualTo(1L);
            
            // Verify field was deleted
            Boolean existsAfterDel = connection.hashCommands().hExists(keyBytes, field1);
            assertThat(existsAfterDel).isFalse();
            
            // Test hDel with multiple fields
            Long delResult2 = connection.hashCommands().hDel(keyBytes, field2, field3);
            assertThat(delResult2).isEqualTo(2L);
            
            // Test hDel on non-existent field
            Long delResult3 = connection.hashCommands().hDel(keyBytes, "nonexistent".getBytes());
            assertThat(delResult3).isEqualTo(0L);
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Structural Operations ====================

    @Test
    void testHLenHKeysHValsHGetAll() {
        String key = "test:hash:structure";
        byte[] keyBytes = key.getBytes();
        byte[] field1 = "field1".getBytes();
        byte[] field2 = "field2".getBytes();
        byte[] field3 = "field3".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        byte[] value3 = "value3".getBytes();
        
        try {
            // Test on empty/non-existent hash
            Long emptyLen = connection.hashCommands().hLen(keyBytes);
            assertThat(emptyLen).isEqualTo(0L);
            
            Set<byte[]> emptyKeys = connection.hashCommands().hKeys(keyBytes);
            assertThat(emptyKeys).isEmpty();
            
            List<byte[]> emptyVals = connection.hashCommands().hVals(keyBytes);
            assertThat(emptyVals).isEmpty();
            
            Map<byte[], byte[]> emptyAll = connection.hashCommands().hGetAll(keyBytes);
            assertThat(emptyAll).isEmpty();
            
            // Set up test data
            Map<byte[], byte[]> hashData = new HashMap<>();
            hashData.put(field1, value1);
            hashData.put(field2, value2);
            hashData.put(field3, value3);
            connection.hashCommands().hMSet(keyBytes, hashData);
            
            // Test hLen
            Long len = connection.hashCommands().hLen(keyBytes);
            assertThat(len).isEqualTo(3L);
            
            // Test hKeys
            Set<byte[]> keys = connection.hashCommands().hKeys(keyBytes);
            assertThat(keys).hasSize(3);
            assertThat(keys).containsExactlyInAnyOrder(field1, field2, field3);
            
            // Test hVals
            List<byte[]> vals = connection.hashCommands().hVals(keyBytes);
            assertThat(vals).hasSize(3);
            assertThat(vals).containsExactlyInAnyOrder(value1, value2, value3);
            
            // Test hGetAll
            Map<byte[], byte[]> all = connection.hashCommands().hGetAll(keyBytes);
            assertThat(all).hasSize(3);

            // for (Map.Entry<byte[], byte[]> entry : all.entrySet()) {
            //     String keyStr = new String(entry.getKey(), StandardCharsets.UTF_8);
            //     String valueStr = new String(entry.getValue(), StandardCharsets.UTF_8);
            //     System.out.println("Key: " + keyStr + ", Value: " + valueStr);
            // }

            // Since byte arrays are compared by reference, not content, we need to check the content differently
            boolean foundField1 = false, foundField2 = false, foundField3 = false;
            for (Map.Entry<byte[], byte[]> entry : all.entrySet()) {
                if (java.util.Arrays.equals(entry.getKey(), field1)) {
                    assertThat(entry.getValue()).isEqualTo(value1);
                    foundField1 = true;
                } else if (java.util.Arrays.equals(entry.getKey(), field2)) {
                    assertThat(entry.getValue()).isEqualTo(value2);
                    foundField2 = true;
                } else if (java.util.Arrays.equals(entry.getKey(), field3)) {
                    assertThat(entry.getValue()).isEqualTo(value3);
                    foundField3 = true;
                }
            }
            assertThat(foundField1).as("field1 should be present in hGetAll result").isTrue();
            assertThat(foundField2).as("field2 should be present in hGetAll result").isTrue();
            assertThat(foundField3).as("field3 should be present in hGetAll result").isTrue();
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Random Field Operations ====================

    @Test
    void testHRandField() {
        String key = "test:hash:random";
        byte[] keyBytes = key.getBytes();
        byte[] field1 = "field1".getBytes();
        byte[] field2 = "field2".getBytes();
        byte[] field3 = "field3".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        byte[] value3 = "value3".getBytes();
        
        try {
            // Test on non-existent key
            byte[] nonExistentField = connection.hashCommands().hRandField("non:existent:key".getBytes());
            assertThat(nonExistentField).isNull();
            
            // Set up test data
            Map<byte[], byte[]> hashData = new HashMap<>();
            hashData.put(field1, value1);
            hashData.put(field2, value2);
            hashData.put(field3, value3);
            connection.hashCommands().hMSet(keyBytes, hashData);
            
            // Test hRandField (single field)
            byte[] randomField = connection.hashCommands().hRandField(keyBytes);
            assertThat(randomField).isIn(field1, field2, field3);
            
            // Test hRandField with count
            List<byte[]> randomFields = connection.hashCommands().hRandField(keyBytes, 2);
            assertThat(randomFields).hasSize(2);
            
            // Test hRandField with count larger than hash size
            List<byte[]> allFields = connection.hashCommands().hRandField(keyBytes, 5);
            assertThat(allFields).hasSize(3); // Should return all available fields
            
            // Test hRandField with negative count (allows repetitions)
            List<byte[]> repeatedFields = connection.hashCommands().hRandField(keyBytes, -5);
            assertThat(repeatedFields).hasSize(5); // Should return exactly 5 elements (may repeat)
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testHRandFieldWithValues() {
        String key = "test:hash:randomwithvals";
        byte[] keyBytes = key.getBytes();
        byte[] field1 = "field1".getBytes();
        byte[] field2 = "field2".getBytes();
        byte[] field3 = "field3".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        byte[] value3 = "value3".getBytes();
        
        try {
            // Test on non-existent key
            Map.Entry<byte[], byte[]> nonExistentEntry = connection.hashCommands().hRandFieldWithValues("non:existent:key".getBytes());
            assertThat(nonExistentEntry).isNull();
            
            // Set up test data
            Map<byte[], byte[]> hashData = new HashMap<>();
            hashData.put(field1, value1);
            hashData.put(field2, value2);
            hashData.put(field3, value3);
            connection.hashCommands().hMSet(keyBytes, hashData);
            
            // Test hRandFieldWithValues (single entry)
            Map.Entry<byte[], byte[]> randomEntry = connection.hashCommands().hRandFieldWithValues(keyBytes);
            assertThat(randomEntry).isNotNull();
            assertThat(randomEntry.getKey()).isIn(field1, field2, field3);
            // Verify the value matches the key
            if (java.util.Arrays.equals(randomEntry.getKey(), field1)) {
                assertThat(randomEntry.getValue()).isEqualTo(value1);
            } else if (java.util.Arrays.equals(randomEntry.getKey(), field2)) {
                assertThat(randomEntry.getValue()).isEqualTo(value2);
            } else if (java.util.Arrays.equals(randomEntry.getKey(), field3)) {
                assertThat(randomEntry.getValue()).isEqualTo(value3);
            }
            
            // Test hRandFieldWithValues with count
            List<Map.Entry<byte[], byte[]>> randomEntries = connection.hashCommands().hRandFieldWithValues(keyBytes, 2);
            assertThat(randomEntries).hasSize(2);
            
            for (Map.Entry<byte[], byte[]> entry : randomEntries) {
                assertThat(entry.getKey()).isIn(field1, field2, field3);
                // Verify each value matches its key
                if (java.util.Arrays.equals(entry.getKey(), field1)) {
                    assertThat(entry.getValue()).isEqualTo(value1);
                } else if (java.util.Arrays.equals(entry.getKey(), field2)) {
                    assertThat(entry.getValue()).isEqualTo(value2);
                } else if (java.util.Arrays.equals(entry.getKey(), field3)) {
                    assertThat(entry.getValue()).isEqualTo(value3);
                }
            }
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Scanning Operations ====================

    @Test
    void testHScan() {
        String key = "test:hash:scan";
        byte[] keyBytes = key.getBytes();
        
        try {
            // Set up test data with a pattern
            Map<byte[], byte[]> hashData = new HashMap<>();
            hashData.put("field1".getBytes(), "value1".getBytes());
            hashData.put("field2".getBytes(), "value2".getBytes());
            hashData.put("other1".getBytes(), "othervalue1".getBytes());
            hashData.put("other2".getBytes(), "othervalue2".getBytes());
            connection.hashCommands().hMSet(keyBytes, hashData);
            
            // Test basic scan
            Cursor<Map.Entry<byte[], byte[]>> cursor = connection.hashCommands().hScan(keyBytes, ScanOptions.NONE);
            
            List<Map.Entry<byte[], byte[]>> scannedEntries = new ArrayList<>();
            while (cursor.hasNext()) {
                scannedEntries.add(cursor.next());
            }
            cursor.close();
            
            assertThat(scannedEntries).hasSize(4);
            
            // Test scan with pattern
            ScanOptions patternOptions = ScanOptions.scanOptions().match("field*").build();
            Cursor<Map.Entry<byte[], byte[]>> patternCursor = connection.hashCommands().hScan(keyBytes, patternOptions);
            
            List<Map.Entry<byte[], byte[]>> patternEntries = new ArrayList<>();
            while (patternCursor.hasNext()) {
                patternEntries.add(patternCursor.next());
            }
            patternCursor.close();
            
            // Should find only entries matching "field*" pattern
            assertThat(patternEntries).hasSize(2);
            for (Map.Entry<byte[], byte[]> entry : patternEntries) {
                String fieldName = new String(entry.getKey());
                assertThat(fieldName).startsWith("field");
            }
            
            // Test scan with count
            ScanOptions countOptions = ScanOptions.scanOptions().count(1).build();
            Cursor<Map.Entry<byte[], byte[]>> countCursor = connection.hashCommands().hScan(keyBytes, countOptions);
            
            List<Map.Entry<byte[], byte[]>> countEntries = new ArrayList<>();
            while (countCursor.hasNext()) {
                countEntries.add(countCursor.next());
            }
            countCursor.close();
            
            assertThat(countEntries).hasSize(4); // Should still get all entries, just in smaller batches
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Field Length Operations ====================

    @Test
    void testHStrLen() {
        String key = "test:hash:strlen";
        byte[] keyBytes = key.getBytes();
        byte[] field1 = "field1".getBytes();
        byte[] field2 = "field2".getBytes();
        byte[] shortValue = "short".getBytes(); // 5 characters
        byte[] longValue = "this is a longer value".getBytes(); // 22 characters
        
        try {
            // Test on non-existent key/field
            Long nonExistentLen = connection.hashCommands().hStrLen("non:existent:key".getBytes(), field1);
            assertThat(nonExistentLen).isEqualTo(0L);
            
            // Set up test data
            connection.hashCommands().hSet(keyBytes, field1, shortValue);
            connection.hashCommands().hSet(keyBytes, field2, longValue);
            
            // Test hStrLen
            Long shortLen = connection.hashCommands().hStrLen(keyBytes, field1);
            assertThat(shortLen).isEqualTo(5L);
            
            Long longLen = connection.hashCommands().hStrLen(keyBytes, field2);
            assertThat(longLen).isEqualTo(22L);
            
            // Test on non-existent field in existing key
            Long nonExistentFieldLen = connection.hashCommands().hStrLen(keyBytes, "nonexistent".getBytes());
            assertThat(nonExistentFieldLen).isEqualTo(0L);
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Field Expiration Operations ====================

    @Test
    void testHExpireAndHpExpire() {
        String key = "test:hash:expire";
        byte[] keyBytes = key.getBytes();
        byte[] field1 = "field1".getBytes();
        byte[] field2 = "field2".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        
        try {
            // Set up test data
            connection.hashCommands().hSet(keyBytes, field1, value1);
            connection.hashCommands().hSet(keyBytes, field2, value2);
            
            // Test hExpire (seconds) - Basic test to ensure command executes
            List<Long> expireResults = connection.hashCommands().hExpire(keyBytes, 10L, 
                ExpirationOptions.Condition.ALWAYS, field1);
            // Note: Result interpretation may vary based on server version and implementation
            // We primarily test that the command executes without error
            assertThat(expireResults).isNotNull();
            
            // Test hpExpire (milliseconds) - Basic test
            List<Long> pExpireResults = connection.hashCommands().hpExpire(keyBytes, 10000L, 
                ExpirationOptions.Condition.ALWAYS, field2);
            assertThat(pExpireResults).isNotNull();
            
            // Test with Duration
            List<Long> durationExpireResults = connection.hashCommands().hExpire(keyBytes, Duration.ofSeconds(10), field1);
            assertThat(durationExpireResults).isNotNull();
            
            // Test on non-existent field
            List<Long> nonExistentResults = connection.hashCommands().hExpire(keyBytes, 10L, 
                ExpirationOptions.Condition.ALWAYS, "nonexistent".getBytes());
            assertThat(nonExistentResults).isNotNull();
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testHExpireAtAndHpExpireAt() {
        String key = "test:hash:expireat";
        byte[] keyBytes = key.getBytes();
        byte[] field1 = "field1".getBytes();
        byte[] field2 = "field2".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        
        try {
            // Set up test data
            connection.hashCommands().hSet(keyBytes, field1, value1);
            connection.hashCommands().hSet(keyBytes, field2, value2);
            
            // Calculate future timestamp (current time + 10 seconds)
            long futureTimestamp = System.currentTimeMillis() / 1000 + 10;
            long futureTimestampMillis = System.currentTimeMillis() + 10000;
            
            // Test hExpireAt (seconds) - Basic test to ensure command executes
            List<Long> expireAtResults = connection.hashCommands().hExpireAt(keyBytes, futureTimestamp, 
                ExpirationOptions.Condition.ALWAYS, field1);
            assertThat(expireAtResults).isNotNull();
            
            // Test hpExpireAt (milliseconds) - Basic test
            List<Long> pExpireAtResults = connection.hashCommands().hpExpireAt(keyBytes, futureTimestampMillis, 
                ExpirationOptions.Condition.ALWAYS, field2);
            assertThat(pExpireAtResults).isNotNull();
            
            // Test on non-existent field
            List<Long> nonExistentResults = connection.hashCommands().hExpireAt(keyBytes, futureTimestamp, 
                ExpirationOptions.Condition.ALWAYS, "nonexistent".getBytes());
            assertThat(nonExistentResults).isNotNull();
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testHPersist() {
        String key = "test:hash:persist";
        byte[] keyBytes = key.getBytes();
        byte[] field1 = "field1".getBytes();
        byte[] field2 = "field2".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        
        try {
            // Set up test data
            connection.hashCommands().hSet(keyBytes, field1, value1);
            connection.hashCommands().hSet(keyBytes, field2, value2);
            
            // Test hPersist - Basic test to ensure command executes
            List<Long> persistResults = connection.hashCommands().hPersist(keyBytes, field1, field2);
            assertThat(persistResults).isNotNull();
            
            // Test on non-existent field
            List<Long> nonExistentResults = connection.hashCommands().hPersist(keyBytes, "nonexistent".getBytes());
            assertThat(nonExistentResults).isNotNull();
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testHTtlAndHpTtl() {
        String key = "test:hash:ttl";
        byte[] keyBytes = key.getBytes();
        byte[] field1 = "field1".getBytes();
        byte[] field2 = "field2".getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        
        try {
            // Set up test data
            connection.hashCommands().hSet(keyBytes, field1, value1);
            connection.hashCommands().hSet(keyBytes, field2, value2);
            
            // Test hTtl - Basic test to ensure command executes
            List<Long> ttlResults = connection.hashCommands().hTtl(keyBytes, field1, field2);
            assertThat(ttlResults).isNotNull();
            
            // Test hTtl with TimeUnit
            List<Long> ttlMillisResults = connection.hashCommands().hTtl(keyBytes, TimeUnit.MILLISECONDS, field1);
            assertThat(ttlMillisResults).isNotNull();
            
            // Test hpTtl (milliseconds) - Basic test
            List<Long> pTtlResults = connection.hashCommands().hpTtl(keyBytes, field1, field2);
            assertThat(pTtlResults).isNotNull();
            
            // Test on non-existent field
            List<Long> nonExistentResults = connection.hashCommands().hTtl(keyBytes, "nonexistent".getBytes());
            assertThat(nonExistentResults).isNotNull();
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Error Handling and Edge Cases ====================

    @Test
    void testHashOperationsOnNonHashTypes() {
        String stringKey = "test:hash:error:string";
        
        try {
            // Create a string key
            connection.stringCommands().set(stringKey.getBytes(), "stringvalue".getBytes());
            
            // Try hash operations on string key - should fail or return appropriate response
            assertThatThrownBy(() -> connection.hashCommands().hSet(stringKey.getBytes(), "field".getBytes(), "value".getBytes()))
                .isInstanceOf(Exception.class);
        } finally {
            cleanupKey(stringKey);
        }
    }

    @Test
    void testEmptyHashOperations() {
        String key = "test:hash:empty";
        byte[] keyBytes = key.getBytes();
        byte[] field = "field".getBytes();
        byte[] emptyValue = new byte[0];
        
        try {
            // Set field with empty value
            Boolean setResult = connection.hashCommands().hSet(keyBytes, field, emptyValue);
            assertThat(setResult).isTrue();
            
            // Get empty value
            byte[] retrievedValue = connection.hashCommands().hGet(keyBytes, field);
            assertThat(retrievedValue).isEqualTo(emptyValue);
            
            // String length should be 0
            Long strLen = connection.hashCommands().hStrLen(keyBytes, field);
            assertThat(strLen).isEqualTo(0L);
            
            // Hash should have 1 field
            Long hashLen = connection.hashCommands().hLen(keyBytes);
            assertThat(hashLen).isEqualTo(1L);
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testHashOperationsWithBinaryData() {
        String key = "test:hash:binary";
        byte[] keyBytes = key.getBytes();
        byte[] binaryField = new byte[]{0x00, 0x01, 0x02, (byte) 0xFF};
        byte[] binaryValue = new byte[]{(byte) 0xFF, (byte) 0xFE, 0x00, 0x01, 0x7F};
        
        try {
            // Test with binary field and value
            Boolean setResult = connection.hashCommands().hSet(keyBytes, binaryField, binaryValue);
            assertThat(setResult).isTrue();
            
            // Retrieve binary value
            byte[] retrievedValue = connection.hashCommands().hGet(keyBytes, binaryField);
            assertThat(retrievedValue).isEqualTo(binaryValue);
            
            // Test exists with binary field
            Boolean exists = connection.hashCommands().hExists(keyBytes, binaryField);
            assertThat(exists).isTrue();
            
        // Test hGetAll with binary data
        Map<byte[], byte[]> all = connection.hashCommands().hGetAll(keyBytes);
        assertThat(all).hasSize(1);
        
        // Since byte arrays are compared by reference, not content, we need to check the content differently
        boolean foundBinaryField = false;
        for (Map.Entry<byte[], byte[]> entry : all.entrySet()) {
            if (java.util.Arrays.equals(entry.getKey(), binaryField)) {
                assertThat(entry.getValue()).isEqualTo(binaryValue);
                foundBinaryField = true;
                break;
            }
        }
        assertThat(foundBinaryField).as("binaryField should be present in hGetAll result").isTrue();
        } finally {
            cleanupKey(key);
        }
    }

}

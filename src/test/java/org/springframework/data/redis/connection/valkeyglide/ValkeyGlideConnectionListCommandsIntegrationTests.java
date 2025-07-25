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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.connection.RedisListCommands.Direction;
import org.springframework.data.redis.connection.RedisListCommands.Position;

/**
 * Comprehensive low-level integration tests for {@link ValkeyGlideConnection} 
 * list functionality using the RedisListCommands interface directly.
 * 
 * These tests validate the implementation of all RedisListCommands methods:
 * - Basic list operations (rPush, lPush, rPushX, lPushX)
 * - List position operations (lPos variants)
 * - List size operations (lLen)
 * - List range operations (lRange, lTrim)
 * - List index operations (lIndex, lSet)
 * - List insertion operations (lInsert)
 * - List movement operations (lMove, bLMove)
 * - List removal operations (lRem, lPop, rPop with count variants)
 * - Blocking operations (bLPop, bRPop)
 * - Pop and push operations (rPopLPush, bRPopLPush)
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideConnectionListCommandsIntegrationTests extends AbstractValkeyGlideIntegrationTests {

    @Override
    protected String[] getTestKeyPatterns() {
        return new String[]{
            "test:list:pushpop", "test:list:pushx:existing", "test:list:pushx:nonexistent",
            "test:list:lpos", "test:list:len:range", "test:list:trim", "test:list:index",
            "test:list:insert", "test:list:move:source", "test:list:move:dest",
            "test:list:blmove:source", "test:list:blmove:dest", "test:list:rem", "test:list:pop",
            "test:list:blpop:key1", "test:list:blpop:key2", "test:list:rpoplpush:source",
            "test:list:rpoplpush:dest", "test:list:brpoplpush:source", "test:list:brpoplpush:dest",
            "test:list:error:string", "test:list:empty", "test:list:binary", "non:existent:key"
        };
    }

    // ==================== Basic List Operations ====================

    @Test
    void testRPushAndLPush() {
        String key = "test:list:pushpop";
        byte[] keyBytes = key.getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        byte[] value3 = "value3".getBytes();
        
        try {
            // Test rPush (right push - append to tail)
            Long rPushResult1 = connection.listCommands().rPush(keyBytes, value1);
            assertThat(rPushResult1).isEqualTo(1L); // List length after push
            
            Long rPushResult2 = connection.listCommands().rPush(keyBytes, value2);
            assertThat(rPushResult2).isEqualTo(2L);
            
            // Test lPush (left push - prepend to head)
            Long lPushResult = connection.listCommands().lPush(keyBytes, value3);
            assertThat(lPushResult).isEqualTo(3L);
            
            // Verify list contents: value3, value1, value2
            List<byte[]> range = connection.listCommands().lRange(keyBytes, 0, -1);
            assertThat(range).hasSize(3);
            assertThat(range.get(0)).isEqualTo(value3); // head
            assertThat(range.get(1)).isEqualTo(value1);
            assertThat(range.get(2)).isEqualTo(value2); // tail
            
            // Test multiple values in single push
            byte[] value4 = "value4".getBytes();
            byte[] value5 = "value5".getBytes();
            Long multiPushResult = connection.listCommands().rPush(keyBytes, value4, value5);
            assertThat(multiPushResult).isEqualTo(5L);
            
            // Verify final list length
            Long listLength = connection.listCommands().lLen(keyBytes);
            assertThat(listLength).isEqualTo(5L);
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testRPushXAndLPushX() {
        String existingKey = "test:list:pushx:existing";
        String nonExistentKey = "test:list:pushx:nonexistent";
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        
        try {
            // Create an existing list
            connection.listCommands().rPush(existingKey.getBytes(), value1);
            
            // Test rPushX on existing list
            Long rPushXResult1 = connection.listCommands().rPushX(existingKey.getBytes(), value2);
            assertThat(rPushXResult1).isEqualTo(2L); // Should succeed
            
            // Test rPushX on non-existent list
            Long rPushXResult2 = connection.listCommands().rPushX(nonExistentKey.getBytes(), value1);
            assertThat(rPushXResult2).isEqualTo(0L); // Should fail
            
            // Test lPushX on existing list
            Long lPushXResult1 = connection.listCommands().lPushX(existingKey.getBytes(), "newhead".getBytes());
            assertThat(lPushXResult1).isEqualTo(3L); // Should succeed
            
            // Test lPushX on non-existent list
            Long lPushXResult2 = connection.listCommands().lPushX(nonExistentKey.getBytes(), value1);
            assertThat(lPushXResult2).isEqualTo(0L); // Should fail
            
            // Verify non-existent key was not created
            Boolean keyExists = connection.keyCommands().exists(nonExistentKey.getBytes());
            assertThat(keyExists).isFalse();
        } finally {
            cleanupKey(existingKey);
            cleanupKey(nonExistentKey);
        }
    }

    // ==================== List Position Operations ====================

    @Test
    void testLPos() {
        String key = "test:list:lpos";
        byte[] keyBytes = key.getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        byte[] value3 = "value1".getBytes(); // duplicate
        byte[] value4 = "value3".getBytes();
        
        try {
            // Set up test data: [value1, value2, value1, value3]
            connection.listCommands().rPush(keyBytes, value1, value2, value3, value4);
            
            // Test basic lPos (find first occurrence)
            Long pos1 = connection.listCommands().lPos(keyBytes, value1);
            assertThat(pos1).isEqualTo(0L); // First occurrence at index 0
            
            Long pos2 = connection.listCommands().lPos(keyBytes, value2);
            assertThat(pos2).isEqualTo(1L); // At index 1
            
            Long pos4 = connection.listCommands().lPos(keyBytes, value4);
            assertThat(pos4).isEqualTo(3L); // At index 3
            
            // Test lPos with non-existent element
            Long posNon = connection.listCommands().lPos(keyBytes, "nonexistent".getBytes());
            assertThat(posNon).isNull();
            
            // Test lPos with rank and count
            List<Long> positions = connection.listCommands().lPos(keyBytes, value1, null, 2);
            assertThat(positions).containsExactly(0L, 2L); // Both occurrences of value1
            
            // Test lPos with rank (second occurrence)
            List<Long> secondOccurrence = connection.listCommands().lPos(keyBytes, value1, 2, null);
            assertThat(secondOccurrence).containsExactly(2L); // Second occurrence
            
            // Test on non-existent key
            Long posNonKey = connection.listCommands().lPos("non:existent:key".getBytes(), value1);
            assertThat(posNonKey).isNull();
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== List Size and Range Operations ====================

    @Test
    void testLLenAndLRange() {
        String key = "test:list:len:range";
        byte[] keyBytes = key.getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        byte[] value3 = "value3".getBytes();
        byte[] value4 = "value4".getBytes();
        byte[] value5 = "value5".getBytes();
        
        try {
            // Test lLen on non-existent key
            Long emptyLen = connection.listCommands().lLen(keyBytes);
            assertThat(emptyLen).isEqualTo(0L);
            
            // Test lRange on non-existent key
            List<byte[]> emptyRange = connection.listCommands().lRange(keyBytes, 0, -1);
            assertThat(emptyRange).isEmpty();
            
            // Set up test data
            connection.listCommands().rPush(keyBytes, value1, value2, value3, value4, value5);
            
            // Test lLen
            Long len = connection.listCommands().lLen(keyBytes);
            assertThat(len).isEqualTo(5L);
            
            // Test lRange - full range
            List<byte[]> fullRange = connection.listCommands().lRange(keyBytes, 0, -1);
            assertThat(fullRange).hasSize(5);
            assertThat(fullRange.get(0)).isEqualTo(value1);
            assertThat(fullRange.get(4)).isEqualTo(value5);
            
            // Test lRange - partial range
            List<byte[]> partialRange = connection.listCommands().lRange(keyBytes, 1, 3);
            assertThat(partialRange).hasSize(3);
            assertThat(partialRange.get(0)).isEqualTo(value2);
            assertThat(partialRange.get(1)).isEqualTo(value3);
            assertThat(partialRange.get(2)).isEqualTo(value4);
            
            // Test lRange - negative indices
            List<byte[]> negativeRange = connection.listCommands().lRange(keyBytes, -2, -1);
            assertThat(negativeRange).hasSize(2);
            assertThat(negativeRange.get(0)).isEqualTo(value4);
            assertThat(negativeRange.get(1)).isEqualTo(value5);
            
            // Test lRange - out of bounds
            List<byte[]> outOfBounds = connection.listCommands().lRange(keyBytes, 10, 20);
            assertThat(outOfBounds).isEmpty();
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testLTrim() {
        String key = "test:list:trim";
        byte[] keyBytes = key.getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        byte[] value3 = "value3".getBytes();
        byte[] value4 = "value4".getBytes();
        byte[] value5 = "value5".getBytes();
        
        try {
            // Set up test data
            connection.listCommands().rPush(keyBytes, value1, value2, value3, value4, value5);
            
            // Trim to keep only middle elements (indices 1-3)
            connection.listCommands().lTrim(keyBytes, 1, 3);
            
            // Verify trim result
            List<byte[]> trimmedList = connection.listCommands().lRange(keyBytes, 0, -1);
            assertThat(trimmedList).hasSize(3);
            assertThat(trimmedList.get(0)).isEqualTo(value2);
            assertThat(trimmedList.get(1)).isEqualTo(value3);
            assertThat(trimmedList.get(2)).isEqualTo(value4);
            
            // Trim to single element
            connection.listCommands().lTrim(keyBytes, 0, 0);
            List<byte[]> singleElement = connection.listCommands().lRange(keyBytes, 0, -1);
            assertThat(singleElement).hasSize(1);
            assertThat(singleElement.get(0)).isEqualTo(value2);
            
            // Trim to empty (out of range)
            connection.listCommands().lTrim(keyBytes, 10, 20);
            List<byte[]> emptyList = connection.listCommands().lRange(keyBytes, 0, -1);
            assertThat(emptyList).isEmpty();
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== List Index Operations ====================

    @Test
    void testLIndexAndLSet() {
        String key = "test:list:index";
        byte[] keyBytes = key.getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        byte[] value3 = "value3".getBytes();
        byte[] newValue = "newvalue".getBytes();
        
        try {
            // Test lIndex on non-existent key
            byte[] nonExistentValue = connection.listCommands().lIndex(keyBytes, 0);
            assertThat(nonExistentValue).isNull();
            
            // Set up test data
            connection.listCommands().rPush(keyBytes, value1, value2, value3);
            
            // Test lIndex with positive indices
            byte[] index0 = connection.listCommands().lIndex(keyBytes, 0);
            assertThat(index0).isEqualTo(value1);
            
            byte[] index1 = connection.listCommands().lIndex(keyBytes, 1);
            assertThat(index1).isEqualTo(value2);
            
            byte[] index2 = connection.listCommands().lIndex(keyBytes, 2);
            assertThat(index2).isEqualTo(value3);
            
            // Test lIndex with negative indices
            byte[] indexNeg1 = connection.listCommands().lIndex(keyBytes, -1);
            assertThat(indexNeg1).isEqualTo(value3); // Last element
            
            byte[] indexNeg2 = connection.listCommands().lIndex(keyBytes, -2);
            assertThat(indexNeg2).isEqualTo(value2); // Second to last
            
            byte[] indexNeg3 = connection.listCommands().lIndex(keyBytes, -3);
            assertThat(indexNeg3).isEqualTo(value1); // Third to last (first)
            
            // Test lIndex out of bounds
            byte[] outOfBounds = connection.listCommands().lIndex(keyBytes, 10);
            assertThat(outOfBounds).isNull();
            
            // Test lSet
            connection.listCommands().lSet(keyBytes, 1, newValue);
            
            // Verify lSet result
            byte[] updatedValue = connection.listCommands().lIndex(keyBytes, 1);
            assertThat(updatedValue).isEqualTo(newValue);
            
            // Verify other elements unchanged
            assertThat(connection.listCommands().lIndex(keyBytes, 0)).isEqualTo(value1);
            assertThat(connection.listCommands().lIndex(keyBytes, 2)).isEqualTo(value3);
            
            // Test lSet with negative index
            connection.listCommands().lSet(keyBytes, -1, "lastvalue".getBytes());
            byte[] lastValue = connection.listCommands().lIndex(keyBytes, -1);
            assertThat(lastValue).isEqualTo("lastvalue".getBytes());
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== List Insertion Operations ====================

    @Test
    void testLInsert() {
        String key = "test:list:insert";
        byte[] keyBytes = key.getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        byte[] value3 = "value3".getBytes();
        byte[] beforeValue = "before".getBytes();
        byte[] afterValue = "after".getBytes();
        
        try {
            // Test lInsert on non-existent key
            Long insertNonExistent = connection.listCommands().lInsert(keyBytes, Position.BEFORE, value1, beforeValue);
            assertThat(insertNonExistent).isEqualTo(0L); // Key doesn't exist
            
            // Set up test data: [value1, value2, value3]
            connection.listCommands().rPush(keyBytes, value1, value2, value3);
            
            // Test lInsert BEFORE
            Long insertBefore = connection.listCommands().lInsert(keyBytes, Position.BEFORE, value2, beforeValue);
            assertThat(insertBefore).isEqualTo(4L); // New list length
            
            // Verify insertion: [value1, before, value2, value3]
            List<byte[]> afterBeforeInsert = connection.listCommands().lRange(keyBytes, 0, -1);
            assertThat(afterBeforeInsert).hasSize(4);
            assertThat(afterBeforeInsert.get(0)).isEqualTo(value1);
            assertThat(afterBeforeInsert.get(1)).isEqualTo(beforeValue);
            assertThat(afterBeforeInsert.get(2)).isEqualTo(value2);
            assertThat(afterBeforeInsert.get(3)).isEqualTo(value3);
            
            // Test lInsert AFTER
            Long insertAfter = connection.listCommands().lInsert(keyBytes, Position.AFTER, value2, afterValue);
            assertThat(insertAfter).isEqualTo(5L); // New list length
            
            // Verify insertion: [value1, before, value2, after, value3]
            List<byte[]> afterAfterInsert = connection.listCommands().lRange(keyBytes, 0, -1);
            assertThat(afterAfterInsert).hasSize(5);
            assertThat(afterAfterInsert.get(0)).isEqualTo(value1);
            assertThat(afterAfterInsert.get(1)).isEqualTo(beforeValue);
            assertThat(afterAfterInsert.get(2)).isEqualTo(value2);
            assertThat(afterAfterInsert.get(3)).isEqualTo(afterValue);
            assertThat(afterAfterInsert.get(4)).isEqualTo(value3);
            
            // Test lInsert with non-existent pivot
            Long insertNonPivot = connection.listCommands().lInsert(keyBytes, Position.BEFORE, 
                "nonexistent".getBytes(), "newvalue".getBytes());
            assertThat(insertNonPivot).isEqualTo(-1L); // Pivot not found
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== List Movement Operations ====================

    @Test
    void testLMove() {
        String sourceKey = "test:list:move:source";
        String destKey = "test:list:move:dest";
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        byte[] value3 = "value3".getBytes();
        
        try {
            // Set up source list: [value1, value2, value3]
            connection.listCommands().rPush(sourceKey.getBytes(), value1, value2, value3);
            
            // Test lMove LEFT to LEFT (head to head)
            byte[] movedValue1 = connection.listCommands().lMove(sourceKey.getBytes(), destKey.getBytes(), 
                Direction.LEFT, Direction.LEFT);
            assertThat(movedValue1).isEqualTo(value1);
            
            // Verify source: [value2, value3]
            List<byte[]> sourceAfterMove1 = connection.listCommands().lRange(sourceKey.getBytes(), 0, -1);
            assertThat(sourceAfterMove1).hasSize(2);
            assertThat(sourceAfterMove1.get(0)).isEqualTo(value2);
            
            // Verify dest: [value1]
            List<byte[]> destAfterMove1 = connection.listCommands().lRange(destKey.getBytes(), 0, -1);
            assertThat(destAfterMove1).hasSize(1);
            assertThat(destAfterMove1.get(0)).isEqualTo(value1);
            
            // Test lMove RIGHT to RIGHT (tail to tail)
            byte[] movedValue2 = connection.listCommands().lMove(sourceKey.getBytes(), destKey.getBytes(), 
                Direction.RIGHT, Direction.RIGHT);
            assertThat(movedValue2).isEqualTo(value3);
            
            // Verify source: [value2]
            List<byte[]> sourceAfterMove2 = connection.listCommands().lRange(sourceKey.getBytes(), 0, -1);
            assertThat(sourceAfterMove2).hasSize(1);
            assertThat(sourceAfterMove2.get(0)).isEqualTo(value2);
            
            // Verify dest: [value1, value3]
            List<byte[]> destAfterMove2 = connection.listCommands().lRange(destKey.getBytes(), 0, -1);
            assertThat(destAfterMove2).hasSize(2);
            assertThat(destAfterMove2.get(0)).isEqualTo(value1);
            assertThat(destAfterMove2.get(1)).isEqualTo(value3);
            
            // Test lMove LEFT to RIGHT (head to tail)
            byte[] movedValue3 = connection.listCommands().lMove(sourceKey.getBytes(), destKey.getBytes(), 
                Direction.LEFT, Direction.RIGHT);
            assertThat(movedValue3).isEqualTo(value2);
            
            // Verify source is empty
            List<byte[]> sourceEmpty = connection.listCommands().lRange(sourceKey.getBytes(), 0, -1);
            assertThat(sourceEmpty).isEmpty();
            
            // Verify dest: [value1, value3, value2]
            List<byte[]> destFinal = connection.listCommands().lRange(destKey.getBytes(), 0, -1);
            assertThat(destFinal).hasSize(3);
            assertThat(destFinal.get(0)).isEqualTo(value1);
            assertThat(destFinal.get(1)).isEqualTo(value3);
            assertThat(destFinal.get(2)).isEqualTo(value2);
            
            // Test lMove on empty source
            byte[] moveFromEmpty = connection.listCommands().lMove(sourceKey.getBytes(), destKey.getBytes(), 
                Direction.LEFT, Direction.LEFT);
            assertThat(moveFromEmpty).isNull();
        } finally {
            cleanupKey(sourceKey);
            cleanupKey(destKey);
        }
    }

    @Test
    void testBLMove() {
        String sourceKey = "test:list:blmove:source";
        String destKey = "test:list:blmove:dest";
        byte[] value1 = "value1".getBytes();
        
        try {
            // Set up source list
            connection.listCommands().rPush(sourceKey.getBytes(), value1);
            
            // Test bLMove with immediate availability (timeout not reached)
            byte[] movedValue = connection.listCommands().bLMove(sourceKey.getBytes(), destKey.getBytes(), 
                Direction.LEFT, Direction.RIGHT, 1.0);
            assertThat(movedValue).isEqualTo(value1);
            
            // Verify movement
            List<byte[]> sourceEmpty = connection.listCommands().lRange(sourceKey.getBytes(), 0, -1);
            assertThat(sourceEmpty).isEmpty();
            
            List<byte[]> destWithValue = connection.listCommands().lRange(destKey.getBytes(), 0, -1);
            assertThat(destWithValue).hasSize(1);
            assertThat(destWithValue.get(0)).isEqualTo(value1);
            
            // Test bLMove on empty source with timeout
            long startTime = System.currentTimeMillis();
            byte[] timeoutResult = connection.listCommands().bLMove(sourceKey.getBytes(), destKey.getBytes(), 
                Direction.LEFT, Direction.LEFT, 0.1); // 100ms timeout
            long endTime = System.currentTimeMillis();
            
            assertThat(timeoutResult).isNull();
            // Verify timeout occurred (allow some margin for execution time)
            assertThat(endTime - startTime).isGreaterThanOrEqualTo(90L).isLessThan(200L);
        } finally {
            cleanupKey(sourceKey);
            cleanupKey(destKey);
        }
    }

    // ==================== List Removal Operations ====================

    @Test
    void testLRem() {
        String key = "test:list:rem";
        byte[] keyBytes = key.getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        byte[] value3 = "value1".getBytes(); // duplicate
        byte[] value4 = "value2".getBytes(); // duplicate
        byte[] value5 = "value1".getBytes(); // duplicate
        
        try {
            // Set up test data: [value1, value2, value1, value2, value1]
            connection.listCommands().rPush(keyBytes, value1, value2, value3, value4, value5);
            
            // Test lRem with positive count (remove from head)
            Long remResult1 = connection.listCommands().lRem(keyBytes, 2, value1);
            assertThat(remResult1).isEqualTo(2L); // Removed 2 occurrences
            
            // Verify: [value2, value2, value1]
            List<byte[]> afterRem1 = connection.listCommands().lRange(keyBytes, 0, -1);
            assertThat(afterRem1).hasSize(3);
            assertThat(afterRem1.get(0)).isEqualTo(value2);
            assertThat(afterRem1.get(1)).isEqualTo(value2);
            assertThat(afterRem1.get(2)).isEqualTo(value1);
            
            // Test lRem with negative count (remove from tail)
            Long remResult2 = connection.listCommands().lRem(keyBytes, -1, value2);
            assertThat(remResult2).isEqualTo(1L); // Removed 1 occurrence from tail
            
            // Verify: [value2, value1]
            List<byte[]> afterRem2 = connection.listCommands().lRange(keyBytes, 0, -1);
            assertThat(afterRem2).hasSize(2);
            assertThat(afterRem2.get(0)).isEqualTo(value2);
            assertThat(afterRem2.get(1)).isEqualTo(value1);
            
            // Test lRem with count 0 (remove all occurrences)
            connection.listCommands().rPush(keyBytes, value1, value1); // Add more duplicates
            Long remResult3 = connection.listCommands().lRem(keyBytes, 0, value1);
            assertThat(remResult3).isEqualTo(3L); // Removed all 3 occurrences
            
            // Verify: [value2]
            List<byte[]> afterRem3 = connection.listCommands().lRange(keyBytes, 0, -1);
            assertThat(afterRem3).hasSize(1);
            assertThat(afterRem3.get(0)).isEqualTo(value2);
            
            // Test lRem on non-existent element
            Long remNonExistent = connection.listCommands().lRem(keyBytes, 1, "nonexistent".getBytes());
            assertThat(remNonExistent).isEqualTo(0L);
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testLPopAndRPop() {
        String key = "test:list:pop";
        byte[] keyBytes = key.getBytes();
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        byte[] value3 = "value3".getBytes();
        byte[] value4 = "value4".getBytes();
        byte[] value5 = "value5".getBytes();
        
        try {
            // Test pop on non-existent key
            byte[] nonExistentPop = connection.listCommands().lPop(keyBytes);
            assertThat(nonExistentPop).isNull();
            
            byte[] nonExistentRPop = connection.listCommands().rPop(keyBytes);
            assertThat(nonExistentRPop).isNull();
            
            // Set up test data: [value1, value2, value3, value4, value5]
            connection.listCommands().rPush(keyBytes, value1, value2, value3, value4, value5);
            
            // Test lPop (left pop - from head)
            byte[] leftPopped1 = connection.listCommands().lPop(keyBytes);
            assertThat(leftPopped1).isEqualTo(value1);
            
            // Test rPop (right pop - from tail)
            byte[] rightPopped1 = connection.listCommands().rPop(keyBytes);
            assertThat(rightPopped1).isEqualTo(value5);
            
            // Verify remaining: [value2, value3, value4]
            List<byte[]> remaining = connection.listCommands().lRange(keyBytes, 0, -1);
            assertThat(remaining).hasSize(3);
            assertThat(remaining.get(0)).isEqualTo(value2);
            assertThat(remaining.get(1)).isEqualTo(value3);
            assertThat(remaining.get(2)).isEqualTo(value4);
            
            // Test lPop with count
            List<byte[]> leftPoppedMultiple = connection.listCommands().lPop(keyBytes, 2);
            assertThat(leftPoppedMultiple).hasSize(2);
            assertThat(leftPoppedMultiple.get(0)).isEqualTo(value2);
            assertThat(leftPoppedMultiple.get(1)).isEqualTo(value3);
            
            // Test rPop with count
            List<byte[]> rightPoppedMultiple = connection.listCommands().rPop(keyBytes, 1);
            assertThat(rightPoppedMultiple).hasSize(1);
            assertThat(rightPoppedMultiple.get(0)).isEqualTo(value4);
            
            // Verify list is empty
            Long finalLength = connection.listCommands().lLen(keyBytes);
            assertThat(finalLength).isEqualTo(0L);
            
            // Test pop with count on empty list
            List<byte[]> emptyPop = connection.listCommands().lPop(keyBytes, 3);
            assertThat(emptyPop).isNull();
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Blocking Operations ====================

    @Test
    void testBLPopAndBRPop() {
        String key1 = "test:list:blpop:key1";
        String key2 = "test:list:blpop:key2";
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        
        try {
            // Set up test data
            connection.listCommands().rPush(key1.getBytes(), value1);
            connection.listCommands().rPush(key2.getBytes(), value2);
            
            // Test bLPop with immediate availability (timeout not reached)
            List<byte[]> blPopResult = connection.listCommands().bLPop(1, key1.getBytes(), key2.getBytes());
            assertThat(blPopResult).hasSize(2);
            assertThat(blPopResult.get(0)).isEqualTo(key1.getBytes()); // Key that had the element
            assertThat(blPopResult.get(1)).isEqualTo(value1); // Popped element
            
            // Test bRPop with immediate availability
            List<byte[]> brPopResult = connection.listCommands().bRPop(1, key1.getBytes(), key2.getBytes());
            assertThat(brPopResult).hasSize(2);
            assertThat(brPopResult.get(0)).isEqualTo(key2.getBytes()); // Key that had the element
            assertThat(brPopResult.get(1)).isEqualTo(value2); // Popped element
            
            // Test blocking with timeout (both keys are empty now)
            long startTime = System.currentTimeMillis();
            List<byte[]> timeoutResult = connection.listCommands().bLPop(1, key1.getBytes(), key2.getBytes());
            long endTime = System.currentTimeMillis();
            
            assertThat(timeoutResult).isEmpty(); // Timeout occurred
            // Verify timeout occurred (allow some margin for execution time)
            assertThat(endTime - startTime).isGreaterThanOrEqualTo(900L).isLessThan(1200L);
        } finally {
            cleanupKey(key1);
            cleanupKey(key2);
        }
    }

    // ==================== Pop and Push Operations ====================

    @Test
    void testRPopLPush() {
        String sourceKey = "test:list:rpoplpush:source";
        String destKey = "test:list:rpoplpush:dest";
        byte[] value1 = "value1".getBytes();
        byte[] value2 = "value2".getBytes();
        byte[] value3 = "value3".getBytes();
        
        try {
            // Test rPopLPush on non-existent source
            byte[] nonExistentResult = connection.listCommands().rPopLPush(sourceKey.getBytes(), destKey.getBytes());
            assertThat(nonExistentResult).isNull();
            
            // Set up source list: [value1, value2, value3]
            connection.listCommands().rPush(sourceKey.getBytes(), value1, value2, value3);
            
            // Test rPopLPush (pop from tail of source, push to head of dest)
            byte[] poppedValue1 = connection.listCommands().rPopLPush(sourceKey.getBytes(), destKey.getBytes());
            assertThat(poppedValue1).isEqualTo(value3);
            
            // Verify source: [value1, value2]
            List<byte[]> sourceAfterPop1 = connection.listCommands().lRange(sourceKey.getBytes(), 0, -1);
            assertThat(sourceAfterPop1).hasSize(2);
            assertThat(sourceAfterPop1.get(0)).isEqualTo(value1);
            assertThat(sourceAfterPop1.get(1)).isEqualTo(value2);
            
            // Verify dest: [value3]
            List<byte[]> destAfterPush1 = connection.listCommands().lRange(destKey.getBytes(), 0, -1);
            assertThat(destAfterPush1).hasSize(1);
            assertThat(destAfterPush1.get(0)).isEqualTo(value3);
            
            // Test another rPopLPush
            byte[] poppedValue2 = connection.listCommands().rPopLPush(sourceKey.getBytes(), destKey.getBytes());
            assertThat(poppedValue2).isEqualTo(value2);
            
            // Verify final state
            // Source: [value1]
            List<byte[]> sourceFinal = connection.listCommands().lRange(sourceKey.getBytes(), 0, -1);
            assertThat(sourceFinal).hasSize(1);
            assertThat(sourceFinal.get(0)).isEqualTo(value1);
            
            // Dest: [value2, value3]
            List<byte[]> destFinal = connection.listCommands().lRange(destKey.getBytes(), 0, -1);
            assertThat(destFinal).hasSize(2);
            assertThat(destFinal.get(0)).isEqualTo(value2);
            assertThat(destFinal.get(1)).isEqualTo(value3);
            
            // Test rPopLPush to same key (rotate list)
            connection.listCommands().rPopLPush(destKey.getBytes(), destKey.getBytes());
            List<byte[]> rotatedDest = connection.listCommands().lRange(destKey.getBytes(), 0, -1);
            assertThat(rotatedDest).hasSize(2);
            assertThat(rotatedDest.get(0)).isEqualTo(value3); // Last element moved to first
            assertThat(rotatedDest.get(1)).isEqualTo(value2);
        } finally {
            cleanupKey(sourceKey);
            cleanupKey(destKey);
        }
    }

    @Test
    void testBRPopLPush() {
        String sourceKey = "test:list:brpoplpush:source";
        String destKey = "test:list:brpoplpush:dest";
        byte[] value1 = "value1".getBytes();
        
        try {
            // Set up source list
            connection.listCommands().rPush(sourceKey.getBytes(), value1);
            
            // Test bRPopLPush with immediate availability
            byte[] poppedValue = connection.listCommands().bRPopLPush(1, sourceKey.getBytes(), destKey.getBytes());
            assertThat(poppedValue).isEqualTo(value1);
            
            // Verify movement
            List<byte[]> sourceEmpty = connection.listCommands().lRange(sourceKey.getBytes(), 0, -1);
            assertThat(sourceEmpty).isEmpty();
            
            List<byte[]> destWithValue = connection.listCommands().lRange(destKey.getBytes(), 0, -1);
            assertThat(destWithValue).hasSize(1);
            assertThat(destWithValue.get(0)).isEqualTo(value1);
            
            // Test bRPopLPush with timeout (source is empty)
            long startTime = System.currentTimeMillis();
            byte[] timeoutResult = connection.listCommands().bRPopLPush(1, sourceKey.getBytes(), destKey.getBytes());
            long endTime = System.currentTimeMillis();
            
            assertThat(timeoutResult).isNull();
            // Verify timeout occurred (allow some margin for execution time)
            assertThat(endTime - startTime).isGreaterThanOrEqualTo(900L).isLessThan(1200L);
        } finally {
            cleanupKey(sourceKey);
            cleanupKey(destKey);
        }
    }

    // ==================== Error Handling and Edge Cases ====================

    @Test
    void testListOperationsOnNonListTypes() {
        String stringKey = "test:list:error:string";
        
        try {
            // Create a string key
            connection.stringCommands().set(stringKey.getBytes(), "stringvalue".getBytes());
            
            // Try list operations on string key - should fail or return appropriate response
            assertThatThrownBy(() -> connection.listCommands().lPush(stringKey.getBytes(), "value".getBytes()))
                .isInstanceOf(Exception.class);
        } finally {
            cleanupKey(stringKey);
        }
    }

    @Test
    void testEmptyListOperations() {
        String key = "test:list:empty";
        byte[] keyBytes = key.getBytes();
        byte[] emptyValue = new byte[0];
        
        try {
            // Push empty value
            Long pushResult = connection.listCommands().rPush(keyBytes, emptyValue);
            assertThat(pushResult).isEqualTo(1L);
            
            // Get empty value
            byte[] retrievedValue = connection.listCommands().lIndex(keyBytes, 0);
            assertThat(retrievedValue).isEqualTo(emptyValue);
            
            // List should have 1 element
            Long listLen = connection.listCommands().lLen(keyBytes);
            assertThat(listLen).isEqualTo(1L);
            
            // Pop empty value
            byte[] poppedValue = connection.listCommands().lPop(keyBytes);
            assertThat(poppedValue).isEqualTo(emptyValue);
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testListOperationsWithBinaryData() {
        String key = "test:list:binary";
        byte[] keyBytes = key.getBytes();
        byte[] binaryValue1 = new byte[]{0x00, 0x01, 0x02, (byte) 0xFF};
        byte[] binaryValue2 = new byte[]{(byte) 0xFF, (byte) 0xFE, 0x00, 0x01, 0x7F};
        
        try {
            // Test with binary values
            Long pushResult = connection.listCommands().rPush(keyBytes, binaryValue1, binaryValue2);
            assertThat(pushResult).isEqualTo(2L);
            
            // Retrieve binary values
            List<byte[]> retrievedValues = connection.listCommands().lRange(keyBytes, 0, -1);
            assertThat(retrievedValues).hasSize(2);
            assertThat(retrievedValues.get(0)).isEqualTo(binaryValue1);
            assertThat(retrievedValues.get(1)).isEqualTo(binaryValue2);
            
            // Test binary value operations
            byte[] poppedBinary = connection.listCommands().lPop(keyBytes);
            assertThat(poppedBinary).isEqualTo(binaryValue1);
        } finally {
            cleanupKey(key);
        }
    }

}

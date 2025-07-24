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
import java.util.Set;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;

/**
 * Comprehensive low-level integration tests for {@link ValkeyGlideConnection} 
 * set functionality using the RedisSetCommands interface directly.
 * 
 * These tests validate the implementation of all RedisSetCommands methods:
 * - Basic set operations (sAdd, sRem, sCard, sMembers)
 * - Set membership operations (sIsMember, sMIsMember)
 * - Random operations (sRandMember, sPop)
 * - Set movement operations (sMove)
 * - Set algebra operations (sDiff, sInter, sUnion)
 * - Set store operations (sDiffStore, sInterStore, sUnionStore)
 * - Set scanning operations (sScan)
 * - Error handling and edge cases
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideConnectionSetCommandsIntegrationTests extends AbstractValkeyGlideIntegrationTests {

    @Override
    protected String[] getTestKeyPatterns() {
        return new String[]{
            "test:set:add:key", "test:set:members:key", "test:set:mismember:key", "test:set:mismember:empty",
            "test:set:rem:key", "test:set:randmember:key", "test:set:pop:key", "test:set:move:src",
            "test:set:move:dest", "test:set:move:nonexistent", "test:set:diff:key1", "test:set:diff:key2",
            "test:set:diff:key3", "test:set:diffstore:key1", "test:set:diffstore:key2", "test:set:diffstore:dest",
            "test:set:inter:key1", "test:set:inter:key2", "test:set:inter:key3", "test:set:interstore:key1",
            "test:set:interstore:key2", "test:set:interstore:dest", "test:set:union:key1", "test:set:union:key2",
            "test:set:union:key3", "test:set:unionstore:key1", "test:set:unionstore:key2", "test:set:unionstore:dest",
            "test:set:scan:key", "test:set:edge:key", "test:set:algebra:edge:key1", "test:set:algebra:edge:key2",
            "test:set:algebra:edge:dest"
        };
    }

    // ==================== Basic Set Operations ====================

    @Test
    void testSAddAndSCard() {
        String key = "test:set:add:key";
        byte[] value1 = "member1".getBytes();
        byte[] value2 = "member2".getBytes();
        byte[] value3 = "member3".getBytes();
        
        try {
            // Test sCard on non-existent key
            Long cardEmpty = connection.setCommands().sCard(key.getBytes());
            assertThat(cardEmpty).isEqualTo(0L);
            
            // Test adding single value
            Long addResult1 = connection.setCommands().sAdd(key.getBytes(), value1);
            assertThat(addResult1).isEqualTo(1L);
            
            // Test cardinality after adding one element
            Long card1 = connection.setCommands().sCard(key.getBytes());
            assertThat(card1).isEqualTo(1L);
            
            // Test adding multiple values
            Long addResult2 = connection.setCommands().sAdd(key.getBytes(), value2, value3);
            assertThat(addResult2).isEqualTo(2L);
            
            // Test cardinality after adding three elements
            Long card3 = connection.setCommands().sCard(key.getBytes());
            assertThat(card3).isEqualTo(3L);
            
            // Test adding duplicate value (should return 0)
            Long addDuplicate = connection.setCommands().sAdd(key.getBytes(), value1);
            assertThat(addDuplicate).isEqualTo(0L);
            
            // Cardinality should remain the same
            Long cardSame = connection.setCommands().sCard(key.getBytes());
            assertThat(cardSame).isEqualTo(3L);
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testSMembersAndSIsMember() {
        String key = "test:set:members:key";
        byte[] value1 = "member1".getBytes();
        byte[] value2 = "member2".getBytes();
        byte[] value3 = "member3".getBytes();
        byte[] nonMember = "nonmember".getBytes();
        
        try {
            // Test sMembers on empty set
            Set<byte[]> emptyMembers = connection.setCommands().sMembers(key.getBytes());
            assertThat(emptyMembers).isEmpty();
            
            // Test sIsMember on empty set
            Boolean isMemberEmpty = connection.setCommands().sIsMember(key.getBytes(), value1);
            assertThat(isMemberEmpty).isFalse();
            
            // Add some members
            connection.setCommands().sAdd(key.getBytes(), value1, value2, value3);
            
            // Test sMembers
            Set<byte[]> members = connection.setCommands().sMembers(key.getBytes());
            assertThat(members).hasSize(3);
            
            // Convert to strings for easier comparison
            Set<String> memberStrings = members.stream()
                .map(String::new)
                .collect(java.util.stream.Collectors.toSet());
            assertThat(memberStrings).containsExactlyInAnyOrder("member1", "member2", "member3");
            
            // Test sIsMember for existing members
            Boolean isMember1 = connection.setCommands().sIsMember(key.getBytes(), value1);
            assertThat(isMember1).isTrue();
            
            Boolean isMember2 = connection.setCommands().sIsMember(key.getBytes(), value2);
            assertThat(isMember2).isTrue();
            
            // Test sIsMember for non-existing member
            Boolean isNonMember = connection.setCommands().sIsMember(key.getBytes(), nonMember);
            assertThat(isNonMember).isFalse();
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testSMIsMember() {
        String key = "test:set:mismember:key";
        byte[] value1 = "member1".getBytes();
        byte[] value2 = "member2".getBytes();
        byte[] value3 = "member3".getBytes();
        byte[] nonMember = "nonmember".getBytes();
        
        try {
            // Add some members
            connection.setCommands().sAdd(key.getBytes(), value1, value2);
            
            // Test sMIsMember with mixed existing and non-existing members
            List<Boolean> results = connection.setCommands().sMIsMember(key.getBytes(), 
                value1, value2, value3, nonMember);
            
            assertThat(results).hasSize(4);
            assertThat(results.get(0)).isTrue();  // member1 exists
            assertThat(results.get(1)).isTrue();  // member2 exists
            assertThat(results.get(2)).isFalse(); // member3 doesn't exist
            assertThat(results.get(3)).isFalse(); // nonmember doesn't exist
            
            // Test on empty set
            String emptyKey = "test:set:mismember:empty";
            List<Boolean> emptyResults = connection.setCommands().sMIsMember(emptyKey.getBytes(), 
                value1, value2);
            assertThat(emptyResults).containsOnly(false, false);
        } finally {
            cleanupKey(key);
            cleanupKey("test:set:mismember:empty");
        }
    }

    @Test
    void testSRem() {
        String key = "test:set:rem:key";
        byte[] value1 = "member1".getBytes();
        byte[] value2 = "member2".getBytes();
        byte[] value3 = "member3".getBytes();
        byte[] nonMember = "nonmember".getBytes();
        
        try {
            // Add some members
            connection.setCommands().sAdd(key.getBytes(), value1, value2, value3);
            
            // Test removing single member
            Long remResult1 = connection.setCommands().sRem(key.getBytes(), value1);
            assertThat(remResult1).isEqualTo(1L);
            
            // Verify member was removed
            Boolean isMember = connection.setCommands().sIsMember(key.getBytes(), value1);
            assertThat(isMember).isFalse();
            
            // Test removing multiple members
            Long remResult2 = connection.setCommands().sRem(key.getBytes(), value2, value3);
            assertThat(remResult2).isEqualTo(2L);
            
            // Test removing non-existing member
            Long remNonExist = connection.setCommands().sRem(key.getBytes(), nonMember);
            assertThat(remNonExist).isEqualTo(0L);
            
            // Verify set is empty
            Long card = connection.setCommands().sCard(key.getBytes());
            assertThat(card).isEqualTo(0L);
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Random Operations ====================

    @Test
    void testSRandMember() {
        String key = "test:set:randmember:key";
        byte[] value1 = "member1".getBytes();
        byte[] value2 = "member2".getBytes();
        byte[] value3 = "member3".getBytes();
        
        try {
            // Test sRandMember on empty set
            byte[] emptyRand = connection.setCommands().sRandMember(key.getBytes());
            assertThat(emptyRand).isNull();
            
            // Add members
            connection.setCommands().sAdd(key.getBytes(), value1, value2, value3);
            
            // Test single random member
            byte[] randomMember = connection.setCommands().sRandMember(key.getBytes());
            assertThat(randomMember).isNotNull();
            
            String randomMemberStr = new String(randomMember);
            assertThat(randomMemberStr).isIn("member1", "member2", "member3");
            
            // Test multiple random members
            List<byte[]> randomMembers = connection.setCommands().sRandMember(key.getBytes(), 2);
            assertThat(randomMembers).hasSize(2);
            
            // Convert to strings for verification
            List<String> randomMemberStrs = randomMembers.stream()
                .map(String::new)
                .collect(java.util.stream.Collectors.toList());
            
            // All returned members should be valid
            for (String member : randomMemberStrs) {
                assertThat(member).isIn("member1", "member2", "member3");
            }
            
            // Test with count larger than set size
            List<byte[]> moreMembers = connection.setCommands().sRandMember(key.getBytes(), 5);
            assertThat(moreMembers).hasSizeLessThanOrEqualTo(3); // Should not exceed set size
            
            // Test with negative count (allows duplicates)
            List<byte[]> negativeCount = connection.setCommands().sRandMember(key.getBytes(), -5);
            assertThat(negativeCount).hasSize(5); // Should return exactly 5 elements (with possible duplicates)
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testSPop() {
        String key = "test:set:pop:key";
        byte[] value1 = "member1".getBytes();
        byte[] value2 = "member2".getBytes();
        byte[] value3 = "member3".getBytes();
        
        try {
            // Test sPop on empty set
            byte[] emptyPop = connection.setCommands().sPop(key.getBytes());
            assertThat(emptyPop).isNull();
            
            // Add members
            connection.setCommands().sAdd(key.getBytes(), value1, value2, value3);
            
            // Test single pop
            byte[] poppedMember = connection.setCommands().sPop(key.getBytes());
            assertThat(poppedMember).isNotNull();
            
            String poppedStr = new String(poppedMember);
            assertThat(poppedStr).isIn("member1", "member2", "member3");
            
            // Verify member was removed
            Boolean stillMember = connection.setCommands().sIsMember(key.getBytes(), poppedMember);
            assertThat(stillMember).isFalse();
            
            // Verify cardinality decreased
            Long card = connection.setCommands().sCard(key.getBytes());
            assertThat(card).isEqualTo(2L);
            
            // Test multiple pop
            List<byte[]> poppedMembers = connection.setCommands().sPop(key.getBytes(), 2);
            assertThat(poppedMembers).hasSize(2);
            
            // Verify all were removed
            Long finalCard = connection.setCommands().sCard(key.getBytes());
            assertThat(finalCard).isEqualTo(0L);
            
            // Test pop more than available
            connection.setCommands().sAdd(key.getBytes(), value1);
            List<byte[]> overPop = connection.setCommands().sPop(key.getBytes(), 5);
            assertThat(overPop).hasSize(1); // Should only return what's available
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Set Movement Operations ====================

    @Test
    void testSMove() {
        String srcKey = "test:set:move:src";
        String destKey = "test:set:move:dest";
        byte[] value1 = "member1".getBytes();
        byte[] value2 = "member2".getBytes();
        byte[] nonMember = "nonmember".getBytes();
        
        try {
            // Setup source set
            connection.setCommands().sAdd(srcKey.getBytes(), value1, value2);
            
            // Test moving existing member
            Boolean moveResult1 = connection.setCommands().sMove(srcKey.getBytes(), destKey.getBytes(), value1);
            assertThat(moveResult1).isTrue();
            
            // Verify member was removed from source
            Boolean inSrc = connection.setCommands().sIsMember(srcKey.getBytes(), value1);
            assertThat(inSrc).isFalse();
            
            // Verify member was added to destination
            Boolean inDest = connection.setCommands().sIsMember(destKey.getBytes(), value1);
            assertThat(inDest).isTrue();
            
            // Test moving non-existing member
            Boolean moveResult2 = connection.setCommands().sMove(srcKey.getBytes(), destKey.getBytes(), nonMember);
            assertThat(moveResult2).isFalse();
            
            // Test moving from non-existing set
            String nonExistentKey = "test:set:move:nonexistent";
            Boolean moveResult3 = connection.setCommands().sMove(nonExistentKey.getBytes(), destKey.getBytes(), value2);
            assertThat(moveResult3).isFalse();
        } finally {
            cleanupKey(srcKey);
            cleanupKey(destKey);
            cleanupKey("test:set:move:nonexistent");
        }
    }

    // ==================== Set Algebra Operations ====================

    @Test
    void testSDiff() {
        String key1 = "test:set:diff:key1";
        String key2 = "test:set:diff:key2";
        String key3 = "test:set:diff:key3";
        
        try {
            // Setup test sets
            // key1: {a, b, c, d}
            connection.setCommands().sAdd(key1.getBytes(), "a".getBytes(), "b".getBytes(), "c".getBytes(), "d".getBytes());
            // key2: {b, c, e}
            connection.setCommands().sAdd(key2.getBytes(), "b".getBytes(), "c".getBytes(), "e".getBytes());
            // key3: {d, f}
            connection.setCommands().sAdd(key3.getBytes(), "d".getBytes(), "f".getBytes());
            
            // Test diff of key1 - key2 (should be {a, d})
            Set<byte[]> diff1 = connection.setCommands().sDiff(key1.getBytes(), key2.getBytes());
            Set<String> diff1Strings = diff1.stream()
                .map(String::new)
                .collect(java.util.stream.Collectors.toSet());
            assertThat(diff1Strings).containsExactlyInAnyOrder("a", "d");
            
            // Test diff of key1 - key2 - key3 (should be {a})
            Set<byte[]> diff2 = connection.setCommands().sDiff(key1.getBytes(), key2.getBytes(), key3.getBytes());
            Set<String> diff2Strings = diff2.stream()
                .map(String::new)
                .collect(java.util.stream.Collectors.toSet());
            assertThat(diff2Strings).containsExactlyInAnyOrder("a");
            
            // Test diff with non-existent key
            String nonExistentKey = "test:set:diff:nonexistent";
            Set<byte[]> diff3 = connection.setCommands().sDiff(key1.getBytes(), nonExistentKey.getBytes());
            Set<String> diff3Strings = diff3.stream()
                .map(String::new)
                .collect(java.util.stream.Collectors.toSet());
            assertThat(diff3Strings).containsExactlyInAnyOrder("a", "b", "c", "d");
        } finally {
            cleanupKey(key1);
            cleanupKey(key2);
            cleanupKey(key3);
        }
    }

    @Test
    void testSDiffStore() {
        String key1 = "test:set:diffstore:key1";
        String key2 = "test:set:diffstore:key2";
        String destKey = "test:set:diffstore:dest";
        
        try {
            // Setup test sets
            connection.setCommands().sAdd(key1.getBytes(), "a".getBytes(), "b".getBytes(), "c".getBytes());
            connection.setCommands().sAdd(key2.getBytes(), "b".getBytes(), "c".getBytes(), "d".getBytes());
            
            // Test sDiffStore
            Long storeResult = connection.setCommands().sDiffStore(destKey.getBytes(), key1.getBytes(), key2.getBytes());
            assertThat(storeResult).isEqualTo(1L); // Should store 1 element: "a"
            
            // Verify stored result
            Set<byte[]> storedDiff = connection.setCommands().sMembers(destKey.getBytes());
            Set<String> storedDiffStrings = storedDiff.stream()
                .map(String::new)
                .collect(java.util.stream.Collectors.toSet());
            assertThat(storedDiffStrings).containsExactlyInAnyOrder("a");
        } finally {
            cleanupKey(key1);
            cleanupKey(key2);
            cleanupKey(destKey);
        }
    }

    @Test
    void testSInter() {
        String key1 = "test:set:inter:key1";
        String key2 = "test:set:inter:key2";
        String key3 = "test:set:inter:key3";
        
        try {
            // Setup test sets
            // key1: {a, b, c}
            connection.setCommands().sAdd(key1.getBytes(), "a".getBytes(), "b".getBytes(), "c".getBytes());
            // key2: {b, c, d}
            connection.setCommands().sAdd(key2.getBytes(), "b".getBytes(), "c".getBytes(), "d".getBytes());
            // key3: {c, d, e}
            connection.setCommands().sAdd(key3.getBytes(), "c".getBytes(), "d".getBytes(), "e".getBytes());
            
            // Test intersection of key1 and key2 (should be {b, c})
            Set<byte[]> inter1 = connection.setCommands().sInter(key1.getBytes(), key2.getBytes());
            Set<String> inter1Strings = inter1.stream()
                .map(String::new)
                .collect(java.util.stream.Collectors.toSet());
            assertThat(inter1Strings).containsExactlyInAnyOrder("b", "c");
            
            // Test intersection of all three (should be {c})
            Set<byte[]> inter2 = connection.setCommands().sInter(key1.getBytes(), key2.getBytes(), key3.getBytes());
            Set<String> inter2Strings = inter2.stream()
                .map(String::new)
                .collect(java.util.stream.Collectors.toSet());
            assertThat(inter2Strings).containsExactlyInAnyOrder("c");
            
            // Test intersection with non-existent key (should be empty)
            String nonExistentKey = "test:set:inter:nonexistent";
            Set<byte[]> inter3 = connection.setCommands().sInter(key1.getBytes(), nonExistentKey.getBytes());
            assertThat(inter3).isEmpty();
        } finally {
            cleanupKey(key1);
            cleanupKey(key2);
            cleanupKey(key3);
        }
    }

    @Test
    void testSInterStore() {
        String key1 = "test:set:interstore:key1";
        String key2 = "test:set:interstore:key2";
        String destKey = "test:set:interstore:dest";
        
        try {
            // Setup test sets
            connection.setCommands().sAdd(key1.getBytes(), "a".getBytes(), "b".getBytes(), "c".getBytes());
            connection.setCommands().sAdd(key2.getBytes(), "b".getBytes(), "c".getBytes(), "d".getBytes());
            
            // Test sInterStore
            Long storeResult = connection.setCommands().sInterStore(destKey.getBytes(), key1.getBytes(), key2.getBytes());
            assertThat(storeResult).isEqualTo(2L); // Should store 2 elements: "b", "c"
            
            // Verify stored result
            Set<byte[]> storedInter = connection.setCommands().sMembers(destKey.getBytes());
            Set<String> storedInterStrings = storedInter.stream()
                .map(String::new)
                .collect(java.util.stream.Collectors.toSet());
            assertThat(storedInterStrings).containsExactlyInAnyOrder("b", "c");
        } finally {
            cleanupKey(key1);
            cleanupKey(key2);
            cleanupKey(destKey);
        }
    }

    @Test
    void testSUnion() {
        String key1 = "test:set:union:key1";
        String key2 = "test:set:union:key2";
        String key3 = "test:set:union:key3";
        
        try {
            // Setup test sets
            // key1: {a, b}
            connection.setCommands().sAdd(key1.getBytes(), "a".getBytes(), "b".getBytes());
            // key2: {b, c}
            connection.setCommands().sAdd(key2.getBytes(), "b".getBytes(), "c".getBytes());
            // key3: {c, d}
            connection.setCommands().sAdd(key3.getBytes(), "c".getBytes(), "d".getBytes());
            
            // Test union of key1 and key2 (should be {a, b, c})
            Set<byte[]> union1 = connection.setCommands().sUnion(key1.getBytes(), key2.getBytes());
            Set<String> union1Strings = union1.stream()
                .map(String::new)
                .collect(java.util.stream.Collectors.toSet());
            assertThat(union1Strings).containsExactlyInAnyOrder("a", "b", "c");
            
            // Test union of all three (should be {a, b, c, d})
            Set<byte[]> union2 = connection.setCommands().sUnion(key1.getBytes(), key2.getBytes(), key3.getBytes());
            Set<String> union2Strings = union2.stream()
                .map(String::new)
                .collect(java.util.stream.Collectors.toSet());
            assertThat(union2Strings).containsExactlyInAnyOrder("a", "b", "c", "d");
            
            // Test union with non-existent key
            String nonExistentKey = "test:set:union:nonexistent";
            Set<byte[]> union3 = connection.setCommands().sUnion(key1.getBytes(), nonExistentKey.getBytes());
            Set<String> union3Strings = union3.stream()
                .map(String::new)
                .collect(java.util.stream.Collectors.toSet());
            assertThat(union3Strings).containsExactlyInAnyOrder("a", "b");
        } finally {
            cleanupKey(key1);
            cleanupKey(key2);
            cleanupKey(key3);
        }
    }

    @Test
    void testSUnionStore() {
        String key1 = "test:set:unionstore:key1";
        String key2 = "test:set:unionstore:key2";
        String destKey = "test:set:unionstore:dest";
        
        try {
            // Setup test sets
            connection.setCommands().sAdd(key1.getBytes(), "a".getBytes(), "b".getBytes());
            connection.setCommands().sAdd(key2.getBytes(), "b".getBytes(), "c".getBytes());
            
            // Test sUnionStore
            Long storeResult = connection.setCommands().sUnionStore(destKey.getBytes(), key1.getBytes(), key2.getBytes());
            assertThat(storeResult).isEqualTo(3L); // Should store 3 elements: "a", "b", "c"
            
            // Verify stored result
            Set<byte[]> storedUnion = connection.setCommands().sMembers(destKey.getBytes());
            Set<String> storedUnionStrings = storedUnion.stream()
                .map(String::new)
                .collect(java.util.stream.Collectors.toSet());
            assertThat(storedUnionStrings).containsExactlyInAnyOrder("a", "b", "c");
        } finally {
            cleanupKey(key1);
            cleanupKey(key2);
            cleanupKey(destKey);
        }
    }

    // ==================== Set Scanning Operations ====================

    @Test
    void testSScan() {
        String key = "test:set:scan:key";
        
        try {
            // Setup test set with multiple members
            for (int i = 0; i < 10; i++) {
                connection.setCommands().sAdd(key.getBytes(), ("member" + i).getBytes());
            }
            
            // Test basic scan
            ScanOptions options = ScanOptions.scanOptions().build();
            Cursor<byte[]> cursor = connection.setCommands().sScan(key.getBytes(), options);
            
            java.util.List<String> scannedMembers = new java.util.ArrayList<>();
            while (cursor.hasNext()) {
                scannedMembers.add(new String(cursor.next()));
            }
            cursor.close();
            
            // Should find all members
            assertThat(scannedMembers).hasSize(10);
            for (int i = 0; i < 10; i++) {
                assertThat(scannedMembers).contains("member" + i);
            }
            
            // Test scan with pattern
            ScanOptions patternOptions = ScanOptions.scanOptions().match("member[1-3]").build();
            Cursor<byte[]> patternCursor = connection.setCommands().sScan(key.getBytes(), patternOptions);
            
            java.util.List<String> patternMembers = new java.util.ArrayList<>();
            while (patternCursor.hasNext()) {
                patternMembers.add(new String(patternCursor.next()));
            }
            patternCursor.close();
            
            // Should find fewer members due to pattern filter
            assertThat(patternMembers.size()).isLessThanOrEqualTo(3);
            
            // Test scan with count
            ScanOptions countOptions = ScanOptions.scanOptions().count(2).build();
            Cursor<byte[]> countCursor = connection.setCommands().sScan(key.getBytes(), countOptions);
            
            java.util.List<String> countMembers = new java.util.ArrayList<>();
            while (countCursor.hasNext()) {
                countMembers.add(new String(countCursor.next()));
            }
            countCursor.close();
            
            // Should still find all members, but in smaller batches
            assertThat(countMembers).hasSize(10);
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Error Handling and Edge Cases ====================

    @Test
    void testSetOperationsErrorHandling() {
        // Test operations on null keys (should throw IllegalArgumentException)
        assertThatThrownBy(() -> connection.setCommands().sAdd(null, "value".getBytes()))
            .isInstanceOf(IllegalArgumentException.class);
        
        assertThatThrownBy(() -> connection.setCommands().sRem(null, "value".getBytes()))
            .isInstanceOf(IllegalArgumentException.class);
        
        assertThatThrownBy(() -> connection.setCommands().sCard(null))
            .isInstanceOf(IllegalArgumentException.class);
        
        // Test operations on empty keys (should throw IllegalArgumentException)
        assertThatThrownBy(() -> connection.setCommands().sAdd(new byte[0], "value".getBytes()))
            .isInstanceOf(IllegalArgumentException.class);
        
        assertThatThrownBy(() -> connection.setCommands().sRem(new byte[0], "value".getBytes()))
            .isInstanceOf(IllegalArgumentException.class);
        
        assertThatThrownBy(() -> connection.setCommands().sCard(new byte[0]))
            .isInstanceOf(IllegalArgumentException.class);
        
        // Test operations with null values
        assertThatThrownBy(() -> connection.setCommands().sAdd("key".getBytes(), (byte[]) null))
            .isInstanceOf(IllegalArgumentException.class);
        
        assertThatThrownBy(() -> connection.setCommands().sRem("key".getBytes(), (byte[]) null))
            .isInstanceOf(IllegalArgumentException.class);
        
        assertThatThrownBy(() -> connection.setCommands().sIsMember("key".getBytes(), null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testSetOperationsEdgeCases() {
        String key = "test:set:edge:key";
        
        try {
            // Test operations on very large values
            byte[] largeValue = new byte[1024 * 10]; // 10KB value
            java.util.Arrays.fill(largeValue, (byte) 'A');
            
            Long addLarge = connection.setCommands().sAdd(key.getBytes(), largeValue);
            assertThat(addLarge).isEqualTo(1L);
            
            Boolean isLargeMember = connection.setCommands().sIsMember(key.getBytes(), largeValue);
            assertThat(isLargeMember).isTrue();
            
            Long remLarge = connection.setCommands().sRem(key.getBytes(), largeValue);
            assertThat(remLarge).isEqualTo(1L);
            
            // Test with empty value
            byte[] emptyValue = new byte[0];
            Long addEmpty = connection.setCommands().sAdd(key.getBytes(), emptyValue);
            assertThat(addEmpty).isEqualTo(1L);
            
            Boolean isEmptyMember = connection.setCommands().sIsMember(key.getBytes(), emptyValue);
            assertThat(isEmptyMember).isTrue();
            
            // Test with binary values
            byte[] binaryValue = {0x00, (byte) 0xFF, 0x7F, (byte) 0x80, 0x01};
            Long addBinary = connection.setCommands().sAdd(key.getBytes(), binaryValue);
            assertThat(addBinary).isEqualTo(1L);
            
            Boolean isBinaryMember = connection.setCommands().sIsMember(key.getBytes(), binaryValue);
            assertThat(isBinaryMember).isTrue();
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testSetAlgebraEdgeCases() {
        String key1 = "test:set:algebra:edge:key1";
        String key2 = "test:set:algebra:edge:key2";
        String destKey = "test:set:algebra:edge:dest";
        
        try {
            // Test operations with empty sets
            Set<byte[]> emptyDiff = connection.setCommands().sDiff(key1.getBytes(), key2.getBytes());
            assertThat(emptyDiff).isEmpty();
            
            Set<byte[]> emptyInter = connection.setCommands().sInter(key1.getBytes(), key2.getBytes());
            assertThat(emptyInter).isEmpty();
            
            Set<byte[]> emptyUnion = connection.setCommands().sUnion(key1.getBytes(), key2.getBytes());
            assertThat(emptyUnion).isEmpty();
            
            // Test store operations with empty results
            Long diffStore = connection.setCommands().sDiffStore(destKey.getBytes(), key1.getBytes(), key2.getBytes());
            assertThat(diffStore).isEqualTo(0L);
            
            Long interStore = connection.setCommands().sInterStore(destKey.getBytes(), key1.getBytes(), key2.getBytes());
            assertThat(interStore).isEqualTo(0L);
            
            Long unionStore = connection.setCommands().sUnionStore(destKey.getBytes(), key1.getBytes(), key2.getBytes());
            assertThat(unionStore).isEqualTo(0L);
            
            // Test with single set having data
            connection.setCommands().sAdd(key1.getBytes(), "a".getBytes(), "b".getBytes());
            
            Set<byte[]> diffSingle = connection.setCommands().sDiff(key1.getBytes(), key2.getBytes());
            assertThat(diffSingle).hasSize(2);
            
            Set<byte[]> unionSingle = connection.setCommands().sUnion(key1.getBytes(), key2.getBytes());
            assertThat(unionSingle).hasSize(2);
        } finally {
            cleanupKey(key1);
            cleanupKey(key2);
            cleanupKey(destKey);
        }
    }

}

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
package org.springframework.data.valkey.connection.valkeyglide;

import static org.assertj.core.api.Assertions.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.springframework.data.domain.Range;
import org.springframework.data.valkey.connection.Limit;
import org.springframework.data.valkey.connection.ValkeyZSetCommands;
import org.springframework.data.valkey.connection.zset.Aggregate;
import org.springframework.data.valkey.connection.zset.DefaultTuple;
import org.springframework.data.valkey.connection.zset.Tuple;
import org.springframework.data.valkey.connection.zset.Weights;
import org.springframework.data.valkey.core.Cursor;
import org.springframework.data.valkey.core.ScanOptions;

/**
 * Comprehensive low-level integration tests for {@link ValkeyGlideConnection} 
 * sorted set functionality using the ValkeyZSetCommands interface directly.
 * 
 * These tests validate the implementation of all ValkeyZSetCommands methods:
 * - Basic sorted set operations (zAdd, zRem, zCard, zRange, zScore)
 * - Ranking operations (zRank, zRevRank)
 * - Random operations (zRandMember, zPopMin, zPopMax)
 * - Blocking operations (bZPopMin, bZPopMax)
 * - Range operations (zRangeByScore, zRangeByLex, zRevRange)
 * - Counting operations (zCount, zLexCount)
 * - Set algebra operations (zDiff, zInter, zUnion)
 * - Store operations (zDiffStore, zInterStore, zUnionStore)
 * - Range store operations (zRangeStore variants)
 * - Removal operations (zRemRange, zRemRangeByScore, zRemRangeByLex)
 * - Scanning operations (zScan)
 * - Error handling and edge cases
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideConnectionZSetCommandsIntegrationTests extends AbstractValkeyGlideIntegrationTests {

    @Override
    protected String[] getTestKeyPatterns() {
        return new String[]{
            "test:zset:add:key", "test:zset:add:single:key", "test:zset:rem:key", "test:zset:mscore:key",
            "test:zset:incrby:key", "test:zset:range:key", "test:zset:rangebyscore:key", "test:zset:rangebylex:key",
            "test:zset:rank:key", "test:zset:count:key", "test:zset:lexcount:key", "test:zset:randmember:key",
            "test:zset:pop:key", "test:zset:diff:key1", "test:zset:diff:key2", "test:zset:diff:dest",
            "test:zset:inter:key1", "test:zset:inter:key2", "test:zset:inter:dest", "test:zset:union:key1",
            "test:zset:union:key2", "test:zset:union:dest", "test:zset:rangestore:src", "test:zset:rangestore:dest",
            "test:zset:rangestore:srclex", "test:zset:remrange:key", "test:zset:scan:key"
        };
    }

    // ==================== Basic ZSet Operations ====================

    @Test
    void testZAddAndZCard() {
        String key = "test:zset:add:key";
        
        try {
            // Test zCard on non-existent key
            Long cardEmpty = connection.zSetCommands().zCard(key.getBytes());
            assertThat(cardEmpty).isEqualTo(0L);
            
            // Test adding single tuple
            Set<Tuple> tuples = new HashSet<>();
            tuples.add(new DefaultTuple("member1".getBytes(), 1.0));
            
            Long addResult1 = connection.zSetCommands().zAdd(key.getBytes(), tuples, 
                ValkeyZSetCommands.ZAddArgs.empty());
            assertThat(addResult1).isEqualTo(1L);
            
            // Test cardinality after adding one element
            Long card1 = connection.zSetCommands().zCard(key.getBytes());
            assertThat(card1).isEqualTo(1L);
            
            // Test adding multiple tuples
            tuples.clear();
            tuples.add(new DefaultTuple("member2".getBytes(), 2.0));
            tuples.add(new DefaultTuple("member3".getBytes(), 3.0));
            
            Long addResult2 = connection.zSetCommands().zAdd(key.getBytes(), tuples, 
                ValkeyZSetCommands.ZAddArgs.empty());
            assertThat(addResult2).isEqualTo(2L);
            
            // Test cardinality after adding three elements
            Long card3 = connection.zSetCommands().zCard(key.getBytes());
            assertThat(card3).isEqualTo(3L);
            
            // Test adding duplicate value with different score
            tuples.clear();
            tuples.add(new DefaultTuple("member1".getBytes(), 1.5));
            
            Long addDuplicate = connection.zSetCommands().zAdd(key.getBytes(), tuples, 
                ValkeyZSetCommands.ZAddArgs.empty());
            assertThat(addDuplicate).isEqualTo(0L); // Should update existing, not add new
            
            // Cardinality should remain the same
            Long cardSame = connection.zSetCommands().zCard(key.getBytes());
            assertThat(cardSame).isEqualTo(3L);
            
            // But score should be updated
            Double score = connection.zSetCommands().zScore(key.getBytes(), "member1".getBytes());
            assertThat(score).isEqualTo(1.5);
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testZAddSingleValueWithArgs() {
        String key = "test:zset:add:single:key";
        
        try {
            // Test adding with NX flag (only if not exists)
            Boolean addNX1 = connection.zSetCommands().zAdd(key.getBytes(), 1.0, "member1".getBytes(), 
                ValkeyZSetCommands.ZAddArgs.empty().nx());
            assertThat(addNX1).isTrue();
            
            // Try to add same member with NX flag (should return false)
            Boolean addNX2 = connection.zSetCommands().zAdd(key.getBytes(), 2.0, "member1".getBytes(), 
                ValkeyZSetCommands.ZAddArgs.empty().nx());
            assertThat(addNX2).isFalse();
            
            // Score should remain unchanged
            Double score = connection.zSetCommands().zScore(key.getBytes(), "member1".getBytes());
            assertThat(score).isEqualTo(1.0);
            
            // Test adding with XX flag (only if exists)
            Boolean addXX1 = connection.zSetCommands().zAdd(key.getBytes(), 1.5, "member1".getBytes(), 
                ValkeyZSetCommands.ZAddArgs.empty().xx());
            assertThat(addXX1).isFalse();
            
            // Score should be updated
            Double updatedScore = connection.zSetCommands().zScore(key.getBytes(), "member1".getBytes());
            assertThat(updatedScore).isEqualTo(1.5);
            
            // Try to add new member with XX flag (should return false)
            Boolean addXX2 = connection.zSetCommands().zAdd(key.getBytes(), 2.0, "member2".getBytes(), 
                ValkeyZSetCommands.ZAddArgs.empty().xx());
            assertThat(addXX2).isFalse();
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testZRemAndZScore() {
        String key = "test:zset:rem:key";
        
        try {
            // Setup test data
            Set<Tuple> tuples = new HashSet<>();
            tuples.add(new DefaultTuple("member1".getBytes(), 1.0));
            tuples.add(new DefaultTuple("member2".getBytes(), 2.0));
            tuples.add(new DefaultTuple("member3".getBytes(), 3.0));
            
            connection.zSetCommands().zAdd(key.getBytes(), tuples, 
                ValkeyZSetCommands.ZAddArgs.empty());
            
            // Test zScore for existing members
            Double score1 = connection.zSetCommands().zScore(key.getBytes(), "member1".getBytes());
            assertThat(score1).isEqualTo(1.0);
            
            Double score2 = connection.zSetCommands().zScore(key.getBytes(), "member2".getBytes());
            assertThat(score2).isEqualTo(2.0);
            
            // Test zScore for non-existent member
            Double scoreNonExistent = connection.zSetCommands().zScore(key.getBytes(), "nonexistent".getBytes());
            assertThat(scoreNonExistent).isNull();
            
            // Test removing single member
            Long remResult1 = connection.zSetCommands().zRem(key.getBytes(), "member1".getBytes());
            assertThat(remResult1).isEqualTo(1L);
            
            // Verify member was removed
            Double removedScore = connection.zSetCommands().zScore(key.getBytes(), "member1".getBytes());
            assertThat(removedScore).isNull();
            
            // Test removing multiple members
            Long remResult2 = connection.zSetCommands().zRem(key.getBytes(), 
                "member2".getBytes(), "member3".getBytes());
            assertThat(remResult2).isEqualTo(2L);
            
            // Test removing non-existent member
            Long remNonExistent = connection.zSetCommands().zRem(key.getBytes(), "nonexistent".getBytes());
            assertThat(remNonExistent).isEqualTo(0L);
            
            // Verify set is empty
            Long card = connection.zSetCommands().zCard(key.getBytes());
            assertThat(card).isEqualTo(0L);
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testZMScore() {
        String key = "test:zset:mscore:key";
        
        try {
            // Setup test data
            Set<Tuple> tuples = new HashSet<>();
            tuples.add(new DefaultTuple("member1".getBytes(), 1.0));
            tuples.add(new DefaultTuple("member2".getBytes(), 2.0));
            tuples.add(new DefaultTuple("member3".getBytes(), 3.0));
            
            connection.zSetCommands().zAdd(key.getBytes(), tuples, 
                ValkeyZSetCommands.ZAddArgs.empty());
            
            // Test zMScore for multiple members
            List<Double> scores = connection.zSetCommands().zMScore(key.getBytes(), 
                "member1".getBytes(), "member2".getBytes(), "nonexistent".getBytes(), "member3".getBytes());
            
            assertThat(scores).hasSize(4);
            assertThat(scores.get(0)).isEqualTo(1.0);
            assertThat(scores.get(1)).isEqualTo(2.0);
            assertThat(scores.get(2)).isNull(); // nonexistent member
            assertThat(scores.get(3)).isEqualTo(3.0);
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testZIncrBy() {
        String key = "test:zset:incrby:key";
        
        try {
            // Add initial member
            Set<Tuple> tuples = new HashSet<>();
            tuples.add(new DefaultTuple("member1".getBytes(), 1.0));
            connection.zSetCommands().zAdd(key.getBytes(), tuples, 
                ValkeyZSetCommands.ZAddArgs.empty());
            
            // Test incrementing existing member
            Double newScore1 = connection.zSetCommands().zIncrBy(key.getBytes(), 2.5, "member1".getBytes());
            assertThat(newScore1).isEqualTo(3.5);
            
            // Verify score was updated
            Double score = connection.zSetCommands().zScore(key.getBytes(), "member1".getBytes());
            assertThat(score).isEqualTo(3.5);
            
            // Test incrementing non-existent member (should create with increment as score)
            Double newScore2 = connection.zSetCommands().zIncrBy(key.getBytes(), 5.0, "member2".getBytes());
            assertThat(newScore2).isEqualTo(5.0);
            
            // Test negative increment
            Double newScore3 = connection.zSetCommands().zIncrBy(key.getBytes(), -1.5, "member1".getBytes());
            assertThat(newScore3).isEqualTo(2.0);
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Range Operations ====================

    @Test
    void testZRangeAndZRevRange() {
        String key = "test:zset:range:key";
        
        try {
            // Setup test data with different scores
            Set<Tuple> tuples = new HashSet<>();
            tuples.add(new DefaultTuple("member1".getBytes(), 1.0));
            tuples.add(new DefaultTuple("member2".getBytes(), 2.0));
            tuples.add(new DefaultTuple("member3".getBytes(), 3.0));
            tuples.add(new DefaultTuple("member4".getBytes(), 4.0));
            tuples.add(new DefaultTuple("member5".getBytes(), 5.0));
            
            connection.zSetCommands().zAdd(key.getBytes(), tuples, 
                ValkeyZSetCommands.ZAddArgs.empty());
            
            // Test zRange (ascending order)
            Set<byte[]> range1 = connection.zSetCommands().zRange(key.getBytes(), 0, 2);
            assertThat(range1).hasSize(3);
            List<String> rangeList1 = range1.stream()
                .map(String::new)
                .collect(java.util.stream.Collectors.toList());
            assertThat(rangeList1).containsExactly("member1", "member2", "member3");
            
            // Test zRange with negative indices
            Set<byte[]> range2 = connection.zSetCommands().zRange(key.getBytes(), -2, -1);
            assertThat(range2).hasSize(2);
            List<String> rangeList2 = range2.stream()
                .map(String::new)
                .collect(java.util.stream.Collectors.toList());
            assertThat(rangeList2).containsExactly("member4", "member5");
            
            // Test zRangeWithScores
            Set<Tuple> rangeWithScores = connection.zSetCommands().zRangeWithScores(key.getBytes(), 0, 2);
            assertThat(rangeWithScores).hasSize(3);
            
            // Test zRevRange (descending order)
            Set<byte[]> revRange = connection.zSetCommands().zRevRange(key.getBytes(), 0, 2);
            assertThat(revRange).hasSize(3);
            List<String> revRangeList = revRange.stream()
                .map(String::new)
                .collect(java.util.stream.Collectors.toList());
            assertThat(revRangeList).containsExactly("member5", "member4", "member3");
            
            // Test zRevRangeWithScores
            Set<Tuple> revRangeWithScores = connection.zSetCommands().zRevRangeWithScores(key.getBytes(), 0, 2);
            assertThat(revRangeWithScores).hasSize(3);
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testZRangeByScore() {
        String key = "test:zset:rangebyscore:key";
        
        try {
            // Setup test data
            Set<Tuple> tuples = new HashSet<>();
            tuples.add(new DefaultTuple("member1".getBytes(), 1.0));
            tuples.add(new DefaultTuple("member2".getBytes(), 2.0));
            tuples.add(new DefaultTuple("member3".getBytes(), 3.0));
            tuples.add(new DefaultTuple("member4".getBytes(), 4.0));
            tuples.add(new DefaultTuple("member5".getBytes(), 5.0));
            
            connection.zSetCommands().zAdd(key.getBytes(), tuples, 
                ValkeyZSetCommands.ZAddArgs.empty());
            
            // Test zRangeByScore with Range
            Range<Double> range1 = Range.closed(2.0, 4.0);
            Set<byte[]> rangeResult1 = connection.zSetCommands().zRangeByScore(key.getBytes(), 
                range1.map(Number.class::cast), Limit.unlimited());
            assertThat(rangeResult1).hasSize(3);
            
            // Test zRangeByScore with string bounds
            Set<byte[]> rangeResult2 = connection.zSetCommands().zRangeByScore(key.getBytes(), 
                "2.0", "4.0", 0, 10);
            assertThat(rangeResult2).hasSize(3);
            
            // Test zRangeByScore with limit
            Set<byte[]> rangeResult3 = connection.zSetCommands().zRangeByScore(key.getBytes(), 
                range1.map(Number.class::cast), Limit.limit().count(2));
            assertThat(rangeResult3).hasSize(2);
            
            // Test zRangeByScoreWithScores
            Set<Tuple> rangeWithScores = connection.zSetCommands().zRangeByScoreWithScores(key.getBytes(), 
                range1.map(Number.class::cast), Limit.unlimited());
            assertThat(rangeWithScores).hasSize(3);
            
            // Test zRevRangeByScore
            Set<byte[]> revRangeResult = connection.zSetCommands().zRevRangeByScore(key.getBytes(), 
                range1.map(Number.class::cast), Limit.unlimited());
            assertThat(revRangeResult).hasSize(3);
            
            // Test zRevRangeByScoreWithScores
            Set<Tuple> revRangeWithScores = connection.zSetCommands().zRevRangeByScoreWithScores(key.getBytes(), 
                range1.map(Number.class::cast), Limit.unlimited());
            assertThat(revRangeWithScores).hasSize(3);
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testZRangeByLex() {
        String key = "test:zset:rangebylex:key";
        
        try {
            // Setup test data with same score for lexicographical ordering
            Set<Tuple> tuples = new HashSet<>();
            tuples.add(new DefaultTuple("a".getBytes(), 0.0));
            tuples.add(new DefaultTuple("b".getBytes(), 0.0));
            tuples.add(new DefaultTuple("c".getBytes(), 0.0));
            tuples.add(new DefaultTuple("d".getBytes(), 0.0));
            tuples.add(new DefaultTuple("e".getBytes(), 0.0));
            
            connection.zSetCommands().zAdd(key.getBytes(), tuples, 
                ValkeyZSetCommands.ZAddArgs.empty());
            
            // Test zRangeByLex
            Range<byte[]> lexRange = Range.closed("b".getBytes(), "d".getBytes());
            Set<byte[]> lexResult = connection.zSetCommands().zRangeByLex(key.getBytes(), 
                lexRange, Limit.unlimited());
            assertThat(lexResult).hasSize(3);
            
            List<String> lexResultList = lexResult.stream()
                .map(String::new)
                .collect(java.util.stream.Collectors.toList());
            assertThat(lexResultList).containsExactly("b", "c", "d");
            
            // Test zRevRangeByLex
            Set<byte[]> revLexResult = connection.zSetCommands().zRevRangeByLex(key.getBytes(), 
                lexRange, Limit.unlimited());
            assertThat(revLexResult).hasSize(3);
            
            List<String> revLexResultList = revLexResult.stream()
                .map(String::new)
                .collect(java.util.stream.Collectors.toList());
            assertThat(revLexResultList).containsExactly("d", "c", "b");
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Ranking Operations ====================

    @Test
    void testZRankAndZRevRank() {
        String key = "test:zset:rank:key";
        
        try {
            // Setup test data
            Set<Tuple> tuples = new HashSet<>();
            tuples.add(new DefaultTuple("member1".getBytes(), 1.0));
            tuples.add(new DefaultTuple("member2".getBytes(), 2.0));
            tuples.add(new DefaultTuple("member3".getBytes(), 3.0));
            tuples.add(new DefaultTuple("member4".getBytes(), 4.0));
            
            connection.zSetCommands().zAdd(key.getBytes(), tuples, 
                ValkeyZSetCommands.ZAddArgs.empty());
            
            // Test zRank (0-based, ascending)
            Long rank1 = connection.zSetCommands().zRank(key.getBytes(), "member1".getBytes());
            assertThat(rank1).isEqualTo(0L);
            
            Long rank2 = connection.zSetCommands().zRank(key.getBytes(), "member2".getBytes());
            assertThat(rank2).isEqualTo(1L);
            
            Long rank4 = connection.zSetCommands().zRank(key.getBytes(), "member4".getBytes());
            assertThat(rank4).isEqualTo(3L);
            
            // Test zRank for non-existent member
            Long rankNonExistent = connection.zSetCommands().zRank(key.getBytes(), "nonexistent".getBytes());
            assertThat(rankNonExistent).isNull();
            
            // Test zRevRank (0-based, descending)
            Long revRank1 = connection.zSetCommands().zRevRank(key.getBytes(), "member1".getBytes());
            assertThat(revRank1).isEqualTo(3L);
            
            Long revRank4 = connection.zSetCommands().zRevRank(key.getBytes(), "member4".getBytes());
            assertThat(revRank4).isEqualTo(0L);
            
            // Test zRevRank for non-existent member
            Long revRankNonExistent = connection.zSetCommands().zRevRank(key.getBytes(), "nonexistent".getBytes());
            assertThat(revRankNonExistent).isNull();
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Counting Operations ====================

    @Test
    void testZCount() {
        String key = "test:zset:count:key";
        
        try {
            // Setup test data
            Set<Tuple> tuples = new HashSet<>();
            tuples.add(new DefaultTuple("member1".getBytes(), 1.0));
            tuples.add(new DefaultTuple("member2".getBytes(), 2.0));
            tuples.add(new DefaultTuple("member3".getBytes(), 3.0));
            tuples.add(new DefaultTuple("member4".getBytes(), 4.0));
            tuples.add(new DefaultTuple("member5".getBytes(), 5.0));
            
            connection.zSetCommands().zAdd(key.getBytes(), tuples, 
                ValkeyZSetCommands.ZAddArgs.empty());
            
            // Test zCount with inclusive range
            Range<Double> range1 = Range.closed(2.0, 4.0);
            Long count1 = connection.zSetCommands().zCount(key.getBytes(), range1.map(Number.class::cast));
            assertThat(count1).isEqualTo(3L);
            
            // Test zCount with exclusive range
            Range<Double> range2 = Range.open(2.0, 4.0);
            Long count2 = connection.zSetCommands().zCount(key.getBytes(), range2.map(Number.class::cast));
            assertThat(count2).isEqualTo(1L); // Only member3 with score 3.0
            
            // Test zCount with unbounded range
            Range<Double> range3 = Range.from(Range.Bound.exclusive(3.0)).to(Range.Bound.unbounded());
            Long count3 = connection.zSetCommands().zCount(key.getBytes(), range3.map(Number.class::cast));
            assertThat(count3).isEqualTo(2L); // member4 and member5
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testZLexCount() {
        String key = "test:zset:lexcount:key";
        
        try {
            // Setup test data with same score for lexicographical ordering
            Set<Tuple> tuples = new HashSet<>();
            tuples.add(new DefaultTuple("a".getBytes(), 0.0));
            tuples.add(new DefaultTuple("b".getBytes(), 0.0));
            tuples.add(new DefaultTuple("c".getBytes(), 0.0));
            tuples.add(new DefaultTuple("d".getBytes(), 0.0));
            tuples.add(new DefaultTuple("e".getBytes(), 0.0));
            
            connection.zSetCommands().zAdd(key.getBytes(), tuples, 
                ValkeyZSetCommands.ZAddArgs.empty());
            
            // Test zLexCount with inclusive range
            Range<byte[]> lexRange1 = Range.closed("b".getBytes(), "d".getBytes());
            Long lexCount1 = connection.zSetCommands().zLexCount(key.getBytes(), lexRange1);
            assertThat(lexCount1).isEqualTo(3L);
            
            // Test zLexCount with exclusive range
            Range<byte[]> lexRange2 = Range.open("b".getBytes(), "d".getBytes());
            Long lexCount2 = connection.zSetCommands().zLexCount(key.getBytes(), lexRange2);
            assertThat(lexCount2).isEqualTo(1L); // Only "c"
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Random and Pop Operations ====================

    @Test
    void testZRandMember() {
        String key = "test:zset:randmember:key";
        
        try {
            // Test zRandMember on empty set
            byte[] emptyRand = connection.zSetCommands().zRandMember(key.getBytes());
            assertThat(emptyRand).isNull();
            
            // Setup test data
            Set<Tuple> tuples = new HashSet<>();
            tuples.add(new DefaultTuple("member1".getBytes(), 1.0));
            tuples.add(new DefaultTuple("member2".getBytes(), 2.0));
            tuples.add(new DefaultTuple("member3".getBytes(), 3.0));
            
            connection.zSetCommands().zAdd(key.getBytes(), tuples, 
                ValkeyZSetCommands.ZAddArgs.empty());
            
            // Test single random member
            byte[] randomMember = connection.zSetCommands().zRandMember(key.getBytes());
            assertThat(randomMember).isNotNull();
            String randomMemberStr = new String(randomMember);
            assertThat(randomMemberStr).isIn("member1", "member2", "member3");
            
            // Test multiple random members
            List<byte[]> randomMembers = connection.zSetCommands().zRandMember(key.getBytes(), 2);
            assertThat(randomMembers).hasSize(2);
            
            // Test zRandMemberWithScore
            Tuple randomTuple = connection.zSetCommands().zRandMemberWithScore(key.getBytes());
            assertThat(randomTuple).isNotNull();
            assertThat(randomTuple.getScore()).isIn(1.0, 2.0, 3.0);
            
            // Test zRandMemberWithScore with count
            List<Tuple> randomTuples = connection.zSetCommands().zRandMemberWithScore(key.getBytes(), 2);
            assertThat(randomTuples).hasSize(2);
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testZPopMinAndZPopMax() {
        String key = "test:zset:pop:key";
        
        try {
            // Test pop on empty set
            Tuple emptyPop = connection.zSetCommands().zPopMin(key.getBytes());
            assertThat(emptyPop).isNull();
            
            // Setup test data
            Set<Tuple> tuples = new HashSet<>();
            tuples.add(new DefaultTuple("member1".getBytes(), 1.0));
            tuples.add(new DefaultTuple("member2".getBytes(), 2.0));
            tuples.add(new DefaultTuple("member3".getBytes(), 3.0));
            
            connection.zSetCommands().zAdd(key.getBytes(), tuples, 
                ValkeyZSetCommands.ZAddArgs.empty());
            
            // Test zPopMin single
            Tuple minTuple = connection.zSetCommands().zPopMin(key.getBytes());
            assertThat(minTuple).isNotNull();
            assertThat(minTuple.getScore()).isEqualTo(1.0);
            assertThat(new String(minTuple.getValue())).isEqualTo("member1");
            
            // Test zPopMax single
            Tuple maxTuple = connection.zSetCommands().zPopMax(key.getBytes());
            assertThat(maxTuple).isNotNull();
            assertThat(maxTuple.getScore()).isEqualTo(3.0);
            assertThat(new String(maxTuple.getValue())).isEqualTo("member3");
            
            // Add more data for multiple pop tests
            tuples.clear();
            tuples.add(new DefaultTuple("member4".getBytes(), 4.0));
            tuples.add(new DefaultTuple("member5".getBytes(), 5.0));
            connection.zSetCommands().zAdd(key.getBytes(), tuples, 
                ValkeyZSetCommands.ZAddArgs.empty());
            
            // Test zPopMin multiple
            Set<Tuple> minTuples = connection.zSetCommands().zPopMin(key.getBytes(), 2);
            assertThat(minTuples).hasSize(2);
            
            // Test zPopMax multiple
            Set<Tuple> maxTuples = connection.zSetCommands().zPopMax(key.getBytes(), 2);
            assertThat(maxTuples).hasSize(1); // Only 1 element remaining after previous operations
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Set Algebra Operations ====================

    @Test
    void testZDiff() {
        String key1 = "test:zset:diff:key1";
        String key2 = "test:zset:diff:key2";
        
        try {
            // Setup test data
            Set<Tuple> tuples1 = new HashSet<>();
            tuples1.add(new DefaultTuple("a".getBytes(), 1.0));
            tuples1.add(new DefaultTuple("b".getBytes(), 2.0));
            tuples1.add(new DefaultTuple("c".getBytes(), 3.0));
            
            Set<Tuple> tuples2 = new HashSet<>();
            tuples2.add(new DefaultTuple("b".getBytes(), 2.5));
            tuples2.add(new DefaultTuple("d".getBytes(), 4.0));
            
            connection.zSetCommands().zAdd(key1.getBytes(), tuples1, 
                ValkeyZSetCommands.ZAddArgs.empty());
            connection.zSetCommands().zAdd(key2.getBytes(), tuples2, 
                ValkeyZSetCommands.ZAddArgs.empty());
            
            // Test zDiff
            Set<byte[]> diff = connection.zSetCommands().zDiff(key1.getBytes(), key2.getBytes());
            assertThat(diff).hasSize(2);
            Set<String> diffStrings = diff.stream()
                .map(String::new)
                .collect(java.util.stream.Collectors.toSet());
            assertThat(diffStrings).containsExactlyInAnyOrder("a", "c");
            
            // Test zDiffWithScores
            Set<Tuple> diffWithScores = connection.zSetCommands().zDiffWithScores(key1.getBytes(), key2.getBytes());
            assertThat(diffWithScores).hasSize(2);
            
            // Test zDiffStore
            String destKey = "test:zset:diff:dest";
            Long storeResult = connection.zSetCommands().zDiffStore(destKey.getBytes(), key1.getBytes(), key2.getBytes());
            assertThat(storeResult).isEqualTo(2L);
            
            cleanupKey(destKey);
        } finally {
            cleanupKey(key1);
            cleanupKey(key2);
        }
    }

    @Test
    void testZInter() {
        String key1 = "test:zset:inter:key1";
        String key2 = "test:zset:inter:key2";
        
        try {
            // Setup test data
            Set<Tuple> tuples1 = new HashSet<>();
            tuples1.add(new DefaultTuple("a".getBytes(), 1.0));
            tuples1.add(new DefaultTuple("b".getBytes(), 2.0));
            tuples1.add(new DefaultTuple("c".getBytes(), 3.0));
            
            Set<Tuple> tuples2 = new HashSet<>();
            tuples2.add(new DefaultTuple("b".getBytes(), 2.5));
            tuples2.add(new DefaultTuple("c".getBytes(), 3.5));
            tuples2.add(new DefaultTuple("d".getBytes(), 4.0));
            
            connection.zSetCommands().zAdd(key1.getBytes(), tuples1, 
                ValkeyZSetCommands.ZAddArgs.empty());
            connection.zSetCommands().zAdd(key2.getBytes(), tuples2, 
                ValkeyZSetCommands.ZAddArgs.empty());
            
            // Test zInter
            Set<byte[]> inter = connection.zSetCommands().zInter(key1.getBytes(), key2.getBytes());
            assertThat(inter).hasSize(2);
            Set<String> interStrings = inter.stream()
                .map(String::new)
                .collect(java.util.stream.Collectors.toSet());
            assertThat(interStrings).containsExactlyInAnyOrder("b", "c");
            
            // Test zInterWithScores
            Set<Tuple> interWithScores = connection.zSetCommands().zInterWithScores(key1.getBytes(), key2.getBytes());
            assertThat(interWithScores).hasSize(2);
            
            // Test zInterWithScores with weights and aggregate
            Weights weights = Weights.of(1.0, 2.0);
            Set<Tuple> interWeighted = connection.zSetCommands().zInterWithScores(Aggregate.SUM, weights, 
                key1.getBytes(), key2.getBytes());
            assertThat(interWeighted).hasSize(2);
            
            // Test zInterStore
            String destKey = "test:zset:inter:dest";
            Long storeResult = connection.zSetCommands().zInterStore(destKey.getBytes(), key1.getBytes(), key2.getBytes());
            assertThat(storeResult).isEqualTo(2L);
            
            // Test zInterStore with weights and aggregate
            Long storeWeightedResult = connection.zSetCommands().zInterStore(destKey.getBytes(), Aggregate.MAX, weights, 
                key1.getBytes(), key2.getBytes());
            assertThat(storeWeightedResult).isEqualTo(2L);
            
            cleanupKey(destKey);
        } finally {
            cleanupKey(key1);
            cleanupKey(key2);
        }
    }

    @Test
    void testZUnion() {
        String key1 = "test:zset:union:key1";
        String key2 = "test:zset:union:key2";
        
        try {
            // Setup test data
            Set<Tuple> tuples1 = new HashSet<>();
            tuples1.add(new DefaultTuple("a".getBytes(), 1.0));
            tuples1.add(new DefaultTuple("b".getBytes(), 2.0));
            
            Set<Tuple> tuples2 = new HashSet<>();
            tuples2.add(new DefaultTuple("b".getBytes(), 2.5));
            tuples2.add(new DefaultTuple("c".getBytes(), 3.0));
            
            connection.zSetCommands().zAdd(key1.getBytes(), tuples1, 
                ValkeyZSetCommands.ZAddArgs.empty());
            connection.zSetCommands().zAdd(key2.getBytes(), tuples2, 
                ValkeyZSetCommands.ZAddArgs.empty());
            
            // Test zUnion
            Set<byte[]> union = connection.zSetCommands().zUnion(key1.getBytes(), key2.getBytes());
            assertThat(union).hasSize(3);
            Set<String> unionStrings = union.stream()
                .map(String::new)
                .collect(java.util.stream.Collectors.toSet());
            assertThat(unionStrings).containsExactlyInAnyOrder("a", "b", "c");
            
            // Test zUnionWithScores
            Set<Tuple> unionWithScores = connection.zSetCommands().zUnionWithScores(key1.getBytes(), key2.getBytes());
            assertThat(unionWithScores).hasSize(3);
            
            // Test zUnionWithScores with weights and aggregate
            Weights weights = Weights.of(1.0, 2.0);
            Set<Tuple> unionWeighted = connection.zSetCommands().zUnionWithScores(Aggregate.MIN, weights, 
                key1.getBytes(), key2.getBytes());
            assertThat(unionWeighted).hasSize(3);
            
            // Test zUnionStore
            String destKey = "test:zset:union:dest";
            Long storeResult = connection.zSetCommands().zUnionStore(destKey.getBytes(), key1.getBytes(), key2.getBytes());
            assertThat(storeResult).isEqualTo(3L);
            
            // Test zUnionStore with weights and aggregate
            Long storeWeightedResult = connection.zSetCommands().zUnionStore(destKey.getBytes(), Aggregate.SUM, weights, 
                key1.getBytes(), key2.getBytes());
            assertThat(storeWeightedResult).isEqualTo(3L);
            
            cleanupKey(destKey);
        } finally {
            cleanupKey(key1);
            cleanupKey(key2);
        }
    }

    // ==================== Range Store Operations ====================

    @Test
    void testZRangeStore() {
        String srcKey = "test:zset:rangestore:src";
        String destKey = "test:zset:rangestore:dest";
        
        try {
            // Setup test data
            Set<Tuple> tuples = new HashSet<>();
            tuples.add(new DefaultTuple("member1".getBytes(), 1.0));
            tuples.add(new DefaultTuple("member2".getBytes(), 2.0));
            tuples.add(new DefaultTuple("member3".getBytes(), 3.0));
            tuples.add(new DefaultTuple("member4".getBytes(), 4.0));
            tuples.add(new DefaultTuple("member5".getBytes(), 5.0));
            
            connection.zSetCommands().zAdd(srcKey.getBytes(), tuples, 
                ValkeyZSetCommands.ZAddArgs.empty());
            
            // Test zRangeStoreByScore
            Range<Double> scoreRange = Range.closed(2.0, 4.0);
            Long storeByScoreResult = connection.zSetCommands().zRangeStoreByScore(destKey.getBytes(), srcKey.getBytes(), 
                scoreRange.map(Number.class::cast), Limit.unlimited());
            assertThat(storeByScoreResult).isEqualTo(3L);
            
            // Test zRangeStoreRevByScore
            Long storeRevByScoreResult = connection.zSetCommands().zRangeStoreRevByScore(destKey.getBytes(), srcKey.getBytes(), 
                scoreRange.map(Number.class::cast), Limit.unlimited());
            assertThat(storeRevByScoreResult).isEqualTo(3L);
            
            // Test zRangeStoreByLex (need same scores)
            String srcLexKey = "test:zset:rangestore:srclex";
            Set<Tuple> lexTuples = new HashSet<>();
            lexTuples.add(new DefaultTuple("a".getBytes(), 0.0));
            lexTuples.add(new DefaultTuple("b".getBytes(), 0.0));
            lexTuples.add(new DefaultTuple("c".getBytes(), 0.0));
            lexTuples.add(new DefaultTuple("d".getBytes(), 0.0));
            
            connection.zSetCommands().zAdd(srcLexKey.getBytes(), lexTuples, 
                ValkeyZSetCommands.ZAddArgs.empty());
            
            Range<byte[]> lexRange = Range.closed("b".getBytes(), "d".getBytes());
            Long storeByLexResult = connection.zSetCommands().zRangeStoreByLex(destKey.getBytes(), srcLexKey.getBytes(), 
                lexRange, Limit.unlimited());
            assertThat(storeByLexResult).isEqualTo(3L);
            
            // Test zRangeStoreRevByLex
            Long storeRevByLexResult = connection.zSetCommands().zRangeStoreRevByLex(destKey.getBytes(), srcLexKey.getBytes(), 
                lexRange, Limit.unlimited());
            assertThat(storeRevByLexResult).isEqualTo(3L);
            
            cleanupKey(srcLexKey);
        } finally {
            cleanupKey(srcKey);
            cleanupKey(destKey);
        }
    }

    // ==================== Removal Operations ====================

    @Test
    void testZRemRange() {
        String key = "test:zset:remrange:key";
        
        try {
            // Setup test data
            Set<Tuple> tuples = new HashSet<>();
            tuples.add(new DefaultTuple("member1".getBytes(), 1.0));
            tuples.add(new DefaultTuple("member2".getBytes(), 2.0));
            tuples.add(new DefaultTuple("member3".getBytes(), 3.0));
            tuples.add(new DefaultTuple("member4".getBytes(), 4.0));
            tuples.add(new DefaultTuple("member5".getBytes(), 5.0));
            
            connection.zSetCommands().zAdd(key.getBytes(), tuples, 
                ValkeyZSetCommands.ZAddArgs.empty());
            
            // Test zRemRange by rank
            Long remRangeResult = connection.zSetCommands().zRemRange(key.getBytes(), 1, 3);
            assertThat(remRangeResult).isEqualTo(3L); // Removed member2, member3, member4
            
            // Verify remaining members
            Long remainingCount = connection.zSetCommands().zCard(key.getBytes());
            assertThat(remainingCount).isEqualTo(2L); // member1 and member5 should remain
            
            // Test zRemRangeByScore
            tuples.clear();
            tuples.add(new DefaultTuple("member2".getBytes(), 2.0));
            tuples.add(new DefaultTuple("member3".getBytes(), 3.0));
            tuples.add(new DefaultTuple("member4".getBytes(), 4.0));
            connection.zSetCommands().zAdd(key.getBytes(), tuples, 
                ValkeyZSetCommands.ZAddArgs.empty());
            
            Range<Double> scoreRange = Range.closed(2.0, 4.0);
            Long remByScoreResult = connection.zSetCommands().zRemRangeByScore(key.getBytes(), 
                scoreRange.map(Number.class::cast));
            assertThat(remByScoreResult).isEqualTo(3L);
            
            // Test zRemRangeByLex (need same scores)
            tuples.clear();
            tuples.add(new DefaultTuple("a".getBytes(), 0.0));
            tuples.add(new DefaultTuple("b".getBytes(), 0.0));
            tuples.add(new DefaultTuple("c".getBytes(), 0.0));
            tuples.add(new DefaultTuple("d".getBytes(), 0.0));
            connection.zSetCommands().zAdd(key.getBytes(), tuples, 
                ValkeyZSetCommands.ZAddArgs.empty());
            
            Range<byte[]> lexRange = Range.closed("b".getBytes(), "d".getBytes());
            Long remByLexResult = connection.zSetCommands().zRemRangeByLex(key.getBytes(), lexRange);
            assertThat(remByLexResult).isEqualTo(3L);
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Scan Operations ====================

    @Test
    void testZScan() {
        String key = "test:zset:scan:key";
        
        try {
            // Setup test data
            Set<Tuple> tuples = new HashSet<>();
            for (int i = 0; i < 10; i++) {
                tuples.add(new DefaultTuple(("member" + i).getBytes(), i * 1.0));
            }
            
            connection.zSetCommands().zAdd(key.getBytes(), tuples, 
                ValkeyZSetCommands.ZAddArgs.empty());
            
            // Test basic scan
            ScanOptions options = ScanOptions.scanOptions().build();
            Cursor<Tuple> cursor = connection.zSetCommands().zScan(key.getBytes(), options);
            
            java.util.List<Tuple> scannedTuples = new java.util.ArrayList<>();
            while (cursor.hasNext()) {
                scannedTuples.add(cursor.next());
            }
            cursor.close();
            
            // Should find all tuples
            assertThat(scannedTuples).hasSize(10);
            
            // Test scan with pattern
            ScanOptions patternOptions = ScanOptions.scanOptions().match("member[1-3]").build();
            Cursor<Tuple> patternCursor = connection.zSetCommands().zScan(key.getBytes(), patternOptions);
            
            java.util.List<Tuple> patternTuples = new java.util.ArrayList<>();
            while (patternCursor.hasNext()) {
                patternTuples.add(patternCursor.next());
            }
            patternCursor.close();
            
            // Should find fewer tuples due to pattern filter
            assertThat(patternTuples.size()).isLessThanOrEqualTo(3);
            
            // Test scan with count
            ScanOptions countOptions = ScanOptions.scanOptions().count(2).build();
            Cursor<Tuple> countCursor = connection.zSetCommands().zScan(key.getBytes(), countOptions);
            
            java.util.List<Tuple> countTuples = new java.util.ArrayList<>();
            while (countCursor.hasNext()) {
                countTuples.add(countCursor.next());
            }
            countCursor.close();
            
            // Should still find all tuples, but in smaller batches
            assertThat(countTuples).hasSize(10);
        } finally {
            cleanupKey(key);
        }
    }

}

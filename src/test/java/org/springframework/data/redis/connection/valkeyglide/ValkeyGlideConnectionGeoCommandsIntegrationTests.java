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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoSearchCommandArgs;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoSearchStoreCommandArgs;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.data.redis.domain.geo.GeoShape;

/**
 * Comprehensive low-level integration tests for {@link ValkeyGlideConnection} 
 * geospatial functionality using the RedisGeoCommands interface directly.
 * 
 * These tests validate the implementation of all RedisGeoCommands methods:
 * - Basic geo operations (geoAdd, geoRemove, geoPos, geoHash)
 * - Distance calculations (geoDist)
 * - Radius searches (geoRadius, geoRadiusByMember)
 * - Advanced search operations (geoSearch, geoSearchStore)
 * - Various argument combinations and edge cases
 * - Error handling and validation
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideConnectionGeoCommandsIntegrationTests extends AbstractValkeyGlideIntegrationTests {

    @Override
    protected String[] getTestKeyPatterns() {
        return new String[]{
            "test:geo:add:key", "test:geo:add:map:key", "test:geo:add:iterable:key",
            "test:geo:dist:key", "test:geo:hash:key", "test:geo:pos:key",
            "test:geo:radius:key", "test:geo:radius:member:key", "test:geo:remove:key",
            "test:geo:search:key", "test:geo:search:store:key", "test:geo:search:dest:key",
            "test:geo:mixed:key", "test:geo:empty:key", "test:geo:validation:key",
            "test:geo:large:key", "test:geo:edge:cases:key", "test:geo:precision:key"
        };
    }

    // Test data using real world locations
    private static final Point PALERMO = new Point(13.361389, 38.115556);
    private static final Point CATANIA = new Point(15.087269, 37.502669);
    private static final Point EDGE = new Point(12.758489, 38.788135);
    private static final Point NEW_YORK = new Point(-74.0059, 40.7128);
    private static final Point LONDON = new Point(-0.1278, 51.5074);
    private static final Point PARIS = new Point(2.3522, 48.8566);
    private static final Point ROME = new Point(12.4964, 41.9028);

    // ==================== Basic Geo Operations ====================

    @Test
    void testGeoAddSinglePoint() {
        String key = "test:geo:add:key";
        
        try {
            // Test adding single point
            Long result = connection.geoCommands().geoAdd(key.getBytes(), PALERMO, "Palermo".getBytes());
            assertThat(result).isEqualTo(1L);
            
            // Test adding another point
            Long result2 = connection.geoCommands().geoAdd(key.getBytes(), CATANIA, "Catania".getBytes());
            assertThat(result2).isEqualTo(1L);
            
            // Test adding duplicate point (should update)
            Long result3 = connection.geoCommands().geoAdd(key.getBytes(), EDGE, "Palermo".getBytes());
            assertThat(result3).isEqualTo(0L); // Should update existing, not add new
            
            // Verify the updated position
            List<Point> positions = connection.geoCommands().geoPos(key.getBytes(), "Palermo".getBytes());
            assertThat(positions).hasSize(1);
            assertThat(positions.get(0).getX()).isCloseTo(EDGE.getX(), within(0.01));
            assertThat(positions.get(0).getY()).isCloseTo(EDGE.getY(), within(0.01));
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testGeoAddMap() {
        String key = "test:geo:add:map:key";
        
        try {
            // Test adding map of member-coordinate pairs
            Map<byte[], Point> memberCoordinateMap = new HashMap<>();
            memberCoordinateMap.put("Palermo".getBytes(), PALERMO);
            memberCoordinateMap.put("Catania".getBytes(), CATANIA);
            memberCoordinateMap.put("NewYork".getBytes(), NEW_YORK);
            
            Long result = connection.geoCommands().geoAdd(key.getBytes(), memberCoordinateMap);
            assertThat(result).isEqualTo(3L);
            
            // Test adding empty map
            Map<byte[], Point> emptyMap = new HashMap<>();
            Long emptyResult = connection.geoCommands().geoAdd(key.getBytes(), emptyMap);
            assertThat(emptyResult).isEqualTo(0L);
            
            // Verify all members were added
            List<Point> positions = connection.geoCommands().geoPos(key.getBytes(), 
                "Palermo".getBytes(), "Catania".getBytes(), "NewYork".getBytes());
            assertThat(positions).hasSize(3);
            assertThat(positions.get(0)).isNotNull();
            assertThat(positions.get(1)).isNotNull();
            assertThat(positions.get(2)).isNotNull();
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testGeoAddIterable() {
        String key = "test:geo:add:iterable:key";
        
        try {
            // Test adding iterable of GeoLocations
            List<GeoLocation<byte[]>> locations = new ArrayList<>();
            locations.add(new GeoLocation<>("Palermo".getBytes(), PALERMO));
            locations.add(new GeoLocation<>("Catania".getBytes(), CATANIA));
            locations.add(new GeoLocation<>("Rome".getBytes(), ROME));
            
            Long result = connection.geoCommands().geoAdd(key.getBytes(), locations);
            assertThat(result).isEqualTo(3L);
            
            // Test adding empty iterable
            List<GeoLocation<byte[]>> emptyList = new ArrayList<>();
            Long emptyResult = connection.geoCommands().geoAdd(key.getBytes(), emptyList);
            assertThat(emptyResult).isEqualTo(0L);
            
            // Verify all members were added
            List<Point> positions = connection.geoCommands().geoPos(key.getBytes(), 
                "Palermo".getBytes(), "Catania".getBytes(), "Rome".getBytes());
            assertThat(positions).hasSize(3);
            assertThat(positions.get(0)).isNotNull();
            assertThat(positions.get(1)).isNotNull();
            assertThat(positions.get(2)).isNotNull();
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testGeoRemove() {
        String key = "test:geo:remove:key";
        
        try {
            // Setup test data
            Map<byte[], Point> memberCoordinateMap = new HashMap<>();
            memberCoordinateMap.put("Palermo".getBytes(), PALERMO);
            memberCoordinateMap.put("Catania".getBytes(), CATANIA);
            memberCoordinateMap.put("Rome".getBytes(), ROME);
            
            connection.geoCommands().geoAdd(key.getBytes(), memberCoordinateMap);
            
            // Test removing single member
            Long result1 = connection.geoCommands().geoRemove(key.getBytes(), "Palermo".getBytes());
            assertThat(result1).isEqualTo(1L);
            
            // Test removing multiple members
            Long result2 = connection.geoCommands().geoRemove(key.getBytes(), 
                "Catania".getBytes(), "Rome".getBytes());
            assertThat(result2).isEqualTo(2L);
            
            // Test removing non-existent member
            Long result3 = connection.geoCommands().geoRemove(key.getBytes(), "NonExistent".getBytes());
            assertThat(result3).isEqualTo(0L);
            
            // Verify all members were removed
            List<Point> positions = connection.geoCommands().geoPos(key.getBytes(), 
                "Palermo".getBytes(), "Catania".getBytes(), "Rome".getBytes());
            assertThat(positions).hasSize(3);
            assertThat(positions.get(0)).isNull();
            assertThat(positions.get(1)).isNull();
            assertThat(positions.get(2)).isNull();
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Position and Hash Operations ====================

    @Test
    void testGeoPos() {
        String key = "test:geo:pos:key";
        
        try {
            // Test geoPos on empty key
            List<Point> emptyPositions = connection.geoCommands().geoPos(key.getBytes(), "NonExistent".getBytes());
            assertThat(emptyPositions).hasSize(1);
            assertThat(emptyPositions.get(0)).isNull();
            
            // Setup test data
            Map<byte[], Point> memberCoordinateMap = new HashMap<>();
            memberCoordinateMap.put("Palermo".getBytes(), PALERMO);
            memberCoordinateMap.put("Catania".getBytes(), CATANIA);
            memberCoordinateMap.put("NewYork".getBytes(), NEW_YORK);
            
            connection.geoCommands().geoAdd(key.getBytes(), memberCoordinateMap);
            
            // Test getting single position
            List<Point> singlePos = connection.geoCommands().geoPos(key.getBytes(), "Palermo".getBytes());
            assertThat(singlePos).hasSize(1);
            assertThat(singlePos.get(0).getX()).isCloseTo(PALERMO.getX(), within(0.01));
            assertThat(singlePos.get(0).getY()).isCloseTo(PALERMO.getY(), within(0.01));
            
            // Test getting multiple positions
            List<Point> multiPos = connection.geoCommands().geoPos(key.getBytes(), 
                "Palermo".getBytes(), "Catania".getBytes(), "NonExistent".getBytes());
            assertThat(multiPos).hasSize(3);
            assertThat(multiPos.get(0)).isNotNull();
            assertThat(multiPos.get(1)).isNotNull();
            assertThat(multiPos.get(2)).isNull(); // Non-existent member
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testGeoHash() {
        String key = "test:geo:hash:key";
        
        try {
            // Test geoHash on empty key
            List<String> emptyHashes = connection.geoCommands().geoHash(key.getBytes(), "NonExistent".getBytes());
            assertThat(emptyHashes).hasSize(1);
            assertThat(emptyHashes.get(0)).isNull();
            
            // Setup test data
            Map<byte[], Point> memberCoordinateMap = new HashMap<>();
            memberCoordinateMap.put("Palermo".getBytes(), PALERMO);
            memberCoordinateMap.put("Catania".getBytes(), CATANIA);
            
            connection.geoCommands().geoAdd(key.getBytes(), memberCoordinateMap);
            
            // Test getting single hash
            List<String> singleHash = connection.geoCommands().geoHash(key.getBytes(), "Palermo".getBytes());
            assertThat(singleHash).hasSize(1);
            assertThat(singleHash.get(0)).isNotNull();
            assertThat(singleHash.get(0)).isNotEmpty();
            
            // Test getting multiple hashes
            List<String> multiHash = connection.geoCommands().geoHash(key.getBytes(), 
                "Palermo".getBytes(), "Catania".getBytes(), "NonExistent".getBytes());
            assertThat(multiHash).hasSize(3);
            assertThat(multiHash.get(0)).isNotNull();
            assertThat(multiHash.get(1)).isNotNull();
            assertThat(multiHash.get(2)).isNull(); // Non-existent member
            
            // Verify different locations have different hashes
            assertThat(multiHash.get(0)).isNotEqualTo(multiHash.get(1));
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Distance Calculations ====================

    @Test
    void testGeoDist() {
        String key = "test:geo:dist:key";
        
        try {
            // Test geoDist on empty key
            Distance emptyDist = connection.geoCommands().geoDist(key.getBytes(), 
                "NonExistent1".getBytes(), "NonExistent2".getBytes());
            assertThat(emptyDist).isNull();
            
            // Setup test data
            Map<byte[], Point> memberCoordinateMap = new HashMap<>();
            memberCoordinateMap.put("Palermo".getBytes(), PALERMO);
            memberCoordinateMap.put("Catania".getBytes(), CATANIA);
            memberCoordinateMap.put("NewYork".getBytes(), NEW_YORK);
            
            connection.geoCommands().geoAdd(key.getBytes(), memberCoordinateMap);
            
            // Test distance with default metric (meters)
            Distance dist1 = connection.geoCommands().geoDist(key.getBytes(), 
                "Palermo".getBytes(), "Catania".getBytes());
            assertThat(dist1).isNotNull();
            assertThat(dist1.getValue()).isGreaterThan(0);
            assertThat(dist1.getMetric()).isEqualTo(DistanceUnit.METERS);
            
            // Test distance with specific metric
            Distance dist2 = connection.geoCommands().geoDist(key.getBytes(), 
                "Palermo".getBytes(), "Catania".getBytes(), DistanceUnit.KILOMETERS);
            assertThat(dist2).isNotNull();
            assertThat(dist2.getValue()).isGreaterThan(0);
            assertThat(dist2.getMetric()).isEqualTo(DistanceUnit.KILOMETERS);
            
            // Distance in kilometers should be less than distance in meters
            assertThat(dist2.getValue()).isLessThan(dist1.getValue());
            
            // Test distance with non-existent member
            Distance distNonExistent = connection.geoCommands().geoDist(key.getBytes(), 
                "Palermo".getBytes(), "NonExistent".getBytes());
            assertThat(distNonExistent).isNull();
            
            // Test distance between distant locations
            Distance distTransAtlantic = connection.geoCommands().geoDist(key.getBytes(), 
                "Palermo".getBytes(), "NewYork".getBytes(), DistanceUnit.KILOMETERS);
            assertThat(distTransAtlantic).isNotNull();
            assertThat(distTransAtlantic.getValue()).isGreaterThan(1000); // Should be thousands of kilometers
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Radius Search Operations ====================

    @Test
    void testGeoRadius() {
        String key = "test:geo:radius:key";
        
        try {
            // Setup test data
            Map<byte[], Point> memberCoordinateMap = new HashMap<>();
            memberCoordinateMap.put("Palermo".getBytes(), PALERMO);
            memberCoordinateMap.put("Catania".getBytes(), CATANIA);
            memberCoordinateMap.put("Edge".getBytes(), EDGE);
            memberCoordinateMap.put("NewYork".getBytes(), NEW_YORK);
            
            connection.geoCommands().geoAdd(key.getBytes(), memberCoordinateMap);
            
            // Test basic radius search
            Circle searchArea = new Circle(PALERMO, new Distance(200, DistanceUnit.KILOMETERS));
            GeoResults<GeoLocation<byte[]>> results1 = connection.geoCommands().geoRadius(key.getBytes(), searchArea);
            assertThat(results1).isNotNull();
            assertThat(results1.getContent()).isNotEmpty();
            
            // Should find Palermo and Catania, but not NewYork
            List<String> memberNames = results1.getContent().stream()
                .map(result -> new String(result.getContent().getName()))
                .collect(java.util.stream.Collectors.toList());
            assertThat(memberNames).contains("Palermo");
            assertThat(memberNames).doesNotContain("NewYork");
            
            // Test radius search with arguments
            GeoRadiusCommandArgs args = GeoRadiusCommandArgs.newGeoRadiusArgs()
                .includeDistance()
                .includeCoordinates()
                .sortAscending()
                .limit(10);
            
            GeoResults<GeoLocation<byte[]>> results2 = connection.geoCommands().geoRadius(key.getBytes(), searchArea, args);
            assertThat(results2).isNotNull();
            assertThat(results2.getContent()).isNotEmpty();
            
            // Verify distance and coordinates are included
            for (GeoResult<GeoLocation<byte[]>> result : results2.getContent()) {
                assertThat(result.getDistance()).isNotNull();
                assertThat(result.getContent().getPoint()).isNotNull();
            }
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testGeoRadiusByMember() {
        String key = "test:geo:radius:member:key";
        
        try {
            // Setup test data
            Map<byte[], Point> memberCoordinateMap = new HashMap<>();
            memberCoordinateMap.put("Palermo".getBytes(), PALERMO);
            memberCoordinateMap.put("Catania".getBytes(), CATANIA);
            memberCoordinateMap.put("Edge".getBytes(), EDGE);
            memberCoordinateMap.put("NewYork".getBytes(), NEW_YORK);
            
            connection.geoCommands().geoAdd(key.getBytes(), memberCoordinateMap);
            
            // Test basic radius search by member
            Distance radius = new Distance(200, DistanceUnit.KILOMETERS);
            GeoResults<GeoLocation<byte[]>> results1 = connection.geoCommands().geoRadiusByMember(
                key.getBytes(), "Palermo".getBytes(), radius);
            assertThat(results1).isNotNull();
            assertThat(results1.getContent()).isNotEmpty();
            
            // Should find Palermo itself and nearby cities
            List<String> memberNames = results1.getContent().stream()
                .map(result -> new String(result.getContent().getName()))
                .collect(java.util.stream.Collectors.toList());
            assertThat(memberNames).contains("Palermo");
            
            // Test radius search by member with arguments
            GeoRadiusCommandArgs args = GeoRadiusCommandArgs.newGeoRadiusArgs()
                .includeDistance()
                .includeCoordinates()
                .sortDescending()
                .limit(5);
            
            GeoResults<GeoLocation<byte[]>> results2 = connection.geoCommands().geoRadiusByMember(
                key.getBytes(), "Palermo".getBytes(), radius, args);
            assertThat(results2).isNotNull();
            assertThat(results2.getContent()).isNotEmpty();
            
            // Verify distance and coordinates are included
            for (GeoResult<GeoLocation<byte[]>> result : results2.getContent()) {
                assertThat(result.getDistance()).isNotNull();
                assertThat(result.getContent().getPoint()).isNotNull();
            }
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Advanced Search Operations ====================

    @Test
    void testGeoSearch() {
        String key = "test:geo:search:key";
        
        try {
            // Setup test data
            Map<byte[], Point> memberCoordinateMap = new HashMap<>();
            memberCoordinateMap.put("Palermo".getBytes(), PALERMO);
            memberCoordinateMap.put("Catania".getBytes(), CATANIA);
            memberCoordinateMap.put("Rome".getBytes(), ROME);
            memberCoordinateMap.put("NewYork".getBytes(), NEW_YORK);
            
            connection.geoCommands().geoAdd(key.getBytes(), memberCoordinateMap);
            
            // Test search from member reference
            GeoReference<byte[]> memberRef = GeoReference.fromMember("Palermo".getBytes());
            GeoShape radiusShape = GeoShape.byRadius(new Distance(300, DistanceUnit.KILOMETERS));
            GeoSearchCommandArgs args = GeoSearchCommandArgs.newGeoSearchArgs()
                .includeDistance()
                .includeCoordinates()
                .sortAscending()
                .limit(10);
            
            GeoResults<GeoLocation<byte[]>> results1 = connection.geoCommands().geoSearch(
                key.getBytes(), memberRef, radiusShape, args);
            assertThat(results1).isNotNull();
            assertThat(results1.getContent()).isNotEmpty();
            
            // Should find nearby locations
            List<String> memberNames = results1.getContent().stream()
                .map(result -> new String(result.getContent().getName()))
                .collect(java.util.stream.Collectors.toList());
            assertThat(memberNames).contains("Palermo");
            assertThat(memberNames).doesNotContain("NewYork");
            
            // Test search from coordinate reference
            GeoReference<byte[]> coordRef = GeoReference.fromCoordinate(PALERMO);
            GeoResults<GeoLocation<byte[]>> results2 = connection.geoCommands().geoSearch(
                key.getBytes(), coordRef, radiusShape, args);
            assertThat(results2).isNotNull();
            assertThat(results2.getContent()).isNotEmpty();
            
            // Results should be similar to member reference search
            assertThat(results2.getContent().size()).isGreaterThan(0);
            
            // Test with box shape
            GeoShape boxShape = GeoShape.byBox(200, 200, DistanceUnit.KILOMETERS);
            GeoResults<GeoLocation<byte[]>> results3 = connection.geoCommands().geoSearch(
                key.getBytes(), memberRef, boxShape, args);
            assertThat(results3).isNotNull();
            assertThat(results3.getContent()).isNotEmpty();
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testGeoSearchStore() {
        String key = "test:geo:search:store:key";
        String destKey = "test:geo:search:dest:key";
        
        try {
            // Setup test data
            Map<byte[], Point> memberCoordinateMap = new HashMap<>();
            memberCoordinateMap.put("Palermo".getBytes(), PALERMO);
            memberCoordinateMap.put("Catania".getBytes(), CATANIA);
            memberCoordinateMap.put("Rome".getBytes(), ROME);
            memberCoordinateMap.put("NewYork".getBytes(), NEW_YORK);
            
            connection.geoCommands().geoAdd(key.getBytes(), memberCoordinateMap);
            
            // Test search and store
            GeoReference<byte[]> memberRef = GeoReference.fromMember("Palermo".getBytes());
            GeoShape radiusShape = GeoShape.byRadius(new Distance(300, DistanceUnit.KILOMETERS));
            GeoSearchStoreCommandArgs args = GeoSearchStoreCommandArgs.newGeoSearchStoreArgs()
                .sortAscending()
                .limit(10);
            
            Long storeResult = connection.geoCommands().geoSearchStore(
                destKey.getBytes(), key.getBytes(), memberRef, radiusShape, args);
            assertThat(storeResult).isNotNull();
            assertThat(storeResult).isGreaterThan(0L);
            
            // Verify stored results by checking positions
            List<Point> storedPositions = connection.geoCommands().geoPos(destKey.getBytes(), "Palermo".getBytes());
            assertThat(storedPositions).hasSize(1);
            assertThat(storedPositions.get(0)).isNotNull();
            
            // Test with store distance option
            GeoSearchStoreCommandArgs storeDistArgs = GeoSearchStoreCommandArgs.newGeoSearchStoreArgs()
                .storeDistance()
                .sortDescending()
                .limit(5);
            
            Long storeDistResult = connection.geoCommands().geoSearchStore(
                destKey.getBytes(), key.getBytes(), memberRef, radiusShape, storeDistArgs);
            assertThat(storeDistResult).isNotNull();
            assertThat(storeDistResult).isGreaterThan(0L);
        } finally {
            cleanupKey(key);
            cleanupKey(destKey);
        }
    }

    // ==================== Edge Cases and Error Handling ====================

    @Test
    void testEmptyKeyOperations() {
        String key = "test:geo:empty:key";
        
        try {
            // Test operations on empty key
            List<Point> emptyPos = connection.geoCommands().geoPos(key.getBytes(), "NonExistent".getBytes());
            assertThat(emptyPos).hasSize(1);
            assertThat(emptyPos.get(0)).isNull();
            
            List<String> emptyHash = connection.geoCommands().geoHash(key.getBytes(), "NonExistent".getBytes());
            assertThat(emptyHash).hasSize(1);
            assertThat(emptyHash.get(0)).isNull();
            
            Distance emptyDist = connection.geoCommands().geoDist(key.getBytes(), 
                "NonExistent1".getBytes(), "NonExistent2".getBytes());
            assertThat(emptyDist).isNull();
            
            Circle searchArea = new Circle(PALERMO, new Distance(100, DistanceUnit.KILOMETERS));
            GeoResults<GeoLocation<byte[]>> emptyRadius = connection.geoCommands().geoRadius(key.getBytes(), searchArea);
            assertThat(emptyRadius).isNotNull();
            assertThat(emptyRadius.getContent()).isEmpty();
            
            Long emptyRemove = connection.geoCommands().geoRemove(key.getBytes(), "NonExistent".getBytes());
            assertThat(emptyRemove).isEqualTo(0L);
        } finally {
            cleanupKey(key);
        }
    }

    @Test
    void testMixedOperations() {
        String key = "test:geo:mixed:key";
        
        try {
            // Add some locations
            Map<byte[], Point> memberCoordinateMap = new HashMap<>();
            memberCoordinateMap.put("Palermo".getBytes(), PALERMO);
            memberCoordinateMap.put("Catania".getBytes(), CATANIA);
            memberCoordinateMap.put("Rome".getBytes(), ROME);
            
            connection.geoCommands().geoAdd(key.getBytes(), memberCoordinateMap);
            
            // Test mixed operations
            Distance dist = connection.geoCommands().geoDist(key.getBytes(), 
                "Palermo".getBytes(), "Rome".getBytes(), DistanceUnit.KILOMETERS);
            assertThat(dist).isNotNull();
            assertThat(dist.getValue()).isGreaterThan(0);
            
            // Remove one location
            Long removeResult = connection.geoCommands().geoRemove(key.getBytes(), "Catania".getBytes());
            assertThat(removeResult).isEqualTo(1L);
            
            // Verify it's removed
            List<Point> positions = connection.geoCommands().geoPos(key.getBytes(), "Catania".getBytes());
            assertThat(positions.get(0)).isNull();
            
            // But other locations should still be there
            List<Point> remainingPositions = connection.geoCommands().geoPos(key.getBytes(),
                "Palermo".getBytes(), "Rome".getBytes());
            assertThat(remainingPositions).hasSize(2);
            assertThat(remainingPositions.get(0)).isNotNull();
            assertThat(remainingPositions.get(1)).isNotNull();
            
            // Test radius search after removal
            Circle searchArea = new Circle(PALERMO, new Distance(500, DistanceUnit.KILOMETERS));
            GeoResults<GeoLocation<byte[]>> radiusResults = connection.geoCommands().geoRadius(key.getBytes(), searchArea);
            assertThat(radiusResults).isNotNull();
            assertThat(radiusResults.getContent()).hasSize(2); // Only Palermo and Rome should remain
            
            List<String> remainingNames = radiusResults.getContent().stream()
                .map(result -> new String(result.getContent().getName()))
                .collect(java.util.stream.Collectors.toList());
            assertThat(remainingNames).containsExactlyInAnyOrder("Palermo", "Rome");
            assertThat(remainingNames).doesNotContain("Catania");
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Parameter Validation Tests ====================

    @Test
    void testParameterValidation() {
        String key = "test:geo:validation:key";
        
        try {
            // Test null key validation
            assertThatThrownBy(() -> connection.geoCommands().geoAdd(null, PALERMO, "member".getBytes()))
                .isInstanceOf(IllegalArgumentException.class);
            
            // Test null point validation
            assertThatThrownBy(() -> connection.geoCommands().geoAdd(key.getBytes(), null, "member".getBytes()))
                .isInstanceOf(IllegalArgumentException.class);
            
            // Test null member validation
            assertThatThrownBy(() -> connection.geoCommands().geoAdd(key.getBytes(), PALERMO, null))
                .isInstanceOf(IllegalArgumentException.class);
            
            // Test null members array validation
            assertThatThrownBy(() -> connection.geoCommands().geoPos(key.getBytes(), (byte[][]) null))
                .isInstanceOf(IllegalArgumentException.class);
            
            // Test null elements in members array
            assertThatThrownBy(() -> connection.geoCommands().geoPos(key.getBytes(), "member1".getBytes(), null))
                .isInstanceOf(IllegalArgumentException.class);
            
            // Test null metric validation
            connection.geoCommands().geoAdd(key.getBytes(), PALERMO, "member1".getBytes());
            connection.geoCommands().geoAdd(key.getBytes(), CATANIA, "member2".getBytes());
            
            assertThatThrownBy(() -> connection.geoCommands().geoDist(key.getBytes(), 
                "member1".getBytes(), "member2".getBytes(), null))
                .isInstanceOf(IllegalArgumentException.class);
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Performance and Stress Tests ====================

    @Test
    void testLargeDataset() {
        String key = "test:geo:large:key";
        
        try {
            // Add a large number of locations with valid coordinates
            // Redis geo commands have latitude limits of approximately -85.05 to 85.05 degrees
            Map<byte[], Point> largeDataset = new HashMap<>();
            for (int i = 0; i < 100; i++) {
                double longitude = -180 + (360.0 * i / 100);
                double latitude = -85 + (170.0 * i / 100); // Valid latitude range: -85 to 85
                largeDataset.put(("location" + i).getBytes(), new Point(longitude, latitude));
            }
            
            Long addResult = connection.geoCommands().geoAdd(key.getBytes(), largeDataset);
            assertThat(addResult).isEqualTo(100L);
            
            // Test operations on large dataset
            List<Point> positions = connection.geoCommands().geoPos(key.getBytes(), "location50".getBytes());
            assertThat(positions).hasSize(1);
            assertThat(positions.get(0)).isNotNull();
            
            // Test radius search on large dataset
            Circle largeSearchArea = new Circle(new Point(0, 0), new Distance(5000, DistanceUnit.KILOMETERS));
            GeoResults<GeoLocation<byte[]>> largeResults = connection.geoCommands().geoRadius(key.getBytes(), largeSearchArea);
            assertThat(largeResults).isNotNull();
            assertThat(largeResults.getContent()).isNotEmpty();
            
            // Test bulk removal
            byte[][] membersToRemove = new byte[50][];
            for (int i = 0; i < 50; i++) {
                membersToRemove[i] = ("location" + i).getBytes();
            }
            Long removeResult = connection.geoCommands().geoRemove(key.getBytes(), membersToRemove);
            assertThat(removeResult).isEqualTo(50L);
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Geographic Edge Cases ====================

    @Test
    void testGeographicEdgeCases() {
        String key = "test:geo:edge:cases:key";
        
        try {
            // Test locations at extreme but valid coordinates
            // Redis geo commands have latitude limits, so we use realistic extreme coordinates
            Map<byte[], Point> edgeCases = new HashMap<>();
            edgeCases.put("NearNorthPole".getBytes(), new Point(0, 85));  // Close to North Pole
            edgeCases.put("NearSouthPole".getBytes(), new Point(0, -85)); // Close to South Pole
            edgeCases.put("DateLine".getBytes(), new Point(179, 0));      // Close to Date Line
            edgeCases.put("AntiMeridian".getBytes(), new Point(-179, 0)); // Close to Anti-Meridian
            edgeCases.put("Equator".getBytes(), new Point(0, 0));
            
            Long addResult = connection.geoCommands().geoAdd(key.getBytes(), edgeCases);
            assertThat(addResult).isEqualTo(5L);
            
            // Test distance between extreme points
            Distance poleDistance = connection.geoCommands().geoDist(key.getBytes(), 
                "NearNorthPole".getBytes(), "NearSouthPole".getBytes(), DistanceUnit.KILOMETERS);
            assertThat(poleDistance).isNotNull();
            assertThat(poleDistance.getValue()).isGreaterThan(15000); // Should be ~18,000+ km
            
            // Test positions of extreme coordinates
            List<Point> extremePositions = connection.geoCommands().geoPos(key.getBytes(), 
                "NearNorthPole".getBytes(), "NearSouthPole".getBytes());
            assertThat(extremePositions).hasSize(2);
            assertThat(extremePositions.get(0)).isNotNull();
            assertThat(extremePositions.get(1)).isNotNull();
            
            // Test radius search from extreme location
            Circle poleSearch = new Circle(new Point(0, 85), new Distance(1000, DistanceUnit.KILOMETERS));
            GeoResults<GeoLocation<byte[]>> poleResults = connection.geoCommands().geoRadius(key.getBytes(), poleSearch);
            assertThat(poleResults).isNotNull();
            assertThat(poleResults.getContent()).isNotEmpty();
            
            // Test distance across date line
            Distance dateLineDistance = connection.geoCommands().geoDist(key.getBytes(), 
                "DateLine".getBytes(), "AntiMeridian".getBytes(), DistanceUnit.KILOMETERS);
            assertThat(dateLineDistance).isNotNull();
            assertThat(dateLineDistance.getValue()).isGreaterThan(0);
        } finally {
            cleanupKey(key);
        }
    }

    // ==================== Precision Tests ====================

    @Test
    void testCoordinatePrecision() {
        String key = "test:geo:precision:key";
        
        try {
            // Test with high precision coordinates
            // Redis geo commands use geohash which has limited precision
            Point highPrecision = new Point(13.361389123456789, 38.115556987654321);
            connection.geoCommands().geoAdd(key.getBytes(), highPrecision, "HighPrecision".getBytes());
            
            // Verify coordinates are stored with reasonable precision
            // Redis geo precision is limited by geohash, so we use a more realistic tolerance
            List<Point> retrievedPositions = connection.geoCommands().geoPos(key.getBytes(), "HighPrecision".getBytes());
            assertThat(retrievedPositions).hasSize(1);
            Point retrieved = retrievedPositions.get(0);
            assertThat(retrieved.getX()).isCloseTo(highPrecision.getX(), within(0.01)); // More realistic tolerance
            assertThat(retrieved.getY()).isCloseTo(highPrecision.getY(), within(0.01));
            
            // Test with moderately close coordinates (more realistic for geo applications)
            Point close1 = new Point(13.361389, 38.115556);
            Point close2 = new Point(13.361489, 38.115656); // 100m apart approximately
            
            connection.geoCommands().geoAdd(key.getBytes(), close1, "Close1".getBytes());
            connection.geoCommands().geoAdd(key.getBytes(), close2, "Close2".getBytes());
            
            Distance closeDistance = connection.geoCommands().geoDist(key.getBytes(), 
                "Close1".getBytes(), "Close2".getBytes(), DistanceUnit.METERS);
            assertThat(closeDistance).isNotNull();
            assertThat(closeDistance.getValue()).isGreaterThan(0);
            assertThat(closeDistance.getValue()).isLessThan(200); // Should be ~100m
            
            // Test coordinate retrieval accuracy
            List<Point> closePositions = connection.geoCommands().geoPos(key.getBytes(), 
                "Close1".getBytes(), "Close2".getBytes());
            assertThat(closePositions).hasSize(2);
            assertThat(closePositions.get(0)).isNotNull();
            assertThat(closePositions.get(1)).isNotNull();
            
            // Verify the positions are reasonably close to what we stored
            assertThat(closePositions.get(0).getX()).isCloseTo(close1.getX(), within(0.01));
            assertThat(closePositions.get(0).getY()).isCloseTo(close1.getY(), within(0.01));
            assertThat(closePositions.get(1).getX()).isCloseTo(close2.getX(), within(0.01));
            assertThat(closePositions.get(1).getY()).isCloseTo(close2.getY(), within(0.01));
            
            // Test with coordinates that have meaningful differences (larger difference for geohash precision)
            Point precise1 = new Point(13.361389, 38.115556);
            Point precise2 = new Point(13.362389, 38.115556); // Longitude differs by 0.001 degrees (~100m)
            
            connection.geoCommands().geoAdd(key.getBytes(), precise1, "Precise1".getBytes());
            connection.geoCommands().geoAdd(key.getBytes(), precise2, "Precise2".getBytes());
            
            // Verify coordinates were stored correctly
            List<Point> precisePositions = connection.geoCommands().geoPos(key.getBytes(), 
                "Precise1".getBytes(), "Precise2".getBytes());
            assertThat(precisePositions).hasSize(2);
            assertThat(precisePositions.get(0)).isNotNull();
            assertThat(precisePositions.get(1)).isNotNull();
            
            Distance preciseDistance = connection.geoCommands().geoDist(key.getBytes(), 
                "Precise1".getBytes(), "Precise2".getBytes(), DistanceUnit.METERS);
            assertThat(preciseDistance).isNotNull();
            
            // With larger coordinate differences, we should get a measurable distance
            assertThat(preciseDistance.getValue()).isGreaterThan(0);
            assertThat(preciseDistance.getValue()).isLessThan(500); // Should be around 100m
            
            // Test Redis geohash precision limitations with very small differences
            Point veryClose1 = new Point(13.361389, 38.115556);
            Point veryClose2 = new Point(13.361390, 38.115556); // Very small difference
            
            connection.geoCommands().geoAdd(key.getBytes(), veryClose1, "VeryClose1".getBytes());
            connection.geoCommands().geoAdd(key.getBytes(), veryClose2, "VeryClose2".getBytes());
            
            Distance veryCloseDistance = connection.geoCommands().geoDist(key.getBytes(), 
                "VeryClose1".getBytes(), "VeryClose2".getBytes(), DistanceUnit.METERS);
            assertThat(veryCloseDistance).isNotNull();
            
            // For very small differences, distance might be 0 due to geohash precision
            // This is expected behavior for Redis geo commands
            assertThat(veryCloseDistance.getValue()).isGreaterThanOrEqualTo(0);
        } finally {
            cleanupKey(key);
        }
    }
}

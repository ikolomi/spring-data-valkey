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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metric;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.domain.geo.BoundingBox;
import org.springframework.data.redis.domain.geo.BoxShape;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.data.redis.domain.geo.GeoShape;
import org.springframework.data.redis.domain.geo.RadiusShape;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Implementation of {@link RedisGeoCommands} for Valkey-Glide.
 *
 * @author Ilya Kolomin
 * @since 2.0
 */
public class ValkeyGlideGeoCommands implements RedisGeoCommands {

    private final ValkeyGlideConnection connection;

    /**
     * Creates a new {@link ValkeyGlideGeoCommands}.
     *
     * @param connection must not be {@literal null}.
     */
    public ValkeyGlideGeoCommands(ValkeyGlideConnection connection) {
        Assert.notNull(connection, "Connection must not be null!");
        this.connection = connection;
    }

    @Override
    @Nullable
    public Long geoAdd(byte[] key, Point point, byte[] member) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(point, "Point must not be null");
        Assert.notNull(member, "Member must not be null");

        try {
            Object result = connection.execute("GEOADD", key, point.getX(), point.getY(), member);
            Object converted = ValkeyGlideConverters.fromGlideResult(result);
            return converted instanceof Number ? ((Number) converted).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long geoAdd(byte[] key, Map<byte[], Point> memberCoordinateMap) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(memberCoordinateMap, "Member coordinate map must not be null");

        if (memberCoordinateMap.isEmpty()) {
            return 0L;
        }

        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(key);
            
            for (Map.Entry<byte[], Point> entry : memberCoordinateMap.entrySet()) {
                Point point = entry.getValue();
                commandArgs.add(point.getX());
                commandArgs.add(point.getY());
                commandArgs.add(entry.getKey());
            }
            
            Object result = connection.execute("GEOADD", commandArgs.toArray());
            Object converted = ValkeyGlideConverters.fromGlideResult(result);
            return converted instanceof Number ? ((Number) converted).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long geoAdd(byte[] key, Iterable<GeoLocation<byte[]>> locations) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(locations, "Locations must not be null");

        List<Object> commandArgs = new ArrayList<>();
        commandArgs.add(key);
        
        boolean hasLocations = false;
        for (GeoLocation<byte[]> location : locations) {
            hasLocations = true;
            Point point = location.getPoint();
            commandArgs.add(point.getX());
            commandArgs.add(point.getY());
            commandArgs.add(location.getName());
        }
        
        if (!hasLocations) {
            return 0L;
        }

        try {
            Object result = connection.execute("GEOADD", commandArgs.toArray());
            Object converted = ValkeyGlideConverters.fromGlideResult(result);
            return converted instanceof Number ? ((Number) converted).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Distance geoDist(byte[] key, byte[] member1, byte[] member2) {
        return geoDist(key, member1, member2, DistanceUnit.METERS);
    }

    @Override
    @Nullable
    public Distance geoDist(byte[] key, byte[] member1, byte[] member2, Metric metric) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(member1, "Member1 must not be null");
        Assert.notNull(member2, "Member2 must not be null");
        Assert.notNull(metric, "Metric must not be null");

        try {
            Object result = connection.execute("GEODIST", key, member1, member2, metric.getAbbreviation());
            Object converted = ValkeyGlideConverters.fromGlideResult(result);
            if (converted == null) {
                return null;
            }
            
            double distance = parseDouble(converted);
            return new Distance(distance, metric);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public List<String> geoHash(byte[] key, byte[]... members) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(members, "Members must not be null");
        Assert.noNullElements(members, "Members must not contain null elements");

        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(key);
            for (byte[] member : members) {
                commandArgs.add(member);
            }
            
            Object result = connection.execute("GEOHASH", commandArgs.toArray());
            Object converted = ValkeyGlideConverters.fromGlideResult(result);
            if (converted == null) {
                return new ArrayList<>();
            }
            
            List<Object> list = convertToList(converted);
            List<String> hashList = new ArrayList<>(list.size());
            for (Object item : list) {
                Object convertedItem = ValkeyGlideConverters.fromGlideResult(item);
                if (convertedItem == null) {
                    hashList.add(null);
                } else if (convertedItem instanceof String) {
                    hashList.add((String) convertedItem);
                } else if (convertedItem instanceof byte[]) {
                    hashList.add(new String((byte[]) convertedItem));
                } else {
                    hashList.add(convertedItem.toString());
                }
            }
            return hashList;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public List<Point> geoPos(byte[] key, byte[]... members) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(members, "Members must not be null");
        Assert.noNullElements(members, "Members must not contain null elements");

        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(key);
            for (byte[] member : members) {
                commandArgs.add(member);
            }
            
            Object result = connection.execute("GEOPOS", commandArgs.toArray());
            Object converted = ValkeyGlideConverters.fromGlideResult(result);
            if (converted == null) {
                return new ArrayList<>();
            }
            
            List<Object> list = convertToList(converted);
            List<Point> pointList = new ArrayList<>(list.size());
            for (Object item : list) {
                Object convertedItem = ValkeyGlideConverters.fromGlideResult(item);
                if (convertedItem == null) {
                    pointList.add(null);
                } else {
                    Point point = convertToPoint(convertedItem);
                    pointList.add(point);
                }
            }
            return pointList;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within) {
        return geoRadius(key, within, GeoRadiusCommandArgs.newGeoRadiusArgs());
    }

    @Override
    @Nullable
    public GeoResults<GeoLocation<byte[]>> geoRadius(byte[] key, Circle within, GeoRadiusCommandArgs args) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(within, "Circle must not be null");
        Assert.notNull(args, "GeoRadiusCommandArgs must not be null");

        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(key);
            commandArgs.add(within.getCenter().getX());
            commandArgs.add(within.getCenter().getY());
            commandArgs.add(within.getRadius().getValue());
            commandArgs.add(within.getRadius().getMetric().getAbbreviation());
            
            appendGeoRadiusArgs(commandArgs, args);
            
            Object result = connection.execute("GEORADIUS", commandArgs.toArray());
            return parseGeoResults(result, args, within.getRadius().getMetric());
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, Distance radius) {
        return geoRadiusByMember(key, member, radius, GeoRadiusCommandArgs.newGeoRadiusArgs());
    }

    @Override
    @Nullable
    public GeoResults<GeoLocation<byte[]>> geoRadiusByMember(byte[] key, byte[] member, Distance radius,
            GeoRadiusCommandArgs args) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(member, "Member must not be null");
        Assert.notNull(radius, "Distance must not be null");
        Assert.notNull(args, "GeoRadiusCommandArgs must not be null");

        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(key);
            commandArgs.add(member);
            commandArgs.add(radius.getValue());
            commandArgs.add(radius.getMetric().getAbbreviation());
            
            appendGeoRadiusArgs(commandArgs, args);
            
            Object result = connection.execute("GEORADIUSBYMEMBER", commandArgs.toArray());
            return parseGeoResults(result, args, radius.getMetric());
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long geoRemove(byte[] key, byte[]... members) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(members, "Members must not be null");
        Assert.noNullElements(members, "Members must not contain null elements");

        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(key);
            for (byte[] member : members) {
                commandArgs.add(member);
            }
            
            Object result = connection.execute("ZREM", commandArgs.toArray());
            Object converted = ValkeyGlideConverters.fromGlideResult(result);
            return converted instanceof Number ? ((Number) converted).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public GeoResults<GeoLocation<byte[]>> geoSearch(byte[] key, GeoReference<byte[]> reference, GeoShape predicate,
            GeoSearchCommandArgs args) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(reference, "GeoReference must not be null");
        Assert.notNull(predicate, "GeoShape must not be null");
        Assert.notNull(args, "GeoSearchCommandArgs must not be null");

        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(key);
            
            appendGeoReference(commandArgs, reference);
            appendGeoShape(commandArgs, predicate);
            appendGeoSearchArgs(commandArgs, args);
            
            Object result = connection.execute("GEOSEARCH", commandArgs.toArray());
            return parseGeoResults(result, args);
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    @Override
    @Nullable
    public Long geoSearchStore(byte[] destKey, byte[] key, GeoReference<byte[]> reference, GeoShape predicate,
            GeoSearchStoreCommandArgs args) {
        Assert.notNull(destKey, "Destination key must not be null");
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(reference, "GeoReference must not be null");
        Assert.notNull(predicate, "GeoShape must not be null");
        Assert.notNull(args, "GeoSearchStoreCommandArgs must not be null");

        try {
            List<Object> commandArgs = new ArrayList<>();
            commandArgs.add(destKey);
            commandArgs.add(key);
            
            appendGeoReference(commandArgs, reference);
            appendGeoShape(commandArgs, predicate);
            appendGeoSearchStoreArgs(commandArgs, args);
            
            Object result = connection.execute("GEOSEARCHSTORE", commandArgs.toArray());
            Object converted = ValkeyGlideConverters.fromGlideResult(result);
            return converted instanceof Number ? ((Number) converted).longValue() : null;
        } catch (Exception ex) {
            throw new ValkeyGlideExceptionConverter().convert(ex);
        }
    }

    // ==================== Helper Methods ====================

    private List<Object> convertToList(Object obj) {
        if (obj instanceof List) {
            return (List<Object>) obj;
        } else if (obj instanceof Object[]) {
            Object[] array = (Object[]) obj;
            List<Object> list = new ArrayList<>(array.length);
            for (Object item : array) {
                list.add(item);
            }
            return list;
        } else {
            throw new IllegalArgumentException("Cannot convert " + obj.getClass() + " to List");
        }
    }

    private double parseDouble(Object obj) {
        if (obj instanceof Number) {
            return ((Number) obj).doubleValue();
        } else if (obj instanceof String) {
            return Double.parseDouble((String) obj);
        } else if (obj instanceof byte[]) {
            return Double.parseDouble(new String((byte[]) obj));
        } else {
            throw new IllegalArgumentException("Cannot parse double from " + obj.getClass());
        }
    }

    private Point convertToPoint(Object obj) {
        if (obj == null) {
            return null;
        }
        
        List<Object> coordinates = convertToList(obj);
        if (coordinates.size() >= 2) {
            Object xObj = ValkeyGlideConverters.fromGlideResult(coordinates.get(0));
            Object yObj = ValkeyGlideConverters.fromGlideResult(coordinates.get(1));
            
            double x = parseDouble(xObj);
            double y = parseDouble(yObj);
            
            return new Point(x, y);
        }
        
        return null;
    }

    private void appendGeoRadiusArgs(List<Object> commandArgs, GeoRadiusCommandArgs args) {
        if (args.getFlags().contains(GeoRadiusCommandArgs.Flag.WITHCOORD)) {
            commandArgs.add("WITHCOORD");
        }
        if (args.getFlags().contains(GeoRadiusCommandArgs.Flag.WITHDIST)) {
            commandArgs.add("WITHDIST");
        }
        if (args.hasLimit()) {
            commandArgs.add("COUNT");
            commandArgs.add(args.getLimit());
            if (args.hasAnyLimit()) {
                commandArgs.add("ANY");
            }
        }
        if (args.hasSortDirection()) {
            commandArgs.add(args.getSortDirection() == Direction.ASC ? "ASC" : "DESC");
        }
    }

    private void appendGeoSearchArgs(List<Object> commandArgs, GeoSearchCommandArgs args) {
        if (args.getFlags().contains(GeoCommandArgs.GeoCommandFlag.withCord())) {
            commandArgs.add("WITHCOORD");
        }
        if (args.getFlags().contains(GeoCommandArgs.GeoCommandFlag.withDist())) {
            commandArgs.add("WITHDIST");
        }
        if (args.hasLimit()) {
            commandArgs.add("COUNT");
            commandArgs.add(args.getLimit());
            if (args.hasAnyLimit()) {
                commandArgs.add("ANY");
            }
        }
        if (args.hasSortDirection()) {
            commandArgs.add(args.getSortDirection() == Direction.ASC ? "ASC" : "DESC");
        }
    }

    private void appendGeoSearchStoreArgs(List<Object> commandArgs, GeoSearchStoreCommandArgs args) {
        if (args.isStoreDistance()) {
            commandArgs.add("STOREDIST");
        }
        if (args.hasLimit()) {
            commandArgs.add("COUNT");
            commandArgs.add(args.getLimit());
            if (args.hasAnyLimit()) {
                commandArgs.add("ANY");
            }
        }
        if (args.hasSortDirection()) {
            commandArgs.add(args.getSortDirection() == Direction.ASC ? "ASC" : "DESC");
        }
    }

    private void appendGeoReference(List<Object> commandArgs, GeoReference<byte[]> reference) {
        if (reference instanceof GeoReference.GeoMemberReference) {
            commandArgs.add("FROMMEMBER");
            commandArgs.add(((GeoReference.GeoMemberReference<byte[]>) reference).getMember());
        } else if (reference instanceof GeoReference.GeoCoordinateReference) {
            commandArgs.add("FROMLONLAT");
            GeoReference.GeoCoordinateReference<?> coordRef = (GeoReference.GeoCoordinateReference<?>) reference;
            commandArgs.add(coordRef.getLongitude());
            commandArgs.add(coordRef.getLatitude());
        }
    }

    private void appendGeoShape(List<Object> commandArgs, GeoShape shape) {
        if (shape instanceof RadiusShape) {
            commandArgs.add("BYRADIUS");
            RadiusShape radiusShape = (RadiusShape) shape;
            commandArgs.add(radiusShape.getRadius().getValue());
            commandArgs.add(radiusShape.getRadius().getMetric().getAbbreviation());
        } else if (shape instanceof BoxShape) {
            commandArgs.add("BYBOX");
            BoxShape boxShape = (BoxShape) shape;
            commandArgs.add(boxShape.getBoundingBox().getWidth().getValue());
            commandArgs.add(boxShape.getBoundingBox().getHeight().getValue());
            commandArgs.add(boxShape.getBoundingBox().getWidth().getMetric().getAbbreviation());
        }
    }

    private GeoResults<GeoLocation<byte[]>> parseGeoResults(Object result, GeoCommandArgs args) {
        return parseGeoResults(result, args, DistanceUnit.METERS);
    }
    
    private GeoResults<GeoLocation<byte[]>> parseGeoResults(Object result, GeoCommandArgs args, Metric defaultMetric) {
        Object converted = ValkeyGlideConverters.fromGlideResult(result);
        if (converted == null) {
            return new GeoResults<>(new ArrayList<>());
        }
        
        List<Object> list = convertToList(converted);
        List<GeoResult<GeoLocation<byte[]>>> geoResults = new ArrayList<>(list.size());
        
        boolean hasDistance = args.getFlags().contains(GeoCommandArgs.GeoCommandFlag.withDist());
        boolean hasCoordinate = args.getFlags().contains(GeoCommandArgs.GeoCommandFlag.withCord());
        
        for (Object item : list) {
            Object convertedItem = ValkeyGlideConverters.fromGlideResult(item);
            
            if (hasDistance || hasCoordinate) {
                // Complex result format with additional information
                List<Object> itemList = convertToList(convertedItem);
                
                byte[] member = (byte[]) ValkeyGlideConverters.fromGlideResult(itemList.get(0));
                Distance distance = null;
                Point point = null;
                
                int index = 1;
                if (hasDistance && index < itemList.size()) {
                    Object distObj = ValkeyGlideConverters.fromGlideResult(itemList.get(index++));
                    if (distObj != null) {
                        try {
                            double dist = parseDouble(distObj);
                            distance = new Distance(dist, defaultMetric);
                        } catch (Exception e) {
                            // If distance parsing fails, use a default distance to avoid null
                            distance = new Distance(0.0, defaultMetric);
                        }
                    } else {
                        distance = new Distance(0.0, defaultMetric);
                    }
                }
                if (hasCoordinate && index < itemList.size()) {
                    Object coordObj = ValkeyGlideConverters.fromGlideResult(itemList.get(index));
                    point = convertToPoint(coordObj);
                }
                
                GeoLocation<byte[]> location = new GeoLocation<>(member, point);
                // Ensure we don't pass null distance to GeoResult
                if (distance == null) {
                    distance = new Distance(0.0, defaultMetric);
                }
                geoResults.add(new GeoResult<>(location, distance));
            } else {
                // Simple result format - just member names
                byte[] member = (byte[]) convertedItem;
                GeoLocation<byte[]> location = new GeoLocation<>(member, null);
                geoResults.add(new GeoResult<>(location, new Distance(0.0, defaultMetric)));
            }
        }
        
        return new GeoResults<>(geoResults);
    }
}

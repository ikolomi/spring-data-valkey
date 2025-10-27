# Migration Guide: Spring Data Redis to Spring Data Valkey

This guide helps you migrate from Spring Data Redis to Spring Data Valkey.

## Overview

Spring Data Valkey is a fork of Spring Data Redis that has been rebranded to work with [Valkey](https://valkey.io/), an open source high-performance key/value datastore that is fully compatible with Redis. The migration primarily involves updating package names, class names, and configuration properties from Redis to Valkey. Spring Data Valkey also adds support for [Valkey GLIDE](https://github.com/valkey-io/valkey-glide), a high-performance client library that is now the recommended default driver alongside existing Lettuce and Jedis support.

## Dependency Changes

### Maven

Update your `pom.xml`:

```xml
<!-- Before (Spring Data Redis with Lettuce) -->
<dependency>
    <groupId>org.springframework.data</groupId>
    <artifactId>spring-data-redis</artifactId>
    <version>${version}</version>
</dependency>
<dependency>
    <groupId>io.lettuce</groupId>
    <artifactId>lettuce-core</artifactId>
</dependency>

<!-- After (Spring Data Valkey with Valkey GLIDE - recommended) -->
<dependency>
    <groupId>io.valkey.springframework.data</groupId>
    <artifactId>spring-data-valkey</artifactId>
    <version>${version}</version>
</dependency>
<dependency>
    <groupId>io.valkey</groupId>
    <artifactId>valkey-glide</artifactId>
    <classifier>${os.detected.classifier}</classifier>
    <version>${version}</version>
</dependency>
```

Valkey GLIDE requires platform-specific native libraries. Add the os-maven-plugin extension to your `pom.xml`:

```xml
<build>
    <extensions>
        <extension>
            <groupId>kr.motd.maven</groupId>
            <artifactId>os-maven-plugin</artifactId>
            <version>${version}</version>
        </extension>
    </extensions>
</build>
```

Note: You can continue using Lettuce or Jedis if preferred, but Valkey GLIDE is recommended for optimal performance and full Valkey support.

### Gradle

Update your `build.gradle`:

```groovy
// Before (Spring Data Redis with Lettuce)
implementation 'org.springframework.data:spring-data-redis:${version}'
implementation 'io.lettuce:lettuce-core'

// After (Spring Data Valkey with Valkey GLIDE - recommended)
implementation 'io.valkey.springframework.data:spring-data-valkey:${version}'
implementation "io.valkey:valkey-glide:${version}:${osdetector.classifier}"
```

Valkey GLIDE requires platform-specific native libraries. Add the osdetector plugin:

```groovy
plugins {
    id 'com.google.osdetector' version '${version}'
}
```

## Package Name Changes

All packages have been renamed from `org.springframework.data.redis` to `io.valkey.springframework.data.valkey`.

### Pattern

```
org.springframework.data.redis.*  →  io.valkey.springframework.data.valkey.*
```

### Examples

```java
// Before
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

// After
import io.valkey.springframework.data.valkey.core.ValkeyTemplate;
import io.valkey.springframework.data.valkey.connection.ValkeyConnectionFactory;
import io.valkey.springframework.data.valkey.connection.lettuce.LettuceConnectionFactory;
```

## Class Name Changes

Classes containing "Redis" in their name have been renamed to use "Valkey" instead.

### Pattern

```
*Redis* → *Valkey*
```

### Examples

The following is a list of the more common classes, it is not exhaustive.

#### Core Classes

| Spring Data Redis | Spring Data Valkey |
|-------------------|-------------------|
| `RedisTemplate` | `ValkeyTemplate` |
| `StringRedisTemplate` | `StringValkeyTemplate` |
| `ReactiveRedisTemplate` | `ReactiveValkeyTemplate` |
| `RedisConnectionFactory` | `ValkeyConnectionFactory` |
| `RedisConnection` | `ValkeyConnection` |
| `RedisOperations` | `ValkeyOperations` |
| `RedisCallback` | `ValkeyCallback` |
| `RedisSerializer` | `ValkeySerializer` |

#### Connection Classes

| Spring Data Redis | Spring Data Valkey |
|-------------------|-------------------|
| `LettuceConnectionFactory` | `LettuceConnectionFactory` (unchanged) |
| `JedisConnectionFactory` | `JedisConnectionFactory` (unchanged) |
| `RedisStandaloneConfiguration` | `ValkeyStandaloneConfiguration` |
| `RedisClusterConfiguration` | `ValkeyClusterConfiguration` |
| `RedisSentinelConfiguration` | `ValkeySentinelConfiguration` |
| `RedisPassword` | `ValkeyPassword` |
| `RedisNode` | `ValkeyNode` |
| `RedisClusterNode` | `ValkeyClusterNode` |

#### Repository Classes

| Spring Data Redis | Spring Data Valkey |
|-------------------|-------------------|
| `@EnableRedisRepositories` | `@EnableValkeyRepositories` |
| `RedisRepository` | `ValkeyRepository` |
| `@RedisHash` | `@ValkeyHash` |

#### Cache Classes

| Spring Data Redis | Spring Data Valkey |
|-------------------|-------------------|
| `RedisCacheManager` | `ValkeyCacheManager` |
| `RedisCacheConfiguration` | `ValkeyCacheConfiguration` |
| `RedisCacheWriter` | `ValkeyCacheWriter` |

#### Reactive Classes

| Spring Data Redis | Spring Data Valkey |
|-------------------|-------------------|
| `ReactiveRedisConnection` | `ReactiveValkeyConnection` |
| `ReactiveRedisConnectionFactory` | `ReactiveValkeyConnectionFactory` |
| `ReactiveRedisOperations` | `ReactiveValkeyOperations` |
| `ReactiveRedisTemplate` | `ReactiveValkeyTemplate` |

#### Pub/Sub Classes

| Spring Data Redis | Spring Data Valkey |
|-------------------|-------------------|
| `RedisMessageListenerContainer` | `ValkeyMessageListenerContainer` |
| `MessageListenerAdapter` | `MessageListenerAdapter` (unchanged) |

#### Scripting Classes

| Spring Data Redis | Spring Data Valkey |
|-------------------|-------------------|
| `RedisScript` | `ValkeyScript` |
| `DefaultRedisScript` | `DefaultValkeyScript` |

#### Migrating to Valkey GLIDE

Spring Data Valkey adds support for [Valkey GLIDE](https://github.com/valkey-io/valkey-glide) as a new high-performance driver. To migrate from Lettuce or Jedis:

| Class | Description |
|-------|-------------|
| `ValkeyGlideConnectionFactory` | Replaces `LettuceConnectionFactory` or `JedisConnectionFactory` |
| `ValkeyGlideConnection` | Connection implementation using GLIDE |
| `ValkeyGlideClientConfiguration` | Configuration for GLIDE client |

## Annotation Changes

```java
// Before
@EnableRedisRepositories
@RedisHash("users")
@Resource(name="redisTemplate")

// After
@EnableValkeyRepositories
@ValkeyHash("users")
@Resource(name="valkeyTemplate")
```

## Configuration Changes

### Simple Configuration

```java
// Before
@Configuration
@EnableRedisRepositories
public class RedisConfig {

    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        return new LettuceConnectionFactory();
    }

    @Bean
    public StringRedisTemplate redisTemplate(RedisConnectionFactory factory) {
        return new StringRedisTemplate(factory);
    }
}

// After
@Configuration
@EnableValkeyRepositories
public class ValkeyConfig {

    @Bean
    public ValkeyConnectionFactory valkeyConnectionFactory() {
        return new ValkeyGlideConnectionFactory();
    }

    @Bean
    public StringValkeyTemplate valkeyTemplate(ValkeyConnectionFactory factory) {
        return new StringValkeyTemplate(factory);
    }
}
```

### Spring Boot Configuration

Since a Spring Boot starter is not yet available, add a custom configuration:

```java
@Configuration
@EnableValkeyRepositories
public class ValkeyConfig {

    @Value("${spring.valkey.host:localhost}")
    private String host;

    @Value("${spring.valkey.port:6379}")
    private int port;

    @Value("${spring.valkey.database:0}")
    private int database;

    @Value("${spring.valkey.password:#{null}}")
    private String password;

    @Value("${spring.valkey.cluster.nodes:#{null}}")
    private List<String> clusterNodes;

    @Bean
    @ConditionalOnProperty("spring.valkey.cluster.nodes") // Use cluster if configured
    public ValkeyConnectionFactory clusterFactory() {
        ValkeyClusterConfiguration config = new ValkeyClusterConfiguration(clusterNodes);
        return new ValkeyGlideConnectionFactory(config);
    }

    @Bean
    @ConditionalOnMissingBean // Otherwise use a standalone instance
    public ValkeyConnectionFactory valkeyConnectionFactory() {
        ValkeyStandaloneConfiguration config = new ValkeyStandaloneConfiguration(host, port);
        config.setDatabase(database);
        if (password != null) {
            config.setPassword(password);
        }
        return new ValkeyGlideConnectionFactory(config);
    }

    @Bean
    public ValkeyTemplate<String, String> valkeyTemplate(ValkeyConnectionFactory factory) {
        return new StringValkeyTemplate(factory);
    }
}
```

The configuration above reads from these properties in your `application.properties` or `application.yml`:

```properties
# Before
spring.redis.host=localhost
spring.redis.port=6379

# After
spring.valkey.host=localhost
spring.valkey.port=6379
```

## Migration Checklist

- [ ] Update Maven/Gradle dependencies
- [ ] Update all package imports from `org.springframework.data.redis` to `io.valkey.springframework.data.valkey`
- [ ] Rename all `*Redis*` classes to `*Valkey*`
- [ ] Update annotations (`@EnableRedisRepositories` → `@EnableValkeyRepositories`, `@RedisHash` → `@ValkeyHash`)
- [ ] Update bean names in code and configuration (e.g., `redisTemplate` to `valkeyTemplate`)
- [ ] Update Spring Boot properties from `spring.redis.*` to `spring.valkey.*`

## Compatibility Notes

- Valkey is fully compatible with Redis features, protocols, and commands
- Existing Redis servers can be used with Spring Data Valkey without changes
- Valkey GLIDE is the recommended driver for new applications

## Additional Resources

- [Valkey Documentation](https://valkey.io/docs/)
- [Valkey GLIDE](https://github.com/valkey-io/valkey-glide)
- [Spring Data Redis Documentation](https://docs.spring.io/spring-data/redis/reference/)
- [Spring Data Redis](https://github.com/spring-projects/spring-data-redis)

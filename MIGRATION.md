# Migration Guide: Spring Data Redis to Spring Data Valkey

This guide helps you migrate from Spring Data Redis to Spring Data Valkey.

## Overview

Spring Data Valkey is a fork of Spring Data Redis that has been rebranded to work with [Valkey](https://valkey.io/), an open source high-performance key/value datastore that is fully compatible with Redis. The migration primarily involves updating package names, class names, and configuration properties. Spring Data Valkey also adds support for [Valkey GLIDE](https://github.com/valkey-io/valkey-glide), a high-performance client library that is now the recommended default driver alongside existing Lettuce and Jedis support.

## Dependency Changes

### Maven

Update your `pom.xml`:

```xml
<!-- Before (Spring Data Redis with Lettuce) -->
<dependency>
    <groupId>org.springframework.data</groupId>
    <artifactId>spring-data-redis</artifactId>
    <version>3.5.1</version>
</dependency>
<dependency>
    <groupId>io.lettuce</groupId>
    <artifactId>lettuce-core</artifactId>
</dependency>

<!-- After (Spring Data Valkey with Valkey GLIDE - recommended) -->
<dependency>
    <groupId>io.valkey.springframework.data</groupId>
    <artifactId>spring-data-valkey</artifactId>
    <version>3.5.1</version>
</dependency>
<dependency>
    <groupId>io.valkey</groupId>
    <artifactId>valkey-glide</artifactId>
    <classifier>${os.detected.classifier}</classifier>
    <version>2.1.1</version>
</dependency>
```

Valkey GLIDE requires platform-specific native libraries. Add the os-maven-plugin extension to your `pom.xml`:

```xml
<build>
    <extensions>
        <extension>
            <groupId>kr.motd.maven</groupId>
            <artifactId>os-maven-plugin</artifactId>
            <version>1.7.1</version>
        </extension>
    </extensions>
</build>
```

Note: You can continue using Lettuce or Jedis if preferred.

### Gradle

Update your `build.gradle`:

```groovy
// Before (Spring Data Redis with Lettuce)
implementation 'org.springframework.data:spring-data-redis:3.5.1'
implementation 'io.lettuce:lettuce-core'

// After (Spring Data Valkey with Valkey GLIDE - recommended)
implementation 'io.valkey.springframework.data:spring-data-valkey:3.5.1'
implementation "io.valkey:valkey-glide:2.1.1:${osdetector.classifier}"
```

Valkey GLIDE requires platform-specific native libraries. Add the osdetector plugin:

```groovy
plugins {
    id 'com.google.osdetector' version '1.7.3'
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
*Redis* →  *Valkey*
```

### Core Classes

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

### Connection Classes

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

### Reactive Classes

| Spring Data Redis | Spring Data Valkey |
|-------------------|-------------------|
| `ReactiveRedisConnection` | `ReactiveValkeyConnection` |
| `ReactiveRedisConnectionFactory` | `ReactiveValkeyConnectionFactory` |
| `ReactiveRedisOperations` | `ReactiveValkeyOperations` |
| `ReactiveRedisTemplate` | `ReactiveValkeyTemplate` |

### Repository Classes

| Spring Data Redis | Spring Data Valkey |
|-------------------|-------------------|
| `@EnableRedisRepositories` | `@EnableValkeyRepositories` |
| `RedisRepository` | `ValkeyRepository` |
| `@RedisHash` | `@ValkeyHash` |

### Cache Classes

| Spring Data Redis | Spring Data Valkey |
|-------------------|-------------------|
| `RedisCacheManager` | `ValkeyCacheManager` |
| `RedisCacheConfiguration` | `ValkeyCacheConfiguration` |
| `RedisCacheWriter` | `ValkeyCacheWriter` |

### Valkey GLIDE Support

Spring Data Valkey adds support for [Valkey GLIDE](https://github.com/valkey-io/valkey-glide) as a new connection driver:

| Class | Description |
|-------|-------------|
| `ValkeyGlideConnectionFactory` | Connection factory for Valkey GLIDE |
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

Since a Spring Boot starter is not yet available, add a custom configuration in that case:

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

### Spring Boot Properties

Update your `application.properties` or `application.yml`:

```properties
# Before
spring.redis.host=localhost
spring.redis.port=6379
spring.redis.password=secret
spring.redis.database=0
spring.redis.timeout=60000

# After
spring.valkey.host=localhost
spring.valkey.port=6379
spring.valkey.password=secret
spring.valkey.database=0
spring.valkey.timeout=60000
```

### XML Configuration

```xml
<!-- Before -->
<beans xmlns:redis="http://www.springframework.org/schema/redis"
       xsi:schemaLocation="http://www.springframework.org/schema/redis
                           https://www.springframework.org/schema/redis/spring-redis.xsd">
    <redis:repositories base-package="com.example.repositories" />
</beans>

<!-- After -->
<beans xmlns:valkey="http://www.springframework.org/schema/valkey"
       xsi:schemaLocation="http://www.springframework.org/schema/valkey
                           https://www.springframework.org/schema/valkey/spring-valkey.xsd">
    <valkey:repositories base-package="com.example.repositories" />
</beans>
```

## URI and Connection String Changes

Update connection URIs from `redis://` to `valkey://`:

```java
// Before
RedisURI uri = RedisURI.create("redis://localhost:6379");

// After
ValkeyURI uri = ValkeyURI.create("valkey://localhost:6379");
```

## Code Examples

### Basic Template Usage

```java
// Before
@Autowired
private StringRedisTemplate redisTemplate;

public void saveValue(String key, String value) {
    redisTemplate.opsForValue().set(key, value);
}

// After
@Autowired
private StringValkeyTemplate valkeyTemplate;

public void saveValue(String key, String value) {
    valkeyTemplate.opsForValue().set(key, value);
}
```

### Repository Usage

```java
// Before
@RedisHash("users")
public class User {
    @Id String id;
    String name;
}

interface UserRepository extends RedisRepository<User, String> {}

// After
@ValkeyHash("users")
public class User {
    @Id String id;
    String name;
}

interface UserRepository extends ValkeyRepository<User, String> {}
```

### Reactive Usage

```java
// Before
@Autowired
private ReactiveRedisTemplate<String, String> reactiveRedisTemplate;

public Mono<String> getValue(String key) {
    return reactiveRedisTemplate.opsForValue().get(key);
}

// After
@Autowired
private ReactiveValkeyTemplate<String, String> reactiveValkeyTemplate;

public Mono<String> getValue(String key) {
    return reactiveValkeyTemplate.opsForValue().get(key);
}
```

## Migration Checklist

- [ ] Update Maven/Gradle dependencies
- [ ] Update all package imports from `org.springframework.data.redis` to `io.valkey.springframework.data.valkey`
- [ ] Rename all `*Redis*` classes to `*Valkey*`
- [ ] Update annotations (`@EnableRedisRepositories` → `@EnableValkeyRepositories`, `@RedisHash` → `@ValkeyHash`)
- [ ] Update Spring Boot properties from `spring.redis.*` to `spring.valkey.*`
- [ ] Update XML schema namespaces from `redis` to `valkey`
- [ ] Update connection URIs from `redis://` to `valkey://`
- [ ] Update bean names in configuration (e.g., `redisTemplate` to `valkeyTemplate`)
- [ ] Run tests to verify functionality

## Compatibility Notes

- Valkey is fully compatible with Redis features, protocols, and commands
- Existing Redis servers can be used with Spring Data Valkey without changes
- Valkey GLIDE is the recommended driver for new applications

## Additional Resources

- [Spring Data Valkey Documentation](https://github.com/aws/spring-data-valkey)
- [Valkey Documentation](https://valkey.io/docs/)
- [Valkey GLIDE](https://github.com/valkey-io/valkey-glide)

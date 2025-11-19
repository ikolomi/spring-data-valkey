# Spring Data Valkey Performance Tests

Performance benchmarks for Spring Data Valkey operations across different clients.

## Running Tests

### Template Performance Test (Spring Data Valkey)

Test ValkeyTemplate operations (SET, GET, DELETE) with different clients:

```bash
# Valkey GLIDE (default)
mvn compile exec:java

# Lettuce
mvn compile exec:java -Dclient=lettuce

# Jedis  
mvn compile exec:java -Dclient=jedis
```

### Direct Client Performance Test (No Spring Data overhead)

Test direct client operations without Spring Data Valkey:

```bash
# Valkey GLIDE (default)
mvn compile exec:java@direct-test

# Lettuce
mvn compile exec:java@direct-test -Dclient=lettuce

# Jedis
mvn compile exec:java@direct-test -Dclient=jedis
```

### Multi-Threaded Performance Test

Test temaplte use across mulitple threads:

```bash
# Valkey GLIDE (default)
mvn compile exec:java@threaded-test

# Lettuce
mvn compile exec:java@threaded-test -Dclient=lettuce

# Jedis
mvn compile exec:java@threaded-test -Dclient=jedis
```

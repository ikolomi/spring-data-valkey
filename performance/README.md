# Spring Data Valkey Performance Tests

Performance benchmarks for Spring Data Valkey operations across different clients.

## Running Tests

### Template Performance Test (Spring Data Valkey)

Test ValkeyTemplate operations (SET, GET, DELETE) with different clients:

```bash
# Default (Valkey GLIDE)
mvn exec:java

# Lettuce client
mvn exec:java -Dclient=lettuce

# Jedis client  
mvn exec:java -Dclient=jedis
```

### Direct Client Performance Test (No Spring Data overhead)

Test direct client operations without Spring Data Valkey:

```bash
# Default (Valkey GLIDE)
mvn exec:java@direct-test -Dclient=valkeyglide

# Lettuce client
mvn exec:java@direct-test -Dclient=lettuce

# Jedis client
mvn exec:java@direct-test -Dclient=jedis
```

## Sample Output

### Spring Data Valkey Template
```
Running ValkeyTemplate Performance Test
Client: valkeyglide
Operations: 10,000
----------------------------------------
SET:    1,006 ops/sec (9934.83 ms total)
GET:    1,121 ops/sec (8913.64 ms total)
DELETE: 1,154 ops/sec (8661.76 ms total)
```

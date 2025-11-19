# Spring Data Valkey Performance Tests

Performance benchmarks for Spring Data Valkey operations across different clients.

## Running Tests

### Template Performance Test (Spring Data Valkey)

Test ValkeyTemplate operations (SET, GET, DELETE) with different clients:

```bash
# Valkey GLIDE client
mvn compile exec:java

# Lettuce client
mvn compile exec:java -Dclient=lettuce

# Jedis client  
mvn compile exec:java -Dclient=jedis
```

### Direct Client Performance Test (No Spring Data overhead)

Test direct client operations without Spring Data Valkey:

```bash
# Direct Valkey GLIDE
mvn compile exec:java@direct-test

# Direct Lettuce
mvn compile exec:java@direct-test -Dclient=lettuce

# Direct Jedis
mvn compile exec:java@direct-test -Dclient=jedis
```

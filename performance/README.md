# Spring Data Valkey Performance Tests

Performance benchmarks for Spring Data Valkey operations across different clients.

## Running Tests

### Template Performance Test (Spring Data Valkey)

Test ValkeyTemplate operations (SET, GET, DELETE) with different clients:

```bash
mvn -q compile exec:java -Dclient=valkeyglide
mvn -q compile exec:java -Dclient=lettuce
mvn -q compile exec:java -Dclient=jedis
```

### Direct Client Performance Test (No Spring Data overhead)

Test direct client operations without Spring Data Valkey:

```bash
mvn -q compile exec:java@direct-test -Dclient=valkeyglide
mvn -q compile exec:java@direct-test -Dclient=lettuce
mvn -q compile exec:java@direct-test -Dclient=jedis
```

### Multi-Threaded Performance Test

Test template use across mulitple threads:

```bash
mvn -q compile exec:java@threaded-test -Dclient=valkeyglide
mvn -q compile exec:java@threaded-test -Dclient=lettuce
mvn -q compile exec:java@threaded-test -Dclient=jedis
```

# Spring Data Valkey Performance Tests

Performance benchmarks for Spring Data Valkey operations across different clients.

## Running Tests

### Template Performance Test

Test ValkeyTemplate operations (SET, GET, DELETE) with different clients:

```bash
# Default (Valkey GLIDE)
mvn exec:java

# Lettuce client
mvn exec:java -Dclient=lettuce

# Jedis client  
mvn exec:java -Dclient=jedis
```

### Adding New Performance Tests

1. Create a new class in `src/main/java/performance/`
2. Follow the pattern of `TemplatePerformanceTest`
3. Update the exec plugin configuration in `pom.xml` if needed

## Test Configuration

- **Operations**: 10,000 per test by default
- **Key Pattern**: `perf:test:{index}`
- **Clients**: valkeyglide (default), lettuce, jedis

## Sample Output

```
Running ValkeyTemplate Performance Test
Client: valkeyglide
Operations: 10,000
----------------------------------------
SET:    45,234 ops/sec (221.05 ms total)
GET:    52,108 ops/sec (191.91 ms total)
DELETE: 48,876 ops/sec (204.60 ms total)
```

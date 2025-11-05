# Spring Data Valkey Examples

This directory contains standalone examples demonstrating various features of Spring Data Valkey using Valkey GLIDE as the driver.

## Prerequisites

- JDK 17 or higher
- Maven 3.8+
- Valkey server running on `localhost:6379` (or configure connection in examples)

Note that the `Makefile` in the root directory can be used to start a Valkey instance for testing.

## Running Examples

Each example can be run independently using Maven, from the examples root directory:

```bash
$ cd examples
$ mvn compile exec:java -pl <example-name>
```

Or the specific example directory:

```bash
$ cd examples/<example-name>
$ mvn compile exec:java
```

To run all examples sequentially:

```bash
$ cd examples
$ for module in $(ls -d */ | grep -v target | sed 's|/||'); do
  echo "=== Running $module ==="
  mvn -q compile exec:java -pl $module
  echo ""
done
```

## Available Examples

| Example | Description |
|---------|-------------|
| **quickstart** | Basic ValkeyTemplate usage for simple key-value operations |
| **repositories** | Spring Data repository abstraction with @ValkeyHash entities and custom finder methods |
| **operations** | Comprehensive examples of all Valkey data structures (List, Set, Hash, ZSet, Geo, Stream, HyperLogLog) |
| **serialization** | Different serialization strategies (String, JSON, JDK) for storing objects |
| **transactions** | MULTI/EXEC transactions with WATCH for optimistic locking |
| **scripting** | Lua script execution (EVAL, EVALSHA) for atomic operations |
| **cache** | Spring Cache abstraction with Valkey backend (@Cacheable, TTL configuration) |
| **collections** | Valkey-backed Java collections (ValkeyList, ValkeySet, ValkeyMap) and atomic counters |

## Notes

- All examples use **Valkey GLIDE** as the connection driver (Lettuce and Jedis are also supported)
- Most examples create resources directly in `main()` for simplicity; see `cache` and `repositories` for Spring `@Configuration` examples
- Each example is self-contained and can be copied as a starting point for your project

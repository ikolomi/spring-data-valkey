/*
 * Copyright 2025 the original author or authors.
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
package performance;

import io.valkey.springframework.data.valkey.connection.ValkeyConnectionFactory;
import io.valkey.springframework.data.valkey.connection.jedis.JedisConnectionFactory;
import io.valkey.springframework.data.valkey.connection.lettuce.LettuceConnectionFactory;
import io.valkey.springframework.data.valkey.connection.valkeyglide.ValkeyGlideConnectionFactory;
import io.valkey.springframework.data.valkey.core.StringValkeyTemplate;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * Multi-threaded performance test to demonstrate client creation bottleneck.
 */
public class MultiThreadedPerformanceTest {

	private static final int THREADS = 100;
	private static final int OPERATIONS_PER_THREAD = 100;
	private static final int TOTAL_OPERATIONS = THREADS * OPERATIONS_PER_THREAD;

	public static void main(String[] args) throws Exception {
		String clientType = System.getProperty("client", "valkeyglide");
		
		System.out.println("Running Multi-threaded Performance Test");
		System.out.println("Client: " + clientType);
		System.out.println("Threads: " + THREADS);
		System.out.println("Operations per thread: " + OPERATIONS_PER_THREAD);
		System.out.println("Total expected operations: " + TOTAL_OPERATIONS);
		System.out.println("----------------------------------------");

		ValkeyConnectionFactory factory = createConnectionFactory(clientType);

		if (factory instanceof org.springframework.beans.factory.InitializingBean) {
			((org.springframework.beans.factory.InitializingBean) factory).afterPropertiesSet();
		}

		try {
			runMultiThreadedTest(factory);
		} finally {
			if (factory instanceof org.springframework.beans.factory.DisposableBean) {
				((org.springframework.beans.factory.DisposableBean) factory).destroy();
			}
		}
	}

	private static ValkeyConnectionFactory createConnectionFactory(String clientType) {
		return switch (clientType.toLowerCase()) {
			case "lettuce" -> new LettuceConnectionFactory();
			case "jedis" -> new JedisConnectionFactory();
			case "valkeyglide" -> new ValkeyGlideConnectionFactory();
			default -> throw new IllegalArgumentException("Unknown client: " + clientType);
		};
	}

	private static void runMultiThreadedTest(ValkeyConnectionFactory factory) throws InterruptedException {
		long startTime = System.currentTimeMillis();

		StringValkeyTemplate template = new StringValkeyTemplate(factory);

		ExecutorService executorService = Executors.newFixedThreadPool(THREADS);

		AtomicInteger setOperations = new AtomicInteger(0);
		AtomicInteger getOperations = new AtomicInteger(0);
		AtomicInteger deleteOperations = new AtomicInteger(0);
		AtomicInteger setFailures = new AtomicInteger(0);
		AtomicInteger getFailures = new AtomicInteger(0);
		AtomicInteger deleteFailures = new AtomicInteger(0);

		try {
			Runnable task = () -> {
				try {
					IntStream.range(0, OPERATIONS_PER_THREAD).forEach(i -> {
						String key = Thread.currentThread().getName() + ":" + i;
						String value = "value" + i;
						
						// SET operation
						try {
							template.opsForValue().set(key, value);
							setOperations.incrementAndGet();
						} catch (Exception e) {
							setFailures.incrementAndGet();
						}
						
						// GET operation
						try {
							String result = template.opsForValue().get(key);
							if (result != null) {
								getOperations.incrementAndGet();
							}
						} catch (Exception e) {
							getFailures.incrementAndGet();
						}
						
						// DELETE operation
						try {
							template.delete(key);
							deleteOperations.incrementAndGet();
						} catch (Exception e) {
							deleteFailures.incrementAndGet();
						}
					});
				} catch (Exception e) {
					System.err.println("Thread failed: " + e.getMessage());
					setFailures.addAndGet(OPERATIONS_PER_THREAD);
					getFailures.addAndGet(OPERATIONS_PER_THREAD);
					deleteFailures.addAndGet(OPERATIONS_PER_THREAD);
				}
			};

			IntStream.range(0, THREADS).forEach(i -> executorService.submit(task));

			executorService.shutdown();
			boolean finished = executorService.awaitTermination(30, TimeUnit.SECONDS);

			long duration = System.currentTimeMillis() - startTime;
			long setTime = duration;
			long getTime = duration;
			long deleteTime = duration;

			printOperationResults("SET", setOperations.get(), setFailures.get(), TOTAL_OPERATIONS, setTime);
			printOperationResults("GET", getOperations.get(), getFailures.get(), TOTAL_OPERATIONS, getTime);
			printOperationResults("DELETE", deleteOperations.get(), deleteFailures.get(), TOTAL_OPERATIONS, deleteTime);
			
			int totalOps = setOperations.get() + getOperations.get() + deleteOperations.get();
			int totalFailures = setFailures.get() + getFailures.get() + deleteFailures.get();
			int expectedOps = TOTAL_OPERATIONS * 3; // SET + GET + DELETE
			
			System.out.println("----------------------------------------");
			System.out.println("Total successful operations: " + totalOps + " / " + expectedOps + " expected");
			System.out.println("Total failures: " + totalFailures);
			System.out.println("Overall success rate: " + String.format("%.1f%%", (totalOps * 100.0 / expectedOps)));
			System.out.println("Completed in " + (duration / 1000.0) + " seconds");
		} finally {
			executorService.shutdown();
		}
	}

	private static void printOperationResults(String operation, int successful, int failed, int expected, long timeMs) {
		if (successful > 0) {
			long opsPerSec = (long) (successful / (timeMs / 1000.0));
			System.out.printf("%s:    %,d ops/sec (%.2f ms total)%n", operation, opsPerSec, timeMs / 1.0);
		} else {
			System.out.printf("%s:    0 ops/sec (%.2f ms total)%n", operation, timeMs / 1.0);
		}
	}
}

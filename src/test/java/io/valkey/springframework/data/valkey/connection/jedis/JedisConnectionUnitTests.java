/*
 * Copyright 2014-2025 the original author or authors.
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
package io.valkey.springframework.data.valkey.connection.jedis;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import redis.clients.jedis.Connection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.args.SaveMode;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import io.valkey.springframework.data.valkey.connection.AbstractConnectionUnitTestBase;
import io.valkey.springframework.data.valkey.connection.ValkeyServerCommands.ShutdownOption;
import io.valkey.springframework.data.valkey.connection.zset.Tuple;
import io.valkey.springframework.data.valkey.core.Cursor;
import io.valkey.springframework.data.valkey.core.KeyScanOptions;
import io.valkey.springframework.data.valkey.core.ScanOptions;

/**
 * @author Christoph Strobl
 */
class JedisConnectionUnitTests {

	@Nested
	public class BasicUnitTests extends AbstractConnectionUnitTestBase<Connection> {

		protected JedisConnection connection;
		private Jedis jedisSpy;

		@BeforeEach
		public void setUp() {

			jedisSpy = spy(new Jedis(getNativeValkeyConnectionMock()));
			connection = new JedisConnection(jedisSpy);
		}

		@Test // DATAVALKEY-184, GH-2153
		void shutdownWithNullShouldDelegateCommandCorrectly() {

			try {
				connection.shutdown(null);
			} catch (InvalidDataAccessApiUsageException ignore) {
			}

			verify(jedisSpy).shutdown();
		}

		@Test // DATAVALKEY-184, GH-2153
		void shutdownNosaveShouldBeSentCorrectly() {

			assertThatExceptionOfType(JedisException.class).isThrownBy(() -> connection.shutdown(ShutdownOption.NOSAVE));

			verify(jedisSpy).shutdown(SaveMode.NOSAVE);
		}

		@Test // DATAVALKEY-184, GH-2153
		void shutdownSaveShouldBeSentCorrectly() {

			assertThatExceptionOfType(JedisException.class).isThrownBy(() -> connection.shutdown(ShutdownOption.SAVE));

			verify(jedisSpy).shutdown(SaveMode.SAVE);
		}

		@Test // DATAVALKEY-267
		public void killClientShouldDelegateCallCorrectly() {

			connection.killClient("127.0.0.1", 1001);
			verify(jedisSpy).clientKill(eq("127.0.0.1:1001"));
		}

		@Test // DATAVALKEY-270
		public void getClientNameShouldSendRequestCorrectly() {

			connection.getClientName();
			verify(jedisSpy).clientGetname();
		}

		@Test // DATAVALKEY-277
		void replicaOfShouldThrowExectpionWhenCalledForNullHost() {
			assertThatIllegalArgumentException().isThrownBy(() -> connection.replicaOf(null, 0));
		}

		@Test // DATAVALKEY-277
		public void replicaOfShouldBeSentCorrectly() {

			connection.replicaOf("127.0.0.1", 1001);
			verify(jedisSpy).replicaof(eq("127.0.0.1"), eq(1001));
		}

		@Test // DATAVALKEY-277
		public void replicaOfNoOneShouldBeSentCorrectly() {

			connection.replicaOfNoOne();
			verify(jedisSpy).replicaofNoOne();
		}

		@Test // DATAVALKEY-330
		void shouldThrowExceptionWhenAccessingValkeySentinelsCommandsWhenNoSentinelsConfigured() {
			assertThatExceptionOfType(InvalidDataAccessResourceUsageException.class)
					.isThrownBy(() -> connection.getSentinelConnection());
		}

		@Test // DATAVALKEY-472
		void restoreShouldThrowExceptionWhenTtlInMillisExceedsIntegerRange() {
			assertThatIllegalArgumentException()
					.isThrownBy(() -> connection.restore("foo".getBytes(), (long) Integer.MAX_VALUE + 1L, "bar".getBytes()));
		}

		@Test // DATAVALKEY-472
		void setExShouldThrowExceptionWhenTimeExceedsIntegerRange() {
			assertThatIllegalArgumentException()
					.isThrownBy(() -> connection.setEx("foo".getBytes(), (long) Integer.MAX_VALUE + 1L, "bar".getBytes()));
		}

		@Test // DATAVALKEY-472
		void sRandMemberShouldThrowExceptionWhenCountExceedsIntegerRange() {
			assertThatIllegalArgumentException()
					.isThrownBy(() -> connection.sRandMember("foo".getBytes(), (long) Integer.MAX_VALUE + 1L));
		}

		@Test // DATAVALKEY-472
		void zRangeByScoreShouldThrowExceptionWhenOffsetExceedsIntegerRange() {
			assertThatIllegalArgumentException().isThrownBy(() -> connection.zRangeByScore("foo".getBytes(), "foo", "bar",
					(long) Integer.MAX_VALUE + 1L, Integer.MAX_VALUE));
		}

		@Test // DATAVALKEY-472
		void zRangeByScoreShouldThrowExceptionWhenCountExceedsIntegerRange() {
			assertThatIllegalArgumentException().isThrownBy(() -> connection.zRangeByScore("foo".getBytes(), "foo", "bar",
					Integer.MAX_VALUE, (long) Integer.MAX_VALUE + 1L));
		}

		@Test // DATAVALKEY-531, GH-2006
		public void scanShouldKeepTheConnectionOpen() {

			doReturn(new ScanResult<>("0", Collections.<String> emptyList())).when(jedisSpy).scan(any(byte[].class),
					any(ScanParams.class));

			connection.scan(ScanOptions.NONE);

			verify(jedisSpy, never()).disconnect();
		}

		@Test // DATAVALKEY-531, GH-2006
		public void scanShouldCloseTheConnectionWhenCursorIsClosed() throws IOException {

			doReturn(new ScanResult<>("0", Collections.<String> emptyList())).when(jedisSpy).scan(any(byte[].class),
					any(ScanParams.class));

			Cursor<byte[]> cursor = connection.scan(ScanOptions.NONE);
			cursor.close();

			verify(jedisSpy, times(1)).disconnect();
		}

		@Test // GH-2796
		void scanShouldOperateUponUnsigned64BitCursorId() {

			String cursorId = "9286422431637962824";
			ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
			doReturn(new ScanResult<>(cursorId, List.of("spring".getBytes()))).when(jedisSpy).scan(any(byte[].class),
					any(ScanParams.class));

			Cursor<byte[]> cursor = connection.scan(KeyScanOptions.NONE);
			cursor.next(); // initial value
			assertThat(cursor.getCursorId()).isEqualTo(Long.parseUnsignedLong(cursorId));

			cursor.next(); // fetch next
			verify(jedisSpy, times(2)).scan(captor.capture(), any(ScanParams.class));
			assertThat(captor.getAllValues()).map(String::new).containsExactly("0", cursorId);
		}

		@Test // DATAVALKEY-531
		public void sScanShouldKeepTheConnectionOpen() {

			doReturn(new ScanResult<>("0", Collections.<String> emptyList())).when(jedisSpy).sscan(any(byte[].class),
					any(byte[].class), any(ScanParams.class));

			connection.sScan("foo".getBytes(), ScanOptions.NONE);

			verify(jedisSpy, never()).disconnect();
		}

		@Test // DATAVALKEY-531
		public void sScanShouldCloseTheConnectionWhenCursorIsClosed() throws IOException {

			doReturn(new ScanResult<>("0", Collections.<String> emptyList())).when(jedisSpy).sscan(any(byte[].class),
					any(byte[].class), any(ScanParams.class));

			Cursor<byte[]> cursor = connection.sScan("foo".getBytes(), ScanOptions.NONE);
			cursor.close();

			verify(jedisSpy, times(1)).disconnect();
		}

		@Test // GH-2796
		void sScanShouldOperateUponUnsigned64BitCursorId() {

			String cursorId = "9286422431637962824";
			ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
			doReturn(new ScanResult<>(cursorId, List.of("spring".getBytes()))).when(jedisSpy).sscan(any(byte[].class),
					any(byte[].class), any(ScanParams.class));

			Cursor<byte[]> cursor = connection.setCommands().sScan("spring".getBytes(), ScanOptions.NONE);
			cursor.next(); // initial value
			assertThat(cursor.getCursorId()).isEqualTo(Long.parseUnsignedLong(cursorId));

			cursor.next(); // fetch next
			verify(jedisSpy, times(2)).sscan(any(byte[].class), captor.capture(), any(ScanParams.class));
			assertThat(captor.getAllValues()).map(String::new).containsExactly("0", cursorId);
		}

		@Test // DATAVALKEY-531
		public void zScanShouldKeepTheConnectionOpen() {

			doReturn(new ScanResult<>("0", Collections.<String> emptyList())).when(jedisSpy).zscan(any(byte[].class),
					any(byte[].class), any(ScanParams.class));

			connection.zScan("foo".getBytes(), ScanOptions.NONE);

			verify(jedisSpy, never()).disconnect();
		}

		@Test // DATAVALKEY-531
		public void zScanShouldCloseTheConnectionWhenCursorIsClosed() throws IOException {

			doReturn(new ScanResult<>("0", Collections.<String> emptyList())).when(jedisSpy).zscan(any(byte[].class),
					any(byte[].class), any(ScanParams.class));

			Cursor<Tuple> cursor = connection.zScan("foo".getBytes(), ScanOptions.NONE);
			cursor.close();

			verify(jedisSpy, times(1)).disconnect();
		}

		@Test // GH-2796
		void zScanShouldOperateUponUnsigned64BitCursorId() {

			String cursorId = "9286422431637962824";
			ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
			doReturn(new ScanResult<>(cursorId, List.of(new redis.clients.jedis.resps.Tuple("spring", 1D)))).when(jedisSpy).zscan(any(byte[].class),
					any(byte[].class), any(ScanParams.class));

			Cursor<Tuple> cursor = connection.zSetCommands().zScan("spring".getBytes(), ScanOptions.NONE);
			cursor.next(); // initial value
			assertThat(cursor.getCursorId()).isEqualTo(Long.parseUnsignedLong(cursorId));

			cursor.next(); // fetch next
			verify(jedisSpy, times(2)).zscan(any(byte[].class), captor.capture(), any(ScanParams.class));
			assertThat(captor.getAllValues()).map(String::new).containsExactly("0", cursorId);
		}

		@Test // DATAVALKEY-531
		public void hScanShouldKeepTheConnectionOpen() {

			doReturn(new ScanResult<>("0", Collections.<String> emptyList())).when(jedisSpy).hscan(any(byte[].class),
					any(byte[].class), any(ScanParams.class));

			connection.hScan("foo".getBytes(), ScanOptions.NONE);

			verify(jedisSpy, never()).disconnect();
		}

		@Test // DATAVALKEY-531
		public void hScanShouldCloseTheConnectionWhenCursorIsClosed() throws IOException {

			doReturn(new ScanResult<>("0", Collections.<String> emptyList())).when(jedisSpy).hscan(any(byte[].class),
					any(byte[].class), any(ScanParams.class));

			Cursor<Entry<byte[], byte[]>> cursor = connection.hScan("foo".getBytes(), ScanOptions.NONE);
			cursor.close();

			verify(jedisSpy, times(1)).disconnect();
		}

		@Test // GH-2796
		void hScanShouldOperateUponUnsigned64BitCursorId() {

			String cursorId = "9286422431637962824";
			ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
			doReturn(new ScanResult<>(cursorId, List.of(Map.entry("spring".getBytes(), "data".getBytes())))).when(jedisSpy).hscan(any(byte[].class),
					any(byte[].class), any(ScanParams.class));

			Cursor<Entry<byte[], byte[]>> cursor = connection.hashCommands().hScan("spring".getBytes(), ScanOptions.NONE);
			cursor.next(); // initial value
			assertThat(cursor.getCursorId()).isEqualTo(Long.parseUnsignedLong(cursorId));

			cursor.next(); // fetch next
			verify(jedisSpy, times(2)).hscan(any(byte[].class), captor.capture(), any(ScanParams.class));
			assertThat(captor.getAllValues()).map(String::new).containsExactly("0", cursorId);
		}

		@Test // DATAVALKEY-714
		void doesNotSelectDbWhenCurrentDbMatchesDesiredOne() {

			Jedis jedisSpy = spy(new Jedis(getNativeValkeyConnectionMock()));
			new JedisConnection(jedisSpy);

			verify(jedisSpy, never()).select(anyInt());
		}

		@Test // DATAVALKEY-714
		void doesNotSelectDbWhenCurrentDbDoesNotMatchDesiredOne() {

			Jedis jedisSpy = spy(new Jedis(getNativeValkeyConnectionMock()));
			when(jedisSpy.getDB()).thenReturn(3);

			new JedisConnection(jedisSpy);

			verify(jedisSpy).select(eq(0));
		}
	}

	@Nested
	public class JedisConnectionPipelineUnitTests extends BasicUnitTests {

		@BeforeEach
		public void setUp() {
			super.setUp();
			connection.openPipeline();
		}

		@Test
		@Disabled
		@Override
		void shutdownWithNullShouldDelegateCommandCorrectly() {}

		@Test
		@Disabled
		@Override
		void shutdownNosaveShouldBeSentCorrectly() {}

		@Test
		@Disabled
		@Override
		void shutdownSaveShouldBeSentCorrectly() {}

		@Test // DATAVALKEY-267
		public void killClientShouldDelegateCallCorrectly() {
			assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
					.isThrownBy(() -> super.killClientShouldDelegateCallCorrectly());
		}

		@Test
		@Override
		// DATAVALKEY-270
		public void getClientNameShouldSendRequestCorrectly() {
			assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
					.isThrownBy(() -> super.getClientNameShouldSendRequestCorrectly());
		}

		@Test
		@Override
		// DATAVALKEY-277
		public void replicaOfShouldBeSentCorrectly() {
			assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
					.isThrownBy(() -> super.replicaOfShouldBeSentCorrectly());
		}

		@Test // DATAVALKEY-277
		public void replicaOfNoOneShouldBeSentCorrectly() {
			assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
					.isThrownBy(() -> super.replicaOfNoOneShouldBeSentCorrectly());
		}

		@Test // DATAVALKEY-531
		public void scanShouldKeepTheConnectionOpen() {
			assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
					.isThrownBy(() -> super.scanShouldKeepTheConnectionOpen());
		}

		@Test // DATAVALKEY-531
		public void scanShouldCloseTheConnectionWhenCursorIsClosed() {
			assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
					.isThrownBy(() -> super.scanShouldCloseTheConnectionWhenCursorIsClosed());
		}

		@Test // DATAVALKEY-531
		public void sScanShouldKeepTheConnectionOpen() {
			assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
					.isThrownBy(() -> super.sScanShouldKeepTheConnectionOpen());
		}

		@Test // DATAVALKEY-531
		public void sScanShouldCloseTheConnectionWhenCursorIsClosed() {
			assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
					.isThrownBy(() -> super.sScanShouldCloseTheConnectionWhenCursorIsClosed());
		}

		@Test // DATAVALKEY-531
		public void zScanShouldKeepTheConnectionOpen() {
			assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
					.isThrownBy(() -> super.zScanShouldKeepTheConnectionOpen());
		}

		@Test // DATAVALKEY-531
		public void zScanShouldCloseTheConnectionWhenCursorIsClosed() {
			assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
					.isThrownBy(() -> super.zScanShouldCloseTheConnectionWhenCursorIsClosed());
		}

		@Test // DATAVALKEY-531
		public void hScanShouldKeepTheConnectionOpen() {
			assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
					.isThrownBy(() -> super.hScanShouldKeepTheConnectionOpen());
		}

		@Test // DATAVALKEY-531
		public void hScanShouldCloseTheConnectionWhenCursorIsClosed() {
			assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
					.isThrownBy(() -> super.hScanShouldCloseTheConnectionWhenCursorIsClosed());
		}

		@Test
		@Disabled("scan not supported in pipeline")
		void scanShouldOperateUponUnsigned64BitCursorId() {

		}

		@Test
		@Disabled("scan not supported in pipeline")
		void sScanShouldOperateUponUnsigned64BitCursorId() {

		}

		@Test
		@Disabled("scan not supported in pipeline")
		void zScanShouldOperateUponUnsigned64BitCursorId() {

		}

		@Test
		@Disabled("scan not supported in pipeline")
		void hScanShouldOperateUponUnsigned64BitCursorId() {

		}
	}

}

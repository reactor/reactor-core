/*
 * Copyright (c) 2011-2016 Pivotal Software Inc., Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.support;

import org.junit.Ignore;
import org.junit.Test;
import reactor.fn.Supplier;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Andy Wilkinson
 */
public class UUIDUtilsTests {

	private static final long          clockNodeAndSeq    = new Random().nextLong();
	private static final ReentrantLock lock               = new ReentrantLock();
	private static final AtomicLong    lastTimeAtomicLong = new AtomicLong();
	private static       long          lastTimeLong       = 0;

	private static final Object MONITOR               = new Object();
	private static final int[]  THREADS               = new int[]{1, 2, 4, 8, 16, 32};
	private static final int[]  ITERATIONS_PER_THREAD = new int[]{1000000, 500000, 250000, 125000, 62500, 31250};
	private static final int    TEST_ITERATIONS       = 10;

	@Test
	public void noop(){

	}

	@Test
	@Ignore
	public void reentrantLockWithAtomicLongPerformance() throws InterruptedException {
		doTest("ReentrantLock with AtomicLong", new Supplier<UUID>() {

			@Override
			public UUID get() {
				long timeMillis = (System.currentTimeMillis() * 10000) + 0x01B21DD213814000L;

				lock.lock();
				try {
					if (lastTimeAtomicLong.get() == timeMillis) {
						timeMillis = lastTimeAtomicLong.incrementAndGet();
					} else {
						lastTimeAtomicLong.set(timeMillis);
					}
				} finally {
					lock.unlock();
				}

				// time low
				long time = timeMillis << 32;

				// time mid
				time |= (timeMillis & 0xFFFF00000000L) >> 16;

				// time hi and version
				time |= 0x1000 | ((timeMillis >> 48) & 0x0FFF); // version 1

				return new UUID(time, clockNodeAndSeq);
			}
		});
	}

	@Test
	@Ignore
	public void synchronizedWithAtomicLongPerformance() throws InterruptedException {
		doTest("Synchronized with AtomicLong", new Supplier<UUID>() {

			@Override
			public UUID get() {
				long timeMillis = (System.currentTimeMillis() * 10000) + 0x01B21DD213814000L;

				synchronized (MONITOR) {
					if (lastTimeAtomicLong.get() == timeMillis) {
						timeMillis = lastTimeAtomicLong.incrementAndGet();
					} else {
						lastTimeAtomicLong.set(timeMillis);
					}
				}

				// time low
				long time = timeMillis << 32;

				// time mid
				time |= (timeMillis & 0xFFFF00000000L) >> 16;

				// time hi and version
				time |= 0x1000 | ((timeMillis >> 48) & 0x0FFF); // version 1

				return new UUID(time, clockNodeAndSeq);
			}

		});
	}

	@Test
	@Ignore
	public void reentrantLockWithLongPerformance() throws InterruptedException {
		doTest("ReentrantLock with long", new Supplier<UUID>() {

			@Override
			public UUID get() {
				long timeMillis = (System.currentTimeMillis() * 10000) + 0x01B21DD213814000L;

				lock.lock();
				try {
					if (lastTimeLong == timeMillis) {
						timeMillis = ++lastTimeLong;
					} else {
						lastTimeLong = timeMillis;
					}
				} finally {
					lock.unlock();
				}

				// time low
				long time = timeMillis << 32;

				// time mid
				time |= (timeMillis & 0xFFFF00000000L) >> 16;

				// time hi and version
				time |= 0x1000 | ((timeMillis >> 48) & 0x0FFF); // version 1

				return new UUID(time, clockNodeAndSeq);
			}
		});
	}

	@Test
	@Ignore
	public void synchronizedWithLongPerformance() throws InterruptedException {
		doTest("Synchronized with long", new Supplier<UUID>() {

			@Override
			public UUID get() {
				long timeMillis = (System.currentTimeMillis() * 10000) + 0x01B21DD213814000L;

				synchronized (MONITOR) {
					if (lastTimeLong == timeMillis) {
						timeMillis = ++lastTimeLong;
					} else {
						lastTimeLong = timeMillis;
					}
				}

				// time low
				long time = timeMillis << 32;

				// time mid
				time |= (timeMillis & 0xFFFF00000000L) >> 16;

				// time hi and version
				time |= 0x1000 | ((timeMillis >> 48) & 0x0FFF); // version 1

				return new UUID(time, clockNodeAndSeq);
			}

		});
	}

	@Test
	@Ignore
	public void timeBasedVersusRandom() throws InterruptedException {
		doTest("Create time-based", new Supplier<UUID>() {
			@Override
			public UUID get() {
				return UUIDUtils.random();
			}
		});
		doTest("Create random", new Supplier<UUID>() {
			@Override
			public UUID get() {
				return UUIDUtils.random();
			}
		});
	}

	private void doTest(String description, final Supplier<UUID> uuidSupplier) throws InterruptedException {
		for (int t = 0; t < THREADS.length; t++) {
			int threads = THREADS[t];
			final int iterations = ITERATIONS_PER_THREAD[t];
			long[] durations = new long[TEST_ITERATIONS];

			for (int i = 0; i < TEST_ITERATIONS; i++) {
				final CountDownLatch latch = new CountDownLatch(threads);

				long startTime = System.currentTimeMillis();

				for (int j = 0; j < threads; j++) {
					new Thread(new Runnable() {

						@Override
						public void run() {
							for (int u = 0; u < iterations; u++) {
								uuidSupplier.get();
							}
							latch.countDown();
						}

					}).start();
				}

				latch.await();

				durations[i] = System.currentTimeMillis() - startTime;

			}
			System.out.println(description + " with " + threads + " thread" + (threads > 1 ? "s" : "") + ":");
			System.out.println("\t average: " + getAverage(durations) + "ms");
			System.out.println("\t     min: " + getMin(durations) + "ms");
			System.out.println("\t     max: " + getMax(durations) + "ms");
		}
	}



	private long getMin(long[] durations) {
		long min = Long.MAX_VALUE;
		for (long duration : durations) {
			min = Math.min(duration, min);
		}
		return min;
	}

	private long getMax(long[] durations) {
		long max = Long.MIN_VALUE;
		for (long duration : durations) {
			max = Math.max(duration, max);
		}
		return max;
	}

	private long getAverage(long[] durations) {
		long total = 0;
		for (long duration : durations) {
			total += duration;
		}
		return total / durations.length;
	}

}

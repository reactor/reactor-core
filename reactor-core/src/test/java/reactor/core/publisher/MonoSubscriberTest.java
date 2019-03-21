/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.publisher.Operators.MonoSubscriber;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.subscriber.AssertSubscriber;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MonoSubscriberTest {

	@Test
	public void queueSubscriptionSyncRejected() {
		MonoSubscriber<Integer, Integer> ds = new MonoSubscriber<>(new AssertSubscriber<>());

		assertEquals(Fuseable.NONE, ds.requestFusion(Fuseable.SYNC));
	}

	@Test
	public void clear() {
		MonoSubscriber<Integer, Integer> ds = new MonoSubscriber<>(new AssertSubscriber<>());

		ds.value = 1;

		ds.clear();

		assertEquals(MonoSubscriber.FUSED_CONSUMED, ds.state);
		assertNull(ds.value);
	}

	@Test
	public void completeCancelRace() {
		for (int i = 0; i < 500; i++) {
			final MonoSubscriber<Integer, Integer> ds = new MonoSubscriber<>(new AssertSubscriber<>());

			Runnable r1 = () -> ds.complete(1);

			Runnable r2 = ds::cancel;

			race(r1, r2, Schedulers.single());
		}
	}

	@Test
	public void requestClearRace() {
		for (int i = 0; i < 5000; i++) {
			AssertSubscriber<Integer> ts = new AssertSubscriber<Integer>(0L);

			final MonoSubscriber<Integer, Integer> ds = new MonoSubscriber<>(ts);
			ts.onSubscribe(ds);
			ds.complete(1);

			Runnable r1 = () -> ds.request(1);

			Runnable r2 = () -> ds.value = null;

			race(r1, r2, Schedulers.single());

			if (ts.values().size() >= 1) {
				ts.assertValues(1);
			}
		}
	}

	@Test
	public void requestCancelRace() {
		for (int i = 0; i < 5000; i++) {
			AssertSubscriber<Integer> ts = new AssertSubscriber<>(0L);

			final MonoSubscriber<Integer, Integer> ds = new MonoSubscriber<>(ts);
			ts.onSubscribe(ds);
			ds.complete(1);

			Runnable r1 = () -> ds.request(1);

			Runnable r2 = ds::cancel;

			race(r1, r2, Schedulers.single());

			if (ts.values().size() >= 1) {
				ts.assertValues(1);
			}
		}
	}

	/**
	 * Synchronizes the execution of two runnables (as much as possible)
	 * to test race conditions.
	 * <p>The method blocks until both have run to completion.
	 * @param r1 the first runnable
	 * @param r2 the second runnable
	 * @param s the scheduler to use
	 */
	//TODO pull into reactor-tests?
	public static void race(final Runnable r1, final Runnable r2, Scheduler s) {
		final AtomicInteger count = new AtomicInteger(2);
		final CountDownLatch cdl = new CountDownLatch(2);

		final Throwable[] errors = { null, null };

		s.schedule(() -> {
			if (count.decrementAndGet() != 0) {
				while (count.get() != 0) { }
			}

			try {
				try {
					r1.run();
				} catch (Throwable ex) {
					errors[0] = ex;
				}
			} finally {
				cdl.countDown();
			}
		});

		if (count.decrementAndGet() != 0) {
			while (count.get() != 0) { }
		}

		try {
			try {
				r2.run();
			} catch (Throwable ex) {
				errors[1] = ex;
			}
		} finally {
			cdl.countDown();
		}

		try {
			if (!cdl.await(5, TimeUnit.SECONDS)) {
				throw new AssertionError("The wait timed out!");
			}
		} catch (InterruptedException ex) {
			throw new RuntimeException(ex);
		}
		if (errors[0] != null && errors[1] == null) {
			throw Exceptions.propagate(errors[0]);
		}

		if (errors[0] == null && errors[1] != null) {
			throw Exceptions.propagate(errors[1]);
		}

		if (errors[0] != null && errors[1] != null) {
			errors[0].addSuppressed(errors[1]);
			throw Exceptions.propagate(errors[0]);
		}
	}
}
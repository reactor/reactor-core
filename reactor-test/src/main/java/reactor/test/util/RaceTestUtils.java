/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.test.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

import reactor.core.Exceptions;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * @author Stephane Maldini
 * @author David Karnok
 */
public class RaceTestUtils {

	/**
	 * Synchronizes the execution of two concurrent state modifications as much as
	 * possible to test race conditions. The method blocks until the given {@link Predicate}
	 * matches. It performs a {@link BiPredicate} test at the end to validate the end
	 * result.
	 *
	 * @param initial the initial state
	 * @param race the state-modification {@link Function}
	 * @param stopRace the stop condition for the race loop, as a {@link Predicate}
	 * @param terminate the validation check, as a {@link BiPredicate}
	 * @return the result of the {@code terminate} check
	 */
	public static <T> boolean race(T initial, Function<? super T, ? extends T> race,
			Predicate<? super T> stopRace,
			BiPredicate<? super T, ? super T> terminate) {

		Scheduler w1 = Schedulers.newSingle("w1");
		Scheduler w2 = Schedulers.newSingle("w2");

		try {

			AtomicReference<T> ref1 = new AtomicReference<>();
			CountDownLatch cdl1 = new CountDownLatch(1);
			AtomicReference<T> ref2 = new AtomicReference<>();
			CountDownLatch cdl2 = new CountDownLatch(1);

			w1.schedule(() -> {
				T state = initial;
				while (!stopRace.test(state)) {
					state = race.apply(state);
					LockSupport.parkNanos(1L);
				}
				ref1.set(state);
				cdl1.countDown();
			});
			w2.schedule(() -> {
				T state = initial;
				while (!stopRace.test(state)) {
					state = race.apply(state);
					LockSupport.parkNanos(1L);
				}
				ref2.set(state);
				cdl2.countDown();
			});

			try {
				cdl1.await();
				cdl2.await();
			}
			catch (InterruptedException e) {
				Thread.currentThread()
				      .interrupt();
			}

			return terminate.test(ref1.get(), ref2.get());
		}
		finally {
			w1.dispose();
			w2.dispose();
		}
	}

	/**
	 * Synchronizes the execution of two {@link Runnable} as much as possible
	 * to test race conditions. The method blocks until both have run to completion.
	 * @param r1 the first runnable
	 * @param r2 the second runnable
	 */
	public static void race(final Runnable r1, final Runnable r2) {
		race(r1, r2, Schedulers.single());
	}

	/**
	 * Synchronizes the execution of two {@link Runnable} as much as possible
	 * to test race conditions. The method blocks until both have run to completion.
	 * @param r1 the first runnable
	 * @param r2 the second runnable
	 * @param s the {@link Scheduler} on which to execute the runnables
	 */
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
			throw Exceptions.multiple(errors);
		}
	}

}

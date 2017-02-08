/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.publisher;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import static org.assertj.core.api.Assertions.assertThat;

public class OperatorsTest {

	volatile long testRequest;
	static final AtomicLongFieldUpdater<OperatorsTest> TEST_REQUEST =
			AtomicLongFieldUpdater.newUpdater(OperatorsTest.class, "testRequest");


	<T> boolean race(T initial, Function<? super T, ? extends T> race,
			Predicate<? super T> stopRace,
			BiPredicate<? super T, ? super T> terminate) {

		Scheduler.Worker w1 = Schedulers.elastic()
		                                .createWorker();
		Scheduler.Worker w2 = Schedulers.elastic()
		                                .createWorker();

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

	@Test
	public void addAndGetAtomicLong() {
		AtomicLong test = new AtomicLong();
		race(0L,
				s -> Operators.addAndGet(test, 1),
				a -> a >= 100_000L,
				(a, b) -> a == b
		);

		assertThat(Operators.addAndGet(test, -1_000_000L))
				.isEqualTo(Long.MAX_VALUE);

		test.set(1L);

		assertThat(Operators.addAndGet(test, Long.MAX_VALUE))
				.isEqualTo(Long.MAX_VALUE);

		assertThat(Operators.addAndGet(test, 0))
				.isEqualTo(Long.MAX_VALUE);
	}

	@Test
	public void addAndGetAtomicField() {
		TEST_REQUEST.set(this, 0L);

		race(0L,
				s -> Operators.addAndGet(TEST_REQUEST, this, 1),
				a -> a >= 100_000L,
				(a, b) -> a == b
		);

		assertThat(Operators.addAndGet(TEST_REQUEST, this, -1_000_000L))
				.isEqualTo(Long.MAX_VALUE);

		TEST_REQUEST.set(this, 1L);

		assertThat(Operators.addAndGet(TEST_REQUEST, this, Long.MAX_VALUE))
				.isEqualTo(Long.MAX_VALUE);

		assertThat(Operators.addAndGet(TEST_REQUEST, this, 0))
				.isEqualTo(Long.MAX_VALUE);
	}


	@Test
	public void addCap() {
		assertThat(Operators.addCap(1, 2)).isEqualTo(3);
		assertThat(Operators.addCap(1, Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
		assertThat(Operators.addCap(0, -1)).isEqualTo(Long.MAX_VALUE);
	}

	@Test
	public void constructor(){
		assertThat(new Operators(){}).isNotNull();
	}

	@Test
	public void castAsQueueSubscription() {
		Fuseable.QueueSubscription<String> qs = new Fuseable.SynchronousSubscription<String>() {
			@Override
			public String poll() {
				return null;
			}

			@Override
			public int size() {
				return 0;
			}

			@Override
			public boolean isEmpty() {
				return false;
			}

			@Override
			public void clear() {

			}

			@Override
			public void request(long n) {

			}

			@Override
			public void cancel() {

			}
		};

		Subscription s = Operators.cancelledSubscription();

		assertThat(Operators.as(qs)).isEqualTo(qs);
		assertThat(Operators.as(s)).isNull();
	}

	@Test
	public void cancelledSubscription(){
		Operators.CancelledSubscription es =
				(Operators.CancelledSubscription)Operators.cancelledSubscription();

		assertThat((Object)es).isEqualTo(Operators.CancelledSubscription.INSTANCE);

		//Noop
		es.cancel();
		es.request(-1);

		assertThat(es.isCancelled()).isTrue();
	}

	@Test //TODO review use of checkRequest
	public void checkRequestThrow(){
		Operators.checkRequest(1);

		try{
			Operators.checkRequest(0);
			Assert.fail();
		}
		catch (IllegalArgumentException iae){
			assertThat(iae).hasMessage(Exceptions.nullOrNegativeRequestException(0)
			                                     .getMessage());
		}

		try{
			Operators.checkRequest(-1);
			Assert.fail();
		}
		catch (IllegalArgumentException iae){
			assertThat(iae).hasMessage(Exceptions.nullOrNegativeRequestException(-1)
			                                     .getMessage());
		}
	}

	@Test //TODO review use of checkRequest
	public void checkRequestErrorSubscriber(){
		DirectProcessor<Void> p = DirectProcessor.create();

		assertThat(Operators.checkRequest(1, null)).isTrue();

		assertThat(Operators.checkRequest(1, p)).isTrue();
		assertThat(p.getError()).isNull();

		assertThat(Operators.checkRequest(0, p)).isFalse();
		assertThat(p.getError()).hasMessage(Exceptions.nullOrNegativeRequestException(0)
		                                               .getMessage());

		p = DirectProcessor.create();
		assertThat(Operators.checkRequest(-1, p)).isFalse();
		assertThat(p.getError()).hasMessage(Exceptions.nullOrNegativeRequestException(-1)
		                                              .getMessage());
	}

	@Test
	public void noopFluxCancelled(){
		Flux.CANCELLED.dispose(); //noop
	}

	@Test
	public void drainSubscriber() {
		AtomicBoolean requested = new AtomicBoolean();
		AtomicBoolean errored = new AtomicBoolean();
		try {
			Hooks.onErrorDropped(e -> {
				assertThat(Exceptions.isErrorCallbackNotImplemented(e)).isTrue();
				assertThat(e.getCause()).hasMessage("test");
				errored.set(true);
			});
			Flux.from(s -> {
				assertThat(s).isEqualTo(Operators.drainSubscriber());
				s.onSubscribe(new Subscription() {
					@Override
					public void request(long n) {
						assertThat(n).isEqualTo(Long.MAX_VALUE);
						requested.set(true);
					}

					@Override
					public void cancel() {

					}
				});
				s.onNext("ignored"); //dropped
				s.onComplete(); //dropped
				s.onError(new Exception("test"));
			})
			    .subscribe(Operators.drainSubscriber());

			assertThat(requested.get()).isTrue();
			assertThat(errored.get()).isTrue();
		}
		finally {
			Hooks.resetOnErrorDropped();
		}
	}
}

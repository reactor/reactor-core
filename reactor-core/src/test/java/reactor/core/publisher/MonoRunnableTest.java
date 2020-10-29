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

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MonoRunnableTest {

	@Test
	public void nullValue() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new MonoRunnable<>(null);
		});
	}

	@Test
	public void normal() {
		AssertSubscriber<Void> ts = AssertSubscriber.create();

		Mono.<Void>fromRunnable(() -> {
		})
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Void> ts = AssertSubscriber.create(0);

		Mono.<Void>fromRunnable(() -> {
		})
		    .hide()
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertComplete()
		  .assertNoError();

	}

	@Test
	public void asyncRunnable() {
		AtomicReference<Thread> t = new AtomicReference<>();
		StepVerifier.create(Mono.fromRunnable(() -> t.set(Thread.currentThread()))
		                        .subscribeOn(Schedulers.single()))
		            .verifyComplete();

		assertThat(t).isNotNull();
		assertThat(t).isNotEqualTo(Thread.currentThread());
	}

	@Test
	public void asyncRunnableBackpressured() {
		AtomicReference<Thread> t = new AtomicReference<>();
		StepVerifier.create(Mono.fromRunnable(() -> t.set(Thread.currentThread()))
		                        .subscribeOn(Schedulers.single()), 0)
		            .verifyComplete();

		assertThat(t).isNotNull();
		assertThat(t).isNotEqualTo(Thread.currentThread());
	}

	@Test
	public void runnableThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Mono.fromRunnable(() -> {
			throw new RuntimeException("forced failure");
		})
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void nonFused() {
		AssertSubscriber<Void> ts = AssertSubscriber.create();

		Mono.<Void>fromRunnable(() -> {
		})
		    .subscribe(ts);

		ts.assertNonFuseableSource()
		  .assertNoValues();
	}

	@Test
	public void test() {
		int c[] = { 0 };
		Flux.range(1, 1000)
		    .flatMap(v -> Mono.fromRunnable(() -> { c[0]++; }))
		    .ignoreElements()
		    .block();

		assertThat(c[0]).isEqualTo(1000);
	}

	//see https://github.com/reactor/reactor-core/issues/1503
	@Test
	public void runnableCancelledBeforeRun() {
		AtomicBoolean actual = new AtomicBoolean(true);
		Mono<?> mono = Mono.fromRunnable(() -> actual.set(false))
		                   .doOnSubscribe(Subscription::cancel);

		StepVerifier.create(mono)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(1))
		            .thenCancel()
		            .verify();

		assertThat(actual).as("cancelled before run").isTrue();
	}

	//see https://github.com/reactor/reactor-core/issues/1503
	//see https://github.com/reactor/reactor-core/issues/1504
	@Test
	public void runnableSubscribeToCompleteMeasurement() {
		AtomicLong subscribeTs = new AtomicLong();
		Mono<Object> mono = Mono.fromRunnable(() -> {
			try {
				Thread.sleep(500);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		})
		                      .doOnSubscribe(sub -> subscribeTs.set(-1 * System.nanoTime()))
		                      .doFinally(fin -> subscribeTs.addAndGet(System.nanoTime()));

		StepVerifier.create(mono)
		            .verifyComplete();

		assertThat(TimeUnit.NANOSECONDS.toMillis(subscribeTs.get())).isCloseTo(500L, Offset.offset(50L));
	}

	@Test
	public void scanOperator(){
		MonoRunnable<String> test = new MonoRunnable<>(() -> {});

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}

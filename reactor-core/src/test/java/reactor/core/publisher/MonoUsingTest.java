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
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;

public class MonoUsingTest {

	@Test
	public void resourceSupplierNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Mono.using(null, r -> Mono.empty(), r -> {
			}, false);
		});
	}

	@Test
	public void sourceFactoryNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Mono.using(() -> 1, null, r -> {
			}, false);
		});
	}

	@Test
	public void resourceCleanupNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Mono.using(() -> 1, r -> Mono.empty(), null, false);
		});
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicInteger cleanup = new AtomicInteger();

		Mono.using(() -> 1, r -> Mono.just(1), cleanup::set, false)
		    .doAfterTerminate(() -> assertThat(cleanup).hasValue(0))
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();

		assertThat(cleanup).hasValue(1);
	}

	@Test
	public void normalEager() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicInteger cleanup = new AtomicInteger();

		Mono.using(() -> 1, r -> Mono.just(1)
		                             .doOnTerminate(() ->  assertThat(cleanup).hasValue(0)),
				cleanup::set,
				true)
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();

		assertThat(cleanup).hasValue(1);
	}

	void checkCleanupExecutionTime(boolean eager, boolean fail) {
		AtomicInteger cleanup = new AtomicInteger();
		AtomicBoolean before = new AtomicBoolean();

		AssertSubscriber<Integer> ts = new AssertSubscriber<Integer>() {
			@Override
			public void onError(Throwable t) {
				super.onError(t);
				before.set(cleanup.get() != 0);
			}

			@Override
			public void onComplete() {
				super.onComplete();
				before.set(cleanup.get() != 0);
			}
		};

		Mono.using(() -> 1, r -> {
			if (fail) {
				return Mono.error(new RuntimeException("forced failure"));
			}
			return Mono.just(1);
		}, cleanup::set, eager)
		    .subscribe(ts);

		if (fail) {
			ts.assertNoValues()
			  .assertError(RuntimeException.class)
			  .assertNotComplete()
			  .assertErrorMessage("forced failure");
		}
		else {
			ts.assertValues(1)
			  .assertComplete()
			  .assertNoError();
		}

		assertThat(cleanup).hasValue(1);
		assertThat(before.get()).isEqualTo(eager);
	}

	@Test
	public void checkNonEager() {
		checkCleanupExecutionTime(false, false);
	}

	@Test
	public void checkEager() {
		checkCleanupExecutionTime(true, false);
	}

	@Test
	public void checkErrorNonEager() {
		checkCleanupExecutionTime(false, true);
	}

	@Test
	public void checkErrorEager() {
		checkCleanupExecutionTime(true, true);
	}

	@Test
	public void resourceThrowsEager() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		AtomicInteger cleanup = new AtomicInteger();

		Mono.using(() -> {
			throw new RuntimeException("forced failure");
		}, r -> Mono.just(1), cleanup::set, false)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");

		assertThat(cleanup).hasValue(0);
	}

	@Test
	public void factoryThrowsEager() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		AtomicInteger cleanup = new AtomicInteger();

		Mono.using(() -> 1, r -> {
			throw new RuntimeException("forced failure");
		}, cleanup::set, false)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");

		assertThat(cleanup).hasValue(1);
	}

	@Test
	public void factoryReturnsNull() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		AtomicInteger cleanup = new AtomicInteger();

		Mono.<Integer, Integer>using(() -> 1,
				r -> null,
				cleanup::set,
				false).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);

		assertThat(cleanup).hasValue(1);
	}

	@Test
	public void subscriberCancels() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicInteger cleanup = new AtomicInteger();

		Sinks.One<Integer> tp = Sinks.unsafe().one();

		Mono.using(() -> 1, r -> tp.asMono(), cleanup::set, true)
		    .subscribe(ts);

		assertThat(tp.currentSubscriberCount()).as("tp has subscriber").isPositive();

		tp.tryEmitValue(1).orThrow();

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();


		assertThat(cleanup).hasValue(1);
	}

	@Test
	public void sourceFactoryAndResourceCleanupThrow() {
		RuntimeException sourceEx = new IllegalStateException("sourceFactory");
		RuntimeException cleanupEx = new IllegalStateException("resourceCleanup");

		Condition<? super Throwable> suppressingFactory = new Condition<>(
				e -> {
					Throwable[] suppressed = e.getSuppressed();
					return suppressed != null && suppressed.length == 1 && suppressed[0] == sourceEx;
				}, "suppressing <%s>", sourceEx);

		Mono<String> test = new MonoUsing<>(() -> "foo",
				o -> { throw sourceEx; },
				s -> { throw cleanupEx; },
				false);

		StepVerifier.create(test)
		            .verifyErrorMatches(
				            e -> assertThat(e)
						            .hasMessage("resourceCleanup")
						            .is(suppressingFactory) != null);

	}

	@Test
	public void cleanupIsRunBeforeOnNext_fusedEager() {
		Mono.using(() -> "resource", s -> Mono.just(s.length()),
				res -> { throw new IllegalStateException("boom"); },
				true)
		    .as(StepVerifier::create)
		    .expectFusion()
		    .expectErrorMessage("boom")
		    .verifyThenAssertThat()
		    .hasDiscarded(8)
		    .hasNotDroppedElements()
		    .hasNotDroppedErrors();
	}

	@Test
	public void cleanupIsRunBeforeOnNext_normalEager() {
		Mono.using(() -> "resource", s -> Mono.just(s.length()).hide(),
				res -> { throw new IllegalStateException("boom"); })
		    .as(StepVerifier::create)
		    .expectNoFusionSupport()
		    .expectErrorMessage("boom")
		    .verifyThenAssertThat()
		    .hasDiscarded(8)
		    .hasNotDroppedElements()
		    .hasNotDroppedErrors();
	}

	@Test
	public void cleanupDropsThrowable_fusedNotEager() {
		Mono.using(() -> "resource", s -> Mono.just(s.length()),
				res -> { throw new IllegalStateException("boom"); },
				false)
		    .as(StepVerifier::create)
		    .expectFusion()
		    .expectNext(8)
		    .expectComplete()
		    .verifyThenAssertThat()
		    .hasNotDiscardedElements()
		    .hasNotDroppedElements()
		    .hasDroppedErrorWithMessage("boom");
	}

	@Test
	public void cleanupDropsThrowable_normalNotEager() {
		Mono.using(() -> "resource", s -> Mono.just(s.length()).hide(),
				res -> { throw new IllegalStateException("boom"); },
				false)
		    .as(StepVerifier::create)
		    .expectNoFusionSupport()
		    .expectNext(8)
		    .expectComplete()
		    .verifyThenAssertThat()
		    .hasNotDiscardedElements()
		    .hasNotDroppedElements()
		    .hasDroppedErrorWithMessage("boom");
	}

	@Test
	public void smokeTestMapReduceGuardedByCleanup_normalEager() {
		AtomicBoolean cleaned = new AtomicBoolean();
		Mono.using(() -> cleaned,
				ab -> Flux.just("foo", "bar", "baz")
				          .delayElements(Duration.ofMillis(100))
				          .count()
				          .map(i -> "" + i + ab.get())
				          .hide(),
				ab -> ab.set(true),
				true)
		    .as(StepVerifier::create)
		    .expectNoFusionSupport()
		    .expectNext("3false")
		    .expectComplete()
		    .verify();

		assertThat(cleaned).isTrue();
	}

	@Test
	public void smokeTestMapReduceGuardedByCleanup_fusedEager() {
		AtomicBoolean cleaned = new AtomicBoolean();
		Mono.using(() -> cleaned,
				ab -> Flux.just("foo", "bar", "baz")
				          .delayElements(Duration.ofMillis(100))
				          .count()
				          .map(i -> "" + i + ab.get()),
				ab -> ab.set(true),
				true)
		    .as(StepVerifier::create)
		    .expectFusion()
		    .expectNext("3false")
		    .expectComplete()
		    .verify();

		assertThat(cleaned).isTrue();
	}

	@Test
	public void smokeTestMapReduceGuardedByCleanup_normalNotEager() {
		AtomicBoolean cleaned = new AtomicBoolean();
		Mono.using(() -> cleaned,
				ab -> Flux.just("foo", "bar", "baz")
				          .delayElements(Duration.ofMillis(100))
				          .count()
				          .map(i -> "" + i + ab.get())
				          .hide(),
				ab -> ab.set(true),
				false)
		    .as(StepVerifier::create)
		    .expectNoFusionSupport()
		    .expectNext("3false")
		    .expectComplete()
		    .verify();

		//since the handler is executed after onComplete, we allow some delay
		await().atMost(100, TimeUnit.MILLISECONDS)
		       .with().pollInterval(10, TimeUnit.MILLISECONDS)
		       .untilAsserted(assertThat(cleaned)::isTrue);
	}

	@Test
	public void smokeTestMapReduceGuardedByCleanup_fusedNotEager() {
		AtomicBoolean cleaned = new AtomicBoolean();
		Mono.using(() -> cleaned,
				ab -> Flux.just("foo", "bar", "baz")
				          .delayElements(Duration.ofMillis(100))
				          .count()
				          .map(i -> "" + i + ab.get()),
				ab -> ab.set(true),
				false)
		    .as(StepVerifier::create)
		    .expectFusion()
		    .expectNext("3false")
		    .expectComplete()
		    .verify();

		//since the handler is executed after onComplete, we allow some delay
		await().atMost(100, TimeUnit.MILLISECONDS)
		       .with().pollInterval(10, TimeUnit.MILLISECONDS)
		       .untilAsserted(assertThat(cleaned)::isTrue);
	}

	@Test
	public void scanOperator(){
		MonoUsing<Integer, Integer> test = new MonoUsing<>(() -> 1, r -> Mono.just(1), c -> {}, false);

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isNull();
	}


	@Test
	public void scanSubscriber() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		MonoUsing.MonoUsingSubscriber<Integer, ?> test = new MonoUsing.MonoUsingSubscriber<>(actual, rc -> {}, "foo", false, false);

		final Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}
}

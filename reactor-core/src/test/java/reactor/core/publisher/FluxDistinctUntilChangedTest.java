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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.FluxDistinctTest.DistinctDefault;
import reactor.core.publisher.FluxDistinctTest.DistinctDefaultCancel;
import reactor.core.publisher.FluxDistinctTest.DistinctDefaultError;
import reactor.core.publisher.FluxDistinctUntilChanged.DistinctUntilChangedConditionalSubscriber;
import reactor.core.publisher.FluxDistinctUntilChanged.DistinctUntilChangedSubscriber;
import reactor.test.MemoryUtils.RetainedDetector;
import reactor.test.MockUtils;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class FluxDistinctUntilChangedTest extends FluxOperatorTest<String, String> {

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.distinctUntilChanged(d -> {
					throw exception();
				})),

				scenario(f -> f.distinctUntilChanged(d -> null))

		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.distinctUntilChanged())
		);
	}

	@Test
	public void sourceNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new FluxDistinctUntilChanged<>(null, v -> v, (k1, k2) -> true);
		});
	}

	@Test
	public void keyExtractorNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.never().distinctUntilChanged(null);
		});
	}

	@Test
	public void predicateNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.never().distinctUntilChanged(v -> v, null);
		});
	}

	@Test
	public void allDistinct() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .distinctUntilChanged(v -> v)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void allDistinctBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .distinctUntilChanged(v -> v)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void someRepetition() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.just(1, 1, 2, 2, 1, 1, 2, 2, 1, 2, 3, 3)
		    .distinctUntilChanged(v -> v)
		    .subscribe(ts);

		ts.assertValues(1, 2, 1, 2, 1, 2, 3)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void someRepetitionBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.just(1, 1, 2, 2, 1, 1, 2, 2, 1, 2, 3, 3)
		    .distinctUntilChanged(v -> v)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(4);

		ts.assertValues(1, 2, 1, 2, 1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 1, 2, 1, 2, 3)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void withKeySelector() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .distinctUntilChanged(v -> v / 3)
		    .subscribe(ts);

		ts.assertValues(1, 3, 6, 9)
		  .assertComplete()
		  .assertNoError();
	}
	
	@Test
	public void withKeyComparator() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .distinctUntilChanged(Function.identity(), (a,b) -> b - a < 4)
		    .subscribe(ts);

		ts.assertValues(1, 5, 9)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void keySelectorThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .distinctUntilChanged(v -> {
			    throw new RuntimeException("forced failure");
		    })
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void keyComparatorThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .distinctUntilChanged(Function.identity(), (v1,v2) -> {
			    throw new RuntimeException("forced failure");
		    })
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void allDistinctConditional() {
		Sinks.Many<Integer> dp = Sinks.unsafe().many().multicast().directBestEffort();

		AssertSubscriber<Integer> ts = dp.asFlux()
										 .distinctUntilChanged()
		                                 .filter(v -> true)
		                                 .subscribeWith(AssertSubscriber.create());

		dp.emitNext(1, FAIL_FAST);
		dp.emitNext(2, FAIL_FAST);
		dp.emitNext(3, FAIL_FAST);
		dp.emitComplete(FAIL_FAST);

		ts.assertValues(1, 2, 3).assertComplete();
	}

	@Test
	public void scanOperator(){
	    FluxDistinctUntilChanged<Integer, Integer> test = new FluxDistinctUntilChanged<>(Flux.just(1), v -> v, (k1, k2) -> true);

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscriber() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		DistinctUntilChangedSubscriber<String, Integer> test = new DistinctUntilChangedSubscriber<>(
			actual, String::hashCode, Objects::equals);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	public void scanConditionalSubscriber() {
		@SuppressWarnings("unchecked")
		Fuseable.ConditionalSubscriber<String> actual = Mockito.mock(MockUtils.TestScannableConditionalSubscriber.class);
		DistinctUntilChangedConditionalSubscriber<String, Integer> test = new DistinctUntilChangedConditionalSubscriber<>(
				actual, String::hashCode, Objects::equals);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}

	@Test
	public void distinctUntilChangedDefaultWithHashcodeCollisions() {
		Object foo = new Object() {
			@Override
			public int hashCode() {
				return 1;
			}
		};
		Object bar = new Object() {
			@Override
			public int hashCode() {
				return 1;
			}
		};

		assertThat(foo).isNotEqualTo(bar)
		               .hasSameHashCodeAs(bar);

		StepVerifier.create(Flux.just(foo, bar).distinctUntilChanged())
		            .expectNext(foo, bar)
		            .verifyComplete();
	}

	@Test
	public void distinctUntilChangedDefaultDoesntRetainObjects() throws InterruptedException {
		RetainedDetector retainedDetector = new RetainedDetector();
		Flux<DistinctDefault> test = Flux.range(1, 100)
		                                                  .map(i -> retainedDetector.tracked(new DistinctDefault(i)))
		                                                  .distinctUntilChanged();

		StepVerifier.create(test)
		            .expectNextCount(100)
		            .verifyComplete();

		System.gc();
		await().untilAsserted(() -> {
			assertThat(retainedDetector.finalizedCount())
					.as("none retained")
					.isEqualTo(100);
		});
	}

	@Test
	public void distinctUntilChangedDefaultErrorDoesntRetainObjects() throws InterruptedException {
		RetainedDetector retainedDetector = new RetainedDetector();
		Flux<DistinctDefaultError> test = Flux.range(1, 100)
		                                                  .map(i -> retainedDetector.tracked(new DistinctDefaultError(i)))
		                                                  .concatWith(Mono.error(new IllegalStateException("boom")))
		                                                  .distinctUntilChanged();

		StepVerifier.create(test)
		            .expectNextCount(100)
		            .verifyErrorMessage("boom");

		System.gc();
		await().untilAsserted(() -> {
			assertThat(retainedDetector.finalizedCount())
					.as("none retained after error")
					.isEqualTo(100);
		});
	}

	@Test
	public void distinctUntilChangedDefaultCancelDoesntRetainObjects() throws InterruptedException {
		RetainedDetector retainedDetector = new RetainedDetector();
		Flux<DistinctDefaultCancel> test = Flux.range(1, 100)
		                                                  .map(i -> retainedDetector.tracked(new DistinctDefaultCancel(i)))
		                                                  .concatWith(Mono.error(new IllegalStateException("boom")))
		                                                  .distinctUntilChanged()
		                                                  .take(50);

		StepVerifier.create(test)
		            .expectNextCount(50)
		            .verifyComplete();

		System.gc();
		await().untilAsserted(() -> {
			System.gc();
			retainedDetector.assertAllFinalized();
		});
	}

	@Test
	public void doesntRetainObjectsWithForcedCompleteOnSubscriber() {
		RetainedDetector retainedDetector = new RetainedDetector();
		AtomicReference<DistinctDefaultCancel> fooRef = new AtomicReference<>(retainedDetector.tracked(new DistinctDefaultCancel(0)));

		DistinctUntilChangedSubscriber<DistinctDefaultCancel, DistinctDefaultCancel> sub = new DistinctUntilChangedSubscriber<>(
				new BaseSubscriber<DistinctDefaultCancel>() {}, Function.identity(), Objects::equals);
		sub.onSubscribe(Operators.emptySubscription());
		sub.onNext(fooRef.getAndSet(null));

		assertThat(retainedDetector.finalizedCount()).isZero();
		assertThat(retainedDetector.trackedTotal()).isOne();

		sub.onComplete();
		System.gc();

		await()
				.atMost(2, TimeUnit.SECONDS)
				.untilAsserted(retainedDetector::assertAllFinalized);
	}

	@Test
	public void doesntRetainObjectsWithForcedCompleteOnSubscriber_conditional() {
		RetainedDetector retainedDetector = new RetainedDetector();
		AtomicReference<DistinctDefaultCancel> fooRef = new AtomicReference<>(retainedDetector.tracked(new DistinctDefaultCancel(0)));

		DistinctUntilChangedConditionalSubscriber<DistinctDefaultCancel, DistinctDefaultCancel> sub = new DistinctUntilChangedConditionalSubscriber<>(
				Operators.toConditionalSubscriber(new BaseSubscriber<DistinctDefaultCancel>() {}), Function.identity(), Objects::equals);
		sub.onSubscribe(Operators.emptySubscription());
		sub.onNext(fooRef.getAndSet(null));

		assertThat(retainedDetector.finalizedCount()).isZero();
		assertThat(retainedDetector.trackedTotal()).isOne();

		sub.onComplete();
		System.gc();

		await()
				.atMost(2, TimeUnit.SECONDS)
				.untilAsserted(retainedDetector::assertAllFinalized);
	}

	@Test
	public void doesntRetainObjectsWithForcedErrorOnSubscriber() {
		RetainedDetector retainedDetector = new RetainedDetector();
		AtomicReference<DistinctDefaultCancel> fooRef = new AtomicReference<>(retainedDetector.tracked(new DistinctDefaultCancel(0)));

		DistinctUntilChangedSubscriber<DistinctDefaultCancel, DistinctDefaultCancel> sub = new DistinctUntilChangedSubscriber<>(
				new BaseSubscriber<DistinctDefaultCancel>() {
					@Override
					protected void hookOnError(Throwable throwable) { }
				}, Function.identity(), Objects::equals);
		sub.onSubscribe(Operators.emptySubscription());
		sub.onNext(fooRef.getAndSet(null));

		assertThat(retainedDetector.finalizedCount()).isZero();
		assertThat(retainedDetector.trackedTotal()).isOne();

		sub.onError(new IllegalStateException("expected"));
		System.gc();

		await()
				.atMost(2, TimeUnit.SECONDS)
				.untilAsserted(retainedDetector::assertAllFinalized);
	}

	@Test
	public void doesntRetainObjectsWithForcedErrorOnSubscriber_conditional() {
		RetainedDetector retainedDetector = new RetainedDetector();
		AtomicReference<DistinctDefaultCancel> fooRef = new AtomicReference<>(retainedDetector.tracked(new DistinctDefaultCancel(0)));

		DistinctUntilChangedConditionalSubscriber<DistinctDefaultCancel, DistinctDefaultCancel> sub = new DistinctUntilChangedConditionalSubscriber<>(
				Operators.toConditionalSubscriber(new BaseSubscriber<DistinctDefaultCancel>() {
					@Override
					protected void hookOnError(Throwable throwable) { }
				}), Function.identity(), Objects::equals);
		sub.onSubscribe(Operators.emptySubscription());
		sub.onNext(fooRef.getAndSet(null));

		assertThat(retainedDetector.finalizedCount()).isZero();
		assertThat(retainedDetector.trackedTotal()).isOne();

		sub.onError(new IllegalStateException("expected"));
		System.gc();

		await()
				.atMost(2, TimeUnit.SECONDS)
				.untilAsserted(retainedDetector::assertAllFinalized);
	}

	@Test
	public void doesntRetainObjectsWithForcedCancelOnSubscriber() {
		RetainedDetector retainedDetector = new RetainedDetector();
		AtomicReference<DistinctDefaultCancel> fooRef = new AtomicReference<>(retainedDetector.tracked(new DistinctDefaultCancel(0)));

		DistinctUntilChangedSubscriber<DistinctDefaultCancel, DistinctDefaultCancel> sub = new DistinctUntilChangedSubscriber<>(
				new BaseSubscriber<DistinctDefaultCancel>() {}, Function.identity(), Objects::equals);
		sub.onSubscribe(Operators.emptySubscription());
		sub.onNext(fooRef.getAndSet(null));

		assertThat(retainedDetector.finalizedCount()).isZero();
		assertThat(retainedDetector.trackedTotal()).isOne();

		sub.cancel();
		System.gc();

		await()
				.atMost(2, TimeUnit.SECONDS)
				.untilAsserted(retainedDetector::assertAllFinalized);
	}

	@Test
	public void doesntRetainObjectsWithForcedCancelOnSubscriber_conditional() {
		RetainedDetector retainedDetector = new RetainedDetector();
		AtomicReference<DistinctDefaultCancel> fooRef = new AtomicReference<>(retainedDetector.tracked(new DistinctDefaultCancel(0)));

		DistinctUntilChangedConditionalSubscriber<DistinctDefaultCancel, DistinctDefaultCancel> sub = new DistinctUntilChangedConditionalSubscriber<>(
				Operators.toConditionalSubscriber(new BaseSubscriber<DistinctDefaultCancel>() {}), Function.identity(), Objects::equals);
		sub.onSubscribe(Operators.emptySubscription());
		sub.onNext(fooRef.getAndSet(null));

		assertThat(retainedDetector.finalizedCount()).isZero();
		assertThat(retainedDetector.trackedTotal()).isOne();

		sub.cancel();
		System.gc();

		await()
				.atMost(2, TimeUnit.SECONDS)
				.untilAsserted(retainedDetector::assertAllFinalized);
	}

}

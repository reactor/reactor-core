/*
 * Copyright (c) 2016-2024 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.MockUtils;
import reactor.test.ParameterizedTestWithName;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class FluxUsingTest extends FluxOperatorTest<String, String> {

	public static List<Flux<Integer>> sourcesNonEager() {
		return Arrays.asList(
				Flux.using(() -> 1, r -> Flux.range(r, 10), cleanup::set, false),
				Flux.using(() -> cleanup::incrementAndGet, r -> Flux.range(1, 10), false)
		);
	}

	public static List<Flux<Integer>> sourcesEager() {
		return Arrays.asList(
				Flux.using(() -> 1, r -> Flux.range(r, 10), cleanup::set),
				Flux.using(() -> 1, r -> Flux.range(r, 10), cleanup::set, true),
				Flux.using(() -> cleanup::incrementAndGet, r -> Flux.range(1, 10)),
				Flux.using(() -> cleanup::incrementAndGet, r -> Flux.range(1, 10), true)
		);
	}

	public static List<Flux<Integer>> sourcesFailNonEager() {
		return Arrays.asList(
				Flux.using(() -> 1, r -> Flux.error(new RuntimeException("forced failure")), cleanup::set, false),
				Flux.using(() -> cleanup::incrementAndGet, r -> Flux.error(new RuntimeException("forced failure")), false)
		);
	}

	public static List<Flux<Integer>> sourcesFailEager() {
		return Arrays.asList(
				Flux.using(() -> 1, r -> Flux.error(new RuntimeException("forced failure")), cleanup::set),
				Flux.using(() -> 1, r -> Flux.error(new RuntimeException("forced failure")), cleanup::set, true),
				Flux.using(() -> cleanup::incrementAndGet, r -> Flux.error(new RuntimeException("forced failure"))),
				Flux.using(() -> cleanup::incrementAndGet, r -> Flux.error(new RuntimeException("forced failure")), true)
		);
	}

	public static List<Flux<Object>> resourcesThrow() {
		return Arrays.asList(
				// non eager
				Flux.using(() -> { throw new RuntimeException("forced failure"); }, r -> Flux.range(r, 10), cleanup::set, false),
				Flux.using(() -> { throw new RuntimeException("forced failure"); }, r -> Flux.range(1, 10), false),
				// eager
				Flux.using(() -> { throw new RuntimeException("forced failure"); }, r -> Flux.range(r, 10), cleanup::set),
				Flux.using(() -> { throw new RuntimeException("forced failure"); }, r -> Flux.range(r, 10), cleanup::set, true),
				Flux.using(() -> { throw new RuntimeException("forced failure"); }, r -> Flux.range(1, 10)),
				Flux.using(() -> { throw new RuntimeException("forced failure"); }, r -> Flux.range(1, 10), true)
		);
	}

	public static List<Flux<Integer>> sourcesThrowNonEager() {
		return Arrays.asList(
				Flux.using(() -> 1, r -> { throw new RuntimeException("forced failure"); }, cleanup::set, false),
				Flux.using(() -> cleanup::incrementAndGet, r -> { throw new RuntimeException("forced failure"); }, false)
		);
	}

	public static List<Flux<Integer>> sourcesThrowEager() {
		return Arrays.asList(
				Flux.using(() -> 1, r -> { throw new RuntimeException("forced failure"); }, cleanup::set),
				Flux.using(() -> 1, r -> { throw new RuntimeException("forced failure"); }, cleanup::set, true),
				Flux.using(() -> cleanup::incrementAndGet, r -> { throw new RuntimeException("forced failure"); }),
				Flux.using(() -> cleanup::incrementAndGet, r -> { throw new RuntimeException("forced failure"); }, true)
		);
	}

	private static final AtomicInteger cleanup = new AtomicInteger();

	@BeforeEach
	public void before() {
		cleanup.set(0);
	}

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.fusionMode(Fuseable.ANY)
		                     .fusionModeThreadBarrier(Fuseable.ANY)
		                     .shouldHitDropNextHookAfterTerminate(false)
		                     .shouldHitDropErrorHookAfterTerminate(false);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> Flux.using(() -> {
					throw exception();
				}, c -> f, c -> {}))
						.fusionMode(Fuseable.NONE),

				scenario(f -> Flux.using(() -> 0, c -> null, c -> {}))
						.fusionMode(Fuseable.NONE),

				scenario(f -> Flux.using(() -> 0, c -> {
					throw exception();
				}, c -> {}))
						.fusionMode(Fuseable.NONE)

				/*scenario(f -> Flux.using(() -> 0, c -> f, c -> {
					throw exception();
				}))
				.verifier(step -> {
					try {
						step.expectNext(item(0), item(1), item(2)).verifyErrorMessage
								("test");
					}
					catch (Exception t){
						assertThat(Exceptions.unwrap(t)).hasMessage("test");
					}
				})*/

		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> Flux.using(() -> 0, c -> f, c -> {})),

				scenario(f -> Flux.using(() -> 0, c -> f, c -> {}, false))
						.shouldAssertPostTerminateState(false)
		);
	}

	@Test
	public void resourceSupplierNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.using(null, r -> Flux.empty(), r -> {
			}, false);
		});
	}

	@Test
	public void sourceFactoryNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.using(() -> 1, null, r -> {
			}, false);
		});
	}

	@Test
	public void resourceCleanupNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.using(() -> 1, r -> Flux.empty(), null, false);
		});
	}

	@ParameterizedTestWithName
	@MethodSource("sourcesNonEager")
	public void normal(Flux<Integer> source) {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		source.doAfterTerminate(() -> assertThat(cleanup).hasValue(0)).subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();

		assertThat(cleanup).hasValue(1);
	}

	@ParameterizedTestWithName
	@MethodSource("sourcesEager")
	public void normalEager(Flux<Integer> source) {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		source.doFinally(event -> assertThat(cleanup).hasValue(0))
			  .doOnTerminate(() -> assertThat(cleanup).hasValue(1))
			  .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();

		assertThat(cleanup).hasValue(1);
	}

	void checkCleanupExecutionTime(Flux<Integer> source, boolean eager, boolean fail) {
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

		source.subscribe(ts);

		if (fail) {
			ts.assertNoValues()
			  .assertError(RuntimeException.class)
			  .assertNotComplete()
			  .assertErrorMessage("forced failure");
		}
		else {
			ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
			  .assertComplete()
			  .assertNoError();
		}

		assertThat(cleanup).hasValue(1);
		assertThat(before.get()).isEqualTo(eager);
	}

	@ParameterizedTestWithName
	@MethodSource("sourcesNonEager")
	public void checkNonEager(Flux<Integer> source) {
		checkCleanupExecutionTime(source, false, false);
	}

	@ParameterizedTestWithName
	@MethodSource("sourcesEager")
	public void checkEager(Flux<Integer> source) {
		checkCleanupExecutionTime(source, true, false);
	}

	@ParameterizedTestWithName
	@MethodSource("sourcesFailNonEager")
	public void checkErrorNonEager(Flux<Integer> source) {
		checkCleanupExecutionTime(source, false, true);
	}

	@ParameterizedTestWithName
	@MethodSource("sourcesFailEager")
	public void checkErrorEager(Flux<Integer> source) {
		checkCleanupExecutionTime(source, true, true);
	}

	@ParameterizedTestWithName
	@MethodSource("resourcesThrow")
	public void resourceThrows(Flux<Object> source) {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		source.subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");

		assertThat(cleanup).hasValue(0);
	}

	@ParameterizedTestWithName
	@MethodSource("sourcesThrowNonEager")
	public void factoryThrowsNonEager(Flux<Object> source) {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		source.doAfterTerminate(() -> assertThat(cleanup).hasValue(0)).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");

		assertThat(cleanup).hasValue(1);
	}

	@ParameterizedTestWithName
	@MethodSource("sourcesThrowEager")
	public void factoryThrowsEager(Flux<Object> source) {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		source.doFinally(event -> assertThat(cleanup).hasValue(0))
			  .doOnTerminate(() -> assertThat(cleanup).hasValue(1))
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

		Flux.<Integer, Integer>using(() -> 1,
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

		Sinks.Many<Integer> tp = Sinks.unsafe().many().multicast().directBestEffort();

		Flux.using(() -> 1, r -> tp.asFlux(), cleanup::set, true)
		    .subscribe(ts);

		assertThat(tp.currentSubscriberCount()).as("tp has subscriber").isPositive();

		tp.emitNext(1, FAIL_FAST);

		ts.assertValues(1)
		  .assertNotComplete()
		  .assertNoError();

		ts.cancel();

		tp.emitNext(2, FAIL_FAST);

		ts.assertValues(1)
		  .assertNotComplete()
		  .assertNoError();

		assertThat(tp.currentSubscriberCount()).as("tp has subscriber").isZero();

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

		Flux<String> test = new FluxUsing<>(() -> "foo",
				o -> { throw sourceEx; },
				s -> { throw cleanupEx; },
				false);

		StepVerifier.create(test)
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .hasMessage("resourceCleanup")
				            .is(suppressingFactory));

	}

	@Test
	public void scanOperator(){
		FluxUsing<Integer, Integer> test = new FluxUsing<>(() -> 1, r -> Flux.range(r, 10), cleanup::set, false);

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
    public void scanSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxUsing.UsingSubscriber<Integer, String> test = new FluxUsing.UsingSubscriber<>(actual,
        		s -> {}, "", true);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onComplete();
        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

	@Test
    public void scanConditionalSubscriber() {
		@SuppressWarnings("unchecked")
		Fuseable.ConditionalSubscriber<Integer> actual = Mockito.mock(MockUtils.TestScannableConditionalSubscriber.class);
		FluxUsing.UsingConditionalSubscriber<Integer, String> test =
				new FluxUsing.UsingConditionalSubscriber<>(actual, s -> {}, "", true);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onComplete();
        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

    @Test
    public void scanFuseableSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxUsing.UsingFuseableSubscriber<Integer, String> test =
				new FluxUsing.UsingFuseableSubscriber<>(actual, s -> {}, "", true);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onComplete();
        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

}

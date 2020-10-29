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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.MockUtils;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class FluxUsingTest extends FluxOperatorTest<String, String> {

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

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicInteger cleanup = new AtomicInteger();

		Flux.using(() -> 1, r -> Flux.range(r, 10), cleanup::set, false)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();

		assertThat(cleanup).hasValue(1);
	}

	@Test
	public void normalEager() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicInteger cleanup = new AtomicInteger();

		Flux.using(() -> 1, r -> Flux.range(r, 10), cleanup::set)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
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

		Flux.using(() -> 1, r -> {
			if (fail) {
				return Flux.error(new RuntimeException("forced failure"));
			}
			return Flux.range(r, 10);
		}, cleanup::set, eager)
		    .subscribe(ts);

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

		Flux.using(() -> {
			throw new RuntimeException("forced failure");
		}, r -> Flux.range(1, 10), cleanup::set, false)
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

		Flux.using(() -> 1, r -> {
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

		AtomicInteger cleanup = new AtomicInteger();

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
		AtomicInteger cleanup = new AtomicInteger();

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

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

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.assertj.core.api.Condition;
import org.junit.jupiter.api.Test;

import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.ParameterizedTestWithName;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;

public class MonoUsingTest {

	public static List<CleanupCase<Integer>> sourcesNonEager() {
		return Arrays.asList(
				new CleanupCase<Integer>("sourceNonEager") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> 1, Mono::just, cleanup::set, false);
					}
				},
				new CleanupCase<Integer>("autocloseableNonEager") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> cleanup::incrementAndGet, r -> Mono.just(1), false);
					}
				}
		);
	}

	public static List<CleanupCase<Integer>> sourcesEager() {
		return Arrays.asList(
				new CleanupCase<Integer>("sourceEager") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> 1, Mono::just, cleanup::set);
					}
				},
				new CleanupCase<Integer>("sourceEagerFlag") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> 1, Mono::just, cleanup::set, true);
					}
				},
				new CleanupCase<Integer>("autocloseableEager") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> cleanup::incrementAndGet, r -> Mono.just(1));
					}
				},
				new CleanupCase<Integer>("autocloseableEagerFlag") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> cleanup::incrementAndGet, r -> Mono.just(1), true);
					}
				}
		);
	}

	public static List<CleanupCase<Integer>> sourcesFailNonEager() {
		return Arrays.asList(
				new CleanupCase<Integer>("sourceFailNonEager") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> 1, r -> Mono.error(new RuntimeException("forced failure")), cleanup::set, false);
					}
				},
				new CleanupCase<Integer>("autocloseableFailNonEager") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> cleanup::incrementAndGet, r -> Mono.error(new RuntimeException("forced failure")), false);
					}
				}
		);
	}

	public static List<CleanupCase<Integer>> sourcesFailEager() {
		return Arrays.asList(
				new CleanupCase<Integer>("sourceFailEager") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> 1, r -> Mono.error(new RuntimeException("forced failure")), cleanup::set);
					}
				},
				new CleanupCase<Integer>("sourceFailEagerFlag") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> 1, r -> Mono.error(new RuntimeException("forced failure")), cleanup::set, true);
					}
				},
				new CleanupCase<Integer>("autocloseableFailEager") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> cleanup::incrementAndGet, r -> Mono.error(new RuntimeException("forced failure")));
					}
				},
				new CleanupCase<Integer>("autocloseableFailEagerFlag") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> cleanup::incrementAndGet, r -> Mono.error(new RuntimeException("forced failure")), true);
					}
				}
		);
	}

	public static List<CleanupCase<Integer>> resourcesThrow() {
		return Arrays.asList(
				new CleanupCase<Integer>("resourceThrowNonEager") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> { throw new RuntimeException("forced failure"); }, Mono::just, cleanup::set, false);
					}
				},
				new CleanupCase<Integer>("autocloseableResourceThrowNonEager") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> { throw new RuntimeException("forced failure"); }, r -> Mono.just(1), false);
					}
				},
				new CleanupCase<Integer>("resourceThrowEager") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> { throw new RuntimeException("forced failure"); }, Mono::just, cleanup::set);
					}
				},
				new CleanupCase<Integer>("resourceThrowEagerFlag") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> { throw new RuntimeException("forced failure"); }, Mono::just, cleanup::set, true);
					}
				},
				new CleanupCase<Integer>("autocloseableResourceThrowEager") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> { throw new RuntimeException("forced failure"); }, r -> Mono.just(1));
					}
				},
				new CleanupCase<Integer>("autocloseableResourceThrowEagerFlag") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> { throw new RuntimeException("forced failure"); }, r -> Mono.just(1), true);
					}
				}
		);
	}

	public static List<CleanupCase<Integer>> sourcesThrowNonEager() {
		return Arrays.asList(
				new CleanupCase<Integer>("sourceThrowNonEager") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> 1, r -> { throw new RuntimeException("forced failure"); }, cleanup::set, false);
					}
				},
				new CleanupCase<Integer>("autocloseableThrowNonEager") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> cleanup::incrementAndGet, r -> { throw new RuntimeException("forced failure"); }, false);
					}
				}
		);
	}

	public static List<CleanupCase<Integer>> sourcesThrowEager() {
		return Arrays.asList(
				new CleanupCase<Integer>("sourceThrowEager") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> 1, r -> { throw new RuntimeException("forced failure"); }, cleanup::set);
					}
				},
				new CleanupCase<Integer>("sourceThrowEagerFlag") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> 1, r -> { throw new RuntimeException("forced failure"); }, cleanup::set, true);
					}
				},
				new CleanupCase<Integer>("autocloseableThrowEager") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> cleanup::incrementAndGet, r -> { throw new RuntimeException("forced failure"); });
					}
				},
				new CleanupCase<Integer>("autocloseableThrowEagerFlag") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> cleanup::incrementAndGet, r -> { throw new RuntimeException("forced failure"); }, true);
					}
				}
		);
	}

	public static List<CleanupCase<Integer>> resourcesCleanupThrowNonEager() {
		return Arrays.asList(
				new CleanupCase<Integer>("resourceCleanupThrowNonEager") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> 1, Mono::just, r -> { throw new IllegalStateException("resourceCleanup"); }, false);
					}
				},
				new CleanupCase<Integer>("autocloseableResourceCleanupThrowNonEager") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> new AutoCloseable() {
							@Override
							public void close() {
								throw new IllegalStateException("resourceCleanup");
							}
						}, r -> Mono.just(1), false);
					}
				}
		);
	}

	public static List<CleanupCase<Integer>> resourcesCleanupThrowEager() {
		return Arrays.asList(
				new CleanupCase<Integer>("resourceCleanupThrowEager") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> 1, Mono::just, r -> { throw new IllegalStateException("resourceCleanup"); });
					}
				},
				new CleanupCase<Integer>("resourceCleanupThrowEagerFlag") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> 1, Mono::just, r -> { throw new IllegalStateException("resourceCleanup"); }, true);
					}
				},
				new CleanupCase<Integer>("autocloseableResourceCleanupThrowEager") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> new AutoCloseable() {
							@Override
							public void close() {
								throw new IllegalStateException("resourceCleanup");
							}
						}, r -> Mono.just(1));
					}
				},
				new CleanupCase<Integer>("autocloseableResourceCleanupThrowEagerFlag") {
					@Override
					public Mono<Integer> get() {
						return Mono.using(() -> new AutoCloseable() {
							@Override
							public void close() {
								throw new IllegalStateException("resourceCleanup");
							}
						}, r -> Mono.just(1), true);
					}
				}
		);
	}

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

	@ParameterizedTestWithName
	@MethodSource("sourcesNonEager")
	public void normal(CleanupCase<Integer> cleanupCase) {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		cleanupCase.get().doAfterTerminate(() -> assertThat(cleanupCase.cleanup).hasValue(0)).subscribe(ts);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();

		assertThat(cleanupCase.cleanup).hasValue(1);
	}

	@ParameterizedTestWithName
	@MethodSource("sourcesEager")
	public void normalEager(CleanupCase<Integer> cleanupCase) {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		cleanupCase.get()
				   .doFinally(event -> assertThat(cleanupCase.cleanup).hasValue(0))
			       .doOnTerminate(() -> assertThat(cleanupCase.cleanup).hasValue(1))
			       .subscribe(ts);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();

		assertThat(cleanupCase.cleanup).hasValue(1);
	}

	void checkCleanupExecutionTime(CleanupCase<Integer> cleanupCase, boolean eager, boolean fail) {
		AtomicBoolean before = new AtomicBoolean();

		AssertSubscriber<Integer> ts = new AssertSubscriber<Integer>() {
			@Override
			public void onError(Throwable t) {
				super.onError(t);
				before.set(cleanupCase.cleanup.get() != 0);
			}

			@Override
			public void onComplete() {
				super.onComplete();
				before.set(cleanupCase.cleanup.get() != 0);
			}
		};

		cleanupCase.get().subscribe(ts);

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

		assertThat(cleanupCase.cleanup).hasValue(1);
		assertThat(before.get()).isEqualTo(eager);
	}

	@ParameterizedTestWithName
	@MethodSource("sourcesNonEager")
	public void checkNonEager(CleanupCase<Integer> cleanupCase) {
		checkCleanupExecutionTime(cleanupCase, false, false);
	}

	@ParameterizedTestWithName
	@MethodSource("sourcesEager")
	public void checkEager(CleanupCase<Integer> cleanupCase) {
		checkCleanupExecutionTime(cleanupCase, true, false);
	}

	@ParameterizedTestWithName
	@MethodSource("sourcesFailNonEager")
	public void checkErrorNonEager(CleanupCase<Integer> cleanupCase) {
		checkCleanupExecutionTime(cleanupCase, false, true);
	}

	@ParameterizedTestWithName
	@MethodSource("sourcesFailEager")
	public void checkErrorEager(CleanupCase<Integer> cleanupCase) {
		checkCleanupExecutionTime(cleanupCase, true, true);
	}

	@ParameterizedTestWithName
	@MethodSource("resourcesThrow")
	public void resourceThrowsEager(CleanupCase<Integer> cleanupCase) {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		cleanupCase.get().subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");

		assertThat(cleanupCase.cleanup).hasValue(0);
	}

	@ParameterizedTestWithName
	@MethodSource("sourcesThrowNonEager")
	public void factoryThrowsNonEager(CleanupCase<Integer> cleanupCase) {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		cleanupCase.get().doAfterTerminate(() -> assertThat(cleanupCase.cleanup).hasValue(0)).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");

		assertThat(cleanupCase.cleanup).hasValue(1);
	}

	@ParameterizedTestWithName
	@MethodSource("sourcesThrowEager")
	public void factoryThrowsEager(CleanupCase<Integer> cleanupCase) {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		cleanupCase.get()
				   .doFinally(event -> assertThat(cleanupCase.cleanup).hasValue(0))
			       .doOnTerminate(() -> assertThat(cleanupCase.cleanup).hasValue(1))
			       .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");

		assertThat(cleanupCase.cleanup).hasValue(1);
	}

	@ParameterizedTestWithName
	@MethodSource("resourcesCleanupThrowNonEager")
	public void resourcesCleanupThrowNonEager(CleanupCase<Integer> cleanupCase) {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		cleanupCase.get().subscribe(ts);

		ts.assertValues(1)
		  .assertComplete()
		  .assertNoError();

		assertThat(cleanupCase.cleanup).hasValue(0);
	}

	@ParameterizedTestWithName
	@MethodSource("resourcesCleanupThrowEager")
	public void resourcesCleanupThrowEager(CleanupCase<Integer> cleanupCase) {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		cleanupCase.get().subscribe(ts);

		ts.assertNoValues()
		  .assertErrorWith(e -> {
			  assertThat(e).hasMessage("resourceCleanup");
			  assertThat(e).isExactlyInstanceOf(IllegalStateException.class);
		  });

		assertThat(cleanupCase.cleanup).hasValue(0);
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
				          .map(i -> "" + i + ab.get()),
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
	public void smokeTestMapReduceGuardedByCleanup_normalNotEager() {
		AtomicBoolean cleaned = new AtomicBoolean();
		Mono.using(() -> cleaned,
				ab -> Flux.just("foo", "bar", "baz")
				          .delayElements(Duration.ofMillis(100))
				          .count()
				          .map(i -> "" + i + ab.get()),
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

	static abstract class CleanupCase<T> implements Supplier<Mono<T>> {

		final AtomicInteger cleanup = new AtomicInteger();
		final String name;

		CleanupCase(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return name;
		}
	}

}

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.util.LoggerUtils;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.TestLogger;
import reactor.util.concurrent.Queues;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.fail;
import static reactor.core.Fuseable.QueueSubscription;
import static reactor.core.scheduler.Schedulers.parallel;

public class FluxPeekTest extends FluxOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.fusionMode(Fuseable.ANY);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(scenario(f -> f.doOnSubscribe(s -> {
				})),

				scenario(f -> f.doOnError(s -> {
				})),

				scenario(f -> f.doOnTerminate(() -> {
				})),

				scenario(f -> f.doAfterTerminate(() -> {
				})),

				scenario(f -> f.doOnCancel(() -> {
				})),

				scenario(f -> f.doOnComplete(() -> {
				})),

				scenario(f -> f.doOnRequest(d -> {
				})),

				scenario(f -> f.doOnRequest(s -> {
					throw new RuntimeException(); //ignored
				})),

				scenario(f -> f.doOnNext(s -> {
				})),

				scenario(f -> f.doOnError(s -> {
				})));
	}

	@Override
	protected List<Scenario<String, String>> scenarios_touchAndAssertState() {
		return Arrays.asList(scenario(f -> f.doOnNext(d -> {
		})));
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(

				scenario(f -> f.doOnSubscribe(s -> {
					throw exception();
				})).producerNever(),

				scenario(f -> f.doOnComplete(() -> {
					throw exception();
				})).receiveValues(item(0), item(1), item(2)),

				scenario(f -> f.doOnNext(s -> {
					throw exception();
				})),

				scenario(f -> f.doOnComplete(() -> {
					throw exception();
				})).producerEmpty(),

				scenario(f -> f.doOnCancel(() -> {
					throw exception();
				})).producerNever()
						.verifier(step -> {
							//fixme Support bubbled error verification in reactor-test
							Hooks.onErrorDropped(d -> assertThat(d).hasMessage(exception().getMessage()));
							step.consumeSubscriptionWith(Subscription::cancel)
									.verifyErrorMessage(exception().getMessage());
						}));
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorFromUpstreamFailure() {
		List<Scenario<String, String>> combinedScenarios = new ArrayList<>();

		combinedScenarios.addAll(scenarios_operatorSuccess());
		combinedScenarios.addAll(Arrays.asList(scenario(f -> f.doAfterTerminate(() -> {
					throw droppedException();
				})).fusionMode(Fuseable.NONE)
						.verifier(step -> {
							try {
								step.verifyErrorMessage(exception().getMessage());
							}
							catch (Exception e) {
								assertThat(Exceptions.unwrap(e)).hasMessage(droppedException().getMessage());
							}
						}),

				scenario(f -> f.doOnNext(s -> {
					throw exception();
				})
						.doOnError(s -> {
							throw Exceptions.errorCallbackNotImplemented(new Exception("unsupported"));
						})).fusionMode(Fuseable.NONE)
						.verifier(step -> {
							try {
								step.verifyErrorMessage("unsupported");
								fail("Exception expected");
							}
							catch (Exception e) {
								assertThat(Exceptions.unwrap(e).getCause()).hasMessage("unsupported");
							}
						}),

				//extra scenarios for coverage using specific combination of fluxpkeek
				// not necessarily open
				scenario(f -> Flux.doOnSignal(f, null, null, s -> {
					if (s.getMessage()
							.equals(exception().getMessage())) {
						throw Exceptions.propagate(s);
					}
				}, null, () -> {
					throw droppedException();
				}, null, null)
						.doOnError(s -> {
							throw Exceptions.errorCallbackNotImplemented(new Exception("unsupported"));
						})).verifier(step -> {
					try {
						step.thenCancel()
								.verify();
					}
					catch (Exception e) {
						assertThat(Exceptions.unwrap(e)).hasMessage(droppedException().getMessage());
					}
				}),

				scenario(f -> Flux.doOnSignal(f, null, null, s -> {
					if (s.getMessage()
							.equals(exception().getMessage())) {
						throw Exceptions.propagate(s);
					}
				}, null, () -> {
					throw droppedException();
				}, null, null)
						.doOnError(s -> {
							throw Exceptions.errorCallbackNotImplemented(s);
						})).verifier(step -> {
					try {
						step.thenCancel()
								.verify();
					}
					catch (Exception e) {
						assertThat(Exceptions.unwrap(e)).hasMessage(droppedException().getMessage());
					}
				}),

				scenario(f -> Flux.doOnSignal(f, null, null, s -> {
					if (s.getMessage()
							.equals(exception().getMessage())) {
						throw Exceptions.propagate(s);
					}
				}, null, () -> {
					throw droppedException();
				}, null, null)
						.doOnError(s -> {
							throw exception();
						})).fusionMode(Fuseable.NONE)
						.verifier(step -> {
							try {
								step.verifyErrorMessage("test");
							}
							catch (Exception e) {
								assertThat(Exceptions.unwrap(e)).hasMessage(droppedException().getMessage());
							}
						})

		));
		return combinedScenarios;
	}

	@Test
	public void nullSource() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new FluxPeek<>(null, null, null, null, null, null, null, null);
		});
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		AtomicLong onRequest = new AtomicLong();
		AtomicBoolean onAfterComplete = new AtomicBoolean();
		AtomicBoolean onCancel = new AtomicBoolean();

		new FluxPeek<>(Flux.just(1)
				.hide(), onSubscribe::set, onNext::set, onError::set, () -> onComplete.set(true), () -> onAfterComplete.set(true), onRequest::set, () -> onCancel.set(true)).subscribe(ts);

		assertThat(onSubscribe.get()).isNotNull();
		assertThat(onNext).hasValue((Integer) 1);
		assertThat(onError.get()).isNull();
		assertThat(onComplete.get()).isTrue();
		assertThat(onAfterComplete.get()).isTrue();
		assertThat(onRequest).hasValue(Long.MAX_VALUE);
		assertThat(onCancel.get()).isFalse();
	}

	@Test
	public void error() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		AtomicLong onRequest = new AtomicLong();
		AtomicBoolean onAfterComplete = new AtomicBoolean();
		AtomicBoolean onCancel = new AtomicBoolean();

		new FluxPeek<>(new FluxError<>(new RuntimeException("forced failure")), onSubscribe::set, onNext::set, onError::set, () -> onComplete.set(true), () -> onAfterComplete.set(true), onRequest::set, () -> onCancel.set(true)).subscribe(ts);

		assertThat(onSubscribe.get()).isNotNull();
		assertThat(onNext.get()).isNull();
		assertThat(onError.get()).isInstanceOf(RuntimeException.class);
		assertThat(onComplete.get()).isFalse();
		assertThat(onAfterComplete.get()).isTrue();
		assertThat(onRequest).hasValue(Long.MAX_VALUE);
		assertThat(onCancel.get()).isFalse();
	}

	@Test
	public void empty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		AtomicLong onRequest = new AtomicLong();
		AtomicBoolean onAfterComplete = new AtomicBoolean();
		AtomicBoolean onCancel = new AtomicBoolean();

		new FluxPeek<>(Flux.empty(), onSubscribe::set, onNext::set, onError::set, () -> onComplete.set(true), () -> onAfterComplete.set(true), onRequest::set, () -> onCancel.set(true)).subscribe(ts);

		assertThat(onSubscribe.get()).isNotNull();
		assertThat(onNext.get()).isNull();
		assertThat(onError.get()).isNull();
		assertThat(onComplete.get()).isTrue();
		assertThat(onAfterComplete.get()).isTrue();
		assertThat(onRequest).hasValue(Long.MAX_VALUE);
		assertThat(onCancel.get()).isFalse();
	}

	@Test
	public void never() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		AtomicLong onRequest = new AtomicLong();
		AtomicBoolean onAfterComplete = new AtomicBoolean();
		AtomicBoolean onCancel = new AtomicBoolean();

		new FluxPeek<>(Flux.never(), onSubscribe::set, onNext::set, onError::set, () -> onComplete.set(true), () -> onAfterComplete.set(true), onRequest::set, () -> onCancel.set(true)).subscribe(ts);

		assertThat(onSubscribe.get()).isNotNull();
		assertThat(onNext.get()).isNull();
		assertThat(onError.get()).isNull();
		assertThat(onComplete.get()).isFalse();
		assertThat(onAfterComplete.get()).isFalse();
		assertThat(onRequest).hasValue(Long.MAX_VALUE);
		assertThat(onCancel.get()).isFalse();
	}

	@Test
	public void neverCancel() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		AtomicLong onRequest = new AtomicLong();
		AtomicBoolean onAfterComplete = new AtomicBoolean();
		AtomicBoolean onCancel = new AtomicBoolean();

		new FluxPeek<>(Flux.never(), onSubscribe::set, onNext::set, onError::set, () -> onComplete.set(true), () -> onAfterComplete.set(true), onRequest::set, () -> onCancel.set(true)).subscribe(ts);

		assertThat(onSubscribe.get()).isNotNull();
		assertThat(onNext.get()).isNull();
		assertThat(onError.get()).isNull();
		assertThat(onComplete.get()).isFalse();
		assertThat(onAfterComplete.get()).isFalse();
		assertThat(onRequest).hasValue(Long.MAX_VALUE);
		assertThat(onCancel.get()).isFalse();

		ts.cancel();

		assertThat(onCancel.get()).isTrue();
	}

	@Test
	public void callbackError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Throwable err = new Exception("test");

		Flux.just(1)
			.hide()
			.doOnNext(d -> {
				throw Exceptions.propagate(err);
			})
			.subscribe(ts);

		//nominal error path (DownstreamException)
		ts.assertErrorMessage("test");

		ts = AssertSubscriber.create();

		try {
			Flux.just(1)
				.hide()
				.doOnNext(d -> {
					throw Exceptions.bubble(err);
				})
				.subscribe(ts);

			fail("Exception expected");
		}
		catch (Exception e) {
			assertThat(Exceptions.unwrap(e)).isSameAs(err);
		}
	}

	@Test
	public void completeCallbackError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Throwable err = new Exception("test");

		Flux.just(1)
			.hide()
			.doOnComplete(() -> {
				throw Exceptions.propagate(err);
			})
			.subscribe(ts);

		//nominal error path (DownstreamException)
		ts.assertErrorMessage("test");

		ts = AssertSubscriber.create();

		try {
			Flux.just(1)
				.hide()
				.doOnComplete(() -> {
					throw Exceptions.bubble(err);
				})
				.subscribe(ts);

			fail("Exception expected");
		}
		catch (Exception e) {
			assertThat(Exceptions.unwrap(e)).isSameAs(err);
		}
	}

	@Test
	public void errorCallbackError() {
		IllegalStateException err = new IllegalStateException("test");

		FluxPeek<String> flux = new FluxPeek<>(Flux.error(new IllegalArgumentException("bar")), null, null, e -> {
			throw err;
		}, null, null, null, null);

		AssertSubscriber<String> ts = AssertSubscriber.create();
		flux.subscribe(ts);

		ts.assertNoValues();
		ts.assertError(IllegalStateException.class);
		ts.assertErrorWith(e -> e.getSuppressed()[0].getMessage()
													.equals("bar"));
	}

	//See https://github.com/reactor/reactor-core/issues/272
	@Test
	public void errorCallbackError2() {
		//test with alternate / wrapped error types

		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Throwable err = new Exception("test");

		Flux.just(1)
			.hide()
			.doOnNext(d -> {
				throw new RuntimeException();
			})
			.doOnError(e -> {
				throw Exceptions.propagate(err);
			})
			.subscribe(ts);

		//nominal error path (DownstreamException)
		ts.assertErrorMessage("test");

		ts = AssertSubscriber.create();
		try {
			Flux.just(1)
				.hide()
				.doOnNext(d -> {
					throw new RuntimeException();
				})
				.doOnError(d -> {
					throw Exceptions.bubble(err);
				})
				.subscribe(ts);

			fail("Exception expected");
		}
		catch (Exception e) {
			assertThat(Exceptions.unwrap(e)).isSameAs(err);
		}
	}

	//See https://github.com/reactor/reactor-core/issues/253
	@Test
	public void errorCallbackErrorWithParallel() {
		AssertSubscriber<Integer> assertSubscriber = new AssertSubscriber<>();

		Mono.just(1)
			.hide()
			.publishOn(parallel())
			.doOnNext(i -> {
				throw new IllegalArgumentException();
			})
			.doOnError(e -> {
				throw new IllegalStateException(e);
			})
			.subscribe(assertSubscriber);

		assertSubscriber.await()
						.assertError(IllegalStateException.class)
						.assertNotComplete();
	}

	@Test
	public void afterTerminateCallbackErrorDoesNotInvokeOnError() {
		TestLogger testLogger = new TestLogger();
		LoggerUtils.enableCaptureWith(testLogger);
		try {
			IllegalStateException e = new IllegalStateException("test");
			AtomicReference<Throwable> errorCallbackCapture = new AtomicReference<>();

			FluxPeek<String> flux = new FluxPeek<>(Flux.empty(),
					null,
					null,
					errorCallbackCapture::set,
					null,
					() -> {
						throw e;
					},
					null,
					null);

			AssertSubscriber<String> ts = AssertSubscriber.create();

			flux.subscribe(ts);

			ts.assertNoValues();
			ts.assertComplete();

			assertThat(errorCallbackCapture.get()).isNull();

			assertThat(testLogger.getErrContent())
					.contains("Operator called default onErrorDropped")
					.contains(e.toString());
		} finally {
			LoggerUtils.disableCapture();
		}
	}

	@Test
	public void afterTerminateCallbackFatalIsThrownDirectly() {
		AtomicReference<Throwable> errorCallbackCapture = new AtomicReference<>();
		Error fatal = new LinkageError();
		FluxPeek<String> flux = new FluxPeek<>(Flux.empty(), null, null, errorCallbackCapture::set, null, () -> {
			throw fatal;
		}, null, null);

		AssertSubscriber<String> ts = AssertSubscriber.create();

		try {
			flux.subscribe(ts);
			fail("expected thrown exception");
		}
		catch (Throwable e) {
			assertThat(e).isSameAs(fatal);
		}
		ts.assertNoValues();
		ts.assertComplete();

		assertThat(errorCallbackCapture).hasValue(null);

		//same with after error
		errorCallbackCapture.set(null);
		flux = new FluxPeek<>(Flux.error(new NullPointerException()), null, null, errorCallbackCapture::set, null, () -> {
			throw fatal;
		}, null, null);

		ts = AssertSubscriber.create();

		try {
			flux.subscribe(ts);
			fail("expected thrown exception");
		}
		catch (Throwable e) {
			assertThat(e).isSameAs(fatal);
		}
		ts.assertNoValues();
		ts.assertError(NullPointerException.class);

		assertThat(errorCallbackCapture.get()).isInstanceOf(NullPointerException.class);
	}

	@Test
	public void afterTerminateCallbackErrorAndErrorCallbackError() {
		TestLogger testLogger = new TestLogger();
		LoggerUtils.enableCaptureWith(testLogger);
		try {
			IllegalStateException error1 = new IllegalStateException("afterTerminate");
			IllegalArgumentException error2 = new IllegalArgumentException("error");

			FluxPeek<String> flux = new FluxPeek<>(Flux.empty(), null, null, e -> {
				throw error2;
			}, null, () -> {
				throw error1;
			}, null, null);

			AssertSubscriber<String> ts = AssertSubscriber.create();

			flux.subscribe(ts);
			assertThat(testLogger.getErrContent())
					.contains("Operator called default onErrorDropped")
					.contains(error1.getMessage());
			assertThat(error2.getSuppressed()).hasSize(0);
			//error2 is never thrown
			ts.assertNoValues();
			ts.assertComplete();
		}
		finally {
			LoggerUtils.disableCapture();
		}
	}

	@Test
	public void afterTerminateCallbackErrorAndErrorCallbackError2() {
		TestLogger testLogger = new TestLogger();
		LoggerUtils.enableCaptureWith(testLogger);
		try {
			IllegalStateException afterTerminate = new IllegalStateException("afterTerminate");
			IllegalArgumentException error = new IllegalArgumentException("error");
			NullPointerException ex = new NullPointerException();

			FluxPeek<String> flux = new FluxPeek<>(Flux.error(ex), null, null, e -> {
				throw error;
			}, null, () -> {
				throw afterTerminate;
			}, null, null);

			AssertSubscriber<String> ts = AssertSubscriber.create();

			flux.subscribe(ts);
			assertThat(testLogger.getErrContent())
					.contains("Operator called default onErrorDropped")
					.contains(afterTerminate.getMessage());
			//afterTerminate suppressed error which itself suppressed original err
			assertThat(afterTerminate.getSuppressed()).hasSize(1);
			assertThat(afterTerminate).hasSuppressedException(error);

			assertThat(error.getSuppressed()).hasSize(1);
			assertThat(error).hasSuppressedException(ex);
			ts.assertNoValues();
			//the subscriber still sees the 'error' message since actual.onError is called before the afterTerminate callback
			ts.assertErrorMessage("error");
		}  finally {
			LoggerUtils.disableCapture();
		}
	}

	@Test
	public void syncFusionAvailable() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 2)
			.doOnNext(v -> {
			})
			.subscribe(ts);

		Subscription s = ts.upstream();
		assertThat(s).as("check QueueSubscription").isInstanceOf(QueueSubscription.class);
	}

	@Test
	public void asyncFusionAvailable() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.unsafe().many()
			 .unicast()
			 .onBackpressureBuffer(Queues.<Integer>get(2).get())
			 .asFlux()
			 .doOnNext(v -> {
			 })
			 .subscribe(ts);

		Subscription s = ts.upstream();
		assertThat(s).as("check QueueSubscription").isInstanceOf(QueueSubscription.class);
	}

	@Test
	public void conditionalFusionAvailable() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.wrap(u -> {
			if (!(u instanceof Fuseable.ConditionalSubscriber)) {
				Operators.error(u, new IllegalArgumentException("The subscriber is not conditional: " + u));
			}
			else {
				Operators.complete(u);
			}
		})
			.doOnNext(v -> {
			})
			.filter(v -> true)
			.subscribe(ts);

		ts.assertNoError()
		  .assertNoValues()
		  .assertComplete();
	}

	@Test
	public void conditionalFusionAvailableWithFuseable() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.wrap(u -> {
			if (!(u instanceof Fuseable.ConditionalSubscriber)) {
				Operators.error(u, new IllegalArgumentException("The subscriber is not conditional: " + u));
			}
			else {
				Operators.complete(u);
			}
		})
			.doOnNext(v -> {
			})
			.filter(v -> true)
			.subscribe(ts);

		ts.assertNoError()
		  .assertNoValues()
		  .assertComplete();
	}

	@Test
	public void syncCompleteCalled() {
		AtomicBoolean onComplete = new AtomicBoolean();

		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(1, 2)
			.hide()
			.doOnComplete(() -> onComplete.set(true))
			.subscribe(ts);

		ts.assertNoError()
		  .assertValues(1, 2)
		  .assertComplete();

		assertThat(onComplete.get()).as("onComplete not called back").isTrue();
	}

	@Test
	public void syncdoAfterTerminateCalled() {
		AtomicBoolean onTerminate = new AtomicBoolean();

		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(1, 2)
			.hide()
			.doAfterTerminate(() -> onTerminate.set(true))
			.subscribe(ts);

		ts.assertNoError()
		  .assertValues(1, 2)
		  .assertComplete();

		assertThat(onTerminate.get()).as("onComplete not called back").isTrue();
	}

	@Test
	public void syncdoOnTerminateCalled() {
		AtomicBoolean onTerminate = new AtomicBoolean();

		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(1, 2)
			.hide()
			.doOnTerminate(() -> onTerminate.set(true))
			.subscribe(ts);

		ts.assertNoError()
		  .assertValues(1, 2)
		  .assertComplete();

		assertThat(onTerminate.get()).as("onComplete not called back").isTrue();
	}

	@Test
	public void syncdoOnTerminateCalled2() {
		AtomicBoolean onTerminate = new AtomicBoolean();

		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.empty()
			.hide()
			.doOnTerminate(() -> onTerminate.set(true))
			.subscribe(ts);

		ts.assertNoError()
		  .assertComplete();

		assertThat(onTerminate.get()).as("onComplete not called back").isTrue();
	}

	@Test
	public void syncdoOnTerminateCalled3() {
		AtomicBoolean onTerminate = new AtomicBoolean();

		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.error(new Exception("test"))
			.hide()
			.doOnTerminate(() -> onTerminate.set(true))
			.subscribe(ts);

		ts.assertErrorMessage("test")
		  .assertNotComplete();

		assertThat(onTerminate.get()).as("onComplete not called back").isTrue();
	}

	@Test
	public void should_reduce_to_10_events() {
		for (int i = 0; i < 20; i++) {
			AtomicInteger count = new AtomicInteger();
			Flux.range(0, 10)
				.flatMap(x -> Flux.range(0, 2)
								  .map(y -> blockingOp(x, y))
								  .subscribeOn(parallel())
								  .reduce((l, r) -> l + "_" + r)
								  .hide()
								  .doOnSuccess(s -> {
									  count.incrementAndGet();
								  }))
				.blockLast();

			assertThat(count).hasValue(10);
		}
	}

	@Test
	public void should_reduce_to_10_events_conditional() {
		for (int i = 0; i < 20; i++) {
			AtomicInteger count = new AtomicInteger();
			Flux.range(0, 10)
				.flatMap(x -> Flux.range(0, 2)
								  .map(y -> blockingOp(x, y))
								  .subscribeOn(parallel())
								  .reduce((l, r) -> l + "_" + r)
								  .hide()
								  .doOnSuccess(s -> {
									  count.incrementAndGet();
								  })
								  .filter(v -> true))
				.blockLast();

			assertThat(count).hasValue(10);
		}
	}

	static String blockingOp(Integer x, Integer y) {
		try {
			sleep(10);
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		return "x" + x + "y" + y;
	}

	static final class TestException extends Exception {

	}

	@Test
	public void doOnErrorPredicate() {
		AtomicReference<Throwable> ref = new AtomicReference<>();

		StepVerifier.create(Flux.error(new TestException())
								.doOnError(TestException.class::isInstance, ref::set))
					.thenAwait()
					.then(() -> assertThat(ref.get()).isInstanceOf(TestException.class))
					.verifyError(TestException.class);
	}

	@Test
	public void doOnErrorPredicateNot() {
		AtomicReference<Throwable> ref = new AtomicReference<>();

		StepVerifier.create(Flux.error(new TestException())
								.doOnError(RuntimeException.class::isInstance, ref::set))
					.thenAwait()
					.then(() -> assertThat(ref.get()).isNull())
					.verifyError(TestException.class);
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxPeek<Integer> test = new FluxPeek<>(parent, s -> {}, s -> {},
				e -> {}, () -> {}, () -> {}, r -> {}, () -> {});

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscriber() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {
		}, null, null);
		FluxPeek<Integer> peek = new FluxPeek<>(Flux.just(1), s -> {
		}, s -> {
		}, e -> {
		}, () -> {
		}, () -> {
		}, r -> {
		}, () -> {
		});
		FluxPeek.PeekSubscriber<Integer> test = new FluxPeek.PeekSubscriber<>(actual, peek);
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
	public void errorStrategyResumeDropsNext() {
		RuntimeException nextError = new IllegalStateException("next");

		List<Throwable> resumedErrors = new ArrayList<>();
		List<Object> resumedValues = new ArrayList<>();

		Flux<Integer> source = Flux.just(1, 2)
								   .hide();

		Flux<Integer> test = new FluxPeek<>(source, null, v -> {
			throw nextError;
		}, null, null, null, null, null).hide()
										.onErrorContinue((t, s) -> {
											resumedErrors.add(t);
											resumedValues.add(s);
										});

		StepVerifier.create(test)
					.expectNoFusionSupport()
					.expectComplete()
					.verifyThenAssertThat()
					.hasNotDroppedElements()
					//classically dropped in all error modes:
					.hasNotDroppedErrors();

		assertThat(resumedValues).containsExactly(1, 2);
		assertThat(resumedErrors).containsExactly(nextError, nextError);
	}
}

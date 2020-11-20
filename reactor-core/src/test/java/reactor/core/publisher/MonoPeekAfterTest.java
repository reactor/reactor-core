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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.junit.jupiter.api.Test;
import org.assertj.core.api.Assertions;

import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.scheduler.Schedulers;
import reactor.test.util.LoggerUtils;
import reactor.test.StepVerifier;
import reactor.test.util.TestLogger;
import reactor.util.Logger;
import reactor.util.Loggers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class MonoPeekAfterTest {

	private static final Logger LOG = Loggers.getLogger(MonoPeekAfterTest.class);

	@Test
	public void onSuccessNormal() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean hasNull = new AtomicBoolean();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.hide()
				.doOnSuccess(v -> {
					if (v == null) hasNull.set(true);
					invoked.increment();
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.ANY, Fuseable.NONE)
	                .expectNext(55)
	                .expectComplete()
	                .verify();

		assertThat(hasNull.get()).as("unexpected call to onSuccess with null").isFalse();
		assertThat(invoked.intValue()).isEqualTo(1);
	}

	@Test
	public void onSuccessNormalConditional() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean hasNull = new AtomicBoolean();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.hide()
				.filter(v -> true)
				.doOnSuccess(v -> {
					if (v == null) hasNull.set(true);
					invoked.increment();
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.ANY, Fuseable.NONE)
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertThat(hasNull.get()).as("unexpected call to onSuccess with null").isFalse();
		assertThat(invoked.intValue()).isEqualTo(1);

	}

	@Test
	public void onSuccessFusion() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean hasNull = new AtomicBoolean();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.doOnSuccess(v -> {
					if (v == null) hasNull.set(true);
					invoked.increment();
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.ASYNC)
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertThat(hasNull.get()).as("unexpected call to onSuccess with null").isFalse();
		assertThat(invoked.intValue()).isEqualTo(1);
	}

	@Test
	public void onSuccessFusionConditional() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean hasNull = new AtomicBoolean();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.filter(v -> true)
				.doOnSuccess(v -> {
					if (v == null) hasNull.set(true);
					invoked.increment();
				});

		StepVerifier.create(mono)
		            .expectFusion()
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertThat(hasNull.get()).as("unexpected call to onSuccess with null").isFalse();
		assertThat(invoked.intValue()).isEqualTo(1);
	}

	@Test
	public void onSuccessOrErrorNormal() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		@SuppressWarnings("deprecation") // Because of doOnSuccessOrError, which will be removed in 3.5.0
		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.hide()
				.doOnSuccessOrError((v, t) -> {
					if (v == null && t == null) completedEmpty.set(true);
					if (t != null) error.set(t);
					invoked.increment();
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.ANY, Fuseable.NONE)
	                .expectNext(55)
	                .expectComplete()
	                .verify();

		assertThat(completedEmpty.get()).as("unexpected empty completion").isFalse();
		assertThat(invoked.intValue()).isEqualTo(1);
		assertThat(error).as("unexpected error").hasValue(null);
	}

	@Test
	public void onSuccessOrErrorNormalConditional() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		@SuppressWarnings("deprecation") // Because of doOnSuccessOrError, which will be removed in 3.5.0
		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.hide()
				.filter(v -> true)
				.doOnSuccessOrError((v, t) -> {
					if (v == null && t == null) completedEmpty.set(true);
					if (t != null) error.set(t);
					invoked.increment();
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.ANY, Fuseable.NONE)
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertThat(completedEmpty.get()).as("unexpected empty completion").isFalse();
		assertThat(invoked.intValue()).isEqualTo(1);
		assertThat(error).as("unexpected error").hasValue(null);
	}

	@Test
	public void onSuccessOrErrorFusion() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		@SuppressWarnings("deprecation") // Because of doOnSuccessOrError, which will be removed in 3.5.0
		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.doOnSuccessOrError((v, t) -> {
					if (v == null && t == null) completedEmpty.set(true);
					if (t != null) error.set(t);
					invoked.increment();
				});

		StepVerifier.create(mono)
		            .expectFusion()
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertThat(completedEmpty.get()).as("unexpected empty completion").isFalse();
		assertThat(invoked.intValue()).isEqualTo(1);
		assertThat(error).as("unexpected error").hasValue(null);
	}

	@Test
	public void onSuccessOrErrorFusionConditional() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		@SuppressWarnings("deprecation") // Because of doOnSuccessOrError, which will be removed in 3.5.0
		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.filter(v -> true)
				.doOnSuccessOrError((v, t) -> {
					if (v == null && t == null) completedEmpty.set(true);
					if (t != null) error.set(t);
					invoked.increment();
				});

		StepVerifier.create(mono)
		            .expectFusion()
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertThat(completedEmpty.get()).as("unexpected empty completion").isFalse();
		assertThat(invoked.intValue()).isEqualTo(1);
		assertThat(error).as("unexpected error").hasValue(null);
	}

	@Test
	public void onAfterSuccessOrErrorNormal() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		@SuppressWarnings("deprecation") // Because of doAfterSuccessOrError, which will be removed in 3.5.0
		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.hide()
				.doAfterSuccessOrError((v, t) -> {
					if (v == null && t == null) completedEmpty.set(true);
					if (t != null) error.set(t);
					invoked.increment();
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.ANY, Fuseable.NONE)
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertThat(completedEmpty.get()).as("unexpected empty completion").isFalse();
		assertThat(invoked.intValue()).isEqualTo(1);
		assertThat(error).as("unexpected error").hasValue(null);
	}

	@Test
	public void onAfterSuccessOrErrorNormalConditional() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		@SuppressWarnings("deprecation") // Because of doAfterSuccessOrError, which will be removed in 3.5.0
		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.hide()
				.filter(v -> true)
				.doAfterSuccessOrError((v, t) -> {
					if (v == null && t == null) completedEmpty.set(true);
					if (t != null) error.set(t);
					invoked.increment();
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.ANY, Fuseable.NONE)
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertThat(completedEmpty.get()).as("unexpected empty completion").isFalse();
		assertThat(invoked.intValue()).isEqualTo(1);
		assertThat(error).as("unexpected error").hasValue(null);
	}

	@Test
	public void onAfterSuccessOrErrorFusion() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		@SuppressWarnings("deprecation") // Because of doAfterSuccessOrError, which will be removed in 3.5.0
		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.doAfterSuccessOrError((v, t) -> {
					if (v == null && t == null) completedEmpty.set(true);
					if (t != null) error.set(t);
					invoked.increment();
				});

		StepVerifier.create(mono.log())
		            .expectFusion()
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertThat(completedEmpty.get()).as("unexpected empty completion").isFalse();
		assertThat(invoked.intValue()).isEqualTo(1);
		assertThat(error).as("unexpected error").hasValue(null);
	}

	@Test
	public void onAfterSuccessOrErrorFusionConditional() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		@SuppressWarnings("deprecation") // Because of doAfterSuccessOrError, which will be removed in 3.5.0
		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.filter(v -> true)
				.doAfterSuccessOrError((v, t) -> {
					if (v == null && t == null) completedEmpty.set(true);
					if (t != null) error.set(t);
					invoked.increment();
				});

		StepVerifier.create(mono)
		            .expectFusion()
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertThat(completedEmpty.get()).as("unexpected empty completion").isFalse();
		assertThat(invoked.intValue()).isEqualTo(1);
		assertThat(error).as("unexpected error").hasValue(null);
	}

	@Test
	public void onAfterTerminateNormalConditional() {
		LongAdder invoked = new LongAdder();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.hide()
				.filter(v -> true)
				.doAfterTerminate(invoked::increment);

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.ANY, Fuseable.NONE)
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertThat(invoked.intValue()).isEqualTo(1);
	}

	@Test
	public void onAfterTerminateFusion() {
		LongAdder invoked = new LongAdder();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.doAfterTerminate(invoked::increment);

		StepVerifier.create(mono.log())
		            .expectFusion()
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertThat(invoked.intValue()).isEqualTo(1);
	}

	@Test
	public void onAfterTerminateFusionConditional() {
		LongAdder invoked = new LongAdder();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.filter(v -> true)
				.doAfterTerminate(invoked::increment);

		StepVerifier.create(mono)
		            .expectFusion()
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertThat(invoked.intValue()).isEqualTo(1);
	}

	@Test
	public void onSuccessCallbackFailureInterruptsOnNext() {
		LongAdder invoked = new LongAdder();
		StepVerifier.create(Mono.just("foo")
		                        .doOnSuccess(s -> {
		                        	invoked.increment();
		                        	throw new IllegalArgumentException(s);
		                        }))
	                .expectErrorMessage("foo")
	                .verify();
		assertThat(invoked.intValue()).isEqualTo(1);
	}

	@Test
	public void onSuccessOrErrorCallbackFailureInterruptsOnNext() {
		LongAdder invoked = new LongAdder();
		@SuppressWarnings("deprecation") // Because of doOnSuccessOrError, which will be removed in 3.5.0
		Mono<String> mono = Mono.just("foo")
		                        .doOnSuccessOrError((v, t) -> {
			                        invoked.increment();
			                        throw new IllegalArgumentException(v);
		                        });
		StepVerifier.create(mono)
		            .expectErrorMessage("foo")
		            .verify();

		assertThat(invoked.intValue()).isEqualTo(1);
	}

	@Test
	public void afterSuccessOrErrorCallbackFailureInterruptsOnNextAndThrows() {
		TestLogger testLogger = new TestLogger();
		LoggerUtils.enableCaptureWith(testLogger);
		try {
			LongAdder invoked = new LongAdder();
			try {
				@SuppressWarnings("deprecation") // Because of doAfterSuccessOrError, which will be removed in 3.5.0
				Mono<String> mono = Mono.just("foo")
				                        .doAfterSuccessOrError((v, t) -> {
					                        invoked.increment();
					                        throw new IllegalArgumentException(v);
				                        });
				StepVerifier.create(mono)
				            .expectNext("bar") //irrelevant
				            .expectErrorMessage("baz") //irrelevant
				            .verify();
				fail("Exception expected");
			}
			catch (Throwable t) {
				Throwable e = Exceptions.unwrap(t);
				assertThat(e).isExactlyInstanceOf(AssertionError.class)
						.hasMessage("expectation \"expectNext(bar)\" failed (expected value: bar; actual value: foo)");
			}

			assertThat(invoked.intValue()).isEqualTo(1);
			Assertions.assertThat(testLogger.getErrContent())
			          .contains("Operator called default onErrorDropped")
			          .contains("IllegalArgumentException")
			          .contains("foo");
		}
		finally {
			LoggerUtils.disableCapture();
		}
	}

	@Test
	public void afterTerminateCallbackFailureInterruptsOnNextAndThrows() {
		TestLogger testLogger = new TestLogger();
		LoggerUtils.enableCaptureWith(testLogger);
		try {
			LongAdder invoked = new LongAdder();
			try {
				@SuppressWarnings("deprecation") // Because of doAfterSuccessOrError, which will be removed in 3.5.0
				Mono<String> mono = Mono.just("foo")
				                        .doAfterSuccessOrError((v, t) -> {
					                        invoked.increment();
					                        throw new IllegalArgumentException(v);
				                        });
				StepVerifier.create(mono)
				            .expectNext("bar") //irrelevant
				            .expectErrorMessage("baz") //irrelevant
				            .verify();
				fail("Exception expected");
			}
			catch (Throwable t) {
				Throwable e = Exceptions.unwrap(t);
				assertThat(e).isExactlyInstanceOf(AssertionError.class)
						.hasMessage("expectation \"expectNext(bar)\" failed (expected value: bar; actual value: foo)");
			}

			assertThat(invoked.intValue()).isEqualTo(1);

			Assertions.assertThat(testLogger.getErrContent())
			          .contains("Operator called default onErrorDropped")
			          .contains("foo")
			          .contains("IllegalArgumentException");
		}
		finally {
			LoggerUtils.disableCapture();
		}
	}

	@Test
	public void onSuccessNotCalledOnError() {
		LongAdder invoked = new LongAdder();
		IllegalArgumentException err = new IllegalArgumentException("boom");

		StepVerifier.create(Mono.error(err)
				.doOnSuccess(v -> invoked.increment()))
	                .expectErrorMessage("boom")
	                .verify();

		assertThat(invoked.intValue()).isEqualTo(0);
	}

	@Test
	public void onSuccessOrErrorForOnError() {
		LongAdder invoked = new LongAdder();
		AtomicReference<String> value = new AtomicReference<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		IllegalArgumentException err = new IllegalArgumentException("boom");

		@SuppressWarnings("deprecation") // Because of doOnSuccessOrError, which will be removed in 3.5.0
		Mono<String> test = Mono.<String>error(err)
				.doOnSuccessOrError((v, t) -> {
					invoked.increment();
					value.set(v);
					error.set(t);
				});
		StepVerifier.create(test)
		            .expectErrorMessage("boom")
		            .verify();

		assertThat(invoked.intValue()).isEqualTo(1);
		assertThat(value).hasValue(null);
		assertThat(error).hasValue(err);
	}

	@Test
	public void afterSuccessOrErrorForOnError() {
		LongAdder invoked = new LongAdder();
		AtomicReference<String> value = new AtomicReference<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		IllegalArgumentException err = new IllegalArgumentException("boom");

		@SuppressWarnings("deprecation") // Because of doAfterSuccessOrError, which will be removed in 3.5.0
		Mono<String> mono = Mono.<String>error(err).doAfterSuccessOrError((v, t) -> {
			invoked.increment();
			value.set(v);
			error.set(t);
		});

		StepVerifier.create(mono)
		            .expectErrorMessage("boom")
		            .verify();

		assertThat(invoked.intValue()).isEqualTo(1);
		assertThat(value).hasValue(null);
		assertThat(error).hasValue(err);
	}

	@Test
	public void afterTerminateForOnError() {
		LongAdder invoked = new LongAdder();

		IllegalArgumentException err = new IllegalArgumentException("boom");

		StepVerifier.create(Mono.<String>error(err)
				.doAfterTerminate(invoked::increment))
		            .expectErrorMessage("boom")
		            .verify();

		assertThat(invoked.intValue()).isEqualTo(1);
	}

	@Test
	public void onSuccessForEmpty() {
		LongAdder invoked = new LongAdder();
		AtomicReference<String> value = new AtomicReference<>();

		StepVerifier.create(Mono.<String>empty()
				.doOnSuccess(v -> {
					invoked.increment();
					value.set(v);
				}))
		            .expectComplete()
		            .verify();

		assertThat(invoked.intValue()).isEqualTo(1);
		assertThat(value).hasValue(null);
	}

	@Test
	public void onSuccessOrErrorForEmpty() {
		LongAdder invoked = new LongAdder();
		AtomicReference<String> value = new AtomicReference<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		@SuppressWarnings("deprecation") // Because of doOnSuccessOrError, which will be removed in 3.5.0
		Mono<String> mono = Mono.<String>empty().doOnSuccessOrError((v, t) -> {
			invoked.increment();
			value.set(v);
			error.set(t);
		});

		StepVerifier.create(mono)
		            .expectComplete()
		            .verify();

		assertThat(invoked.intValue()).isEqualTo(1);
		assertThat(value).hasValue(null);
		assertThat(error).hasValue(null);
	}

	@Test
	public void afterSuccessOrErrorForEmpty() {
		LongAdder invoked = new LongAdder();
		AtomicReference<String> value = new AtomicReference<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		@SuppressWarnings("deprecation") // Because of doAfterSuccessOrError, which will be removed in 3.5.0
		Mono<String> mono = Mono.<String>empty()
				.doAfterSuccessOrError((v, t) -> {
					invoked.increment();
					value.set(v);
					error.set(t);
				});
		StepVerifier.create(mono)
		            .expectComplete()
		            .verify();

		assertThat(invoked.intValue()).isEqualTo(1);
		assertThat(value).hasValue(null);
		assertThat(error).hasValue(null);
	}

	@Test
	public void afterTerminateForEmpty() {
		LongAdder invoked = new LongAdder();

		StepVerifier.create(Mono.<String>empty()
				.doAfterTerminate(() -> {
					invoked.increment();
				}))
		            .expectComplete()
		            .verify();

		assertThat(invoked.intValue()).isEqualTo(1);
	}

	@Test
	public void testCallbacksNoFusion() {
		AtomicReference<Integer> successInvocation = new AtomicReference<>();
		AtomicReference<Throwable> errorInvocation = new AtomicReference<>();
		AtomicReference<Integer> afterTerminateInvocation = new AtomicReference<>();

		Mono<Integer> source = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.hide();

		Mono<Integer> mono = new MonoPeekTerminal<>(source,
				successInvocation::set,
				errorInvocation::set,
				(v, t) -> {
					afterTerminateInvocation.set(v);
					errorInvocation.set(t);
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.NONE)
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertThat((Object) successInvocation.get()).isEqualTo(55);
		assertThat((Object) afterTerminateInvocation.get()).isEqualTo(55);
		assertThat(errorInvocation).hasValue(null);
	}

	@Test
	public void testCallbacksFusionSync() {
		AtomicReference<Integer> successInvocation = new AtomicReference<>();
		AtomicReference<Integer> afterTerminateInvocation = new AtomicReference<>();
		AtomicReference<Throwable> errorInvocation = new AtomicReference<>();

		Mono<Integer> source = Mono.fromDirect(Flux.range(55, 1));

		Mono<Integer> mono = new MonoPeekTerminal<>(source,
				successInvocation::set,
				errorInvocation::set,
				(v, t) -> {
					afterTerminateInvocation.set(v);
					errorInvocation.set(t);
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.SYNC, Fuseable.SYNC) //TODO in 3.0.3 this doesn't work
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertThat((Object) successInvocation.get()).isEqualTo(55);
		assertThat((Object) afterTerminateInvocation.get()).isEqualTo(55);
		assertThat(errorInvocation).hasValue(null);
	}

	@Test
	public void testCallbacksFusionAsync() {
		AtomicReference<Integer> successInvocation = new AtomicReference<>();
		AtomicReference<Throwable> errorInvocation = new AtomicReference<>();
		AtomicReference<Integer> afterTerminateInvocation = new AtomicReference<>();

		Mono<Integer> source = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b);

		Mono<Integer> mono = new MonoPeekTerminal<>(source,
				successInvocation::set,
				errorInvocation::set,
				(v, t) -> {
					afterTerminateInvocation.set(v);
					errorInvocation.set(t);
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.ASYNC)
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertThat((Object) successInvocation.get()).isEqualTo(55);
		assertThat(errorInvocation).hasValue(null);
		assertThat((Object) afterTerminateInvocation.get()).isEqualTo(55);
	}

	@Test
	public void should_reduce_to_10_events() {
		for (int i = 0; i < 20; i++) {
			AtomicInteger count = new AtomicInteger();
			Flux.range(0, 10)
			    .flatMap(x -> Flux.range(0, 2)
			                      .map(y -> FluxPeekFuseableTest.blockingOp(x, y))
			                      .subscribeOn(Schedulers.parallel())
			                      .reduce((l, r) -> l + "_" + r)
			                      .doOnSuccess(s -> {
				                      LOG.debug("success " + x + ": " + s);
				                      count.incrementAndGet();
			                      }))
			    .blockLast();

			assertThat(count).hasValue(10);
		}
	}

}

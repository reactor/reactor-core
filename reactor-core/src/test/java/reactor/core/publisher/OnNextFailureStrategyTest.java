/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.in;

public class OnNextFailureStrategyTest {

	@Test
	public void resumeDrop() {
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<Object> value = new AtomicReference<>();
		Hooks.onErrorDropped(error::set);
		Hooks.onNextDropped(value::set);

		try {
			OnNextFailureStrategy strategy = OnNextFailureStrategy.resumeDrop();

			String data = "foo";
			Throwable exception = new NullPointerException("foo");

			assertThat(strategy.test(exception, data)).isTrue();
			Throwable t = strategy.process(exception, data, Context.empty());

			assertThat(t).isNull();
			assertThat(error.get()).isInstanceOf(NullPointerException.class).hasMessage("foo");
			assertThat(value.get()).isEqualTo("foo");
		}
		finally {
			Hooks.resetOnErrorDropped();
			Hooks.resetOnNextDropped();
		}
	}

	@Test
	public void resumeDropWithFatal() {
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<Object> value = new AtomicReference<>();
		Hooks.onErrorDropped(error::set);
		Hooks.onNextDropped(value::set);

		try {
			OnNextFailureStrategy strategy = OnNextFailureStrategy.resumeDrop();

			String data = "foo";
			Throwable exception = new NoSuchMethodError("foo");

			assertThat(strategy.test(exception, data)).isTrue();

			assertThatExceptionOfType(NoSuchMethodError.class)
					.isThrownBy(() -> strategy.process(exception, data, Context.empty()));

			assertThat(error.get()).isNull();
			assertThat(value.get()).isNull();
		}
		finally {
			Hooks.resetOnErrorDropped();
			Hooks.resetOnNextDropped();
		}
	}


	@Test
	public void resumeDropIfMatch() {
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<Object> value = new AtomicReference<>();
		Hooks.onErrorDropped(error::set);
		Hooks.onNextDropped(value::set);

		try {
			OnNextFailureStrategy strategy = OnNextFailureStrategy.resumeDropIf(
					e -> e instanceof NullPointerException);

			String data = "foo";
			Throwable exception = new NullPointerException("foo");

			assertThat(strategy.test(exception, data)).isTrue();
			Throwable t = strategy.process(exception, data, Context.empty());

			assertThat(t).isNull();
			assertThat(error.get()).isInstanceOf(NullPointerException.class).hasMessage("foo");
			assertThat(value.get()).isEqualTo("foo");
		}
		finally {
			Hooks.resetOnErrorDropped();
			Hooks.resetOnNextDropped();
		}
	}

	@Test
	public void resumeDropIfNoMatch() {
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<Object> value = new AtomicReference<>();
		Hooks.onErrorDropped(error::set);
		Hooks.onNextDropped(value::set);

		try {
			OnNextFailureStrategy strategy = OnNextFailureStrategy.resumeDropIf(
					e -> e instanceof IllegalArgumentException);

			String data = "foo";
			Throwable exception = new NullPointerException("foo");

			assertThat(strategy.test(exception, data)).isFalse();
			Throwable t = strategy.process(exception, data, Context.empty());

			assertThat(t)
					.isSameAs(exception)
					.hasNoSuppressedExceptions();
			assertThat(error.get()).isNull();
			assertThat(value.get()).isNull();
		}
		finally {
			Hooks.resetOnErrorDropped();
			Hooks.resetOnNextDropped();
		}
	}

	@Test
	public void resumeDropIfWithFatalMatch() {
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<Object> value = new AtomicReference<>();
		Hooks.onErrorDropped(error::set);
		Hooks.onNextDropped(value::set);

		try {
			OnNextFailureStrategy strategy = OnNextFailureStrategy.resumeDropIf(
					e -> e instanceof NoSuchMethodError);

			String data = "foo";
			Throwable exception = new NoSuchMethodError("foo");

			assertThat(strategy.test(exception, data)).isTrue();
			Throwable t = strategy.process(exception, data, Context.empty());

			assertThat(t).isNull();
			assertThat(error.get())
					.isInstanceOf(NoSuchMethodError.class)
					.hasMessage("foo");
			assertThat(value.get()).isEqualTo("foo");
		}
		finally {
			Hooks.resetOnErrorDropped();
			Hooks.resetOnNextDropped();
		}
	}

	@Test
	public void resumeDropIfWithFatalNoMatch() {
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<Object> value = new AtomicReference<>();
		Hooks.onErrorDropped(error::set);
		Hooks.onNextDropped(value::set);

		try {
			OnNextFailureStrategy strategy = OnNextFailureStrategy.resumeDropIf(
					e -> e instanceof NullPointerException);

			String data = "foo";
			Throwable exception = new NoSuchMethodError("foo");

			assertThat(strategy.test(exception, data)).isFalse();

			assertThatExceptionOfType(NoSuchMethodError.class)
					.isThrownBy(() -> strategy.process(exception, data, Context.empty()));

			assertThat(error.get()).isNull();
			assertThat(value.get()).isNull();
		}
		finally {
			Hooks.resetOnErrorDropped();
			Hooks.resetOnNextDropped();
		}
	}

	@Test
	public void resumeDropIfPredicateFails() {
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<Object> value = new AtomicReference<>();
		Hooks.onErrorDropped(error::set);
		Hooks.onNextDropped(value::set);

		try {
			IllegalStateException failurePredicate = new IllegalStateException("boomInPredicate");

			OnNextFailureStrategy strategy = OnNextFailureStrategy.resumeDropIf(
					e -> { throw failurePredicate; });

			String data = "foo";
			Throwable exception = new NullPointerException("foo");

			assertThatExceptionOfType(IllegalStateException.class)
					.isThrownBy(() -> strategy.test(exception, data))
					.withMessage("boomInPredicate");

			assertThatExceptionOfType(IllegalStateException.class)
					.isThrownBy(() -> strategy.process(exception, data, Context.empty()))
					.withMessage("boomInPredicate");

			assertThat(error.get()).isNull();
			assertThat(value.get()).isNull();
		}
		finally {
			Hooks.resetOnErrorDropped();
			Hooks.resetOnNextDropped();
		}
	}

	@Test
	public void resumeDropValueHookFails() {
		AtomicReference<Throwable> error = new AtomicReference<>();
		UnsupportedOperationException failure = new UnsupportedOperationException("value hook");
		Hooks.onErrorDropped(error::set);
		Hooks.onNextDropped(v -> { throw failure; });

		try {
			OnNextFailureStrategy strategy = OnNextFailureStrategy.resumeDrop();

			String data = "foo";
			Throwable exception = new NullPointerException("foo");

			Throwable t = strategy.process(exception, data, Context.empty());
			assertThat(t)
					.hasMessage("value hook")
					.hasSuppressedException(exception);

			assertThat(error.get()).isNull();
		}
		finally {
			Hooks.resetOnErrorDropped();
			Hooks.resetOnNextDropped();
		}
	}

	@Test
	public void resumeDropErrorHookFails() {
		AtomicReference<Object> value = new AtomicReference<>();
		UnsupportedOperationException failure = new UnsupportedOperationException("error hook");
		Hooks.onNextDropped(value::set);
		Hooks.onErrorDropped(v -> { throw failure; });

		try {
			OnNextFailureStrategy strategy = OnNextFailureStrategy.resumeDrop();

			String data = "foo";
			Throwable exception = new NullPointerException("foo");

			Throwable t = strategy.process(exception, data, Context.empty());
			assertThat(t)
					.hasMessage("error hook")
					.hasSuppressedException(exception);

			assertThat(value.get()).isEqualTo("foo");
		}
		finally {
			Hooks.resetOnErrorDropped();
			Hooks.resetOnNextDropped();
		}
	}

	@Test
	public void resume() {
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<Object> value = new AtomicReference<>();

		OnNextFailureStrategy strategy = OnNextFailureStrategy.resume(
				(t, v) -> {
					error.set(t);
					value.set(v);
				});

		String data = "foo";
		Throwable exception = new NullPointerException("foo");

		assertThat(strategy.test(exception, data)).isTrue();
		Throwable t = strategy.process(exception, data, Context.empty());

		assertThat(t).isNull();
		assertThat(error.get()).isInstanceOf(NullPointerException.class).hasMessage("foo");
		assertThat(value.get()).isEqualTo("foo");
	}

	@Test
	public void resumeWithFatal() {
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<Object> value = new AtomicReference<>();

		OnNextFailureStrategy strategy = OnNextFailureStrategy.resume(
				(t, v) -> {
					error.set(t);
					value.set(v);
				});

		String data = "foo";
		Throwable exception = new NoSuchMethodError("foo");

		assertThat(strategy.test(exception, data)).isTrue();

		assertThatExceptionOfType(NoSuchMethodError.class)
				.isThrownBy(() -> strategy.process(exception, data, Context.empty()));

		assertThat(error.get()).isNull();
		assertThat(value.get()).isNull();
	}

	@Test
	public void resumeErrorConsumerFails() {
		AtomicReference<Object> value = new AtomicReference<>();
		IllegalStateException failureError = new IllegalStateException("boomInErrorConsumer");

		OnNextFailureStrategy strategy = OnNextFailureStrategy.resume(
				(t, v) -> {
					value.set(v);
					throw failureError;
				});

		String data = "foo";
		Throwable exception = new NullPointerException("foo");

		assertThat(strategy.test(exception, data)).isTrue();
		Throwable t = strategy.process(exception, data, Context.empty());

		assertThat(t).isSameAs(failureError)
		             .hasSuppressedException(exception);

		assertThat(value.get()).isEqualTo("foo");
	}

	@Test
	public void resumeIfMatch() {
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<Object> value = new AtomicReference<>();

		OnNextFailureStrategy strategy = OnNextFailureStrategy.resumeIf(
				e -> e instanceof NullPointerException,
				(t, v) -> {
					error.set(t);
					value.set(v);
				});

		String data = "foo";
		Throwable exception = new NullPointerException("foo");

		assertThat(strategy.test(exception, data)).isTrue();
		Throwable t = strategy.process(exception, data, Context.empty());

		assertThat(t).isNull();
		assertThat(error.get()).isInstanceOf(NullPointerException.class).hasMessage("foo");
		assertThat(value.get()).isEqualTo("foo");
	}

	@Test
	public void resumeIfNoMatch() {
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<Object> value = new AtomicReference<>();

		OnNextFailureStrategy strategy = OnNextFailureStrategy.resumeIf(
				e -> e instanceof IllegalArgumentException,
				(t, v) -> {
					error.set(t);
					value.set(v);
				});

		String data = "foo";
		Throwable exception = new NullPointerException("foo");

		assertThat(strategy.test(exception, data)).isFalse();
		Throwable t = strategy.process(exception, data, Context.empty());

		assertThat(t)
				.isSameAs(exception)
				.hasNoSuppressedExceptions();
		assertThat(error.get()).isNull();
		assertThat(value.get()).isNull();
	}

	@Test
	public void resumeIfWithFatalMatch() {
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<Object> value = new AtomicReference<>();

		OnNextFailureStrategy strategy = OnNextFailureStrategy.resumeIf(
				e -> e instanceof NoSuchMethodError,
				(t, v) -> {
					error.set(t);
					value.set(v);
				});

		String data = "foo";
		Throwable exception = new NoSuchMethodError("foo");

		assertThat(strategy.test(exception, data)).isTrue();
		Throwable t = strategy.process(exception, data, Context.empty());

		assertThat(t).isNull();
		assertThat(error.get())
				.isInstanceOf(NoSuchMethodError.class)
				.hasMessage("foo");
		assertThat(value.get()).isEqualTo("foo");
	}

	@Test
	public void resumeIfWithFatalNoMatch() {
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<Object> value = new AtomicReference<>();

		OnNextFailureStrategy strategy = OnNextFailureStrategy.resumeIf(
				e -> e instanceof IllegalArgumentException,
				(t, v) -> {
					error.set(t);
					value.set(v);
				});

		String data = "foo";
		Throwable exception = new NoSuchMethodError("foo");

		assertThat(strategy.test(exception, data)).isFalse();

		assertThatExceptionOfType(NoSuchMethodError.class)
				.isThrownBy(() -> strategy.process(exception, data, Context.empty()));

		assertThat(error.get()).isNull();
		assertThat(value.get()).isNull();
	}

	@Test
	public void resumeIfErrorConsumerFails() {
		AtomicReference<Object> value = new AtomicReference<>();
		IllegalStateException failureError = new IllegalStateException("boomInErrorConsumer");

		OnNextFailureStrategy strategy = OnNextFailureStrategy.resumeIf(
				e -> true,
				(t, v) -> {
					value.set(v);
					throw failureError;
				});

		String data = "foo";
		Throwable exception = new NullPointerException("foo");

		assertThat(strategy.test(exception, data)).isTrue();
		Throwable t = strategy.process(exception, data, Context.empty());

		assertThat(t).isSameAs(failureError)
		             .hasSuppressedException(exception);

		assertThat(value.get()).isEqualTo("foo");
	}

	@Test
	public void resumeIfPredicateFails() {
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<Object> value = new AtomicReference<>();
		IllegalStateException failurePredicate = new IllegalStateException("boomInPredicate");

		OnNextFailureStrategy strategy = OnNextFailureStrategy.resumeIf(
				e -> { throw failurePredicate; },
				(t, v) -> {
					error.set(t);
					value.set(v);
				});

		String data = "foo";
		Throwable exception = new NullPointerException("foo");

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(() -> strategy.test(exception, data))
				.withMessage("boomInPredicate");

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(() -> strategy.process(exception, data, Context.empty()))
				.withMessage("boomInPredicate");

		assertThat(error.get()).isNull();
		assertThat(value.get()).isNull();
	}

	@Test
	public void stopCannotResume() {
		OnNextFailureStrategy strategy = OnNextFailureStrategy.stop();
		assertThat(strategy.test(new IllegalStateException(), null))
				.isFalse();
		assertThat(strategy.test(new NoSuchMethodError(), null))
				.isFalse();
	}

	@Test
	public void stopProcessReturnsNewException() {
		OnNextFailureStrategy strategy = OnNextFailureStrategy.stop();
		Throwable exception = new NullPointerException("foo");

		Throwable t = strategy.process(exception, null, Context.empty());

		assertThat(t).isInstanceOf(IllegalStateException.class)
		             .hasMessage("STOP strategy cannot process errors")
		             .hasSuppressedException(exception);
	}

	@Test
	public void stopProcessWithFatal() {
		OnNextFailureStrategy strategy = OnNextFailureStrategy.stop();
		Throwable exception = new NoSuchMethodError("foo");

		assertThatExceptionOfType(NoSuchMethodError.class)
				.isThrownBy(() -> strategy.process(exception, null, Context.empty()))
				.satisfies(e -> assertThat(e)
						.hasMessage("foo")
						.hasNoSuppressedExceptions());
	}

	@Test
	public void fluxApiErrorDrop() {
		Flux<String> test = Flux.just("foo", "", "bar", "baz")
		                        .filter(s -> 3 / s.length() == 1)
		                        .errorStrategyContinue();

		StepVerifier.create(test)
		            .expectNext("foo", "bar", "baz")
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDroppedExactly("")
		            .hasDroppedErrorWithMessage("/ by zero");
	}

	@Test
	public void fluxApiErrorDropConditional() {
		Flux<String> test = Flux.just("foo", "", "bar", "baz")
		                        .filter(s -> 3 / s.length() == 1)
		                        .errorStrategyContinue(t -> t instanceof ArithmeticException);

		StepVerifier.create(test)
		            .expectNext("foo", "bar", "baz")
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDroppedExactly("")
		            .hasDroppedErrorWithMessage("/ by zero");
	}

	@Test
	public void fluxApiErrorDropConditionalErrorNotMatching() {
		Flux<String> test = Flux.just("foo", "", "bar", "baz")
				.filter(s -> 3 / s.length() == 1)
				.errorStrategyContinue(t -> t instanceof IllegalStateException);

		StepVerifier.create(test)
				.expectNext("foo")
				.verifyError(ArithmeticException.class);
	}

	@Test
	public void fluxApiErrorDropConditionalByClass() {
		Flux<String> test = Flux.just("foo", "", "bar", "baz")
				.filter(s -> 3 / s.length() == 1)
				.errorStrategyContinue(ArithmeticException.class);

		StepVerifier.create(test)
				.expectNext("foo", "bar", "baz")
				.expectComplete()
				.verifyThenAssertThat()
				.hasDroppedExactly("")
				.hasDroppedErrorWithMessage("/ by zero");
	}

	@Test
	public void fluxApiErrorDropConditionalErrorByClassNotMatching() {
		Flux<String> test = Flux.just("foo", "", "bar", "baz")
				.filter(s -> 3 / s.length() == 1)
				.errorStrategyContinue(StackOverflowError.class);

		StepVerifier.create(test)
				.expectNext("foo")
				.verifyError(ArithmeticException.class);
	}

	@Test
	public void fluxApiErrorContinue() {
		List<String> valueDropped = new ArrayList<>();
		List<Throwable> errorDropped = new ArrayList<>();
		Flux<String> test = Flux.just("foo", "", "bar", "baz")
		                        .filter(s -> 3 / s.length() == 1)
		                        .errorStrategyContinue((t, v) -> {
									errorDropped.add(t);
									valueDropped.add(v);
								});


		StepVerifier.create(test)
		            .expectNext("foo", "bar", "baz")
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasNotDroppedElements()
		            .hasNotDroppedErrors();

		assertThat(valueDropped).containsExactly("");
		assertThat(errorDropped)
				.hasSize(1)
				.allSatisfy(e -> assertThat(e).hasMessage("/ by zero"));
	}

	@Test
	public void fluxApiErrorContinueConditional() {
		List<String> valueDropped = new ArrayList<>();
		List<Throwable> errorDropped = new ArrayList<>();
		Flux<String> test = Flux.just("foo", "", "bar", "baz")
		                        .filter(s -> 3 / s.length() == 1)
		                        .errorStrategyContinue(
				                        t -> t instanceof ArithmeticException,
										(t, v) -> {
											errorDropped.add(t);
											valueDropped.add(v);
										});

		StepVerifier.create(test)
		            .expectNext("foo", "bar", "baz")
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasNotDroppedElements()
		            .hasNotDroppedErrors();

		assertThat(valueDropped).containsExactly("");
		assertThat(errorDropped)
				.hasSize(1)
				.allSatisfy(e -> assertThat(e).hasMessage("/ by zero"));
	}

	@Test
	public void fluxApiErrorContinueConditionalErrorNotMatch() {
		List<String> valueDropped = new ArrayList<>();
		List<Throwable> errorDropped = new ArrayList<>();
		Flux<String> test = Flux.just("foo", "", "bar", "baz")
		                        .filter(s -> 3 / s.length() == 1)
		                        .errorStrategyContinue(
				                        t -> t instanceof IllegalStateException,
										(t, v) -> {
											errorDropped.add(t);
											valueDropped.add(v);
										});

		StepVerifier.create(test)
		            .expectNext("foo")
		            .expectErrorMessage("/ by zero")
		            .verifyThenAssertThat()
		            .hasNotDroppedElements()
		            .hasNotDroppedErrors();

		assertThat(valueDropped).isEmpty();
		assertThat(errorDropped).isEmpty();
	}

	@Test
	public void fluxApiErrorContinueConditionalByClass() {
		List<String> valueDropped = new ArrayList<>();
		List<Throwable> errorDropped = new ArrayList<>();
		Flux<String> test = Flux.just("foo", "", "bar", "baz")
				.filter(s -> 3 / s.length() == 1)
				.errorStrategyContinue(ArithmeticException.class,
									   (t, v) -> {
										   errorDropped.add(t);
										   valueDropped.add(v);
									   });

		StepVerifier.create(test)
				.expectNext("foo", "bar", "baz")
				.expectComplete()
				.verifyThenAssertThat()
				.hasNotDroppedElements()
				.hasNotDroppedErrors();

		assertThat(valueDropped).containsExactly("");
		assertThat(errorDropped)
				.hasSize(1)
				.allSatisfy(e -> assertThat(e).hasMessage("/ by zero"));
	}

	@Test
	public void fluxApiErrorContinueConditionalByClassNotMatch() {
		List<String> valueDropped = new ArrayList<>();
		List<Throwable> errorDropped = new ArrayList<>();
		Flux<String> test = Flux.just("foo", "", "bar", "baz")
				.filter(s -> 3 / s.length() == 1)
				.errorStrategyContinue(IllegalStateException.class,
									   (t, v) -> {
										   errorDropped.add(t);
										   valueDropped.add(v);
									   });

		StepVerifier.create(test)
				.expectNext("foo")
				.expectErrorMessage("/ by zero")
				.verifyThenAssertThat()
				.hasNotDroppedElements()
				.hasNotDroppedErrors();

		assertThat(valueDropped).isEmpty();
		assertThat(errorDropped).isEmpty();
	}

	@Test
	public void fluxApiWithinFlatMap() {
		Flux<Integer> test = Flux.just(1, 2, 3)
		                         .flatMap(i -> Flux.range(0, i + 1)
		                                           .map(v -> 30 / v))
		                         .errorStrategyContinue();

		StepVerifier.create(test)
		            .expectNext(30, 30, 15, 30, 15, 10)
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDroppedExactly(0, 0, 0)
		            .hasDroppedErrorsSatisfying(
		            		errors -> assertThat(errors)
						            .hasSize(3)
						            .allMatch(e -> e instanceof ArithmeticException));
	}

	@Test
	public void monoApiWithinFlatMap() {
		Flux<Integer> test = Flux.just(0, 1, 2, 3)
				.flatMap(i -> Mono.just(i).map(v -> 30 / v))
				.errorStrategyContinue();

		StepVerifier.create(test)
				.expectNext(30, 15, 10)
				.expectComplete()
				.verifyThenAssertThat()
				.hasDroppedExactly(0)
				.hasDroppedErrorsSatisfying(
						errors -> assertThat(errors)
								.hasSize(1)
								.allMatch(e -> e instanceof ArithmeticException));
	}

	@Test
	public void overrideInheritedErrorStrategyInFlatMap() {
		Flux<Integer> test = Flux.just(1, 2, 3)
				.flatMap(i -> Flux.range(0, i + 1)
						.map(v -> 30 / v)
						.onErrorReturn(100)
						.errorStrategyStop())
				.errorStrategyContinue();

		StepVerifier.create(test)
				.expectNext(100, 100, 100)
				.expectComplete()
				.verify();
	}

	@Test
	public void errorStrategyConfiguredInFlatMapDoesNotLeak() {
		Flux<Integer> test = Flux.just(0, 1, 2)
				.map(i -> i / 0)
				.flatMap(i -> Flux.just(i).errorStrategyContinue());

		StepVerifier.create(test)
				.expectError(ArithmeticException.class)
				.verify();
	}

	@Test
	public void errorStrategySimpleScoping() {
		Flux<Integer> test = Flux.just(0, 1, 2, 3)
				.map(i -> {
					if (i == 3) {
						throw new IllegalStateException();
					}
					else {
						return i;
					}
				})
				.errorStrategyStop()
				.map(i -> 10 / i)
				.errorStrategyContinue();

		StepVerifier.create(test)
				.expectNext(10, 5)
				.expectError(IllegalStateException.class)
				.verifyThenAssertThat()
				.hasDropped(0)
				.hasDroppedErrors(1);
	}


}

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
import static org.assertj.core.api.Assertions.fail;

public class OnNextFailureStrategyTest {

	@Test
	public void resume() throws Exception {
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<Object> value = new AtomicReference<>();

		OnNextFailureStrategy strategy = OnNextFailureStrategy.resume(
				error::set, value::set);

		String data = "foo";
		Throwable exception = new NullPointerException("foo");

		assertThat(strategy.canResume(exception, data)).isTrue();

		strategy.process(exception, data, Context.empty());

		assertThat(error.get()).isInstanceOf(NullPointerException.class).hasMessage("foo");
		assertThat(value.get()).isEqualTo("foo");
	}

	@Test
	public void resumeFailingConsumer() throws Exception {
		IllegalStateException failureValue = new IllegalStateException("boomInValueConsumer");
		IllegalStateException failureError = new IllegalStateException("boomInErrorConsumer");
		NullPointerException npe = new NullPointerException("foo");

		OnNextFailureStrategy strategy = OnNextFailureStrategy.resume(
				e -> { throw failureError; },
				v -> { throw failureValue; });

		assertThat(strategy.canResume(npe, "foo")).isTrue();

		assertThatExceptionOfType(IllegalStateException.class)
				.isThrownBy(() -> strategy.process(npe, "foo", Context.empty()))
				.isSameAs(failureValue);
	}

	@Test
	public void conditionalResume() throws Exception {
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<Object> value = new AtomicReference<>();
		AtomicReference<Throwable> errorDropped = new AtomicReference<>();
		AtomicReference<Object> valueDropped = new AtomicReference<>();

		Hooks.onNextDropped(valueDropped::set);
		Hooks.onErrorDropped(errorDropped::set);

		String data = "foo";
		Throwable exception = new NullPointerException("foo");

		try {
			OnNextFailureStrategy strategy = OnNextFailureStrategy.resumeIf(
					e -> e instanceof NullPointerException,
					error::set, value::set);

			assertThat(strategy.canResume(exception, data)).isTrue();

			strategy.process(exception, data, Context.empty());

			assertThat(error.get()).isInstanceOf(NullPointerException.class).hasMessage("foo");
			assertThat(value.get()).isEqualTo("foo");
			assertThat(errorDropped.get()).isNull();
			assertThat(valueDropped.get()).isNull();
		}
		finally {
			Hooks.resetOnNextDropped();
			Hooks.resetOnErrorDropped();
		}
	}

	@Test
	public void conditionalResumeNoMatch() throws Exception {
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<Object> value = new AtomicReference<>();

		OnNextFailureStrategy strategy = OnNextFailureStrategy.resumeIf(
				e -> e instanceof IllegalArgumentException,
				error::set, value::set);

		assertThat(strategy.canResume(new NullPointerException("foo"), "foo"))
				.isFalse();

		assertThat(error.get()).isNull();
		assertThat(value.get()).isNull();
	}

	@Test
	public void conditionalResumeFailingPredicate() throws Exception {
		NullPointerException npe = new NullPointerException("foo");
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<Object> value = new AtomicReference<>();
		AtomicReference<Throwable> onOperatorError = new AtomicReference<>();
		AtomicReference<Object> onOperatorValue = new AtomicReference<>();

		Hooks.onOperatorError("test", (e, v) -> {
			onOperatorError.set(e);
			onOperatorValue.set(v);
			return e;
		});

		try {
			OnNextFailureStrategy strategy = OnNextFailureStrategy.resumeIf(
					e -> { throw new IllegalStateException("boom"); },
					error::set, value::set);

			assertThatExceptionOfType(IllegalStateException.class)
					.isThrownBy(() -> strategy.canResume(npe, "foo"))
					.withMessage("boom")
					.satisfies(e -> assertThat(e).hasNoSuppressedExceptions());

			assertThat(error.get()).isNull();
			assertThat(value.get()).isNull();
			assertThat(onOperatorError.get()).isNull();
			assertThat(onOperatorValue.get()).isNull();
		}
		finally {
			Hooks.resetOnOperatorError("test");
		}
	}

	@Test
	public void conditionalResumeFailingConsumer() throws Exception {
		NullPointerException error = new NullPointerException("foo");
		AtomicReference<Throwable> onOperatorError = new AtomicReference<>();
		AtomicReference<Object> onOperatorValue = new AtomicReference<>();

		Hooks.onOperatorError("test", (e, v) -> {
			onOperatorError.set(e);
			onOperatorValue.set(v);
			return e;
		});

		try {
			OnNextFailureStrategy strategy = OnNextFailureStrategy.resumeIf(
					e -> e instanceof NullPointerException,
					e -> { throw new IllegalStateException("boomError"); },
					v -> { throw new IllegalStateException("boomValue"); });

			assertThat(strategy.canResume(error, "foo")).isTrue();

			assertThatExceptionOfType(IllegalStateException.class)
					.isThrownBy(() -> strategy.process(error, "foo", Context.empty()))
					.withMessage("boomValue")
					.satisfies(e -> assertThat(e).hasNoSuppressedExceptions());

			assertThat(onOperatorError.get()).isNull();
			assertThat(onOperatorValue.get()).isNull();
		}
		finally {
			Hooks.resetOnOperatorError("test");
		}
	}

	@Test
	public void fluxApiErrorDrop() {
		Flux<String> test = Flux.just("foo", "", "bar", "baz")
		                        .filter(s -> 3 / s.length() == 1)
		                        .onErrorContinue();

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
		                        .onErrorContinue(t -> t instanceof ArithmeticException);

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
		                        .onErrorContinue(t -> t instanceof IllegalStateException);

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
		                        .onErrorContinue(errorDropped::add, valueDropped::add);

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
		                        .onErrorContinue(
				                        t -> t instanceof ArithmeticException,
				                        errorDropped::add, valueDropped::add);

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
		                        .onErrorContinue(
				                        t -> t instanceof IllegalStateException,
					                        errorDropped::add,
					                        valueDropped::add);

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
		                         .onErrorContinue();

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

}
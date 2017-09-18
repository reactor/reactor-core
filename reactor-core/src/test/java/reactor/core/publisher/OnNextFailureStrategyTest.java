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

public class OnNextFailureStrategyTest {

	@Test
	public void resume() throws Exception {
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<Object> value = new AtomicReference<>();
		Operators.DeferredSubscription s = new Operators.DeferredSubscription();

		OnNextFailureStrategy strategy = OnNextFailureStrategy.resume((e, v) -> {
			error.set(e);
			value.set(v);
		});

		Throwable result = strategy.apply("foo", new NullPointerException("foo"),
				Context.empty(), s);

		assertThat(result).isNull();
		assertThat(s.isCancelled()).as("s cancelled").isFalse();
		assertThat(error.get()).isInstanceOf(NullPointerException.class).hasMessage("foo");
		assertThat(value.get()).isEqualTo("foo");
	}

	@Test
	public void resumeFailingConsumer() throws Exception {
		IllegalStateException failure = new IllegalStateException("boom");
		NullPointerException npe = new NullPointerException("foo");
		Operators.DeferredSubscription s = new Operators.DeferredSubscription();
		OnNextFailureStrategy strategy = OnNextFailureStrategy.resume((e, v) -> {
			throw failure;
		});

		Throwable result = strategy.apply("foo", npe, Context.empty(), s);

		assertThat(result).isSameAs(failure)
		                  .hasSuppressedException(npe);
		assertThat(s.isCancelled()).as("s cancelled").isTrue();
	}

	@Test
	public void conditionalResume() throws Exception {
		Operators.DeferredSubscription s = new Operators.DeferredSubscription();
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<Object> value = new AtomicReference<>();
		AtomicReference<Throwable> errorDropped = new AtomicReference<>();
		AtomicReference<Object> valueDropped = new AtomicReference<>();

		Hooks.onNextDropped(valueDropped::set);
		Hooks.onErrorDropped(errorDropped::set);

		try {
			OnNextFailureStrategy strategy = OnNextFailureStrategy.resumeIf(
					(e, v) -> e instanceof NullPointerException,
					(e, v) -> {
						error.set(e);
						value.set(v);
					});

			Throwable result = strategy.apply("foo", new NullPointerException("foo"),
					Context.empty(), s);

			assertThat(result).isNull();
			assertThat(s.isCancelled()).isFalse();
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
	public void conditionalResumeFallback() throws Exception {
		Operators.DeferredSubscription s = new Operators.DeferredSubscription();
		AtomicReference<Throwable> error = new AtomicReference<>();
		AtomicReference<Object> value = new AtomicReference<>();
		AtomicReference<Throwable> errorDropped = new AtomicReference<>();
		AtomicReference<Object> valueDropped = new AtomicReference<>();

		Hooks.onNextDropped(valueDropped::set);
		Hooks.onErrorDropped(errorDropped::set);

		try {
			OnNextFailureStrategy strategy = OnNextFailureStrategy.resumeIf(
					(e, v) -> e instanceof IllegalArgumentException,
					(e, v) -> {
						error.set(e);
						value.set(v);
					});

			Throwable result = strategy.apply("foo", new NullPointerException("foo"),
					Context.empty(), s);

			assertThat(result).isInstanceOf(NullPointerException.class).hasMessage("foo");
			assertThat(s.isCancelled()).isTrue();
			assertThat(error.get()).isNull();
			assertThat(value.get()).isNull();
			assertThat(errorDropped.get()).isNull();
			assertThat(valueDropped.get()).isNull();
		}
		finally {
			Hooks.resetOnNextDropped();
			Hooks.resetOnErrorDropped();
		}
	}

	@Test
	public void conditionalResumeFailingPredicate() throws Exception {
		NullPointerException npe = new NullPointerException("foo");
		Operators.DeferredSubscription s = new Operators.DeferredSubscription();
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
					(e, v) -> { throw new IllegalStateException("boom"); },
					(e, v) -> {
						error.set(e);
						value.set(v);
					});

			Throwable result = strategy.apply("foo", npe, Context.empty(), s);

			assertThat(result).isInstanceOf(IllegalStateException.class)
			                  .hasMessage("boom")
			                  .hasSuppressedException(npe);
			assertThat(s.isCancelled()).isTrue();
			assertThat(error.get()).isNull();
			assertThat(value.get()).isNull();
			assertThat(onOperatorError.get())
					.isInstanceOf(IllegalStateException.class)
					.hasMessage("boom")
					.hasSuppressedException(npe);
			assertThat(onOperatorValue.get()).isEqualTo("foo");
		}
		finally {
			Hooks.resetOnOperatorError("test");
		}
	}

	@Test
	public void conditionalResumeFailingConsumer() throws Exception {
		NullPointerException error = new NullPointerException("foo");
		Operators.DeferredSubscription s = new Operators.DeferredSubscription();
		AtomicReference<Throwable> onOperatorError = new AtomicReference<>();
		AtomicReference<Object> onOperatorValue = new AtomicReference<>();

		Hooks.onOperatorError("test", (e, v) -> {
			onOperatorError.set(e);
			onOperatorValue.set(v);
			return e;
		});

		try {
			OnNextFailureStrategy strategy = OnNextFailureStrategy.resumeIf(
					(e, v) -> e instanceof NullPointerException,
					(e, v) -> { throw new IllegalStateException("boom"); });

			Throwable result = strategy.apply("foo", error, Context.empty(), s);

			assertThat(result).isInstanceOf(IllegalStateException.class)
			                  .hasMessage("boom")
			                  .hasSuppressedException(error);
			assertThat(s.isCancelled()).isTrue();
			assertThat(onOperatorError.get())
					.isInstanceOf(IllegalStateException.class)
					.hasMessage("boom")
					.hasSuppressedException(error);
			assertThat(onOperatorValue.get()).isEqualTo("foo");
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
		                        .onErrorContinue((t, v) -> t instanceof ArithmeticException);

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
		                        .onErrorContinue((t, v) -> t instanceof IllegalStateException);

		StepVerifier.create(test)
		            .expectNext("foo")
		            .verifyError(ArithmeticException.class);
	}

	@Test
	public void fluxApiErrorDropConditionalValueNotMatching() {
		Flux<String> test = Flux.just("foo", "", "bar", "baz")
		                        .filter(s -> 3 / s.length() == 1)
		                        .onErrorContinue((t, v) -> v.length() == 3);

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
		                        .onErrorContinue((t, v) -> {
			                        valueDropped.add(v);
			                        errorDropped.add(t);
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
		                        .onErrorContinue(
				                        (t, v) -> t instanceof ArithmeticException,
				                        (t, v) -> {
					                        valueDropped.add(v);
					                        errorDropped.add(t);
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
		                        .onErrorContinue(
				                        (t, v) -> t instanceof IllegalStateException,
				                        (t, v) -> {
					                        valueDropped.add(v);
					                        errorDropped.add(t);
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
	public void fluxApiErrorContinueConditionalValueNotMatch() {
		List<String> valueDropped = new ArrayList<>();
		List<Throwable> errorDropped = new ArrayList<>();
		Flux<String> test = Flux.just("foo", "", "bar", "baz")
		                        .filter(s -> 3 / s.length() == 1)
		                        .onErrorContinue(
				                        (t, v) -> v.length() == 3,
				                        (t, v) -> {
					                        valueDropped.add(v);
					                        errorDropped.add(t);
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

}
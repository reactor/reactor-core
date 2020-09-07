/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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
package reactor.test;

import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.*;

public class StepVerifierAssertionsTests {

	@Test
	public void assertDroppedElementsAllPass() {
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onNext("foo");
			s.onComplete();
			s.onNext("bar");
			s.onNext("baz");
		}).take(3))
		            .expectNext("foo")
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDroppedElements()
		            .hasDropped("baz")
		            .hasDroppedExactly("baz", "bar");
	}

	@Test
	public void assertNotDroppedElementsFailureOneDrop() {
		try {
			StepVerifier.create(Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				s.onNext("foo");
				s.onComplete();
				s.onNext("bar");
			}).take(2))
			            .expectNext("foo")
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasNotDroppedElements();
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected no dropped elements, found <[bar]>.");
		}
	}

	@Test
	public void assertDroppedElementsFailureNoDrop() {
		try {
			StepVerifier.create(Mono.empty())
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasNotDroppedElements()
			            .hasDroppedElements();
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected dropped elements, none found.");
		}
	}

	@Test
	public void assertDroppedElementsFailureOneExtra() {
		try {
			StepVerifier.create(Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				s.onNext("foo");
				s.onComplete();
				s.onNext("bar");
				s.onNext("baz");
			}).take(3))
			            .expectNext("foo")
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDropped("foo");
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected dropped elements to contain <[foo]>, was <[bar, baz]>.");
		}
	}

	@Test
	public void assertDroppedElementsFailureOneMissing() {
		try {
			StepVerifier.create(Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				s.onNext("foo");
				s.onComplete();
				s.onNext("bar");
				s.onNext("baz");
			}).take(3))
			            .expectNext("foo")
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDroppedExactly("baz");
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected dropped elements to contain exactly <[baz]>, was <[bar, baz]>.");
		}
	}

	@Test
	public void assertDiscardedElementsAllPass() {
		StepVerifier.create(Flux.just(1, 2, 3).filter(i -> i == 2))
		            .expectNext(2)
		            .expectComplete()
			    .verifyThenAssertThat()
			    .hasDiscardedElements()
			    .hasDiscardedExactly(1, 3)
			    .hasDiscarded(1)
			    .hasDiscardedElementsMatching(list -> list.stream().allMatch(e -> (int)e % 2 != 0))
			    .hasDiscardedElementsSatisfying(list -> assertThat(list).containsOnly(1, 3));
	}

	@Test
	public void assertNotDiscardedElementsFailureOneDiscarded() {
		try {
			StepVerifier.create(Flux.just(1, 2).filter(i -> i == 2))
			            .expectNext(2)
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasNotDiscardedElements();
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected no discarded elements, found <[1]>.");
		}
	}

	@Test
	public void assertDiscardedElementsFailureNoDiscarded() {
		try {
			StepVerifier.create(Mono.empty())
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasNotDiscardedElements()
			            .hasDiscardedElements();
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected discarded elements, none found.");
		}
	}

	@Test
	public void assertDiscardedElementsFailureOneExtra() {
		try {
			StepVerifier.create(Flux.just(1, 2, 3).filter(i -> i == 2))
			            .expectNext(2)
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDiscarded(4);
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected discarded elements to contain <[4]>, was <[1, 3]>.");
		}
	}

        @Test
        public void assertDiscardedElementsSatisfyingFailureOneExtra() {
		try {
	                 StepVerifier.create(Flux.just(1, 2, 3).filter(i -> i == 2))
		                     .expectNext(2)
		                     .expectComplete()
		                     .verifyThenAssertThat()
		                     .hasDiscardedElementsSatisfying(list -> assertThat(list).hasSize(3));
	                 fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
	                 assertThat(ae).hasMessageContaining("Expected size:<3> but was:<2> in:");
	        }
        }

	@Test
	public void assertDiscardedElementsMatchingFailureOneExtra() {
		try {
			StepVerifier.create(Flux.just(1, 2, 3).filter(i -> i == 2))
					.expectNext(2)
					.expectComplete()
					.verifyThenAssertThat()
					.hasDiscardedElementsMatching(list -> list.stream().allMatch(e -> (int)e % 2 == 0));
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected collection of discarded elements matching the given predicate, did not match: <[1, 3]>.");
		}
	}


	@Test
	public void assertDiscardedElementsFailureOneMissing() {
		try {
			StepVerifier.create(Flux.just(1, 2, 3).filter(i -> i == 2))
			            .expectNext(2)
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasDiscardedExactly(1);
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected discarded elements to contain exactly <[1]>, was <[1, 3]>.");
		}
	}

	@Test
	public void assertDroppedErrorAllPass() {
		Throwable err1 = new IllegalStateException("boom1");
		Throwable err2 = new IllegalStateException("boom2");
		StepVerifier.create(Flux.from(s -> {
			s.onSubscribe(Operators.emptySubscription());
			s.onError(err1);
			s.onError(err2);
		}).buffer(1))
		            .expectError()
		            .verifyThenAssertThat()
		            .hasDroppedErrors()
		            .hasDroppedErrors(1)
		            .hasDroppedErrorOfType(IllegalStateException.class)
		            .hasDroppedErrorWithMessageContaining("boom")
		            .hasDroppedErrorWithMessage("boom2")
		            .hasDroppedErrorMatching(t -> t instanceof IllegalStateException && "boom2".equals(t.getMessage()));
	}


	@Test
	public void assertNotDroppedErrorsFailureOneDrop() {
		try {
			StepVerifier.create(Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				s.onNext("foo");
				s.onComplete();
				s.onError(new IllegalStateException("boom"));
			}).take(2))
			            .expectNext("foo")
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasNotDroppedErrors();
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected no dropped errors, found <[java.lang.IllegalStateException: boom]>.");
		}
	}

	@Test
	public void assertDroppedErrorFailureNoDrop() {
		try {
			StepVerifier.create(Mono.empty())
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasNotDroppedErrors()
			            .hasDroppedErrors();
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected at least 1 dropped error, none found.");
		}
	}

	@Test
	public void assertDroppedErrorFailureWrongType() {
		try {
			Throwable err1 = new IllegalStateException("boom1");
			Throwable err2 = new IllegalStateException("boom2");
			StepVerifier.create(Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				s.onError(err1);
				s.onError(err2);
			}).buffer(1))
			            .expectError()
			            .verifyThenAssertThat()
			            .hasDroppedErrorOfType(IllegalArgumentException.class);
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected dropped error to be of type java.lang.IllegalArgumentException, was java.lang.IllegalStateException.");
		}
	}

	@Test
	public void assertDroppedErrorFailureWrongContains() {
		try {
			Throwable err1 = new IllegalStateException("boom1");
			Throwable err2 = new IllegalStateException("boom2");
			StepVerifier.create(Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				s.onError(err1);
				s.onError(err2);
			}).buffer(1))
			            .expectError()
			            .verifyThenAssertThat()
			            .hasDroppedErrorWithMessageContaining("foo");
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected dropped error with message containing <\"foo\">, was <\"boom2\">.");
		}
	}

	@Test
	public void assertDroppedErrorFailureWrongMessage() {
		try {
			Throwable err1 = new IllegalStateException("boom1");
			Throwable err2 = new IllegalStateException("boom2");
			StepVerifier.create(Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				s.onError(err1);
				s.onError(err2);
			}).buffer(1))
			            .expectError()
			            .verifyThenAssertThat()
			            .hasDroppedErrorWithMessage("boom1");
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected dropped error with message <\"boom1\">, was <\"boom2\">.");
		}
	}

	@Test
	public void assertDroppedErrorFailureWrongMatch() {
		try {
			Throwable err1 = new IllegalStateException("boom1");
			Throwable err2 = new IllegalStateException("boom2");
			StepVerifier.create(Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				s.onError(err1);
				s.onError(err2);
			}).buffer(1))
			            .expectError()
			            .verifyThenAssertThat()
			            .hasDroppedErrorMatching(t -> t instanceof IllegalStateException && "foo".equals(t.getMessage()));
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected dropped error matching the given predicate, did not match: <java.lang.IllegalStateException: boom2>.");
		}
	}

	@Test
	public void assertDroppedErrorsFailureWrongCount() {
		Throwable err1 = new IllegalStateException("boom1");
		Throwable err2 = new IllegalStateException("boom2");
		Throwable err3 = new IllegalStateException("boom3");
		try {
			StepVerifier.create(Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				s.onError(err1);
				s.onError(err2);
				s.onError(err3);
			}).buffer(1))
			            .expectError()
			            .verifyThenAssertThat()
			            .hasDroppedErrors()
			            .hasDroppedErrors(3);
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected exactly 3 dropped errors, 2 found.");
		}
	}

	@Test
	public void assertDroppedErrorsNotSatisfying() {
		Throwable err1 = new IllegalStateException("boom1");
		Throwable err2 = new IllegalStateException("boom2");
		Throwable err3 = new IllegalStateException("boom3");
		try {
			StepVerifier.create(Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				s.onError(err1);
				s.onError(err2);
				s.onError(err3);
			}).buffer(1))
			            .expectError()
			            .verifyThenAssertThat()
			            .hasDroppedErrorsSatisfying(c -> assertThat(c).hasSize(3));
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessageContaining("Expected size:<3> but was:<2> in:");
		}
	}

	@Test
	public void assertDroppedErrorsNotMatching() {
		Throwable err1 = new IllegalStateException("boom1");
		Throwable err2 = new IllegalStateException("boom2");
		Throwable err3 = new IllegalStateException("boom3");
		try {
			StepVerifier.create(Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				s.onError(err1);
				s.onError(err2);
				s.onError(err3);
			}).buffer(1))
			            .expectError()
			            .verifyThenAssertThat()
			            .hasDroppedErrorsMatching(c -> c.size() == 3);
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected collection of dropped errors matching the " +
					"given predicate, did not match: <[java.lang.IllegalStateException: boom2, " +
					"java.lang.IllegalStateException: boom3]>.");
		}
	}

	@Test
	public void assertOperatorErrorAllPass() {
		IllegalStateException err1 = new IllegalStateException("boom1");
		StepVerifier.create(Flux.just("test").map(d -> {
			throw err1;
		}))
		            .expectError()
		            .verifyThenAssertThat()
		            .hasOperatorErrors()
		            .hasOperatorErrors(1)
		            .hasOperatorErrorOfType(IllegalStateException.class)
		            .hasOperatorErrorWithMessageContaining("boom")
		            .hasOperatorErrorWithMessage("boom1")
		            .hasOperatorErrorMatching(t -> t instanceof IllegalStateException && "boom1".equals(t.getMessage()));
	}

	@Test
	public void assertOperatorErrorFailureNoDrop() {
		try {
			StepVerifier.create(Mono.empty())
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasOperatorErrors();
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected at least 1 operator error, none found.");
		}
	}

	@Test
	public void assertOperatorErrorFailureWrongType() {
		try {
			IllegalStateException err1 = new IllegalStateException("boom1");
			StepVerifier.create(Flux.just("test").map(d -> {
				throw err1;
			}))
			            .expectError()
			            .verifyThenAssertThat()
			            .hasOperatorErrorOfType(IllegalArgumentException.class);
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected operator error to be of type java.lang.IllegalArgumentException, was java.lang.IllegalStateException.");
		}
	}

	@Test
	public void assertOperatorErrorFailureWrongContains() {
		try {
			IllegalStateException err1 = new IllegalStateException("boom1");
			StepVerifier.create(Flux.just("test").map(d -> {
				throw err1;
			}))
			            .expectError()
			            .verifyThenAssertThat()
			            .hasOperatorErrorWithMessageContaining("foo");
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected operator error with message containing <\"foo\">, was <\"boom1\">.");
		}
	}

	@Test
	public void assertOperatorErrorFailureWrongMessage() {
		try {
			IllegalStateException err1 = new IllegalStateException("boom1");
			StepVerifier.create(Flux.just("test").map(d -> {
				throw err1;
			}))
			            .expectError()
			            .verifyThenAssertThat()
			            .hasOperatorErrorWithMessage("boom2");
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected operator error with message <\"boom2\">, was <\"boom1\">.");
		}
	}

	@Test
	public void assertOperatorErrorFailureWrongMatch() {
		try {
			IllegalStateException err1 = new IllegalStateException("boom1");
			StepVerifier.create(Flux.just("test").map(d -> {
				throw err1;
			}))
			            .expectError()
			            .verifyThenAssertThat()
			            .hasOperatorErrorMatching(t -> t instanceof IllegalStateException && "foo".equals(t.getMessage()));
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected operator error matching the given predicate, " +
					"did not match: <[Optional[java.lang.IllegalStateException: boom1],Optional[test]]>.");
		}
	}

	@Test
	public void assertOperatorErrorsFailureWrongCount() {
		Throwable err1 = new IllegalStateException("boom1");
		Throwable err2 = new IllegalStateException("boom2");
		Throwable err3 = new IllegalStateException("boom3");
		try {
			StepVerifier.create(Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				s.onError(err1);
				Operators.onOperatorError(err2, Context.empty());
				Operators.onOperatorError(err3, Context.empty());
			}).buffer(1))
			            .expectError()
			            .verifyThenAssertThat()
			            .hasOperatorErrors()
			            .hasOperatorErrors(3);
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected exactly 3 operator errors, 2 found.");
		}
	}

	@Test
	public void assertOperatorErrorsNotSatisfying() {
		Throwable err1 = new IllegalStateException("boom1");
		Throwable err2 = new IllegalStateException("boom2");
		Throwable err3 = new IllegalStateException("boom3");
		try {
			StepVerifier.create(Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				s.onError(err1);
				Operators.onOperatorError(err2, Context.empty());
				Operators.onOperatorError(err3, Context.empty());
			}).buffer(1))
			            .expectError()
			            .verifyThenAssertThat()
			            .hasOperatorErrorsSatisfying(c -> assertThat(c).hasSize(3));
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessageStartingWith("\nExpected size:<3> but was:<2> in:\n")
			              .hasMessageContaining("boom2")
			              .hasMessageContaining("boom3");
		}
	}

	@Test
	public void assertOperatorErrorsNotMatching() {
		Throwable err1 = new IllegalStateException("boom1");
		Throwable err2 = new IllegalStateException("boom2");
		Throwable err3 = new IllegalStateException("boom3");
		try {
			StepVerifier.create(Flux.from(s -> {
				s.onSubscribe(Operators.emptySubscription());
				s.onError(err1);
				Operators.onOperatorError(err2, Context.empty());
				Operators.onOperatorError(err3, Context.empty());
			}).buffer(1))
			            .expectError()
			            .verifyThenAssertThat()
			            .hasOperatorErrorsMatching(c -> c.size() == 3);
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessageStartingWith("Expected collection of operator errors matching the" +
					" given predicate, did not match: <[[")
			              .hasMessageContaining("boom2")
			              .hasMessageContaining("boom3");
		}
	}

	@Test
	public void assertDurationLessThanOk() {
		StepVerifier.create(Mono.delay(Duration.ofMillis(500)).then())
		            .expectComplete()
		            .verifyThenAssertThat()
		            .tookLessThan(Duration.ofSeconds(1));
	}

	@Test
	public void assertDurationLessThanFailure() {
		try {
			StepVerifier.create(Mono.delay(Duration.ofMillis(500)).then())
		                .expectComplete()
		                .verifyThenAssertThat()
		                .tookLessThan(Duration.ofMillis(200));

			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae)
					//the actual duration can vary a bit
					.hasMessageStartingWith("Expected scenario to be verified in less than 200ms, took 5")
					.hasMessageEndingWith("ms.");
		}
	}

	@Test
	public void assertDurationConsidersEqualsASuccess() {
		new DefaultStepVerifierBuilder.DefaultStepVerifierAssertions(null,
				Duration.ofSeconds(3),
				new MessageFormatter(null, null, null))
				.tookLessThan(Duration.ofMillis(3000L))
				.tookMoreThan(Duration.ofSeconds(3));
	}

	@Test
	public void assertDurationFailureWithScenarioName() {
		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() ->
						new DefaultStepVerifierBuilder.DefaultStepVerifierAssertions(null
								, Duration.ofSeconds(3),
								new MessageFormatter("fooScenario", null, null))
								.tookLessThan(Duration.ofMillis(200))
				)
				.withMessage("[fooScenario] Expected scenario to be verified in less than 200ms, took 3000ms.");
	}

	@Test
	public void assertDurationMoreThanOk() {
		StepVerifier.create(Mono.delay(Duration.ofMillis(500)).then())
		            .expectComplete()
		            .verifyThenAssertThat()
		            .tookMoreThan(Duration.ofMillis(100));
	}

	@Test
	public void assertDurationMoreThanFailure() {
		try {
			StepVerifier.create(Mono.delay(Duration.ofMillis(500)).then())
		                .expectComplete()
		                .verifyThenAssertThat()
		                .tookMoreThan(Duration.ofMillis(800));

			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae)
					//the actual duration can vary a bit
					.hasMessageStartingWith("Expected scenario to be verified in more than 800ms, took 5")
					.hasMessageEndingWith("ms.");
		}
	}

	@Test
	public void assertOperationErrorShortcutTestExactCount() {
		try {
			StepVerifier.create(Flux.empty())
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasOperatorErrorWithMessage("boom2");
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected exactly one operator error, 0 found.");
		}
	}

	@Test
	public void assertOperationErrorShortcutTestTupleContainsError() {
		try {
			StepVerifier.create(Flux.from(f -> {
				f.onSubscribe(Operators.emptySubscription());
				Operators.onOperatorError(null, null, "foo", Context.empty());
				f.onComplete();
			}))
			            .expectComplete()
			            .verifyThenAssertThat()
			            .hasOperatorErrorWithMessage("boom2");
			fail("expected an AssertionError");
		}
		catch (AssertionError ae) {
			assertThat(ae).hasMessage("Expected exactly one operator error with an actual throwable content, no throwable found.");
		}
	}

	@Test
	public void contextDiscardCaptureWithNoInitialContext() {
		StepVerifier.create(Mono.deferContextual(Mono::just)
		                        .flatMapIterable(ctx -> ctx.stream()
		                                                   .map(Map.Entry::getKey)
		                                                   .map(String::valueOf)
		                                                   .collect(Collectors.toList())
		                        ).concatWithValues("A", "B")
		                        .filter(s -> s.length() > 1)
		)
		            .expectNext("reactor.onDiscard.local")
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDiscardedExactly("A", "B");
	}

	@Test
	public void contextDiscardCaptureWithInitialContext() {
		Context initial = Context.of("foo", "bar");
		StepVerifier.create(Mono.deferContextual(Mono::just)
				.flatMapIterable(ctx -> ctx.stream()
				                           .map(Map.Entry::getKey)
				                           .map(String::valueOf)
				                           .collect(Collectors.toList())
		                        ).concatWithValues("A", "B")
		                        .filter(s -> s.length() > 1)
				, StepVerifierOptions.create().withInitialContext(initial))
		            .expectNext("foo")
		            .expectNext("reactor.onDiscard.local")
		            .expectComplete()
		            .verifyThenAssertThat()
		            .hasDiscardedExactly("A", "B");
	}

}

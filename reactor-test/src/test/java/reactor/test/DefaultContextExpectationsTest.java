/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowableAssertAlternative;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.test.DefaultStepVerifierBuilder.DefaultContextExpectations;
import reactor.test.StepVerifier.ContextExpectations;
import reactor.test.StepVerifier.Step;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class DefaultContextExpectationsTest {

	private void assertContextExpectation(
			Function<Flux<Integer>, Flux<Integer>> sourceTransformer,
			Function<DefaultContextExpectations<Integer>, ContextExpectations<Integer>> expectations,
			long count) {
		Flux<Integer> source = sourceTransformer.apply(Flux.range(1, 10));
		Step<Integer> step = StepVerifier.create(source);
		final DefaultContextExpectations<Integer> base = new DefaultContextExpectations<>(step, new MessageFormatter(null, null, null));

		expectations
				.apply(base)
				.then()
				.expectNextCount(count)
				.verifyComplete();
	}

	private void assertContextExpectation(
			Function<Flux<Integer>, Flux<Integer>> sourceTransformer,
			Function<DefaultContextExpectations<Integer>, ContextExpectations<Integer>> expectations) {
		assertContextExpectation(sourceTransformer, expectations, 10);
	}

	private ThrowableAssertAlternative<AssertionError> assertContextExpectationFails(
			Function<Flux<Integer>, Flux<Integer>> sourceTransformer,
			Function<DefaultContextExpectations<Integer>, ContextExpectations<Integer>> expectations) {
		return assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> assertContextExpectation(sourceTransformer, expectations));
	}

	@AfterEach
	void tearDown() {
		Hooks.resetOnOperatorDebug();
	}

	@Test
	public void contextAccessibleLastInChain() {
		assertContextExpectation(s -> s.take(3).contextWrite(Context.of("a", "b")),
				e -> e, 3);
	}

	@Test
	public void contextAccessibleFirstInChain() {
		assertContextExpectation(s -> s.contextWrite(Context.of("a", "b")).take(3),
				e -> e, 3);
	}

	@Test
	public void contextAccessibleSoloInChain() {
		assertContextExpectation(s -> s.contextWrite(Context.of("a", "b")), e -> e);
	}

	@Test
	public void notContextAccessibleDueToPublisher() {
		Publisher<Integer> publisher = subscriber -> subscriber.onSubscribe(new Subscription() {
			@Override
			public void request(long l) {
				subscriber.onComplete();
			}

			@Override
			public void cancel() {
				//NO-OP
			}
		});

		Step<Integer> step = StepVerifier.create(publisher);
		DefaultContextExpectations<Integer> expectations = new DefaultContextExpectations<>(step, new MessageFormatter(null, null, null));

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(() -> expectations.then().verifyComplete())
				.withMessage("No propagated Context");
	}

	@Test
	public void hasKey() throws Exception {
		assertContextExpectation(s -> s.contextWrite(Context.of("foo", "bar")),
				e -> e.hasKey("foo"));
	}
	@Test
	public void notHasKey() throws Exception {
		assertContextExpectationFails(s -> s.contextWrite(Context.of("foo", "bar")),
				e -> e.hasKey("bar"))
				.withMessageStartingWith("" +
						"Key bar not found\n" +
						"Context: Context1{foo=bar}"
				);
	}

	@Test
	public void hasSize() throws Exception {
		assertContextExpectation(s -> s.contextWrite(Context.of("foo", "bar", "foobar", "baz")),
				e -> e.hasSize(2));
	}

	@Test
	public void notHasSize() throws Exception {
		assertContextExpectationFails(s -> s.contextWrite(Context.of("foo", "bar", "foobar", "baz"))
		                                    .contextWrite(Context.of("fails", true)),
				e -> e.hasSize(2))
				.withMessageStartingWith("" +
						"Expected Context of size 2, got 3\n" +
						"Context: Context3{fails=true, foo=bar, foobar=baz}"
				);
	}

	@Test
	public void contains() throws Exception {
		assertContextExpectation(s -> s.contextWrite(Context.of("foo", "bar", "foobar", "baz")),
				e -> e.contains("foo", "bar"));
	}

	@Test
	public void notContainsKey() throws Exception {
		assertContextExpectationFails(s -> s.contextWrite(Context.of("foo", "bar", "foobar", "baz")),
				e -> e.contains("fooz", "bar"))
				.withMessageStartingWith("" +
						"Expected value bar for key fooz, key not present\n" +
						"Context: Context2{foo=bar, foobar=baz}"
				);
	}

	@Test
	public void notContainsValue() throws Exception {
		assertContextExpectationFails(s -> s.contextWrite(Context.of("foo", "bar", "foobar", "baz")),
				e -> e.contains("foo", "baz"))
				.withMessageStartingWith("" +
						"Expected value baz for key foo, got bar\n" +
						"Context: Context2{foo=bar, foobar=baz}"
				);
	}

	@Test
	public void containsAllOfContext() throws Exception {
		assertContextExpectation(s -> s.contextWrite(Context.of("foo", "bar", "foobar", "baz")),
				e -> e.containsAllOf(Context.of("foo", "bar")));
	}

	@Test
	public void notContainsAllOfContext() throws Exception {
		assertContextExpectationFails(s -> s.contextWrite(Context.of("foo", "bar", "foobar", "baz")),
				e -> e.containsAllOf(Context.of("foo", "bar", "other", "stuff")))
				.withMessageStartingWith("" +
						"Expected Context to contain all of Context2{foo=bar, other=stuff}\n" +
						"Context: Context2{foo=bar, foobar=baz}"
				);
	}

	@Test
	public void containsAllOfMap() throws Exception {
		assertContextExpectation(s -> s.contextWrite(Context.of("foo", "bar", "foobar", "baz")),
				e -> e.containsAllOf(Collections.singletonMap("foo", "bar")));
	}

	@Test
	public void notContainsAllOfMap() throws Exception {
		Map<String, String> expected = new HashMap<>();
		expected.put("foo", "bar");
		expected.put("other", "stuff");

		assertContextExpectationFails(s -> s.contextWrite(Context.of("foo", "bar", "foobar", "baz")),
				e -> e.containsAllOf(expected))
				.withMessageStartingWith("" +
						"Expected Context to contain all of {other=stuff, foo=bar}\n" +
						"Context: Context2{foo=bar, foobar=baz}"
				);
	}

	@Test
	public void containsOnlyOfContext() throws Exception {
		assertContextExpectation(s -> s.contextWrite(Context.of("foo", "bar")),
				e -> e.containsOnly(Context.of("foo", "bar")));
	}

	@Test
	public void notContainsOnlyOfContextSize() throws Exception {
		Context expected = Context.of("foo", "bar", "other", "stuff");

		assertContextExpectationFails(s -> s.contextWrite(Context.of("foo", "bar")),
				e -> e.containsOnly(expected))
				.withMessageStartingWith("" +
						"Expected Context to contain same values as Context2{foo=bar, other=stuff}, but they differ in size\n" +
						"Context: Context1{foo=bar}"
				);
	}

	@Test
	public void notContainsOnlyOfContextContent() throws Exception {
		Context expected = Context.of("foo", "bar", "other", "stuff");

		assertContextExpectationFails(s -> s.contextWrite(Context.of("foo", "bar", "foobar", "baz")),
				e -> e.containsOnly(expected))
				.withMessageStartingWith("" +
						"Expected Context to contain same values as Context2{foo=bar, other=stuff}, but they differ in content\n" +
						"Context: Context2{foo=bar, foobar=baz}"
				);
	}

	@Test
	public void containsOnlyOfMap() throws Exception {
		assertContextExpectation(s -> s.contextWrite(Context.of("foo", "bar")),
				e -> e.containsOnly(Collections.singletonMap("foo", "bar")));
	}

	@Test
	public void notContainsOnlyOfMapSize() throws Exception {
		Map<String, String> expected = new HashMap<>();
		expected.put("foo", "bar");
		expected.put("other", "stuff");

		assertContextExpectationFails(s -> s.contextWrite(Context.of("foo", "bar")),
				e -> e.containsOnly(expected))
				.withMessageStartingWith("" +
						"Expected Context to contain same values as {other=stuff, foo=bar}, but they differ in size\n" +
						"Context: Context1{foo=bar}"
				);
	}

	@Test
	public void notContainsOnlyOfMapContent() throws Exception {
		Map<String, String> expected = new HashMap<>();
		expected.put("foo", "bar");
		expected.put("other", "stuff");

		assertContextExpectationFails(s -> s.contextWrite(Context.of("foo", "bar", "foobar", "baz")),
				e -> e.containsOnly(expected))
				.withMessageStartingWith("" +
						"Expected Context to contain same values as {other=stuff, foo=bar}, but they differ in content\n" +
						"Context: Context2{foo=bar, foobar=baz}"
				);
	}

	@Test
	public void assertThat() throws Exception {
		assertContextExpectation(s -> s.contextWrite(Context.of("foo", "bar")),
				e -> e.assertThat(c -> Assertions.assertThat(c).isNotNull()));
	}

	@Test
	public void notAssertThat() throws Exception {
		assertContextExpectationFails(s -> s.contextWrite(Context.of("foo", "bar")),
				e -> e.assertThat(c -> { throw new AssertionError("boom"); }))
				.withMessage("boom");
	}

	@Test
	public void matches() throws Exception {
		assertContextExpectation(s -> s.contextWrite(Context.of("foo", "bar")),
				e -> e.matches(Objects::nonNull));
	}

	@Test
	public void notMatches() throws Exception {
		assertContextExpectationFails(s -> s.contextWrite(Context.of("foo", "bar")),
				e -> e.matches(Objects::isNull))
				.withMessageStartingWith("" +
						"Context doesn't match predicate\n" +
						"Context: Context1{foo=bar}"
				);
	}

	@Test
	public void matchesWithDescription() throws Exception {
		assertContextExpectation(s -> s.contextWrite(Context.of("foo", "bar")),
				e -> e.matches(Objects::nonNull, "desc"));
	}

	@Test
	public void notMatchesWithDescription() throws Exception {
		assertContextExpectationFails(s -> s.contextWrite(Context.of("foo", "bar")),
				e -> e.matches(Objects::isNull, "desc"))
				.withMessageStartingWith("" +
						"Context doesn't match predicate desc\n" +
						"Context: Context1{foo=bar}"
				);
	}

	@Test
	public void notMatchesWithDescriptionAndScenarioName() {
		Flux<Integer> source = Flux.range(1, 10)
		                           .contextWrite(Context.of("foo", "bar"));

		Step<Integer> step = StepVerifier.create(source);
		final DefaultContextExpectations<Integer> base = new DefaultContextExpectations<>(step, new MessageFormatter("scenario", null, null));

		assertThatExceptionOfType(AssertionError.class)
				.isThrownBy(
						base.matches(Objects::isNull, "someDescription")
						    .then()
						    .expectNextCount(10)::verifyComplete)
				.withMessageStartingWith("" +
						"[scenario] Context doesn't match predicate someDescription\n" +
						"Context: Context1{foo=bar}"
				);
	}

	@Test
	public void capturedOperator() {
		assertContextExpectationFails(
				s -> s.doOnEach(__ -> {}),
				e -> e.hasKey("foo")
		).withMessageEndingWith("Captured at: range");
	}

	@Test
	public void capturedOperatorWithDebug() {
		Hooks.onOperatorDebug();

		assertContextExpectationFails(
				s -> s.doOnEach(__ -> {}),
				e -> e.hasKey("foo")
		).withMessageContaining("Captured at: Flux.range ⇢ at reactor.test.DefaultContextExpectationsTest.assertContextExpectation(DefaultContextExpectationsTest.java:");
	}

	@Test
	public void capturedOperatorWithDebugAndConditionalSubscriber() {
		Hooks.onOperatorDebug();

		assertContextExpectationFails(
				s -> s.contextWrite(Context.empty()),
				e -> e.hasKey("foo")
		).withMessageContaining("Captured at: Flux.range ⇢ at reactor.test.DefaultContextExpectationsTest.assertContextExpectation(DefaultContextExpectationsTest.java:");
	}

}
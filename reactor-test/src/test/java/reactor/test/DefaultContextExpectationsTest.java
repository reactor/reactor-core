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

package reactor.test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowableAssertAlternative;
import org.junit.Test;
import reactor.core.publisher.Flux;
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
		final DefaultContextExpectations<Integer> base = new DefaultContextExpectations<>(step);

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

	@Test
	public void contextAccessibleLastInChain() {
		assertContextExpectation(s -> s.take(3).subscriberContext(Context.of("a", "b")),
				e -> e, 3);
	}

	@Test
	public void contextAccessibleFirstInChain() {
		assertContextExpectation(s -> s.subscriberContext(Context.of("a", "b")).take(3),
				e -> e, 3);
	}

	@Test
	public void contextAccessibleSoloInChain() {
		assertContextExpectation(s -> s.subscriberContext(Context.of("a", "b")), e -> e);
	}

	@Test
	public void notContextAccessible() {
		assertContextExpectationFails(s -> s, e -> e)
				.withMessage("No propagated Context");
	}

	@Test
	public void hasKey() throws Exception {
		assertContextExpectation(s -> s.subscriberContext(Context.of("foo", "bar")),
				e -> e.hasKey("foo"));
	}
	@Test
	public void notHasKey() throws Exception {
		assertContextExpectationFails(s -> s.subscriberContext(Context.of("foo", "bar")),
				e -> e.hasKey("bar"))
				.withMessage("Key bar not found in Context Context1{foo=bar}");
	}

	@Test
	public void hasSize() throws Exception {
		assertContextExpectation(s -> s.subscriberContext(Context.of("foo", "bar", "foobar", "baz")),
				e -> e.hasSize(2));
	}

	@Test
	public void notHasSize() throws Exception {
		assertContextExpectationFails(s -> s.subscriberContext(Context.of("foo", "bar", "foobar", "baz"))
		                                    .subscriberContext(Context.of("fails", true)),
				e -> e.hasSize(2))
				.withMessageStartingWith("Expected Context of size 2, got 3 for Context Context3{");
	}

	@Test
	public void contains() throws Exception {
		assertContextExpectation(s -> s.subscriberContext(Context.of("foo", "bar", "foobar", "baz")),
				e -> e.contains("foo", "bar"));
	}

	@Test
	public void notContainsKey() throws Exception {
		assertContextExpectationFails(s -> s.subscriberContext(Context.of("foo", "bar", "foobar", "baz")),
				e -> e.contains("fooz", "bar"))
				.withMessage("Expected value bar for key fooz, key not present in Context " +
						"Context2{foo=bar, foobar=baz}");
	}

	@Test
	public void notContainsValue() throws Exception {
		assertContextExpectationFails(s -> s.subscriberContext(Context.of("foo", "bar", "foobar", "baz")),
				e -> e.contains("foo", "baz"))
				.withMessage("Expected value baz for key foo, got bar in Context " +
						"Context2{foo=bar, foobar=baz}");
	}

	@Test
	public void containsAllOfContext() throws Exception {
		assertContextExpectation(s -> s.subscriberContext(Context.of("foo", "bar", "foobar", "baz")),
				e -> e.containsAllOf(Context.of("foo", "bar")));
	}

	@Test
	public void notContainsAllOfContext() throws Exception {
		assertContextExpectationFails(s -> s.subscriberContext(Context.of("foo", "bar", "foobar", "baz")),
				e -> e.containsAllOf(Context.of("foo", "bar", "other", "stuff")))
				.withMessage("Expected Context Context2{foo=bar, foobar=baz} to contain all " +
						"of Context2{foo=bar, other=stuff}");
	}

	@Test
	public void containsAllOfMap() throws Exception {
		assertContextExpectation(s -> s.subscriberContext(Context.of("foo", "bar", "foobar", "baz")),
				e -> e.containsAllOf(Collections.singletonMap("foo", "bar")));
	}

	@Test
	public void notContainsAllOfMap() throws Exception {
		Map<String, String> expected = new HashMap<>();
		expected.put("foo", "bar");
		expected.put("other", "stuff");

		assertContextExpectationFails(s -> s.subscriberContext(Context.of("foo", "bar", "foobar", "baz")),
				e -> e.containsAllOf(expected))
				.withMessage("Expected Context Context2{foo=bar, foobar=baz} to contain all " +
						"of {other=stuff, foo=bar}");
	}

	@Test
	public void containsOnlyOfContext() throws Exception {
		assertContextExpectation(s -> s.subscriberContext(Context.of("foo", "bar")),
				e -> e.containsOnly(Context.of("foo", "bar")));
	}

	@Test
	public void notContainsOnlyOfContextSize() throws Exception {
		Context expected = Context.of("foo", "bar", "other", "stuff");

		assertContextExpectationFails(s -> s.subscriberContext(Context.of("foo", "bar")),
				e -> e.containsOnly(expected))
				.withMessage("Expected Context Context1{foo=bar} to contain same values as " +
						"Context2{foo=bar, other=stuff}, but they differ in size");
	}

	@Test
	public void notContainsOnlyOfContextContent() throws Exception {
		Context expected = Context.of("foo", "bar", "other", "stuff");

		assertContextExpectationFails(s -> s.subscriberContext(Context.of("foo", "bar", "foobar", "baz")),
				e -> e.containsOnly(expected))
				.withMessage("Expected Context Context2{foo=bar, foobar=baz} to contain " +
						"same values as Context2{foo=bar, other=stuff}, but they differ in content");
	}

	@Test
	public void containsOnlyOfMap() throws Exception {
		assertContextExpectation(s -> s.subscriberContext(Context.of("foo", "bar")),
				e -> e.containsOnly(Collections.singletonMap("foo", "bar")));
	}

	@Test
	public void notContainsOnlyOfMapSize() throws Exception {
		Map<String, String> expected = new HashMap<>();
		expected.put("foo", "bar");
		expected.put("other", "stuff");

		assertContextExpectationFails(s -> s.subscriberContext(Context.of("foo", "bar")),
				e -> e.containsOnly(expected))
				.withMessage("Expected Context Context1{foo=bar} to contain same values as " +
						"{other=stuff, foo=bar}, but they differ in size");
	}

	@Test
	public void notContainsOnlyOfMapContent() throws Exception {
		Map<String, String> expected = new HashMap<>();
		expected.put("foo", "bar");
		expected.put("other", "stuff");

		assertContextExpectationFails(s -> s.subscriberContext(Context.of("foo", "bar", "foobar", "baz")),
				e -> e.containsOnly(expected))
				.withMessage("Expected Context Context2{foo=bar, foobar=baz} to contain " +
						"same values as {other=stuff, foo=bar}, but they differ in content");
	}

	@Test
	public void assertThat() throws Exception {
		assertContextExpectation(s -> s.subscriberContext(Context.of("foo", "bar")),
				e -> e.assertThat(c -> Assertions.assertThat(c).isNotNull()));
	}

	@Test
	public void notAssertThat() throws Exception {
		assertContextExpectationFails(s -> s.subscriberContext(Context.of("foo", "bar")),
				e -> e.assertThat(c -> { throw new AssertionError("boom"); }))
				.withMessage("boom");
	}

	@Test
	public void matches() throws Exception {
		assertContextExpectation(s -> s.subscriberContext(Context.of("foo", "bar")),
				e -> e.matches(Objects::nonNull));
	}

	@Test
	public void notMatches() throws Exception {
		assertContextExpectationFails(s -> s.subscriberContext(Context.of("foo", "bar")),
				e -> e.matches(Objects::isNull))
				.withMessage("Context Context1{foo=bar} doesn't match predicate");
	}

	@Test
	public void matchesWithDescription() throws Exception {
		assertContextExpectation(s -> s.subscriberContext(Context.of("foo", "bar")),
				e -> e.matches(Objects::nonNull, "desc"));
	}

	@Test
	public void notMatchesWithDescription() throws Exception {
		assertContextExpectationFails(s -> s.subscriberContext(Context.of("foo", "bar")),
				e -> e.matches(Objects::isNull, "desc"))
				.withMessage("Context Context1{foo=bar} doesn't match predicate desc");
	}

}
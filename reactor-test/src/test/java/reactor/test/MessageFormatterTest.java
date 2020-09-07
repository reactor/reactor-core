/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Signal;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageFormatterTest {

	@Test
	public void noScenarioEmpty() {
		assertThat(new MessageFormatter("", null, null).scenarioPrefix)
				.isNotNull()
				.isEmpty();
	}

	@Test
	public void nullScenarioEmpty() {
		assertThat(new MessageFormatter(null, null, null).scenarioPrefix)
				.isNotNull()
				.isEmpty();
	}

	@Test
	public void givenScenarioWrapped() {
		assertThat(new MessageFormatter("foo", null, null).scenarioPrefix)
				.isEqualTo("[foo] ");
	}

	// === Tests with an empty scenario name ===
	static final MessageFormatter noScenario = new MessageFormatter("", null, null);

	@Test
	public void noScenarioFailNullEventNoArgs() {
		assertThat(noScenario.fail(null, "details"))
				.hasMessage("expectation failed (details)");
	}

	@Test
	public void noScenarioFailNoDescriptionNoArgs() {
		DefaultStepVerifierBuilder.Event event = new DefaultStepVerifierBuilder.NoEvent(Duration.ofMillis(5),
				"");

		assertThat(noScenario.fail(event, "details"))
				.hasMessage("expectation failed (details)");
	}

	@Test
	public void noScenarioFailDescriptionNoArgs() {
		DefaultStepVerifierBuilder.Event event = new DefaultStepVerifierBuilder.NoEvent(Duration.ofMillis(5),
				"eventDescription");

		assertThat(noScenario.fail(event, "details"))
				.hasMessage("expectation \"eventDescription\" failed (details)");
	}

	@Test
	public void noScenarioFailNullEventHasArgs() {
		assertThat(noScenario.fail(null, "details = %s", "bar"))
				.hasMessage("expectation failed (details = bar)");
	}


	@Test
	public void noScenarioFailNoDescriptionHasArgs() {
		DefaultStepVerifierBuilder.Event event = new DefaultStepVerifierBuilder.NoEvent(Duration.ofMillis(5),
				"");

		assertThat(noScenario.fail(event, "details = %s", "bar"))
				.hasMessage("expectation failed (details = bar)");
	}

	@Test
	public void noScenarioFailDescriptionHasArgs() {
		DefaultStepVerifierBuilder.Event event = new DefaultStepVerifierBuilder.NoEvent(Duration.ofMillis(5),
				"eventDescription");

		assertThat(noScenario.fail(event, "details = %s", "bar"))
				.hasMessage("expectation \"eventDescription\" failed (details = bar)");
	}

	@Test
	public void noScenarioFailOptional() {
		assertThat(noScenario.failOptional(null, "foo"))
				.hasValueSatisfying(ae -> assertThat(ae).hasMessage("expectation failed (foo)"));
	}

	@Test
	public void noScenarioFailPrefixNoArgs() {
		assertThat(noScenario.failPrefix("firstPart", "secondPart"))
				.hasMessage("firstPartsecondPart)"); //note the prefix doesn't have an opening parenthesis
	}

	@Test
	public void noScenarioFailPrefixHasArgs() {
		assertThat(noScenario.failPrefix("firstPart(", "secondPart = %s", "foo"))
				.hasMessage("firstPart(secondPart = foo)");
	}

	@Test
	public void noScenarioAssertionError() {
		assertThat(noScenario.assertionError("plain"))
				.hasMessage("plain")
				.hasNoCause();
	}

	@Test
	public void noScenarioAssertionErrorWithCause() {
		Throwable cause = new IllegalArgumentException("boom");
		assertThat(noScenario.assertionError("plain", cause))
				.hasMessage("plain")
				.hasCause(cause);
	}

	@Test
	public void noScenarioAssertionErrorWithNullCause() {
		assertThat(noScenario.assertionError("plain", null))
				.hasMessage("plain")
				.hasNoCause();
	}

	@Test
	public void noScenarioIllegalStateException() {
		assertThat(noScenario.<Throwable>error(IllegalStateException::new, "plain"))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("plain");
	}


	// === Tests with a scenario name ===
	static final MessageFormatter
			withScenario = new MessageFormatter("MessageFormatterTest", null, null);

	@Test
	public void withScenarioFailNullEventNoArgs() {
		assertThat(withScenario.fail(null, "details"))
				.hasMessage("[MessageFormatterTest] expectation failed (details)");
	}

	@Test
	public void withScenarioFailNoDescriptionNoArgs() {
		DefaultStepVerifierBuilder.Event event = new DefaultStepVerifierBuilder.NoEvent(Duration.ofMillis(5),
				"");

		assertThat(withScenario.fail(event, "details"))
				.hasMessage("[MessageFormatterTest] expectation failed (details)");
	}

	@Test
	public void withScenarioFailDescriptionNoArgs() {
		DefaultStepVerifierBuilder.Event event = new DefaultStepVerifierBuilder.NoEvent(Duration.ofMillis(5),
				"eventDescription");

		assertThat(withScenario.fail(event, "details"))
				.hasMessage("[MessageFormatterTest] expectation \"eventDescription\" failed (details)");
	}

	@Test
	public void withScenarioFailNullEventHasArgs() {
		assertThat(withScenario.fail(null, "details = %s", "bar"))
				.hasMessage("[MessageFormatterTest] expectation failed (details = bar)");
	}


	@Test
	public void withScenarioFailNoDescriptionHasArgs() {
		DefaultStepVerifierBuilder.Event event = new DefaultStepVerifierBuilder.NoEvent(Duration.ofMillis(5),
				"");

		assertThat(withScenario.fail(event, "details = %s", "bar"))
				.hasMessage("[MessageFormatterTest] expectation failed (details = bar)");
	}

	@Test
	public void withScenarioFailDescriptionHasArgs() {
		DefaultStepVerifierBuilder.Event event = new DefaultStepVerifierBuilder.NoEvent(Duration.ofMillis(5),
				"eventDescription");

		assertThat(withScenario.fail(event, "details = %s", "bar"))
				.hasMessage("[MessageFormatterTest] expectation \"eventDescription\" failed (details = bar)");
	}

	@Test
	public void withScenarioFailOptional() {
		assertThat(withScenario.failOptional(null, "foo"))
				.hasValueSatisfying(ae -> assertThat(ae).hasMessage("[MessageFormatterTest] expectation failed (foo)"));
	}

	@Test
	public void withScenarioFailPrefixNoArgs() {
		assertThat(withScenario.failPrefix("firstPart", "secondPart"))
				.hasMessage("[MessageFormatterTest] firstPartsecondPart)"); //note the prefix doesn't have an opening parenthesis
	}

	@Test
	public void withScenarioFailPrefixHasArgs() {
		assertThat(withScenario.failPrefix("firstPart(", "secondPart = %s", "foo"))
				.hasMessage("[MessageFormatterTest] firstPart(secondPart = foo)");
	}

	@Test
	public void withScenarioAssertionError() {
		assertThat(withScenario.assertionError("plain"))
				.hasMessage("[MessageFormatterTest] plain")
				.hasNoCause();
	}

	@Test
	public void withScenarioAssertionErrorWithCause() {
		Throwable cause = new IllegalArgumentException("boom");
		assertThat(withScenario.assertionError("plain", cause))
				.hasMessage("[MessageFormatterTest] plain")
				.hasCause(cause);
	}

	@Test
	public void withScenarioAssertionErrorWithNullCause() {
		assertThat(withScenario.assertionError("plain", null))
				.hasMessage("[MessageFormatterTest] plain")
				.hasNoCause();
	}

	@Test
	public void withScenarioIllegalStateException() {
		assertThat(withScenario.<Throwable>error(IllegalStateException::new, "plain"))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("[MessageFormatterTest] plain");
	}

	// === Tests with a value formatter ===
	static final MessageFormatter withCustomFormatter = new MessageFormatter("withCustomFormatter",
			ValueFormatters.forClass(String.class, o -> o.getClass().getSimpleName() + "=>" + o),
			Arrays.asList(ValueFormatters.DEFAULT_SIGNAL_EXTRACTOR, ValueFormatters.DEFAULT_ITERABLE_EXTRACTOR, ValueFormatters.arrayExtractor(Object[].class)));

	@Test
	public void withCustomFormatterFailNullEventNoArgs() {
		assertThat(withCustomFormatter.fail(null, "details"))
				.hasMessage("[withCustomFormatter] expectation failed (details)");
	}

	@Test
	public void withCustomFormatterFailNoDescriptionNoArgs() {
		DefaultStepVerifierBuilder.Event event = new DefaultStepVerifierBuilder.NoEvent(Duration.ofMillis(5),
				"");

		assertThat(withCustomFormatter.fail(event, "details"))
				.hasMessage("[withCustomFormatter] expectation failed (details)");
	}

	@Test
	public void withCustomFormatterFailDescriptionNoArgs() {
		DefaultStepVerifierBuilder.Event event = new DefaultStepVerifierBuilder.NoEvent(Duration.ofMillis(5),
				"eventDescription");

		assertThat(withCustomFormatter.fail(event, "details"))
				.hasMessage("[withCustomFormatter] expectation \"eventDescription\" failed (details)");
	}

	@Test
	public void withCustomFormatterFailNullEventHasArgs() {
		assertThat(withCustomFormatter.fail(null, "details = %s", "bar"))
				.hasMessage("[withCustomFormatter] expectation failed (details = String=>bar)");
	}


	@Test
	public void withCustomFormatterFailNoDescriptionHasArgs() {
		DefaultStepVerifierBuilder.Event event = new DefaultStepVerifierBuilder.NoEvent(Duration.ofMillis(5),
				"");

		assertThat(withCustomFormatter.fail(event, "details = %s", "bar"))
				.hasMessage("[withCustomFormatter] expectation failed (details = String=>bar)");
	}

	@Test
	public void withCustomFormatterFailDescriptionHasArgs() {
		DefaultStepVerifierBuilder.Event event = new DefaultStepVerifierBuilder.NoEvent(Duration.ofMillis(5),
				"eventDescription");

		assertThat(withCustomFormatter.fail(event, "details = %s", "bar"))
				.hasMessage("[withCustomFormatter] expectation \"eventDescription\" failed (details = String=>bar)");
	}

	@Test
	public void withCustomFormatterFailOptional() {
		assertThat(withCustomFormatter.failOptional(null, "foo"))
				.hasValueSatisfying(ae -> assertThat(ae).hasMessage("[withCustomFormatter] expectation failed (foo)"));
	}

	@Test
	public void withCustomFormatterFailPrefixNoArgs() {
		assertThat(withCustomFormatter.failPrefix("firstPart", "secondPart"))
				.hasMessage("[withCustomFormatter] firstPartsecondPart)"); //note the prefix doesn't have an opening parenthesis
	}

	@Test
	public void withCustomFormatterFailPrefixHasArgs() {
		assertThat(withCustomFormatter.failPrefix("firstPart(", "secondPart = %s", "foo"))
				.hasMessage("[withCustomFormatter] firstPart(secondPart = String=>foo)");
	}

	@Test
	public void withCustomFormatterAssertionError() {
		assertThat(withCustomFormatter.assertionError("plain"))
				.hasMessage("[withCustomFormatter] plain")
				.hasNoCause();
	}

	@Test
	public void withCustomFormatterAssertionErrorWithCause() {
		Throwable cause = new IllegalArgumentException("boom");
		assertThat(withCustomFormatter.assertionError("plain", cause))
				.hasMessage("[withCustomFormatter] plain")
				.hasCause(cause);
	}

	@Test
	public void withCustomFormatterAssertionErrorWithNullCause() {
		assertThat(withCustomFormatter.assertionError("plain", null))
				.hasMessage("[withCustomFormatter] plain")
				.hasNoCause();
	}

	@Test
	public void withCustomFormatterIllegalStateException() {
		assertThat(withCustomFormatter.<Throwable>error(IllegalStateException::new, "plain"))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("[withCustomFormatter] plain");
	}

	@Test
	public void withCustomFormatterFormatSignal() {
		assertThat(withCustomFormatter.format("expectation %s expected %s got %s",
				Signal.next("foo"), "foo", Signal.next("bar")))
				.isEqualTo("expectation onNext(String=>foo) expected String=>foo got onNext(String=>bar)");
	}

	@Test
	public void withCustomFormatterFormatIterable() {
		assertThat(withCustomFormatter.format("expectation %s expected %s got %s",
				Arrays.asList("foo","bar"), "foo", Collections.singletonList("bar")))
				.isEqualTo("expectation [String=>foo, String=>bar] expected String=>foo got [String=>bar]");
	}

	@Test
	public void withCustomFormatterFormatArray() {
		assertThat(withCustomFormatter.format("expectation %s expected %s got %s",
				new Object[] {"foo","bar"}, "foo", new Object[] {"bar"}))
				.isEqualTo("expectation [String=>foo, String=>bar] expected String=>foo got [String=>bar]");
	}
}
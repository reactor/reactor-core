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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ErrorFormatterTest {

	@Test
	public void noScenarioEmpty() {
		assertThat(new ErrorFormatter("").scenarioPrefix)
				.isNotNull()
				.isEmpty();
	}

	@Test
	public void nullScenarioEmpty() {
		assertThat(new ErrorFormatter(null).scenarioPrefix)
				.isNotNull()
				.isEmpty();
	}

	@Test
	public void givenScenarioWrapped() {
		assertThat(new ErrorFormatter("foo").scenarioPrefix)
				.isEqualTo("[foo] ");
	}

	// === Tests with an empty scenario name ===
	static final ErrorFormatter noScenario = new ErrorFormatter("");

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
	public void noScenarioIllegalStateException() {
		assertThat(noScenario.<Throwable>error(IllegalStateException::new, "plain"))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("plain");
	}
	
	
	// === Tests with a scenario name ===
	static final ErrorFormatter withScenario = new ErrorFormatter("ErrorFormatterTest");

	@Test
	public void withScenarioFailNullEventNoArgs() {
		assertThat(withScenario.fail(null, "details"))
				.hasMessage("[ErrorFormatterTest] expectation failed (details)");
	}

	@Test
	public void withScenarioFailNoDescriptionNoArgs() {
		DefaultStepVerifierBuilder.Event event = new DefaultStepVerifierBuilder.NoEvent(Duration.ofMillis(5),
				"");

		assertThat(withScenario.fail(event, "details"))
				.hasMessage("[ErrorFormatterTest] expectation failed (details)");
	}

	@Test
	public void withScenarioFailDescriptionNoArgs() {
		DefaultStepVerifierBuilder.Event event = new DefaultStepVerifierBuilder.NoEvent(Duration.ofMillis(5),
				"eventDescription");

		assertThat(withScenario.fail(event, "details"))
				.hasMessage("[ErrorFormatterTest] expectation \"eventDescription\" failed (details)");
	}

	@Test
	public void withScenarioFailNullEventHasArgs() {
		assertThat(withScenario.fail(null, "details = %s", "bar"))
				.hasMessage("[ErrorFormatterTest] expectation failed (details = bar)");
	}


	@Test
	public void withScenarioFailNoDescriptionHasArgs() {
		DefaultStepVerifierBuilder.Event event = new DefaultStepVerifierBuilder.NoEvent(Duration.ofMillis(5),
				"");

		assertThat(withScenario.fail(event, "details = %s", "bar"))
				.hasMessage("[ErrorFormatterTest] expectation failed (details = bar)");
	}

	@Test
	public void withScenarioFailDescriptionHasArgs() {
		DefaultStepVerifierBuilder.Event event = new DefaultStepVerifierBuilder.NoEvent(Duration.ofMillis(5),
				"eventDescription");

		assertThat(withScenario.fail(event, "details = %s", "bar"))
				.hasMessage("[ErrorFormatterTest] expectation \"eventDescription\" failed (details = bar)");
	}

	@Test
	public void withScenarioFailOptional() {
		assertThat(withScenario.failOptional(null, "foo"))
				.hasValueSatisfying(ae -> assertThat(ae).hasMessage("[ErrorFormatterTest] expectation failed (foo)"));
	}

	@Test
	public void withScenarioFailPrefixNoArgs() {
		assertThat(withScenario.failPrefix("firstPart", "secondPart"))
				.hasMessage("[ErrorFormatterTest] firstPartsecondPart)"); //note the prefix doesn't have an opening parenthesis
	}

	@Test
	public void withScenarioFailPrefixHasArgs() {
		assertThat(withScenario.failPrefix("firstPart(", "secondPart = %s", "foo"))
				.hasMessage("[ErrorFormatterTest] firstPart(secondPart = foo)");
	}

	@Test
	public void withScenarioAssertionError() {
		assertThat(withScenario.assertionError("plain"))
				.hasMessage("[ErrorFormatterTest] plain")
				.hasNoCause();
	}

	@Test
	public void withScenarioAssertionErrorWithCause() {
		Throwable cause = new IllegalArgumentException("boom");
		assertThat(withScenario.assertionError("plain", cause))
				.hasMessage("[ErrorFormatterTest] plain")
				.hasCause(cause);
	}
	
	@Test
	public void withScenarioIllegalStateException() {
		assertThat(withScenario.<Throwable>error(IllegalStateException::new, "plain"))
				.isInstanceOf(IllegalStateException.class)
				.hasMessage("[ErrorFormatterTest] plain");
	}
}
/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import org.junit.Test;

import reactor.core.publisher.Signal;

import static org.assertj.core.api.Assertions.assertThat;

public class ValueFormattersTest {

	/**
	 * Enum with obscure toString and more meaningful alternative {@link String} representation.
	 */
	private static enum Obscure {

		OB1("foo"), OB2("bar"), OB3("baz");

		private final String alternative;

		Obscure(String alternative) {
			this.alternative = alternative;
		}

		public String getAlternative() {
			return alternative;
		}
	}

	@Test
	public void classBasedNull() {
		Function<Object, String> formatter = ValueFormatters.forClass(Obscure.class, Obscure::getAlternative);

		assertThat(formatter.apply(null))
				.isNotNull()
				.isEqualTo("null");
	}

	@Test
	public void classBasedMatching() {
		Function<Object, String> formatter = ValueFormatters.forClass(Obscure.class, Obscure::getAlternative);

		assertThat(formatter.apply(Obscure.OB1))
				.isEqualTo("foo");
	}

	@Test
	public void classBasedNotMatching() {
		Function<Object, String> formatter = ValueFormatters.forClass(Integer.class, i -> "int(" + i + ")");

		assertThat(formatter.apply(Obscure.OB1))
				.isEqualTo("OB1");
	}

	@Test
	public void predicateBasedNull() {
		Function<Object, String> formatter = ValueFormatters.filtering(o -> o instanceof Obscure, o -> ((Obscure) o).getAlternative());

		assertThat(formatter.apply(null))
				.isNotNull()
				.isEqualTo("null");
	}

	@Test
	public void predicateCanConvertNull() {
		Function<Object, String> formatter = ValueFormatters.filtering(Objects::isNull, o -> "THE BILLION DOLLAR MISTAKE");

		assertThat(formatter.apply(null))
				.isNotNull()
				.isEqualTo("THE BILLION DOLLAR MISTAKE");
	}

	@Test
	public void predicateBasedMatching() {
		Function<Object, String> formatter = ValueFormatters.filtering(o -> o instanceof Obscure, o -> ((Obscure) o).getAlternative());

		assertThat(formatter.apply(Obscure.OB2))
				.isEqualTo("bar");
	}

	@Test
	public void predicateBasedNotMatching() {
		Function<Object, String> formatter = ValueFormatters.filtering(o -> o instanceof Integer, o -> "int(" + o + ")");

		assertThat(formatter.apply(Obscure.OB2))
				.isEqualTo("OB2");
	}

	@Test
	public void signalDoesntConsiderNonSignal() {
		Function<Object, String> formatter = ValueFormatters.signalOf(Object.class, o -> o.getClass().getSimpleName());
		Object target = Obscure.OB1;

		assertThat(formatter.apply(target)).isEqualTo(target.toString());
	}

	@Test
	public void signalDoesntConsiderNonMatchingContent() {
		Function<Object, String> formatter = ValueFormatters.signalOf(Number.class, o -> o.getClass().getSimpleName());
		Signal<Object> target = Signal.next(Obscure.OB1);

		assertThat(formatter.apply(target)).isEqualTo(target.toString());
	}

	@Test
	public void signalDoesntConvertComplete() {
		Function<Object, String> formatter = ValueFormatters.signalOf(Number.class, o -> o.getClass().getSimpleName());
		Signal<Number> target = Signal.complete();

		assertThat(formatter.apply(target)).isEqualTo(target.toString());
	}

	@Test
	public void signalDoesntConvertError() {
		Function<Object, String> formatter = ValueFormatters.signalOf(Number.class, o -> o.getClass().getSimpleName());
		Signal<Number> target = Signal.error(new IllegalStateException("foo"));

		assertThat(formatter.apply(target)).isEqualTo(target.toString());
	}

	@Test
	public void signalDoesntConvertErrorEvenIfThrowableClass() {
		Function<Object, String> formatter = ValueFormatters.signalOf(Throwable.class, o -> o.getClass().getSimpleName());
		Signal<Number> target = Signal.error(new IllegalStateException("foo"));

		assertThat(formatter.apply(target)).isEqualTo(target.toString());
	}

	@Test
	public void signalConvertsOnNextContentMatching() {
		Function<Object, String> formatter = ValueFormatters.signalOf(Obscure.class, Obscure::getAlternative);
		Signal<Obscure> target = Signal.next(Obscure.OB1);

		assertThat(formatter.apply(target))
				.isNotEqualTo(target.toString())
				.isEqualTo("onNext(foo)");
	}

	@Test
	public void iterableDoesntConsiderNonIterable() {
		Function<Object, String> formatter = ValueFormatters.iterableOf(Object.class, o -> o.getClass().getSimpleName());
		Object target = Obscure.OB1;

		assertThat(formatter.apply(target)).isEqualTo(target.toString());
	}

	@Test
	public void iterableDoesntConsiderNonMatchingContent() {
		Function<Object, String> formatter = ValueFormatters.iterableOf(Number.class, o -> o.getClass().getSimpleName());
		List<Object> target = Collections.singletonList(Obscure.OB1);

		assertThat(formatter.apply(target)).isEqualTo(target.toString());
	}

	@Test
	public void iterableConvertsContentMatching() {
		Function<Object, String> formatter = ValueFormatters.iterableOf(Obscure.class, Obscure::getAlternative);
		List<Object> target = Arrays.asList(1L, 2d, "foo", Obscure.OB2);

		assertThat(formatter.apply(target))
				.isNotEqualTo(target.toString())
				.isEqualTo("[1, 2.0, foo, bar]")
				.isEqualTo(target.toString().replace("OB2", "bar"));
	}

	@Test
	public void arrayDoesntConsiderNonArray() {
		Function<Object, String> formatter = ValueFormatters.arrayOf(Object.class, o -> o.getClass().getSimpleName());
		Object target = Obscure.OB1;

		assertThat(formatter.apply(target)).isEqualTo(target.toString());
	}

	@Test
	public void arrayDisplaysToStringOfNonMatchingContent() {
		Function<Object, String> formatter = ValueFormatters.arrayOf(Number.class, o -> o.getClass().getSimpleName());
		String[] target = new String[] { "foo", "bar" };

		assertThat(formatter.apply(target))
				.isNotEqualTo(target.toString())
				.isEqualTo("[foo, bar]");
	}

	@Test
	public void arrayConvertsContentMatching() {
		Function<Object, String> formatter = ValueFormatters.arrayOf(Obscure.class, Obscure::getAlternative);
		Object[] target = new Object[]{1L, 2d, "foo", Obscure.OB2};

		assertThat(formatter.apply(target))
				.isNotEqualTo(target.toString())
				.isNotEqualTo(target.toString().replace("OB2", "bar"))
				.isEqualTo("[1, 2.0, foo, bar]");
	}
}
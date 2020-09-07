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
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Signal;
import reactor.test.ValueFormatters.Extractor;
import reactor.test.ValueFormatters.ToStringConverter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

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
	public void classPredicateBasedNull() {
		Function<Object, String> formatter = ValueFormatters.forClassMatching(Obscure.class, t -> true, Obscure::getAlternative);

		assertThat(formatter.apply(null))
				.isNotNull()
				.isEqualTo("null");
	}

	@Test
	public void classPredicateBasedMatching() {
		Function<Object, String> formatter = ValueFormatters.forClassMatching(Obscure.class, t -> true, Obscure::getAlternative);

		assertThat(formatter.apply(Obscure.OB1))
				.isEqualTo("foo");
	}

	@Test
	public void classPredicateBasedNotMatchingClass() {
		Function<Object, String> formatter = ValueFormatters.forClassMatching(Integer.class, t -> true, i -> "int(" + i + ")");

		assertThat(formatter.apply(Obscure.OB1))
				.isEqualTo("OB1");
	}

	@Test
	public void classPredicateBasedNotMatchingPredicate() {
		Function<Object, String> formatter = ValueFormatters.forClassMatching(Obscure.class,
				t -> t.ordinal() > Obscure.OB1.ordinal(),
				Obscure::getAlternative);

		assertThat(formatter.apply(Obscure.OB1))
				.as("OB1 not matching predicate")
				.isEqualTo("OB1");

		assertThat(formatter.apply(Obscure.OB2))
				.as("OB2 matching predicate")
				.isEqualTo(Obscure.OB2.alternative);
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
	public void predicateBasedTestPredicate() {
		Predicate<Object> formatter = ValueFormatters.filtering(o -> o instanceof Integer, o -> "int(" + o + ")");

		assertThat(formatter)
				.accepts(1)
				.rejects(1L, "foo");
	}


	// == Extractor tests
	@Test
	public void signalDoesntConsiderNonSignal() {
		Extractor<Signal> extractor = ValueFormatters.signalExtractor();
		ToStringConverter converter = ValueFormatters.forClass(Obscure.class, Obscure::getAlternative);
		Object target = Obscure.OB1;

		assertThat(extractor.apply(target, converter)).isEqualTo(target.toString());
	}

	@Test
	public void signalDoesntConsiderNonMatchingContent() {
		Extractor<Signal> extractor = ValueFormatters.signalExtractor();
		ToStringConverter converter = ValueFormatters.forClass(String.class, String::toUpperCase);
		Signal<Object> target = Signal.next(Obscure.OB1);

		assertThat(extractor.apply(target, converter)).isEqualTo(target.toString());
	}

	@Test
	public void signalDoesntConvertComplete() {
		Extractor<Signal> extractor = ValueFormatters.signalExtractor();
		ToStringConverter converter = ValueFormatters.forClass(Obscure.class, Obscure::getAlternative);
		Signal<Number> target = Signal.complete();

		assertThat(extractor.apply(target, converter)).isEqualTo(target.toString());
	}

	@Test
	public void signalDoesntConvertError() {
		Extractor<Signal> extractor = ValueFormatters.signalExtractor();
		ToStringConverter converter = ValueFormatters.forClass(Obscure.class, Obscure::getAlternative);
		Signal<Number> target = Signal.error(new IllegalStateException("foo"));

		assertThat(extractor.apply(target, converter)).isEqualTo(target.toString());
	}

	@Test
	public void signalDoesntConvertErrorEvenIfThrowableClass() {
		Extractor<Signal> extractor = ValueFormatters.signalExtractor();
		ToStringConverter converter = ValueFormatters.forClass(Throwable.class, t -> t.getMessage().toUpperCase());
		Signal<Number> target = Signal.error(new IllegalStateException("foo"));

		assertThat(extractor.apply(target, converter)).isEqualTo(target.toString());
	}

	@Test
	public void signalConvertsOnNextContentMatching() {
		Extractor<Signal> extractor = ValueFormatters.signalExtractor();
		ToStringConverter converter = ValueFormatters.forClass(Obscure.class, Obscure::getAlternative);
		Signal<Obscure> target = Signal.next(Obscure.OB1);

		assertThat(extractor.apply(target, converter))
				.isNotEqualTo(target.toString())
				.isEqualTo("onNext(foo)");
	}

	@Test
	public void iterableDoesntConsiderNonIterable() {
		Extractor<Iterable> extractor = ValueFormatters.iterableExtractor();
		ToStringConverter converter = ValueFormatters.forClass(Obscure.class, Obscure::getAlternative);
		Object target = Obscure.OB1;

		assertThat(extractor.apply(target, converter)).isEqualTo(target.toString());
	}

	@Test
	public void iterableDoesntConsiderNonMatchingContent() {
		Extractor<Iterable> extractor = ValueFormatters.iterableExtractor();
		ToStringConverter converter = ValueFormatters.forClass(String.class, s -> "" + s.length());
		List<Object> target = Collections.singletonList(Obscure.OB1);

		assertThat(extractor.apply(target, converter)).isEqualTo(target.toString());
	}

	@Test
	public void iterableConvertsContentMatching() {
		Extractor<Iterable> extractor = ValueFormatters.iterableExtractor();
		ToStringConverter converter = ValueFormatters.forClass(Obscure.class, Obscure::getAlternative);
		List<Object> target = Arrays.asList(1L, 2d, "foo", Obscure.OB2);

		assertThat(extractor.apply(target, converter))
				.isNotEqualTo(target.toString())
				.isEqualTo("[1, 2.0, foo, bar]")
				.isEqualTo(target.toString().replace("OB2", "bar"));
	}

	@Test
	public void arrayDoesntConsiderNonArray() {
		Extractor<Object[]> extractor = ValueFormatters.arrayExtractor(Object[].class);
		ToStringConverter converter = ValueFormatters.forClass(Obscure.class, Obscure::getAlternative);
		Object target = Obscure.OB1;

		assertThat(extractor.apply(target, converter)).isEqualTo(target.toString());
	}

	@SuppressWarnings("ImplicitArrayToString")
	@Test
	public void arrayDoesntConsiderArrayOfDifferentType() {
		Extractor<Number[]> extractor = ValueFormatters.arrayExtractor(Number[].class);
		ToStringConverter converter = ValueFormatters.forClass(Number.class, o -> o.getClass().getSimpleName());
		String[] target = new String[] { "foo", "bar" };

		assertThat(extractor.apply(target, converter))
				.isEqualTo(target.toString());
	}

	@Test
	public void arrayDoesntConsiderNonMatchingContent() {
		Extractor<Object[]> extractor = ValueFormatters.arrayExtractor(Object[].class);
		ToStringConverter converter = ValueFormatters.forClass(String.class, s -> "" + s.length());
		Object[] target = new Object[] { Obscure.OB1 };

		assertThat(extractor.apply(target, converter))
				.isEqualTo("[OB1]");
	}

	@SuppressWarnings("ImplicitArrayToString")
	@Test
	public void arrayConvertsContentMatching() {
		Extractor<Object[]> extractor = ValueFormatters.arrayExtractor(Object[].class);
		ToStringConverter converter = ValueFormatters.forClass(Obscure.class, Obscure::getAlternative);
		Object[] target = new Object[] { 1L, 2d, "foo", Obscure.OB2 };

		assertThat(extractor.apply(target, converter))
				.isNotEqualTo(target.toString())
				.isNotEqualTo("[1, 2.0, foo, OB2]")
				.isEqualTo("[1, 2.0, foo, bar]");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void arrayOnNonArrayTypeFails() {
		Class fakeArrayClass = String.class;
		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> ValueFormatters.arrayExtractor(fakeArrayClass))
				.withMessage("arrayClass must be array");
	}

	//== convertVarArgs tests

	@Test
	public void convertVarargsSignalNotExtractedIfConverterMatches() {
		Extractor<Signal> extractor = ValueFormatters.signalExtractor();
		ToStringConverter converter = ValueFormatters.forClass(Object.class, o -> o.getClass().getSimpleName() + "=>" + o);
		Signal<Obscure> target = Signal.next(Obscure.OB1);

		Object[] converted = ValueFormatters.convertVarArgs(converter, Collections.singleton(extractor), target);

		assertThat(converted)
				.hasSize(1)
				.allSatisfy(o -> assertThat(o)
						.isInstanceOf(String.class)
						.isNotEqualTo(target.toString())
						.isNotEqualTo("onNext(foo)")
						.isEqualTo("ImmutableSignal=>onNext(OB1)")
				);
	}

	@Test
	public void convertVarargsIterableNotExtractedIfConverterMatches() {
		Extractor<Iterable> extractor = ValueFormatters.iterableExtractor();
		ToStringConverter converter = ValueFormatters.forClass(Object.class, o -> "<" + o + ">");
		List<Object> target = Arrays.asList(1L, 2d, "foo", Obscure.OB2);

		Object[] converted = ValueFormatters.convertVarArgs(converter, Collections.singleton(extractor), target);

		assertThat(converted)
				.hasSize(1)
				.allSatisfy(o -> assertThat(o)
						.isNotEqualTo(target.toString())
						.isEqualTo("<" + target + ">")
				);
	}

	@Test
	public void convertVarargsArrayNotExtractedIfConverterMatchesAndPassedAsSingleArg() {
		Extractor<Object[]> extractor = ValueFormatters.arrayExtractor(Object[].class);
		ToStringConverter converter = ValueFormatters.forClass(Object.class, o -> "<" + o + ">");
		Object target = new Object[] { 1L, 2d, "foo", Obscure.OB2 };

		Object[] converted = ValueFormatters.convertVarArgs(converter, Collections.singleton(extractor), target);

		assertThat(converted)
				.hasSize(1)
				.allSatisfy(o -> assertThat(o)
						.isNotEqualTo(target.toString())
						.isNotEqualTo("[<1>, <2.0>, <foo>, <OB2>]")
						.isNotEqualTo("[<1>, <2.0>, <foo>, <bar>]")
						.isEqualTo("<" + target + ">")
				);
	}

	@Test
	public void convertVarargsNullArgument() {
		ToStringConverter converter = ValueFormatters.forClass(Object.class, o -> "<" + o + ">");

		assertThat(ValueFormatters.convertVarArgs(converter, null, 1L, null))
				.containsExactly("<1>", "null");
	}

	@Test
	public void convertVarargsNullConverter() {
		assertThat(ValueFormatters.convertVarArgs(null, null, 1L, Obscure.OB1))
				.containsExactly("1", "OB1");
	}

	@Test
	public void convertVarargsNonMatchingConverterNullExtractors() {
		ToStringConverter converter = ValueFormatters.forClass(Obscure.class, o -> "<" + o + ">");

		assertThat(ValueFormatters.convertVarArgs(converter, null, 1L, Obscure.OB1, Collections.singleton(Obscure.OB2)))
				.containsExactly("1", "<OB1>", "[OB2]");
	}

	@Test
	public void convertVarargsNonMatchingConverterNorExtractors() {
		ToStringConverter converter = ValueFormatters.forClass(Obscure.class, o -> "<" + o + ">");
		Extractor<Iterable> extractor = ValueFormatters.iterableExtractor();

		assertThat(ValueFormatters.convertVarArgs(converter, Collections.singleton(extractor), 1L,
				Obscure.OB1, Collections.singleton(Obscure.OB2),
				Collections.singleton(2L)))
				.containsExactly("1", "<OB1>", "[<OB2>]", "[2]");

	}
}
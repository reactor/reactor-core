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

import java.util.function.Function;

import org.junit.Test;

import reactor.core.publisher.Signal;

import static org.assertj.core.api.Assertions.assertThat;

public class CustomizableObjectFormatterTest {

	@Test
	public void simpleFactorySetsCatchAll() {
		Function<Object, String> converter = o -> o.toString();
		CustomizableObjectFormatter cof = CustomizableObjectFormatter.simple(converter);

		assertThat(cof.catchAll).isSameAs(converter);
		assertThat(cof.converters).isEmpty();
		assertThat(cof.unwrap).as("unwrap").isTrue();
	}

	@Test
	public void unwrapTrueByDefault() {
		CustomizableObjectFormatter cof = new CustomizableObjectFormatter();
		assertThat(cof.unwrap).as("unwrap").isTrue();
	}

	@Test
	public void convertSignalUnwrapVsNoUnwrap() {
		CustomizableObjectFormatter cof = new CustomizableObjectFormatter();
		cof.addConverter(String.class, String::toUpperCase);

		cof.setUnwrap(false);
		assertThat(cof.apply(Signal.next("foo"))).isEqualTo("onNext(foo)");

		cof.setUnwrap(true);
		assertThat(cof.apply(Signal.next("foo"))).isEqualTo("onNext(FOO)");
	}

	@Test
	public void convertSignalUnwrapNull() {
		CustomizableObjectFormatter cof = new CustomizableObjectFormatter();
		cof.addConverter(String.class, String::toUpperCase);

		assertThat(cof.apply(Signal.next(null))).isEqualTo("onNext(null)");
	}

	@Test
	public void convertSignalUnwrapWithSignalConverter() {
		CustomizableObjectFormatter cof = new CustomizableObjectFormatter();
		cof.addConverter(String.class, String::toUpperCase);
		cof.addConverter(Signal.class, v -> v.getType().name());

		assertThat(cof.apply(Signal.next("foo"))).isEqualTo("ON_NEXT");
	}

	@Test
	public void convertSignalUnwrapDoesntApplyCatchAll() {
		CustomizableObjectFormatter cof = new CustomizableObjectFormatter();
		cof.setCatchAll(o -> String.valueOf(o).toUpperCase());

		assertThat(cof.apply(Signal.next("foo"))).isEqualTo("onNext(FOO)");
	}

	@Test
	public void convertSignalNoUnwrapWithSignalConverterAppliesOnlySignalConverter() {
		CustomizableObjectFormatter cof = new CustomizableObjectFormatter();
		cof.setUnwrap(false);
		cof.addConverter(String.class, String::toUpperCase);
		cof.addConverter(Signal.class, v -> v.getType().toString());

		assertThat(cof.apply(Signal.next("foo"))).isEqualTo("onNext");
	}

	@Test
	public void convertSignalNoUnwrapDoesApplyCatchAll() {
		CustomizableObjectFormatter cof = new CustomizableObjectFormatter();
		cof.setUnwrap(false);
		cof.setCatchAll(o -> String.valueOf(o).toUpperCase());

		assertThat(cof.apply(Signal.next("foo"))).isEqualTo("ONNEXT(FOO)");
	}

	@Test
	public void convertVarArgs() {
		CustomizableObjectFormatter cof = new CustomizableObjectFormatter();
		cof.setCatchAll(o -> o.getClass().getSimpleName());

		Object[] converted = CustomizableObjectFormatter.convertVarArgs(cof,"A", 1L, null);

		assertThat(converted).containsExactly("String", "Long", "null");
	}

	@Test
	public void convertVarArgsEmpty() {
		CustomizableObjectFormatter cof = new CustomizableObjectFormatter();
		cof.setCatchAll(o -> o.getClass().getSimpleName());

		Object[] converted = CustomizableObjectFormatter.convertVarArgs(cof);

		assertThat(converted).isEmpty();
	}

	@Test
	public void convertVarArgsContainsNullOnly() {
		CustomizableObjectFormatter cof = new CustomizableObjectFormatter();
		cof.setCatchAll(o -> o.getClass().getSimpleName());

		Object[] converted = CustomizableObjectFormatter.convertVarArgs(cof, (Object) null);

		assertThat(converted).containsExactly("null");
	}

	@Test
	public void convertVarArgsNull() {
		CustomizableObjectFormatter cof = new CustomizableObjectFormatter();
		cof.setCatchAll(o -> o.getClass().getSimpleName());

		Object[] converted = CustomizableObjectFormatter.convertVarArgs(cof, (Object[]) null);

		assertThat(converted).isNull();
	}

	@Test
	public void specificOrderMatters() {
		CustomizableObjectFormatter cof = new CustomizableObjectFormatter();
		cof.addConverter(o -> o instanceof Number, o -> "" + ((Number) o).doubleValue());
		cof.addConverter(o -> o instanceof Long, o -> "Long " + o + "L");

		assertThat(cof.apply(21L)).isEqualTo("21.0");

		//reverse order
		CustomizableObjectFormatter cof2 = new CustomizableObjectFormatter();
		cof2.addConverter(o -> o instanceof Long, o -> "Long " + o + "L");
		cof2.addConverter(o -> o instanceof Number, o -> "" + ((Number) o).doubleValue());

		assertThat(cof2.apply(22L)).isEqualTo("Long 22L");
	}
}
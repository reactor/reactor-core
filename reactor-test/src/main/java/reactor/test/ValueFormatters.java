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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import reactor.core.publisher.Signal;
import reactor.util.annotation.Nullable;

/**
 * An utility class to create class-specific, predicate-specific and container-specific
 * {@link Function} that convert objects (or contained objects) to {@link String}.
 * They can be applied to any {@link Object} but will restrict the types they actually
 * convert by filtering and defaulting to {@link String#valueOf(Object)}.
 * <p>
 * An additional static utility method {@link #convertVarArgs(Function, Object...)} can
 * be used to apply such functions to varargs.
 *
 * @author Simon Baslé
 */
public final class ValueFormatters {

	private ValueFormatters() {}

	/**
	 * Create a value formatter that is specific to a particular {@link Class}, only
	 * applying the given String conversion {@link Function} if the object is an instance
	 * of that Class.
	 *
	 * @param tClass the {@link Class} to match
	 * @param tToString the {@link String} conversion {@link Function} for objects of that class
	 * @param <T> the generic type of the matching objects
	 * @return the class-specific formatter
	 */
	public static <T> Function<Object, String> forClass(Class<T> tClass,
			Function<T, String> tToString) {
		return new ClassBasedConverter<>(tClass, tToString);
	}

	/**
	 * Create a value formatter that applies the given String conversion {@link Function}
	 * only to objects matching a given {@link Predicate}
	 *
	 * @param predicate the {@link Predicate} used to filter objects to format
	 * @param anyToString the {@link String} conversion {@link Function} (without input typing)
	 * @return the predicate-specific formatter
	 */
	public static Function<Object, String> filtering(Predicate<Object> predicate,
			Function<Object, String> anyToString) {
		return new PredicateBasedConverter(predicate, anyToString);
	}

	/**
	 * Create a value formatter that applies to {@link Signal} instances and unwraps these,
	 * verify the {@link Signal} is a {@link Signal#isOnNext() onNext} and the content is
	 * an instance of {@link Class}, in which case it applies the provided {@link String}
	 * conversion {@link Function}. The result is then wrapped to obtain a Signal representation
	 * with the alternative content {@link String}, like so: {@code "onNext(CONVERTED)"}.
	 *
	 * @param signalContentClass the {@link Class} the onNext value must match to be converted
	 * @param signalContentToString the {@link Function} to format instances of this class
	 * @param <T> the type of the content of the {@link Signal}
	 * @return the {@link Signal}-unwrapping formatter
	 */
	public static <T> Function<Object, String> signalOf(Class<T> signalContentClass,
			Function<T, String> signalContentToString) {
		return new SignalExtractingConverter<>(signalContentClass, signalContentToString);
	}

	/**
	 * Create a value formatter that applies to {@link Iterable} instances and unwraps these,
	 * formatting each element that matches a given {@link Class} with the provided {@link String}
	 * conversion {@link Function}. The result is then wrapped to obtain a list-like
	 * representation with the alternative element {@link String}, like so: {@code "(CONVERTED1, CONVERTED2, CONVERTED3)"}.
	 *
	 * @param iterableElementClass the {@link Class} an iterable element must match to be converted
	 * @param iterableElementToString the {@link Function} to format instances of this class
	 * @param <T> the type of the convertible elements of the {@link Iterable}
	 * @return the {@link Iterable}-unwrapping formatter
	 */
	public static <T> Function<Object, String> iterableOf(Class<T> iterableElementClass,
			Function<T, String> iterableElementToString) {
		return new IterableExtractingConverter<>(iterableElementClass, iterableElementToString);
	}

	/**
	 * Create a value formatter that applies to arrays and unwraps these, formatting each
	 * element that matches a given {@link Class} with the provided {@link String}
	 * conversion {@link Function}. The result is then wrapped to obtain a square brackets
	 * enclosed representation with the alternative element {@link String}, like so:
	 * {@code "[CONVERTED1, CONVERTED2, CONVERTED3]"}.
	 *
	 * @param arrayElementClass the {@link Class} an array element must match to be converted
	 * @param arrayElementToString the {@link Function} to format instances of this class
	 * @param <T> the type of the convertible elements of the array
	 * @return the array-unwrapping formatter
	 */
	public static <T> Function<Object, String> arrayOf(Class<T> arrayElementClass,
			Function<T, String> arrayElementToString) {
		return new ArrayExtractingConverter<>(arrayElementClass, arrayElementToString);
	}


	/**
	 * Convert the whole vararg array by applying this formatter to each element in it.
	 * @param args the vararg to format
	 * @return a formatted array usable in replacement of the vararg
	 */
	@Nullable
	static Object[] convertVarArgs(Function<Object, String> converter, @Nullable Object... args) {
		if (args == null) return null;
		Object[] converted = new Object[args.length];
		for (int i = 0; i < args.length; i++) {
			Object arg = args[i];
			converted[i] = converter.apply(arg);
		}
		return converted;
	}

	static class PredicateBasedConverter implements Function<Object, String> {

		private final Predicate<Object>        predicate;
		private final Function<Object, String> function;

		PredicateBasedConverter(Predicate<Object> predicate, Function<Object, String> function) {
			this.predicate = predicate;
			this.function = function;
		}

		@Override
		public String apply(Object o) {
			if (predicate.test(o)) {
				return function.apply(o);
			}
			return String.valueOf(o);
		}
	}

	static class ClassBasedConverter<T> implements Function<Object, String> {

		private final Class<T> tClass;
		private final Function<T, String> function;

		ClassBasedConverter(Class<T> aClass, Function<T, String> function) {
			this.tClass = aClass;
			this.function = function;
		}

		@Override
		public String apply(Object o) {
			if (tClass.isInstance(o)) {
				T t = tClass.cast(o);
				return function.apply(t);
			}
			return String.valueOf(o);
		}
	}

	static abstract class ExtractingConverter<CONTAINER, CONTENT> implements Function<Object, String> {

		final Class<CONTAINER>          containerClass;
		final Class<CONTENT>            contentClass;
		final Function<CONTENT, String> stringify;

		ExtractingConverter(Class<CONTAINER> containerClass,
				Class<CONTENT> contentClass,
				Function<CONTENT, String> stringify) {
			this.containerClass = containerClass;
			this.contentClass = contentClass;
			this.stringify = stringify;
		}

		abstract Stream<Object> extractContent(CONTAINER container);

		abstract CharSequence prefixFor(CONTAINER container);
		abstract CharSequence suffixFor(CONTAINER container);

		@Override
		public String apply(Object potentialContainer) {
			if (containerClass.isInstance(potentialContainer)) {
				CONTAINER container = containerClass.cast(potentialContainer);
				return extractContent(container)
						.map(o -> {
							if (contentClass.isInstance(o)) {
								CONTENT c = contentClass.cast(o);
								return this.stringify.apply(c);
							}
							return String.valueOf(o);
						})
						.collect(Collectors.joining(", ", prefixFor(container), suffixFor(container)));
			}
			return String.valueOf(potentialContainer);
		}

	}

	static final class SignalExtractingConverter<T> extends ExtractingConverter<Signal, T> {

		SignalExtractingConverter(Class<T> contentClass, Function<T, String> stringify) {
			super(Signal.class, contentClass, stringify);
		}

		@Override
		Stream<Object> extractContent(Signal signal) {
			if (signal.isOnNext()) {
				return Stream.of(signal.get());
			}
			return Stream.empty();
		}

		@Override
		CharSequence prefixFor(Signal signal) {
			if (signal.isOnNext()) {
				return "onNext(";
			}
			return signal.toString();
		}

		@Override
		CharSequence suffixFor(Signal signal) {
			if (signal.isOnNext()) {
				return ")";
			}
			return "";
		}
	}

	static final class ArrayExtractingConverter<T> extends ExtractingConverter<Object[], T> {

		ArrayExtractingConverter(Class<T> contentClass,
				Function<T, String> stringify) {
			super(Object[].class, contentClass, stringify);
		}

		@Override
		Stream<Object> extractContent(Object[] ts) {
			return Stream.of(ts);
		}

		@Override
		CharSequence prefixFor(Object[] ts) {
			return "[";
		}

		@Override
		CharSequence suffixFor(Object[] ts) {
			return "]";
		}
	}

	static final class IterableExtractingConverter<T> extends ExtractingConverter<Iterable, T> {

		IterableExtractingConverter(Class<T> contentClass,
				Function<T, String> stringify) {
			super(Iterable.class, contentClass, stringify);
		}

		@Override
		Stream<Object> extractContent(Iterable iterable) {
			return StreamSupport.stream(iterable.spliterator(), false);
		}

		@Override
		CharSequence prefixFor(Iterable iterable) {
			return "[";
		}

		@Override
		CharSequence suffixFor(Iterable iterable) {
			return "]";
		}
	}
}

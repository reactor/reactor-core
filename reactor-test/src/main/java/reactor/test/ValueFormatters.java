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

import java.time.Duration;
import java.util.Collection;
import java.util.Spliterator;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import reactor.core.Fuseable;
import reactor.core.publisher.Signal;
import reactor.util.annotation.Nullable;

/**
 * An utility class to create {@link ToStringConverter} {@link Function} that convert objects to {@link String}.
 * They can be applied to any {@link Object} but will restrict the types they actually
 * convert by filtering and defaulting to {@link String#valueOf(Object)}.
 * <p>
 * Also defines {@link Extractor} {@link BiFunction} that extract multiple elements from
 * containers and applies a {@link Function} (most probably a {@link ToStringConverter})
 * to its elements, reforming a complete {@link String} representation. Again, this matches
 * on at least a given {@link Class} and potentially an additional {@link Predicate}.
 * <p>
 * An additional static utility method {@link #convertVarArgs(ToStringConverter, Collection, Object...)} can
 * be used to apply such functions and  to varargs.
 *
 * @author Simon Baslé
 */
public final class ValueFormatters {

	private ValueFormatters() {}

	/**
	 * Default {@link Duration#toString() Duration} {@link ToStringConverter} that removes the PT prefix and
	 * switches {@link String#toLowerCase() to lower case}.
	 */
	public static final ToStringConverter DURATION_CONVERTER = new ClassBasedToStringConverter<>(
			Duration.class,
			d -> true,
			d -> d.toString().replaceFirst("PT", "").toLowerCase());

	/**
	 * A generic {@link Object} to {@link String} conversion {@link Function} which is
	 * also a {@link Predicate}, and which only applies a custom conversion to targets that
	 * match said {@link Predicate}. Other targets are converted using {@link String#valueOf(Object)}.
	 */
	public interface ToStringConverter extends Predicate<Object>, Function<Object, String> {}

	/**
	 * An extractor of data wrapped in a {@link BiFunction} aiming at producing a customized {@link String}
	 * representation of a container type and its contained elements, each element being
	 * potentially itself converted to {@link String} using a {@link ToStringConverter}:
	 * <ul>
	 *     <li>it only considers specific container types, see {@link #getTargetClass()}</li>
	 *     <li>it can further filter these container instances using {@link #matches(Object)}</li>
	 *     <li>it can be applied to arbitrary objects, as it will default to {@link String#valueOf(Object)}
	 *     on non-matching containers</li>
	 *     <li>it can apply a {@link ToStringConverter} to the content, passed as the second
	 *     parameter of the {@link BiFunction}</li>
	 *     <li>it reconstructs the {@link String} representation of the container by
	 *     {@link #explode(Object) exploding} it and then {@link Collectors#joining(CharSequence, CharSequence, CharSequence) joining}
	 *     it with {#code ", "} delimiter, as well as custom {@link #prefix(Object)} and {@link #suffix(Object)}</li>
	 * </ul>
	 *
	 * @param <CONTAINER> the type of container
	 */
	public interface Extractor<CONTAINER> extends Predicate<Object>, BiFunction<Object, Function<Object, String>, String> {

		/**
		 * Return the targeted container {@link Class}. The {@link BiFunction} shouldn't be
		 * applied to objects that are not of that class, although it will default to using
		 * {@link String#valueOf(Object)} on them. This verification is included in {@link #test(Object)}.
		 *
		 * @return the target container {@link Class}
		 */
		Class<CONTAINER> getTargetClass();

		/**
		 * An additional test to perform on a matching container to further decide to convert
		 * it or not. The {@link BiFunction} shouldn't be applied to container that do not
		 * match that predicate, although it will default to using {@link String#valueOf(Object)}
		 * on them. This verification is included in {@link #test(Object)}.
		 * <p>
		 * Defaults to always matching instances of the {@link #getTargetClass() target Class}.
		 *
		 * @param value the candidate container
		 * @return true if it can be extracted and converted, false otherwise
		 */
		default boolean matches(CONTAINER value) {
			return true;
		}

		/**
		 * Return the prefix to use in the container's {@link String} representation, given
		 * the original container.
		 * <p>
		 * Defaults to {@code "["}.
		 *
		 * @param original the original container
		 * @return the prefix to use
		 */
		default String prefix(CONTAINER original) {
			return "[";
		}

		/**
		 * Return the suffix to use in the container's {@link String} representation, given
		 * the original container.
		 * <p>
		 * Defaults to {@code "]"}.
		 *
		 * @param original the original container
		 * @return the suffix to use
		 */
		default String suffix(CONTAINER original) {
			return "]";
		}

		/**
		 * Explode the container into a {@link Stream} of {@link Object}, each of which
		 * is a candidate for individual {@link String} conversion by a {@link ToStringConverter}
		 * when applied as a {@link BiFunction}.
		 *
		 * @param original the container to extract contents from
		 * @return the {@link Stream} of elements contained in the container
		 */
		Stream<Object> explode(CONTAINER original);

		/**
		 * Test if an object is a container that can be extracted and converted by this
		 * {@link Extractor}. Defaults to testing {@link #getTargetClass()} and {@link #matches(Object)}.
		 * The {@link BiFunction} shouldn't be applied to objects that do not match this
		 * test, although it will default to using {@link String#valueOf(Object)} on them.
		 *
		 * @param o the arbitrary object to test.
		 * @return true if the object can be extracted and converted to {@link String}
		 */
		@Override
		default boolean test(Object o) {
			Class<CONTAINER> containerClass = getTargetClass();
			if (containerClass.isInstance(o)) {
				CONTAINER container = containerClass.cast(o);
				return matches(container);
			}
			return false;
		}

		/**
		 * Given an arbitrary object and a {@link ToStringConverter}, if the object passes
		 * the {@link #test(Object)}, extract elements from it and convert them using the
		 * {@link ToStringConverter}, joining the result together to obtain a customized
		 * {@link String} representation of both the container and its contents.
		 * Any object that doesn't match this {@link Extractor} is naively transformed
		 * using {@link String#valueOf(Object)}, use {@link #test(Object)} to avoid that
		 * when choosing between multiple {@link Extractor}.
		 *
		 * @param target the arbitrary object to potentially convert.
		 * @param contentFormatter the {@link ToStringConverter} to apply on each element
		 * contained in the target
		 * @return the {@link String} representation of the target, customized as needed
		 */
		@Nullable
		default String apply(Object target, Function<Object, String> contentFormatter) {
			Class<CONTAINER> containerClass = getTargetClass();
			if (containerClass.isInstance(target)) {
				CONTAINER container = containerClass.cast(target);
				if (matches(container)) {
					return explode(container)
							.map(contentFormatter)
							.collect(Collectors.joining(", ", prefix(container), suffix(container)));
				}
			}
			return String.valueOf(target);
		}
	}

	/**
	 * Create a value formatter that is specific to a particular {@link Class}, applying
	 * the given String conversion {@link Function} provided the object is an instance of
	 * that Class.
	 *
	 * @param tClass the {@link Class} to convert
	 * @param tToString the {@link String} conversion {@link Function} for objects of that class
	 * @param <T> the generic type of the matching objects
	 * @return the class-specific formatter
	 */
	public static <T> ToStringConverter forClass(Class<T> tClass,
			Function<T, String> tToString) {
		return new ClassBasedToStringConverter<>(tClass, t -> true, tToString);
	}

	/**
	 * Create a value formatter that is specific to a particular {@link Class} and filters
	 * based on a provided {@link Predicate}. All objects of said class that additionally
	 * pass the {@link Predicate} are converted to {@link String} using the provided
	 * conversion {@link Function}.
	 *
	 * @param tClass the {@link Class} to convert
	 * @param tPredicate the {@link Predicate} to further filter what to convert to string
	 * @param tToString the {@link String} conversion {@link Function} for objects of that class matching the predicate
	 * @param <T> the generic type of the matching objects
	 * @return the class-specific predicate-filtering formatter
	 */
	public static <T> ToStringConverter forClassMatching(Class<T> tClass,
			Predicate<T> tPredicate,
			Function<T, String> tToString) {
		return new ClassBasedToStringConverter<>(tClass, tPredicate, tToString);
	}

	/**
	 * Create a value formatter that applies the given String conversion {@link Function}
	 * only to objects matching a given {@link Predicate}
	 *
	 * @param predicate the {@link Predicate} used to filter objects to format
	 * @param anyToString the {@link String} conversion {@link Function} (without input typing)
	 * @return the predicate-specific formatter
	 */
	public static ToStringConverter filtering(Predicate<Object> predicate,
			Function<Object, String> anyToString) {
		return new PredicateBasedToStringConverter(predicate, anyToString);
	}

	/**
	 * Default {@link Signal} extractor that unwraps only {@link Signal#isOnNext() onNext}.
	 * These {@link Signal} instances have a Signal representation like so: {@code "onNext(CONVERTED)"}.
	 *
	 * @return the default {@link Signal} extractor
	 */
	public static Extractor<Signal> signalExtractor() {
		return DEFAULT_SIGNAL_EXTRACTOR;
	}

	/**
	 * Default {@link Iterable} extractor that use the {@code [CONVERTED1, CONVERTED2]}
	 * representation.
	 *
	 * @return the default {@link Iterable} extractor
	 */
	public static Extractor<Iterable> iterableExtractor() {
		return DEFAULT_ITERABLE_EXTRACTOR;
	}

	/**
	 * Default array extractor that use the {@code [CONVERTED1, CONVERTED2]}
	 * representation.
	 *
	 * @param arrayClass the {@link Class} representing the array type
	 * @param <T> the type of the array
	 * @return the default array extractor for arrays of T
	 */
	public static <T> Extractor<T[]> arrayExtractor(final Class<T[]> arrayClass) {
		if (!arrayClass.isArray()) {
			throw new IllegalArgumentException("arrayClass must be array");
		}
		return new Extractor<T[]>() {
			@Override
			public Class<T[]> getTargetClass() {
				return arrayClass;
			}

			@Override
			public boolean matches(T[] value) {
				return true;
			}

			@Override
			public String prefix(T[] original) {
				return "[";
			}

			@Override
			public String suffix(T[] original) {
				return "]";
			}

			@Override
			public Stream<Object> explode(T[] original) {
				return Stream.of(original);
			}
		};
	}

	/**
	 * Convert the whole vararg array by applying this formatter to each element in it.
	 * @param args the vararg to format
	 * @return a formatted array usable in replacement of the vararg
	 */
	@Nullable
	static Object[] convertVarArgs(@Nullable ToStringConverter toStringConverter,
			@Nullable Collection<Extractor<?>> extractors,
			@Nullable Object... args) {
		if (args == null) return null;
		Object[] convertedArgs = new Object[args.length];
		for (int i = 0; i < args.length; i++) {
			Object arg = args[i];

			String converted;
			if (arg == null || toStringConverter == null) {
				converted = String.valueOf(arg);
			}
			else if (toStringConverter.test(arg)) {
				converted = toStringConverter.apply(arg);
			}
			else if (extractors == null) {
				converted = String.valueOf(arg);
			}
			else {
				converted = null;
				for (Extractor<?> extractor : extractors) {
					if (extractor.test(arg)) {
						converted = extractor.apply(arg, toStringConverter);
						break;
					}
				}
				if (converted == null) {
					converted = String.valueOf(arg);
				}
			}
			convertedArgs[i] = converted;
		}
		return convertedArgs;
	}

	static class PredicateBasedToStringConverter implements ToStringConverter {

		private final Predicate<Object>        predicate;
		private final Function<Object, String> function;

		PredicateBasedToStringConverter(Predicate<Object> predicate, Function<Object, String> function) {
			this.predicate = predicate;
			this.function = function;
		}

		@Override
		public boolean test(Object o) {
			return predicate.test(o);
		}

		@Override
		public String apply(Object o) {
			if (predicate.test(o)) {
				return function.apply(o);
			}
			return String.valueOf(o);
		}
	}

	static class ClassBasedToStringConverter<T> implements ToStringConverter {

		private final Class<T> tClass;
		private final Predicate<T> tPredicate;
		private final Function<T, String> function;

		ClassBasedToStringConverter(Class<T> aClass, Predicate<T> predicate, Function<T, String> function) {
			this.tClass = aClass;
			this.tPredicate = predicate;
			this.function = function;
		}

		@Override
		public boolean test(Object o) {
			if (tClass.isInstance(o)) {
				return tPredicate.test(tClass.cast(o));
			}
			return false;
		}

		@Override
		public String apply(Object o) {
			if (tClass.isInstance(o)) {
				T t = tClass.cast(o);
				if (tPredicate.test(t)) {
					return function.apply(t);
				}
			}
			return String.valueOf(o);
		}
	}

	static final Extractor<Signal> DEFAULT_SIGNAL_EXTRACTOR = new Extractor<Signal>() {

		@Override
		public Class<Signal> getTargetClass() {
			return Signal.class;
		}

		@Override
		public boolean matches(Signal value) {
			return value.isOnNext() && value.hasValue();
		}

		@Override
		public String prefix(Signal original) {
			return "onNext(";
		}

		@Override
		public String suffix(Signal original) {
			return ")";
		}

		@Override
		public Stream<Object> explode(Signal original) {
			return Stream.of(original.get());
		}
	};

	static final Extractor<Iterable> DEFAULT_ITERABLE_EXTRACTOR = new Extractor<Iterable>() {

		@Override
		public Class<Iterable> getTargetClass() {
			return Iterable.class;
		}

		@Override
		public boolean matches(Iterable value) {
			return !(value instanceof Fuseable.QueueSubscription);
		}

		@Override
		public String prefix(Iterable original) {
			return "[";
		}

		@Override
		public String suffix(Iterable original) {
			return "]";
		}

		@Override
		public Stream<Object> explode(Iterable original) {
			@SuppressWarnings("unchecked") Spliterator<Object> spliterator = ((Iterable<Object>) original).spliterator();
			return StreamSupport.stream(spliterator, false);
		}
	};

}
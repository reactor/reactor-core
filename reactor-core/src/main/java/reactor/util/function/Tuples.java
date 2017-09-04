/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.util.function;

import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A {@literal Tuples} is an immutable {@link Collection} of objects, each of which can be of an arbitrary type.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
@SuppressWarnings({"rawtypes"})
public abstract class Tuples implements Function {

	/**
	 * Create a {@link Tuple2} with the given array if it is small
	 * enough to fit inside a {@link Tuple2} to {@link Tuple8}.
	 *
	 * @param list the content of the Tuple (size 1 to 8)
	 * @return The new {@link Tuple2}.
	 * @throws IllegalArgumentException if the array is not of length 1-8
	 */
	public static Tuple2 fromArray(Object[] list) {
		//noinspection ConstantConditions
		if (list == null || list.length < 2) {
			throw new IllegalArgumentException("null or too small array, need between 2 and 8 values");
		}

		switch (list.length){
			case 2:
				return of(list[0], list[1]);
			case 3:
				return of(list[0], list[1], list[2]);
			case 4:
				return of(list[0], list[1], list[2], list[3]);
			case 5:
				return of(list[0], list[1], list[2], list[3], list[4]);
			case 6:
				return of(list[0], list[1], list[2], list[3], list[4], list[5]);
			case 7:
				return of(list[0], list[1], list[2], list[3], list[4], list[5], list[6]);
			case 8:
				return of(list[0], list[1], list[2], list[3], list[4], list[5], list[6], list[7]);
		}
		throw new IllegalArgumentException("too many arguments ("+ list.length + "), need between 2 and 8 values");
	}

	/**
	 * Create a {@link Tuple2} with the given objects.
	 *
	 * @param t1   The first value in the tuple. Not null.
	 * @param t2   The second value in the tuple. Not null.
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @return The new {@link Tuple2}.
	 */
	public static <T1, T2> Tuple2<T1, T2> of(T1 t1, T2 t2) {
		return new Tuple2<>(t1, t2);
	}

	/**
	 * Create a {@link Tuple3} with the given objects.
	 *
	 * @param t1   The first value in the tuple. Not null.
	 * @param t2   The second value in the tuple. Not null.
	 * @param t3   The third value in the tuple. Not null.
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @return The new {@link Tuple3}.
	 */
	public static <T1, T2, T3> Tuple3<T1, T2, T3> of(T1 t1, T2 t2, T3 t3) {
		return new Tuple3<>(t1, t2, t3);
	}

	/**
	 * Create a {@link Tuple4} with the given objects.
	 *
	 * @param t1   The first value in the tuple. Not null.
	 * @param t2   The second value in the tuple. Not null.
	 * @param t3   The third value in the tuple. Not null.
	 * @param t4   The fourth value in the tuple. Not null.
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @return The new {@link Tuple4}.
	 */
	public static <T1, T2, T3, T4> Tuple4<T1, T2, T3, T4> of(T1 t1, T2 t2, T3 t3, T4 t4) {
		return new Tuple4<>(t1, t2, t3, t4);
	}

	/**
	 * Create a {@link Tuple5} with the given objects.
	 *
	 * @param t1   The first value in the tuple. Not null.
	 * @param t2   The second value in the tuple. Not null.
	 * @param t3   The third value in the tuple. Not null.
	 * @param t4   The fourth value in the tuple. Not null.
	 * @param t5   The fifth value in the tuple. Not null.
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @return The new {@link Tuple5}.
	 */
	public static <T1, T2, T3, T4, T5> Tuple5<T1, T2, T3, T4, T5> of(
			T1 t1,
			T2 t2,
			T3 t3,
			T4 t4,
			T5 t5) {
		return new Tuple5<>(t1, t2, t3, t4, t5);
	}

	/**
	 * Create a {@link Tuple6} with the given objects.
	 *
	 * @param t1   The first value in the tuple. Not null.
	 * @param t2   The second value in the tuple. Not null.
	 * @param t3   The third value in the tuple. Not null.
	 * @param t4   The fourth value in the tuple. Not null.
	 * @param t5   The fifth value in the tuple. Not null.
	 * @param t6   The sixth value in the tuple. Not null.
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 * @return The new {@link Tuple6}.
	 */
	public static <T1, T2, T3, T4, T5, T6> Tuple6<T1, T2, T3, T4, T5, T6> of(
			T1 t1,
			T2 t2,
			T3 t3,
			T4 t4,
			T5 t5,
			T6 t6) {
		return new Tuple6<>(t1, t2, t3, t4, t5, t6);
	}

	/**
	 * Create a {@link Tuple7} with the given objects.
	 *
	 * @param t1   The first value in the tuple. Not null.
	 * @param t2   The second value in the tuple. Not null.
	 * @param t3   The third value in the tuple. Not null.
	 * @param t4   The fourth value in the tuple. Not null.
	 * @param t5   The fifth value in the tuple. Not null.
	 * @param t6   The sixth value in the tuple. Not null.
	 * @param t7   The seventh value in the tuple. Not null.
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 * @param <T7> The type of the seventh value.
	 * @return The new {@link Tuple7}.
	 */
	public static <T1, T2, T3, T4, T5, T6, T7> Tuple7<T1, T2, T3, T4, T5, T6, T7> of(
			T1 t1,
			T2 t2,
			T3 t3,
			T4 t4,
			T5 t5,
			T6 t6,
			T7 t7) {
		return new Tuple7<>(t1, t2, t3, t4, t5, t6, t7);
	}

	/**
	 * Create a {@link Tuple8} with the given objects.
	 *
	 * @param t1   The first value in the tuple. Not Null.
	 * @param t2   The second value in the tuple.Not Null.
	 * @param t3   The third value in the tuple. Not Null.
	 * @param t4   The fourth value in the tuple. Not Null.
	 * @param t5   The fifth value in the tuple. Not Null.
	 * @param t6   The sixth value in the tuple. Not Null.
	 * @param t7   The seventh value in the tuple. Not Null.
	 * @param t8   The eighth value in the tuple. Not Null.
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 * @param <T7> The type of the seventh value.
	 * @param <T8> The type of the eighth value.
	 * @return The new {@link Tuple8}.
	 */
	public static <T1, T2, T3, T4, T5, T6, T7, T8> Tuple8<T1, T2, T3, T4, T5, T6, T7, T8> of(
			T1 t1,
			T2 t2,
			T3 t3,
			T4 t4,
			T5 t5,
			T6 t6,
			T7 t7,
			T8 t8) {
		return new Tuple8<>(t1, t2, t3, t4, t5, t6, t7, t8);
	}

	/**
	 * A converting function from Object array to {@link Tuples}
	 *
	 * @return The unchecked conversion function to {@link Tuples}.
	 */
	@SuppressWarnings("unchecked")
	public static Function<Object[], Tuple2> fnAny() {
		return empty;
	}

	/**
	 * A converting function from Object array to {@link Tuples} to R.
	 *
	 * @param <R> The type of the return value.
	 * @param delegate the function to delegate to
	 *
	 * @return The unchecked conversion function to R.
	 */
	public static <R> Function<Object[], R> fnAny(final Function<Tuple2, R> delegate) {
		return objects -> delegate.apply(Tuples.fnAny().apply(objects));
	}

	/**
	 * A converting function from Object array to {@link Tuple2}
	 *
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 *
	 * @return The unchecked conversion function to {@link Tuple2}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2> Function<Object[], Tuple2<T1, T2>> fn2() {
		return empty;
	}


	/**
	 * A converting function from Object array to {@link Tuple3}
	 *
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 *
	 * @return The unchecked conversion function to {@link Tuple3}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3> Function<Object[], Tuple3<T1, T2, T3>> fn3() {
		return empty;
	}

	/**
	 * A converting function from Object array to {@link Tuple3} to R.
	 *
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <R> The type of the return value.
     * @param delegate the function to delegate to
	 *
	 * @return The unchecked conversion function to R.
	 */
	public static <T1, T2, T3, R> Function<Object[], R> fn3(final Function<Tuple3<T1, T2, T3>, R> delegate) {
		return objects -> delegate.apply(Tuples.<T1, T2, T3>fn3().apply(objects));
	}

	/**
	 * A converting function from Object array to {@link Tuple4}
	 *
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 *
	 * @return The unchecked conversion function to {@link Tuple4}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4> Function<Object[], Tuple4<T1, T2, T3, T4>> fn4() {
		return empty;
	}

	/**
	 * A converting function from Object array to {@link Tuple4} to R.
	 *
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <R> The type of the return value.
     * @param delegate the function to delegate to
	 *
	 * @return The unchecked conversion function to R.
	 */
	public static <T1, T2, T3, T4, R> Function<Object[], R> fn4(final Function<Tuple4<T1, T2, T3, T4>, R> delegate) {
		return objects -> delegate.apply(Tuples.<T1, T2, T3, T4>fn4().apply(objects));
	}

	/**
	 * A converting function from Object array to {@link Tuple5}
	 *
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 *
	 * @return The unchecked conversion function to {@link Tuple5}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5> Function<Object[], Tuple5<T1, T2, T3, T4, T5>> fn5() {
		return empty;
	}

	/**
	 * A converting function from Object array to {@link Tuple4} to R.
	 *
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <R> The type of the return value.
     * @param delegate the function to delegate to
	 *
	 * @return The unchecked conversion function to R.
	 */
	public static <T1, T2, T3, T4, T5, R> Function<Object[], R> fn5(final Function<Tuple5<T1, T2, T3, T4, T5>, R> delegate) {
		return objects -> delegate.apply(Tuples.<T1, T2, T3, T4, T5>fn5().apply(objects));
	}

	/**
	 * A converting function from Object array to {@link Tuple6}
	 *
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 *
	 * @return The unchecked conversion function to {@link Tuple6}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, T6> Function<Object[], Tuple6<T1, T2, T3, T4, T5, T6>> fn6() {
		return empty;
	}

	/**
	 * A converting function from Object array to {@link Tuple6} to R.
	 *
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 * @param <R> The type of the return value.
     * @param delegate the function to delegate to
	 *
	 * @return The unchecked conversion function to R.
	 */
	public static <T1, T2, T3, T4, T5, T6, R> Function<Object[], R> fn6(final Function<Tuple6<T1, T2, T3, T4, T5, T6>, R> delegate) {
		return objects -> delegate.apply(Tuples.<T1, T2, T3, T4, T5, T6>fn6().apply(objects));
	}

	/**
	 * A converting function from Object array to {@link Tuple7}
	 *
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 * @param <T7> The type of the seventh value.
	 *
	 * @return The unchecked conversion function to {@link Tuple7}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, T6, T7> Function<Object[], Tuple7<T1, T2, T3, T4, T5, T6, T7>> fn7() {
		return empty;
	}

	/**
	 * A converting function from Object array to {@link Tuple7} to R.
	 *
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 * @param <T7> The type of the seventh value.
	 * @param <R> The type of the return value.
     * @param delegate the function to delegate to
	 *
	 * @return The unchecked conversion function to R.
	 */
	public static <T1, T2, T3, T4, T5, T6, T7, R> Function<Object[], R> fn7(final Function<Tuple7<T1, T2, T3, T4, T5, T6, T7>, R> delegate) {
		return objects -> delegate.apply(Tuples.<T1, T2, T3, T4, T5, T6, T7>fn7().apply(objects));
	}

	/**
	 * A converting function from Object array to {@link Tuple8}
	 *
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 * @param <T7> The type of the seventh value.
	 * @param <T8> The type of the eighth value.
	 *
	 * @return The unchecked conversion function to {@link Tuple8}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, T6, T7, T8> Function<Object[], Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>> fn8() {
		return empty;
	}

	/**
	 * A converting function from Object array to {@link Tuple8}
	 *
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 * @param <T7> The type of the seventh value.
     * @param <T8> The type of the eighth value.
	 * @param <R> The type of the return value.
     * @param delegate the function to delegate to
	 *
	 * @return The unchecked conversion function to {@link Tuple8}.
	 */
	public static <T1, T2, T3, T4, T5, T6, T7, T8, R> Function<Object[], R> fn8(final Function<Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>, R> delegate) {
		return objects -> delegate.apply(Tuples.<T1, T2, T3, T4, T5, T6, T7, T8>fn8().apply(objects));
	}

    /**
     * Returns a {@link Consumer} of {@link Tuple2} that wraps a consumer of the component values of the tuple.
     *
     * @param consumer The component value consumer.
     * @param <T1> The type of the first value.
     * @param <T2> The type of the second value.
     * @return The wrapper consumer
     */
    public static <T1, T2> Consumer<Tuple2<T1, T2>> consumer(BiConsumer<T1, T2> consumer) {
        return tuple -> consumer.accept(tuple.getT1(), tuple.getT2());
    }

    /**
     * Returns a {@link Consumer} of {@link Tuple3} that wraps a consumer of the component values of the tuple.
     *
     * @param consumer The component value consumer.
     * @param <T1> The type of the first value.
     * @param <T2> The type of the second value.
     * @param <T3> The type of the third value.
     * @return The wrapper consumer
     */
    public static <T1, T2, T3> Consumer<Tuple3<T1, T2, T3>> consumer(Consumer3<T1, T2, T3> consumer) {
        return tuple -> consumer.accept(tuple.getT1(), tuple.getT2(), tuple.getT3());
    }

    /**
     * Returns a {@link Consumer} of {@link Tuple4} that wraps a consumer of the component values of the tuple.
     *
     * @param consumer The component value consumer.
     * @param <T1> The type of the first value.
     * @param <T2> The type of the second value.
     * @param <T3> The type of the third value.
     * @param <T4> The type of the fourth value.
     * @return The wrapper consumer
     */
    public static <T1, T2, T3, T4> Consumer<Tuple4<T1, T2, T3, T4>> consumer(Consumer4<T1, T2, T3, T4> consumer) {
        return tuple -> consumer.accept(tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4());
    }

    /**
     * Returns a {@link Consumer} of {@link Tuple5} that wraps a consumer of the component values of the tuple.
     *
     * @param consumer The component value consumer.
     * @param <T1> The type of the first value.
     * @param <T2> The type of the second value.
     * @param <T3> The type of the third value.
     * @param <T4> The type of the fourth value.
     * @param <T5> The type of the fifth value.
     * @return The wrapper consumer
     */
    public static <T1, T2, T3, T4, T5> Consumer<Tuple5<T1, T2, T3, T4, T5>> consumer(Consumer5<T1, T2, T3, T4, T5> consumer) {
        return tuple -> consumer.accept(tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4(), tuple.getT5());
    }

    /**
     * Returns a {@link Consumer} of {@link Tuple6} that wraps a consumer of the component values of the tuple.
     *
     * @param consumer The component value consumer.
     * @param <T1> The type of the first value.
     * @param <T2> The type of the second value.
     * @param <T3> The type of the third value.
     * @param <T4> The type of the fourth value.
     * @param <T5> The type of the fifth value.
     * @param <T6> The type of the sixth value.
     * @return The wrapper consumer
     */
    public static <T1, T2, T3, T4, T5, T6> Consumer<Tuple6<T1, T2, T3, T4, T5, T6>> consumer(Consumer6<T1, T2, T3, T4, T5, T6> consumer) {
        return tuple -> consumer.accept(tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4(), tuple.getT5(), tuple.getT6());
    }

    /**
     * Returns a {@link Consumer} of {@link Tuple7} that wraps a consumer of the component values of the tuple.
     *
     * @param consumer The component value consumer.
     * @param <T1> The type of the first value.
     * @param <T2> The type of the second value.
     * @param <T3> The type of the third value.
     * @param <T4> The type of the fourth value.
     * @param <T5> The type of the fifth value.
     * @param <T6> The type of the sixth value.
     * @param <T7> The type of the seventh value.
     * @return The wrapper consumer
     */
    public static <T1, T2, T3, T4, T5, T6, T7> Consumer<Tuple7<T1, T2, T3, T4, T5, T6, T7>> consumer(Consumer7<T1, T2, T3, T4, T5, T6, T7> consumer) {
        return tuple -> consumer.accept(tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4(), tuple.getT5(), tuple.getT6(), tuple.getT7());
    }

    /**
     * Returns a {@link Consumer} of {@link Tuple8} that wraps a consumer of the component values of the tuple.
     *
     * @param consumer The component value consumer.
     * @param <T1> The type of the first value.
     * @param <T2> The type of the second value.
     * @param <T3> The type of the third value.
     * @param <T4> The type of the fourth value.
     * @param <T5> The type of the fifth value.
     * @param <T6> The type of the sixth value.
     * @param <T7> The type of the seventh value.
     * @param <T8> The type of the eighth value.
     * @return The wrapper consumer
     */
    public static <T1, T2, T3, T4, T5, T6, T7, T8> Consumer<Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>> consumer(Consumer8<T1, T2, T3, T4, T5, T6, T7, T8> consumer) {
        return tuple -> consumer.accept(tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4(), tuple.getT5(), tuple.getT6(), tuple.getT7(), tuple.getT8());
    }

    /**
     * Returns a {@link Function} of {@link Tuple2} that wraps a function of the component values of the tuple.
     *
     * @param function The component value function.
     * @param <T1> The type of the first value.
     * @param <T2> The type of the second value.
     * @param <R> The type of the result of the function
     * @return The wrapper function
     */
    public static <T1, T2, R> Function<Tuple2<T1, T2>, R> function(BiFunction<T1, T2, R> function) {
        return tuple -> function.apply(tuple.getT1(), tuple.getT2());
    }

    /**
     * Returns a {@link Function} of {@link Tuple3} that wraps a function of the component values of the tuple.
     *
     * @param function The component value function.
     * @param <T1> The type of the first value.
     * @param <T2> The type of the second value.
     * @param <T3> The type of the third value.
     * @param <R> The type of the result of the function
     * @return The wrapper function
     */
    public static <T1, T2, T3, R> Function<Tuple3<T1, T2, T3>, R> function(Function3<T1, T2, T3, R> function) {
        return tuple -> function.apply(tuple.getT1(), tuple.getT2(), tuple.getT3());
    }

    /**
     * Returns a {@link Function} of {@link Tuple4} that wraps a function of the component values of the tuple.
     *
     * @param function The component value function.
     * @param <T1> The type of the first value.
     * @param <T2> The type of the second value.
     * @param <T3> The type of the third value.
     * @param <T4> The type of the fourth value.
     * @param <R> The type of the result of the function
     * @return The wrapper function
     */
    public static <T1, T2, T3, T4, R> Function<Tuple4<T1, T2, T3, T4>, R> function(Function4<T1, T2, T3, T4, R> function) {
        return tuple -> function.apply(tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4());
    }

    /**
     * Returns a {@link Function} of {@link Tuple5} that wraps a function of the component values of the tuple.
     *
     * @param function The component value function.
     * @param <T1> The type of the first value.
     * @param <T2> The type of the second value.
     * @param <T3> The type of the third value.
     * @param <T4> The type of the fourth value.
     * @param <T5> The type of the fifth value.
     * @param <R> The type of the result of the function
     * @return The wrapper function
     */
    public static <T1, T2, T3, T4, T5, R> Function<Tuple5<T1, T2, T3, T4, T5>, R> function(Function5<T1, T2, T3, T4, T5, R> function) {
        return tuple -> function.apply(tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4(), tuple.getT5());
    }

    /**
     * Returns a {@link Function} of {@link Tuple6} that wraps a function of the component values of the tuple.
     *
     * @param function The component value function.
     * @param <T1> The type of the first value.
     * @param <T2> The type of the second value.
     * @param <T3> The type of the third value.
     * @param <T4> The type of the fourth value.
     * @param <T5> The type of the fifth value.
     * @param <T6> The type of the sixth value.
     * @param <R> The type of the result of the function
     * @return The wrapper function
     */
    public static <T1, T2, T3, T4, T5, T6, R> Function<Tuple6<T1, T2, T3, T4, T5, T6>, R> function(Function6<T1, T2, T3, T4, T5, T6, R> function) {
        return tuple -> function.apply(tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4(), tuple.getT5(), tuple.getT6());
    }

    /**
     * Returns a {@link Function} of {@link Tuple7} that wraps a function of the component values of the tuple.
     *
     * @param function The component value function.
     * @param <T1> The type of the first value.
     * @param <T2> The type of the second value.
     * @param <T3> The type of the third value.
     * @param <T4> The type of the fourth value.
     * @param <T5> The type of the fifth value.
     * @param <T6> The type of the sixth value.
     * @param <T7> The type of the seventh value.
     * @param <R> The type of the result of the function
     * @return The wrapper function
     */
    public static <T1, T2, T3, T4, T5, T6, T7, R> Function<Tuple7<T1, T2, T3, T4, T5, T6, T7>, R> function(Function7<T1, T2, T3, T4, T5, T6, T7, R> function) {
        return tuple -> function.apply(tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4(), tuple.getT5(), tuple.getT6(), tuple.getT7());
    }

    /**
     * Returns a {@link Function} of {@link Tuple8} that wraps a function of the component values of the tuple.
     *
     * @param function The component value function.
     * @param <T1> The type of the first value.
     * @param <T2> The type of the second value.
     * @param <T3> The type of the third value.
     * @param <T4> The type of the fourth value.
     * @param <T5> The type of the fifth value.
     * @param <T6> The type of the sixth value.
     * @param <T7> The type of the seventh value.
     * @param <T8> The type of the eighth value.
     * @param <R> The type of the result of the function
     * @return The wrapper function
     */
    public static <T1, T2, T3, T4, T5, T6, T7, T8, R> Function<Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>, R> function(Function8<T1, T2, T3, T4, T5, T6, T7, T8, R> function) {
        return tuple -> function.apply(tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4(), tuple.getT5(), tuple.getT6(), tuple.getT7(), tuple.getT8());
    }

    /**
     * Returns a {@link Predicate} of {@link Tuple2} that wraps a predicate of the component values of the tuple.
     *
     * @param predicate The component value predicate.
     * @param <T1> The type of the first value.
     * @param <T2> The type of the second value.
     * @return The wrapper predicate
     */
    public static <T1, T2> Predicate<Tuple2<T1, T2>> predicate(BiPredicate<T1, T2> predicate) {
        return tuple -> predicate.test(tuple.getT1(), tuple.getT2());
    }

    /**
     * Returns a {@link Predicate} of {@link Tuple3} that wraps a predicate of the component values of the tuple.
     *
     * @param predicate The component value predicate.
     * @param <T1> The type of the first value.
     * @param <T2> The type of the second value.
     * @param <T3> The type of the third value.
     * @return The wrapper predicate
     */
    public static <T1, T2, T3> Predicate<Tuple3<T1, T2, T3>> predicate(Predicate3<T1, T2, T3> predicate) {
        return tuple -> predicate.test(tuple.getT1(), tuple.getT2(), tuple.getT3());
    }

    /**
     * Returns a {@link Predicate} of {@link Tuple4} that wraps a predicate of the component values of the tuple.
     *
     * @param predicate The component value predicate.
     * @param <T1> The type of the first value.
     * @param <T2> The type of the second value.
     * @param <T3> The type of the third value.
     * @param <T4> The type of the fourth value.
     * @return The wrapper predicate
     */
    public static <T1, T2, T3, T4> Predicate<Tuple4<T1, T2, T3, T4>> predicate(Predicate4<T1, T2, T3, T4> predicate) {
        return tuple -> predicate.test(tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4());
    }

    /**
     * Returns a {@link Predicate} of {@link Tuple5} that wraps a predicate of the component values of the tuple.
     *
     * @param predicate The component value predicate.
     * @param <T1> The type of the first value.
     * @param <T2> The type of the second value.
     * @param <T3> The type of the third value.
     * @param <T4> The type of the fourth value.
     * @param <T5> The type of the fifth value.
     * @return The wrapper predicate
     */
    public static <T1, T2, T3, T4, T5> Predicate<Tuple5<T1, T2, T3, T4, T5>> predicate(Predicate5<T1, T2, T3, T4, T5> predicate) {
        return tuple -> predicate.test(tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4(), tuple.getT5());
    }

    /**
     * Returns a {@link Predicate} of {@link Tuple6} that wraps a predicate of the component values of the tuple.
     *
     * @param predicate The component value predicate.
     * @param <T1> The type of the first value.
     * @param <T2> The type of the second value.
     * @param <T3> The type of the third value.
     * @param <T4> The type of the fourth value.
     * @param <T5> The type of the fifth value.
     * @param <T6> The type of the sixth value.
     * @return The wrapper predicate
     */
    public static <T1, T2, T3, T4, T5, T6> Predicate<Tuple6<T1, T2, T3, T4, T5, T6>> predicate(Predicate6<T1, T2, T3, T4, T5, T6> predicate) {
        return tuple -> predicate.test(tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4(), tuple.getT5(), tuple.getT6());
    }

    /**
     * Returns a {@link Predicate} of {@link Tuple7} that wraps a predicate of the component values of the tuple.
     *
     * @param predicate The component value predicate.
     * @param <T1> The type of the first value.
     * @param <T2> The type of the second value.
     * @param <T3> The type of the third value.
     * @param <T4> The type of the fourth value.
     * @param <T5> The type of the fifth value.
     * @param <T6> The type of the sixth value.
     * @param <T7> The type of the seventh value.
     * @return The wrapper predicate
     */
    public static <T1, T2, T3, T4, T5, T6, T7> Predicate<Tuple7<T1, T2, T3, T4, T5, T6, T7>> predicate(Predicate7<T1, T2, T3, T4, T5, T6, T7> predicate) {
        return tuple -> predicate.test(tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4(), tuple.getT5(), tuple.getT6(), tuple.getT7());
    }

    /**
     * Returns a {@link Predicate} of {@link Tuple8} that wraps a predicate of the component values of the tuple.
     *
     * @param predicate The component value predicate.
     * @param <T1> The type of the first value.
     * @param <T2> The type of the second value.
     * @param <T3> The type of the third value.
     * @param <T4> The type of the fourth value.
     * @param <T5> The type of the fifth value.
     * @param <T6> The type of the sixth value.
     * @param <T7> The type of the seventh value.
     * @param <T8> The type of the eighth value.
     * @return The wrapper predicate
     */
    public static <T1, T2, T3, T4, T5, T6, T7, T8> Predicate<Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>> predicate(Predicate8<T1, T2, T3, T4, T5, T6, T7, T8> predicate) {
        return tuple -> predicate.test(tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4(), tuple.getT5(), tuple.getT6(), tuple.getT7(), tuple.getT8());
    }

	@Override
	public Tuple2 apply(Object o) {
		return fromArray((Object[])o);
	}

	/**
	 * Prepare a string representation of the values suitable for a Tuple of any
	 * size by accepting an array of elements. This builds a {@link StringBuilder}
	 * containing the String representation of each object, comma separated. It manages
	 * nulls as well by putting an empty string and the comma.
	 *
	 * @param values the values of the tuple to represent
	 * @return a {@link StringBuilder} initialized with the string representation of the
	 * values in the Tuple.
	 */
	static StringBuilder tupleStringRepresentation(Object... values) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < values.length; i++) {
			Object t = values[i];
			if (i != 0) {
				sb.append(',');
			}
			if (t != null) {
				sb.append(t);
			}
		}
		return sb;
	}


	static final Tuples   empty            = new Tuples(){};

	Tuples(){}
}

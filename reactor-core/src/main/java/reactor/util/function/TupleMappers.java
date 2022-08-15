/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.util.function;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A {@literal TupleMappers} is a utility class to convert function of X arguments into {@link Function} with single
 * Tuple argument that holds X values. It's useful while transforming {@link org.reactivestreams.Publisher} of Tuple X.
 * The following example illustrates computing sum of tuple values
 * <pre>{@code
 *     Mono<Integer> sum = Mono.just(Tuples.of(1, 2, 3))
 *                      	   .map(fn((one, two, three) -> one + two + three));
 * }</pre>
 * @author Bohdan Petrenko
 */
public final class TupleMappers {

	private TupleMappers() {
	}

	/**
	 * A converting function from {@link BiFunction} to {@link Function} of {@link Tuple2} to R.
	 *
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <R> The type of the return value.
	 * @param delegate the function to delegate to
	 *
	 * @return The conversion function to R.
	 */
	public static <T1, T2, R> Function<Tuple2<T1, T2>, R> fn(BiFunction<T1, T2, R> delegate) {
		return tuple2 -> delegate.apply(tuple2.getT1(), tuple2.getT2());
	}

	/**
	 * A converting function from {@link Function3} to {@link Function} of {@link Tuple3} to R.
	 *
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <R> The type of the return value.
	 * @param delegate the function to delegate to
	 *
	 * @return The conversion function to R.
	 */
	public static <T1, T2, T3, R> Function<Tuple3<T1, T2, T3>, R> fn(Function3<T1, T2, T3, R> delegate) {
		return tuple3 -> delegate.apply(tuple3.getT1(), tuple3.getT2(), tuple3.getT3());
	}

	/**
	 * A converting function from {@link Function4} to {@link Function} of {@link Tuple4} to R.
	 *
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <R> The type of the return value.
	 * @param delegate the function to delegate to
	 *
	 * @return The conversion function to R.
	 */
	public static <T1, T2, T3, T4, R> Function<Tuple4<T1, T2, T3, T4>, R> fn(Function4<T1, T2, T3, T4, R> delegate) {
		return tuple4 -> delegate.apply(tuple4.getT1(), tuple4.getT2(), tuple4.getT3(), tuple4.getT4());
	}

	/**
	 * A converting function from {@link Function5} to {@link Function} of {@link Tuple5} to R.
	 *
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <R> The type of the return value.
	 * @param delegate the function to delegate to
	 *
	 * @return The conversion function to R.
	 */
	public static <T1, T2, T3, T4, T5, R> Function<Tuple5<T1, T2, T3, T4, T5>, R> fn(
			Function5<T1, T2, T3, T4, T5, R> delegate) {
		return tuple5 -> delegate.apply(tuple5.getT1(), tuple5.getT2(), tuple5.getT3(), tuple5.getT4(), tuple5.getT5());
	}

	/**
	 * A converting function from {@link Function6} to {@link Function} of {@link Tuple6} to R.
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
	 * @return The conversion function to R.
	 */
	public static <T1, T2, T3, T4, T5, T6, R> Function<Tuple6<T1, T2, T3, T4, T5, T6>, R> fn(
			Function6<T1, T2, T3, T4, T5, T6, R> delegate) {
		return tuple6 -> delegate.apply(tuple6.getT1(), tuple6.getT2(), tuple6.getT3(), tuple6.getT4(),
				tuple6.getT5(), tuple6.getT6());
	}

	/**
	 * A converting function from {@link Function7} to {@link Function} of {@link Tuple7} to R.
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
	 * @return The conversion function to R.
	 */
	public static <T1, T2, T3, T4, T5, T6, T7, R> Function<Tuple7<T1, T2, T3, T4, T5, T6, T7>, R> fn(
			Function7<T1, T2, T3, T4, T5, T6, T7, R> delegate) {
		return tuple7 -> delegate.apply(tuple7.getT1(), tuple7.getT2(), tuple7.getT3(), tuple7.getT4(),
				tuple7.getT5(), tuple7.getT6(), tuple7.getT7());
	}

	/**
	 * A converting function from {@link Function8} to {@link Function} of {@link Tuple8} to R.
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
	 * @return The conversion function to R.
	 */
	public static <T1, T2, T3, T4, T5, T6, T7, T8, R> Function<Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>, R> fn(
			Function8<T1, T2, T3, T4, T5, T6, T7, T8, R> delegate) {
		return tuple8 -> delegate.apply(tuple8.getT1(), tuple8.getT2(), tuple8.getT3(), tuple8.getT4(),
				tuple8.getT5(), tuple8.getT6(), tuple8.getT7(), tuple8.getT8());
	}
}

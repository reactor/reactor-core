/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A {@literal Tuple} is an immutable {@link Collection} of objects, each of which can be of an arbitrary type.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
@SuppressWarnings({"rawtypes"})
public class Tuple implements Iterable, Serializable, Function {

	static final long     serialVersionUID = 8777121294502020843L;
	static final Object[] emptyArray       = new Object[0];
	static final Tuple    empty            = new Tuple(0);


	protected final int size;

	/**
	 * Creates a new {@code Tuple} that holds the given {@code values}.
	 *
	 * @param size The number of values to hold
	 */
	protected Tuple(int size) {
		this.size = size;
	}

	/**
	 * @return An empty tuple
	 */
	public static Tuple empty() {
		return empty;
	}

	/**
	 * Create a {@link Tuple} with the given object.
	 *
	 * @param list Build an unbounded tuple
	 * @return The new {@link TupleN}.
	 */
	@SuppressWarnings("rawtypes")
	public static Tuple of(Object[] list) {
		if(list == null) return empty();
		switch (list.length){
			case 1:
				return of(list[0], null);
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
		}
		return new TupleN(list);
	}

	/**
	 * Create a {@link TupleN} with the given object.
	 *
	 * @param list Build an unbounded tuple
	 * @return The new {@link TupleN}.
	 */
	@SuppressWarnings("rawtypes")
	public static TupleN of(List<?> list) {
		return new TupleN(list.toArray());
	}

	/**
	 * Create a {@link Tuple2} with the given objects.
	 *
	 * @param t1   The first value in the tuple.
	 * @param t2   The second value in the tuple.
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @return The new {@link Tuple2}.
	 */
	public static <T1, T2> Tuple2<T1, T2> of(T1 t1, T2 t2) {
		return new Tuple2<>(2, t1, t2);
	}

	/**
	 * Create a {@link Tuple3} with the given objects.
	 *
	 * @param t1   The first value in the tuple.
	 * @param t2   The second value in the tuple.
	 * @param t3   The third value in the tuple.
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @return The new {@link Tuple3}.
	 */
	public static <T1, T2, T3> Tuple3<T1, T2, T3> of(T1 t1, T2 t2, T3 t3) {
		return new Tuple3<>(3, t1, t2, t3);
	}

	/**
	 * Create a {@link Tuple4} with the given objects.
	 *
	 * @param t1   The first value in the tuple.
	 * @param t2   The second value in the tuple.
	 * @param t3   The third value in the tuple.
	 * @param t4   The fourth value in the tuple.
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @return The new {@link Tuple4}.
	 */
	public static <T1, T2, T3, T4> Tuple4<T1, T2, T3, T4> of(T1 t1, T2 t2, T3 t3, T4 t4) {
		return new Tuple4<>(4, t1, t2, t3, t4);
	}

	/**
	 * Create a {@link Tuple5} with the given objects.
	 *
	 * @param t1   The first value in the tuple.
	 * @param t2   The second value in the tuple.
	 * @param t3   The third value in the tuple.
	 * @param t4   The fourth value in the tuple.
	 * @param t5   The fifth value in the tuple.
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @return The new {@link Tuple5}.
	 */
	public static <T1, T2, T3, T4, T5> Tuple5<T1, T2, T3, T4, T5> of(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) {
		return new Tuple5<>(5, t1, t2, t3, t4, t5);
	}

	/**
	 * Create a {@link Tuple6} with the given objects.
	 *
	 * @param t1   The first value in the tuple.
	 * @param t2   The second value in the tuple.
	 * @param t3   The third value in the tuple.
	 * @param t4   The fourth value in the tuple.
	 * @param t5   The fifth value in the tuple.
	 * @param t6   The sixth value in the tuple.
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 * @return The new {@link Tuple6}.
	 */
	public static <T1, T2, T3, T4, T5, T6> Tuple6<T1, T2, T3, T4, T5, T6> of(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6
	  t6) {
		return new Tuple6<>(6, t1, t2, t3, t4, t5, t6);
	}

	/**
	 * Create a {@link Tuple7} with the given objects.
	 *
	 * @param t1   The first value in the tuple.
	 * @param t2   The second value in the tuple.
	 * @param t3   The third value in the tuple.
	 * @param t4   The fourth value in the tuple.
	 * @param t5   The fifth value in the tuple.
	 * @param t6   The sixth value in the tuple.
	 * @param t7   The seventh value in the tuple.
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 * @param <T7> The type of the seventh value.
	 * @return The new {@link Tuple7}.
	 */
	public static <T1, T2, T3, T4, T5, T6, T7> Tuple7<T1, T2, T3, T4, T5, T6, T7> of(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5,
	                                                                                 T6 t6, T7 t7) {
		return new Tuple7<>(7, t1, t2, t3, t4, t5, t6, t7);
	}

	/**
	 * Create a {@link Tuple8} with the given objects.
	 *
	 * @param t1   The first value in the tuple.
	 * @param t2   The second value in the tuple.
	 * @param t3   The third value in the tuple.
	 * @param t4   The fourth value in the tuple.
	 * @param t5   The fifth value in the tuple.
	 * @param t6   The sixth value in the tuple.
	 * @param t7   The seventh value in the tuple.
	 * @param t8   The eighth value in the tuple.
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
	public static <T1, T2, T3, T4, T5, T6, T7, T8> Tuple8<T1, T2, T3, T4, T5, T6, T7, T8> of(T1 t1, T2 t2, T3 t3, T4
	  t4,
	                                                                                         T5 t5, T6 t6, T7 t7,
	                                                                                         T8 t8) {
		return new Tuple8<>(8, t1, t2, t3, t4, t5, t6, t7, t8);
	}

	/**
	 * A converting function from Object array to {@link TupleN}
	 *
	 * @return The unchecked conversion function to {@link TupleN}.
	 */
	@SuppressWarnings("unchecked")
	public static Function<Object[], Tuple> fnAny() {
		return (Function<Object[], Tuple>) empty;
	}

	/**
	 * A converting function from Object array to {@link TupleN} to R.
	 *
	 * @param <R> The type of the return value.
	 *
	 * @return The unchecked conversion function to R.
	 */
	public static <R> Function<Object[], R> fnAny(Function<Tuple, R> delegate) {
		return new Function<Object[], R>() {
			@Override
			public R apply(Object[] objects) {
				return delegate.apply(Tuple.fnAny()
				                           .apply(objects));
			}
		};
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
		return (Function<Object[], Tuple2<T1, T2>>) empty;
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
		return (Function<Object[], Tuple3<T1, T2, T3>>) empty;
	}

	/**
	 * A converting function from Object array to {@link Tuple3} to R.
	 *
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <R> The type of the return value.
	 *
	 * @return The unchecked conversion function to R.
	 */
	public static <T1, T2, T3, R> Function<Object[], R> fn3(Function<Tuple3<T1, T2, T3>, R> delegate) {
		return objects -> delegate.apply(Tuple.<T1, T2, T3>fn3().apply(objects));
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
		return (Function<Object[], Tuple4<T1, T2, T3, T4>>) empty;
	}

	/**
	 * A converting function from Object array to {@link Tuple4} to R.
	 *
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <R> The type of the return value.
	 *
	 * @return The unchecked conversion function to R.
	 */
	public static <T1, T2, T3, T4, R> Function<Object[], R> fn4(Function<Tuple4<T1, T2, T3, T4>, R> delegate) {
		return objects -> delegate.apply(Tuple.<T1, T2, T3, T4>fn4().apply(objects));
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
		return (Function<Object[], Tuple5<T1, T2, T3, T4, T5>>) empty;
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
	 *
	 * @return The unchecked conversion function to R.
	 */
	public static <T1, T2, T3, T4, T5, R> Function<Object[], R> fn5(Function<Tuple5<T1, T2, T3, T4, T5>, R> delegate) {
		return objects -> delegate.apply(Tuple.<T1, T2, T3, T4, T5>fn5().apply(objects));
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
		return (Function<Object[], Tuple6<T1, T2, T3, T4, T5, T6>>) empty;
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
	 *
	 * @return The unchecked conversion function to R.
	 */
	public static <T1, T2, T3, T4, T5, T6, R> Function<Object[], R> fn6(Function<Tuple6<T1, T2, T3, T4, T5, T6>, R> delegate) {
		return objects -> delegate.apply(Tuple.<T1, T2, T3, T4, T5, T6>fn6().apply(objects));
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
		return (Function<Object[], Tuple7<T1, T2, T3, T4, T5, T6, T7>>) empty;
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
	 *
	 * @return The unchecked conversion function to R.
	 */
	public static <T1, T2, T3, T4, T5, T6, T7, R> Function<Object[], R> fn7(Function<Tuple7<T1, T2, T3, T4, T5, T6, T7>, R> delegate) {
		return objects -> delegate.apply(Tuple.<T1, T2, T3, T4, T5, T6, T7>fn7().apply(objects));
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
		return (Function<Object[], Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>>) empty;
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
	 *
	 * @return The unchecked conversion function to {@link Tuple8}.
	 */
	public static <T1, T2, T3, T4, T5, T6, T7, T8, R> Function<Object[], R> fn8(Function<Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>, R> delegate) {
		return objects -> delegate.apply(Tuple.<T1, T2, T3, T4, T5, T6, T7, T8>fn8().apply(objects));
	}

	/**
	 * Create a {@link Consumer} that dereferences a tuple.
	 *
	 * @param consumer an implementation that consumes the dereferenced {@link Tuple} arguments
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @return The new {@link Consumer}.
	 */
	public static <T1, T2> Consumer<Tuple2<T1, T2>> consumer(Consumer2<T1, T2> consumer) {
		return tuple -> consumer.accept(tuple.t1, tuple.t2);
	}

	/**
	 * Create a {@link Consumer} that dereferences a tuple.
	 *
	 * @param consumer an implementation that consumes the dereferenced {@link Tuple} arguments
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @return The new {@link Consumer}.
	 */
	public static <T1, T2, T3> Consumer<Tuple3<T1, T2, T3>> consumer(Consumer3<T1, T2, T3> consumer) {
		return tuple -> consumer.accept(tuple.t1, tuple.t2, tuple.t3);
	}

	/**
	 * Create a {@link Consumer} that dereferences a tuple.
	 *
	 * @param consumer an implementation that consumes the dereferenced {@link Tuple} arguments
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @return The new {@link Consumer}.
	 */
	public static <T1, T2, T3, T4> Consumer<Tuple4<T1, T2, T3, T4>> consumer(Consumer4<T1, T2, T3, T4> consumer) {
		return tuple -> consumer.accept(tuple.t1, tuple.t2, tuple.t3, tuple.t4);
	}

	/**
	 * Create a {@link Consumer} that dereferences a tuple.
	 *
	 * @param consumer an implementation that consumes the dereferenced {@link Tuple} arguments
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @return The new {@link Consumer}.
	 */
	public static <T1, T2, T3, T4, T5> Consumer<Tuple5<T1, T2, T3, T4, T5>> consumer(Consumer5<T1, T2, T3, T4, T5> consumer) {
		return tuple -> consumer.accept(tuple.t1, tuple.t2, tuple.t3, tuple.t4, tuple.t5);
	}

	/**
	 * Create a {@link Consumer} that dereferences a tuple.
	 *
	 * @param consumer an implementation that consumes the dereferenced {@link Tuple} arguments
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 * @return The new {@link Consumer}.
	 */
	public static <T1, T2, T3, T4, T5, T6> Consumer<Tuple6<T1, T2, T3, T4, T5, T6>> consumer(Consumer6<T1, T2, T3, T4, T5, T6> consumer) {
		return tuple -> consumer.accept(tuple.t1, tuple.t2, tuple.t3, tuple.t4, tuple.t5, tuple.t6);
	}

	/**
	 * Create a {@link Consumer} that dereferences a tuple.
	 *
	 * @param consumer an implementation that consumes the dereferenced {@link Tuple} arguments
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 * @param <T7> The type of the seventh value.
	 * @return The new {@link Consumer}.
	 */
	public static <T1, T2, T3, T4, T5, T6, T7> Consumer<Tuple7<T1, T2, T3, T4, T5, T6, T7>> consumer(Consumer7<T1, T2, T3, T4, T5, T6, T7> consumer) {
		return tuple -> consumer.accept(tuple.t1, tuple.t2, tuple.t3, tuple.t4, tuple.t5, tuple.t6, tuple.t7);
	}

	/**
	 * Create a {@link Consumer} that dereferences a tuple.
	 *
	 * @param consumer an implementation that consumes the dereferenced {@link Tuple} arguments
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 * @param <T7> The type of the seventh value.
	 * @param <T8> The type of the eighth value.
	 * @return The new {@link Consumer}.
	 */
	public static <T1, T2, T3, T4, T5, T6, T7, T8> Consumer<Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>> consumer(Consumer8<T1, T2, T3, T4, T5, T6, T7, T8> consumer) {
		return tuple -> consumer.accept(tuple.t1, tuple.t2, tuple.t3, tuple.t4, tuple.t5, tuple.t6, tuple.t7, tuple.t8);
	}

	/**
	 * Create a {@link Function} that dereferences a tuple.
	 *
	 * @param function an implementation that consumes the dereferenced {@link Tuple} arguments
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <R>  The type of the result.
	 * @return The new {@link Function}.
	 */
	public static <T1, T2, R> Function<Tuple2<T1, T2>, R> function(Function2<T1, T2, R> function) {
		return tuple -> function.apply(tuple.t1, tuple.t2);
	}

	/**
	 * Create a {@link Function} that dereferences a tuple.
	 *
	 * @param function an implementation that consumes the dereferenced {@link Tuple} arguments
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <R>  The type of the result.
	 * @return The new {@link Function}.
	 */
	public static <T1, T2, T3, R> Function<Tuple3<T1, T2, T3>, R> function(Function3<T1, T2, T3, R> function) {
		return tuple -> function.apply(tuple.t1, tuple.t2, tuple.t3);
	}

	/**
	 * Create a {@link Function} that dereferences a tuple.
	 *
	 * @param function an implementation that consumes the dereferenced {@link Tuple} arguments
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <R>  The type of the result.
	 * @return The new {@link Function}.
	 */
	public static <T1, T2, T3, T4, R> Function<Tuple4<T1, T2, T3, T4>, R> function(Function4<T1, T2, T3, T4, R> function) {
		return tuple -> function.apply(tuple.t1, tuple.t2, tuple.t3, tuple.t4);
	}

	/**
	 * Create a {@link Function} that dereferences a tuple.
	 *
	 * @param function an implementation that consumes the dereferenced {@link Tuple} arguments
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <R>  The type of the result.
	 * @return The new {@link Function}.
	 */
	public static <T1, T2, T3, T4, T5, R> Function<Tuple5<T1, T2, T3, T4, T5>, R> function(Function5<T1, T2, T3, T4, T5, R> function) {
		return tuple -> function.apply(tuple.t1, tuple.t2, tuple.t3, tuple.t4, tuple.t5);
	}

	/**
	 * Create a {@link Function} that dereferences a tuple.
	 *
	 * @param function an implementation that consumes the dereferenced {@link Tuple} arguments
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 * @param <R>  The type of the result.
	 * @return The new {@link Function}.
	 */
	public static <T1, T2, T3, T4, T5, T6, R> Function<Tuple6<T1, T2, T3, T4, T5, T6>, R> function(Function6<T1, T2, T3, T4, T5, T6, R> function) {
		return tuple -> function.apply(tuple.t1, tuple.t2, tuple.t3, tuple.t4, tuple.t5, tuple.t6);
	}

	/**
	 * Create a {@link Function} that dereferences a tuple.
	 *
	 * @param function an implementation that consumes the dereferenced {@link Tuple} arguments
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 * @param <T7> The type of the seventh value.
	 * @param <R>  The type of the result.
	 * @return The new {@link Function}.
	 */
	public static <T1, T2, T3, T4, T5, T6, T7, R> Function<Tuple7<T1, T2, T3, T4, T5, T6, T7>, R> function(Function7<T1, T2, T3, T4, T5, T6, T7, R> function) {
		return tuple -> function.apply(tuple.t1, tuple.t2, tuple.t3, tuple.t4, tuple.t5, tuple.t6, tuple.t7);
	}

	/**
	 * Create a {@link Function} that dereferences a tuple.
	 *
	 * @param function an implementation that consumes the dereferenced {@link Tuple} arguments
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 * @param <T7> The type of the seventh value.
	 * @param <T8> The type of the eighth value.
	 * @param <R>  The type of the result.
	 * @return The new {@link Function}.
	 */
	public static <T1, T2, T3, T4, T5, T6, T7, T8, R> Function<Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>, R> function(Function8<T1, T2, T3, T4, T5, T6, T7, T8, R> function) {
		return tuple -> function.apply(tuple.t1, tuple.t2, tuple.t3, tuple.t4, tuple.t5, tuple.t6, tuple.t7, tuple.t8);
	}

	/**
	 * Create a {@link Predicate} that dereferences a tuple.
	 *
	 * @param predicate an implementation that consumes the dereferenced {@link Tuple} arguments
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @return The new {@link Predicate}.
	 */
	public static <T1, T2> Predicate<Tuple2<T1, T2>> predicate(Predicate2<T1, T2> predicate) {
		return tuple -> predicate.test(tuple.t1, tuple.t2);
	}

	/**
	 * Create a {@link Predicate} that dereferences a tuple.
	 *
	 * @param predicate an implementation that consumes the dereferenced {@link Tuple} arguments
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @return The new {@link Predicate}.
	 */
	public static <T1, T2, T3> Predicate<Tuple3<T1, T2, T3>> predicate(Predicate3<T1, T2, T3> predicate) {
		return tuple -> predicate.test(tuple.t1, tuple.t2, tuple.t3);
	}

	/**
	 * Create a {@link Predicate} that dereferences a tuple.
	 *
	 * @param predicate an implementation that consumes the dereferenced {@link Tuple} arguments
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @return The new {@link Predicate}.
	 */
	public static <T1, T2, T3, T4> Predicate<Tuple4<T1, T2, T3, T4>> predicate(Predicate4<T1, T2, T3, T4> predicate) {
		return tuple -> predicate.test(tuple.t1, tuple.t2, tuple.t3, tuple.t4);
	}

	/**
	 * Create a {@link Predicate} that dereferences a tuple.
	 *
	 * @param predicate an implementation that consumes the dereferenced {@link Tuple} arguments
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @return The new {@link Predicate}.
	 */
	public static <T1, T2, T3, T4, T5> Predicate<Tuple5<T1, T2, T3, T4, T5>> predicate(Predicate5<T1, T2, T3, T4, T5> predicate) {
		return tuple -> predicate.test(tuple.t1, tuple.t2, tuple.t3, tuple.t4, tuple.t5);
	}

	/**
	 * Create a {@link Predicate} that dereferences a tuple.
	 *
	 * @param predicate an implementation that consumes the dereferenced {@link Tuple} arguments
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 * @return The new {@link Predicate}.
	 */
	public static <T1, T2, T3, T4, T5, T6> Predicate<Tuple6<T1, T2, T3, T4, T5, T6>> predicate(Predicate6<T1, T2, T3, T4, T5, T6> predicate) {
		return tuple -> predicate.test(tuple.t1, tuple.t2, tuple.t3, tuple.t4, tuple.t5, tuple.t6);
	}

	/**
	 * Create a {@link Predicate} that dereferences a tuple.
	 *
	 * @param predicate an implementation that consumes the dereferenced {@link Tuple} arguments
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 * @param <T7> The type of the seventh value.
	 * @return The new {@link Predicate}.
	 */
	public static <T1, T2, T3, T4, T5, T6, T7> Predicate<Tuple7<T1, T2, T3, T4, T5, T6, T7>> predicate(Predicate7<T1, T2, T3, T4, T5, T6, T7> predicate) {
		return tuple -> predicate.test(tuple.t1, tuple.t2, tuple.t3, tuple.t4, tuple.t5, tuple.t6, tuple.t7);
	}

	/**
	 * Create a {@link Predicate} that dereferences a tuple.
	 *
	 * @param predicate an implementation that consumes the dereferenced {@link Tuple} arguments
	 * @param <T1> The type of the first value.
	 * @param <T2> The type of the second value.
	 * @param <T3> The type of the third value.
	 * @param <T4> The type of the fourth value.
	 * @param <T5> The type of the fifth value.
	 * @param <T6> The type of the sixth value.
	 * @param <T7> The type of the seventh value.
	 * @param <T8> The type of the eighth value.
	 * @return The new {@link Predicate}.
	 */
	public static <T1, T2, T3, T4, T5, T6, T7, T8> Predicate<Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>> predicate(Predicate8<T1, T2, T3, T4, T5, T6, T7, T8> predicate) {
		return tuple -> predicate.test(tuple.t1, tuple.t2, tuple.t3, tuple.t4, tuple.t5, tuple.t6, tuple.t7, tuple.t8);
	}

	/**
	 * Get the object at the given index.
	 *
	 * @param index The index of the object to retrieve. Starts at 0.
	 * @return The object. Might be {@literal null}.
	 */
	@Nullable
	public Object get(int index) {
		return null;
	}

	/**
	 * Turn this {@literal Tuple} into a plain Object array.
	 *
	 * @return A new Object array.
	 */
	public Object[] toArray() {
		return emptyArray;
	}

	/**
	 * Turn this {@literal Tuple} into a plain Object list.
	 *
	 * @return A new Object list.
	 */
	public List<Object> toList() {
		return Arrays.asList(toArray());
	}

	/**
	 * Return the number of elements in this {@literal Tuple}.
	 *
	 * @return The size of this {@literal Tuple}.
	 */
	public int size() {
		return size;
	}

	@Override
	@Nonnull
	public Iterator<?> iterator() {
		return Arrays.asList(emptyArray).iterator();
	}


	@Override
	public int hashCode() {
		return 0;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Tuple apply(Object o) {
		return of((Object[])o);
	}

	@Override
	public boolean equals(Object o) {
		if (o == null) return false;

		if (!(o instanceof Tuple)) return false;

		Tuple cast = (Tuple) o;

		return this.size == cast.size;
	}
}

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

import java.util.Objects;
import java.util.function.Function;

import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;

/**
 * A tuple that holds four non-null values
 *
 * @param <T1> The type of the first non-null value held by this tuple
 * @param <T2> The type of the second non-null value held by this tuple
 * @param <T3> The type of the third non-null value held by this tuple
 * @param <T4> The type of the fourth non-null value held by this tuple
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class Tuple4<T1, T2, T3, T4> extends Tuple3<T1, T2, T3> {

	private static final long serialVersionUID = -4898704078143033129L;

	@NonNull final T4 t4;

	Tuple4(T1 t1, T2 t2, T3 t3, T4 t4) {
		super( t1, t2, t3);
		this.t4 = Objects.requireNonNull(t4, "t4");
	}

	/**
	 * Type-safe way to get the fourth object of this {@link Tuples}.
	 *
	 * @return The fourth object
	 */
	public T4 getT4() {
		return t4;
	}

	/**
	 * Map the 1st part (T1) of this {@link Tuple4} into a different value and type,
	 * keeping the other parts.
	 *
	 * @param mapper the mapping {@link Function} for the T1 part
	 * @param <R> the new type for the T1 part
	 * @return a new {@link Tuple4} with a different T1 value
	 */
	public <R> Tuple4<R, T2, T3, T4> mapT1(Function<T1, R> mapper) {
		return new Tuple4<>(mapper.apply(t1), t2, t3, t4);
	}

	/**
	 * Map the 2nd part (T2) of this {@link Tuple4} into a different value and type,
	 * keeping the other parts.
	 *
	 * @param mapper the mapping {@link Function} for the T2 part
	 * @param <R> the new type for the T2 part
	 * @return a new {@link Tuple4} with a different T2 value
	 */
	public <R> Tuple4<T1, R, T3, T4> mapT2(Function<T2, R> mapper) {
		return new Tuple4<>(t1, mapper.apply(t2), t3, t4);
	}

	/**
	 * Map the 3rd part (T3) of this {@link Tuple4} into a different value and type,
	 * keeping the other parts.
	 *
	 * @param mapper the mapping {@link Function} for the T3 part
	 * @param <R> the new type for the T3 part
	 * @return a new {@link Tuple4} with a different T3 value
	 */
	public <R> Tuple4<T1, T2, R, T4> mapT3(Function<T3, R> mapper) {
		return new Tuple4<>(t1, t2, mapper.apply(t3), t4);
	}

	/**
	 * Map the 4th part (T4) of this {@link Tuple4} into a different value and type,
	 * keeping the other parts.
	 *
	 * @param mapper the mapping {@link Function} for the T4 part
	 * @param <R> the new type for the T4 part
	 * @return a new {@link Tuple4} with a different T4 value
	 */
	public <R> Tuple4<T1, T2, T3, R> mapT4(Function<T4, R> mapper) {
		return new Tuple4<>(t1, t2, t3, mapper.apply(t4));
	}

	@Nullable
	@Override
	public Object get(int index) {
		switch (index) {
			case 0:
				return t1;
			case 1:
				return t2;
			case 2:
				return t3;
			case 3:
				return t4;
			default:
				return null;
		}
	}

	@Override
	public Object[] toArray() {
		return new Object[]{t1, t2, t3, t4};
	}

	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o) return true;
		if (!(o instanceof Tuple4)) return false;
		if (!super.equals(o)) return false;

		@SuppressWarnings("rawtypes")
        Tuple4 tuple4 = (Tuple4) o;

		return t4.equals(tuple4.t4);

	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + t4.hashCode();
		return result;
	}

	@Override
	public int size() {
		return 4;
	}
}

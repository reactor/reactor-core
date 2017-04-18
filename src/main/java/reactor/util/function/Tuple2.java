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

package reactor.util.function;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A tuple that holds two values
 *
 * @param <T1> The type of the first value held by this tuple
 * @param <T2> The type of the second value held by this tuple
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
@SuppressWarnings("rawtypes")
public class Tuple2<T1, T2> implements Iterable<Object>, Serializable {

	/** */
    private static final long serialVersionUID = 4839927936743208499L;

	final T1 t1;
	final T2 t2;

	Tuple2(T1 t1, T2 t2) {
		this.t1 = t1;
		this.t2 = t2;
	}

	/**
	 * Type-safe way to get the fist object of this {@link Tuples}.
	 *
	 * @return The first object
	 */
	public T1 getT1() {
		return t1;
	}

	/**
	 * Type-safe way to get the second object of this {@link Tuples}.
	 *
	 * @return The second object
	 */
	public T2 getT2() {
		return t2;
	}


	/**
	 * Get the object at the given index.
	 *
	 * @param index The index of the object to retrieve. Starts at 0.
	 * @return The object. Might be {@literal null}.
	 */
	@Nullable
	public Object get(int index) {
		switch (index) {
			case 0:
				return t1;
			case 1:
				return t2;
			default:
				return null;
		}
	}

	/**
	 * Turn this {@literal Tuples} into a plain Object list.
	 *
	 * @return A new Object list.
	 */
	public List<Object> toList() {
		return Arrays.asList(toArray());
	}

	/**
	 * Turn this {@literal Tuples} into a plain Object array.
	 *
	 * @return A new Object array.
	 */
	public Object[] toArray() {
		return new Object[]{t1, t2};
	}

	@Nonnull
	@Override
	public Iterator<Object> iterator() {
		return Collections.unmodifiableList(toList()).iterator();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		Tuple2<?, ?> tuple2 = (Tuple2<?, ?>) o;

		return (t1 != null ? t1.equals(tuple2.t1) : tuple2.t1 == null) &&
				(t2 != null ? t2.equals(tuple2.t2) : tuple2.t2 == null);

	}

	@Override
	public int hashCode() {
		int result = size();
		result = 31 * result + (t1 != null ? t1.hashCode() : 0);
		result = 31 * result + (t2 != null ? t2.hashCode() : 0);
		return result;
	}

	/**
	 * Return the number of elements in this {@literal Tuples}.
	 *
	 * @return The size of this {@literal Tuples}.
	 */
	public int size() {
		return 2;
	}

	/**
	 * String representation that can be adjusted for higher-cardinality Tuples in order
	 * to show all values hold. Default to the t1 and t2 values separated by a comma.
	 */
	protected StringBuilder innerToString() {
		StringBuilder sb = new StringBuilder();
		if (t1 != null) sb.append(t1);
		sb.append(',');
		if (t2 != null) sb.append(t2);
		return sb;
	}

	/**
	 * A Tuple String representation is the comma separated list of values, enclosed
	 * in square brackets. Note that intermediate {@literal null} values are represented
	 * as the empty String, like {@code [value1,,value3]} for a Tuple3 or {@code [,value2]}
	 * for a Tuple2.
	 * @return the Tuple String representation
	 */
	@Override
	public final String toString() {
		return innerToString().insert(0, '[').append(']').toString();
	}
}

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

import java.util.Arrays;
import java.util.Iterator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import reactor.util.function.Tuple;
import reactor.util.function.Tuple3;

/**
 * A tuple that holds four values
 *
 * @param <T1> The type of the first value held by this tuple
 * @param <T2> The type of the second value held by this tuple
 * @param <T3> The type of the third value held by this tuple
 * @param <T4> The type of the fourth value held by this tuple
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class Tuple4<T1, T2, T3, T4> extends Tuple3<T1, T2, T3> {

	private static final long serialVersionUID = 8075447176142642390L;

	public final T4 t4;

	Tuple4(int size, T1 t1, T2 t2, T3 t3, T4 t4) {
		super(size, t1, t2, t3);
		this.t4 = t4;
	}

	/**
	 * Type-safe way to get the fourth object of this {@link Tuple}.
	 *
	 * @return The fourth object
	 */
	public T4 getT4() {
		return t4;
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

	@Nonnull
	@Override
	public Iterator<?> iterator() {
		return Arrays.asList(t1, t2, t3, t4).iterator();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof Tuple4)) return false;
		if (!super.equals(o)) return false;

		@SuppressWarnings("rawtypes")
        Tuple4 tuple4 = (Tuple4) o;

		return t4 != null ? t4.equals(tuple4.t4) : tuple4.t4 == null;

	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (t4 != null ? t4.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return super.toString() +
		  (t4 != null ? "," + t4.toString() : "");
	}
}

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

import java.util.Arrays;
import java.util.Iterator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A tuple that holds two values
 *
 * @param <T1> The type of the first value held by this tuple
 * @param <T2> The type of the second balue held by this tuple
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class Tuple2<T1, T2> extends Tuple1<T1> {

	private static final long serialVersionUID = -565933838909569191L;

	public final T2 t2;

	Tuple2(int size, T1 t1, T2 t2) {
		super(size, t1);
		this.t2 = t2;
	}

	/**
	 * Type-safe way to get the second object of this {@link Tuple}.
	 *
	 * @return The second object
	 */
	public T2 getT2() {
		return t2;
	}

	@Nullable
	@Override
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

	@Override
	public Object[] toArray() {
		return new Object[]{t1, t2};
	}

	@Nonnull
	@Override
	public Iterator<?> iterator() {
		return Arrays.asList(t1, t2).iterator();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof Tuple2)) return false;
		if (!super.equals(o)) return false;

		Tuple2 tuple2 = (Tuple2) o;

		return t2 != null ? t2.equals(tuple2.t2) : tuple2.t2 == null;

	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (t2 != null ? t2.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return super.toString() +
		  (t2 != null ? "," + t2.toString() : "");
	}
}

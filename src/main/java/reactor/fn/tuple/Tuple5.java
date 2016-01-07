/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.fn.tuple;

import java.util.Arrays;
import java.util.Iterator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A tuple that holds five values
 *
 * @param <T1> The type of the first value held by this tuple
 * @param <T2> The type of the second value held by this tuple
 * @param <T3> The type of the third value held by this tuple
 * @param <T4> The type of the fourth value held by this tuple
 * @param <T5> The type of the fifth value held by this tuple
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class Tuple5<T1, T2, T3, T4, T5> extends Tuple4<T1, T2, T3, T4> {

	private static final long serialVersionUID = -5866370282498275773L;

	public final T5 t5;

	Tuple5(int size, T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) {
		super(size, t1, t2, t3, t4);
		this.t5 = t5;
	}

	/**
	 * Type-safe way to get the fifth object of this {@link Tuple}.
	 *
	 * @return The fifth object
	 */
	public T5 getT5() {
		return t5;
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
			case 4:
				return t5;
			default:
				return null;
		}
	}

	@Override
	public Object[] toArray() {
		return new Object[]{t1, t2, t3, t4, t5};
	}

	@Nonnull
	@Override
	public Iterator<?> iterator() {
		return Arrays.asList(t1, t2, t3, t4, t5).iterator();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof Tuple5)) return false;
		if (!super.equals(o)) return false;

		Tuple5 tuple5 = (Tuple5) o;

		return t5 != null ? t5.equals(tuple5.t5) : tuple5.t5 == null;

	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (t5 != null ? t5.hashCode() : 0);
		return result;
	}


	@Override
	public String toString() {
		return super.toString() +
		  (t5 != null ? "," + t5.toString() : "");
	}
}

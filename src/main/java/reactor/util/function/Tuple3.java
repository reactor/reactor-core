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

/**
 * A tuple that holds three values
 *
 * @param <T1> The type of the first value held by this tuple
 * @param <T2> The type of the second value held by this tuple
 * @param <T3> The type of the third value held by this tuple
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class Tuple3<T1, T2, T3> extends Tuple2<T1, T2> {

	private static final long serialVersionUID = 6315773492205460562L;

	final T3 t3;

	Tuple3(T1 t1, T2 t2, T3 t3) {
		super(t1, t2);
		this.t3 = t3;
	}

	/**
	 * Type-safe way to get the third object of this {@link Tuples}.
	 *
	 * @return The third object
	 */
	public T3 getT3() {
		return t3;
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
			default:
				return null;
		}
	}

	@Override
	public Object[] toArray() {
		return new Object[]{t1, t2, t3};
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof Tuple3)) return false;
		if (!super.equals(o)) return false;

		@SuppressWarnings("rawtypes")
        Tuple3 tuple3 = (Tuple3) o;

		return t3 != null ? t3.equals(tuple3.t3) : tuple3.t3 == null;
	}

	@Override
	public int size() {
		return 3;
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (t3 != null ? t3.hashCode() : 0);
		return result;
	}

	@Override
	public StringBuilder innerToString() {
		StringBuilder sb = super.innerToString().append(',');
		if (t3 != null) sb.append(t3);
		return sb;
	}
}

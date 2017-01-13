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
 * A tuple that holds seven values
 *
 * @param <T1> The type of the first value held by this tuple
 * @param <T2> The type of the second value held by this tuple
 * @param <T3> The type of the third value held by this tuple
 * @param <T4> The type of the fourth value held by this tuple
 * @param <T5> The type of the fifth value held by this tuple
 * @param <T6> The type of the sixth value held by this tuple
 * @param <T7> The type of the seventh value held by this tuple
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class Tuple7<T1, T2, T3, T4, T5, T6, T7> extends Tuple6<T1, T2, T3, T4, T5, T6> {

	private static final long serialVersionUID = 8273600047065201704L;

	final T7 t7;

	Tuple7(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6, T7 t7) {
		super( t1, t2, t3, t4, t5, t6);
		this.t7 = t7;
	}

	/**
	 * Type-safe way to get the seventh object of this {@link Tuples}.
	 *
	 * @return The seventh object
	 */
	public T7 getT7() {
		return t7;
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
			case 5:
				return t6;
			case 6:
				return t7;
			default:
				return null;
		}
	}

	@Override
	public Object[] toArray() {
		return new Object[]{t1, t2, t3, t4, t5, t6, t7};
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof Tuple7)) return false;
		if (!super.equals(o)) return false;

		@SuppressWarnings("rawtypes")
        Tuple7 tuple7 = (Tuple7) o;

		return t7 != null ? t7.equals(tuple7.t7) : tuple7.t7 == null;

	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (t7 != null ? t7.hashCode() : 0);
		return result;
	}

	@Override
	public int size() {
		return 7;
	}

	@Override
	public StringBuilder innerToString() {
		StringBuilder sb = super.innerToString().append(',');
		if (t7 != null) sb.append(t7);
		return sb;
	}
}

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

package reactor.fn.tuple;

import java.util.Arrays;
import java.util.Iterator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A tuple that holds a single value
 *
 * @param <T1> The type held by this tuple
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class Tuple1<T1> extends Tuple {

	private static final long serialVersionUID = -1467756857377152573L;

	public final T1 t1;

	Tuple1(int size, T1 t1) {
		super(size);
		this.t1 = t1;
	}

	/**
	 * Type-safe way to get the first object of this {@link Tuple}.
	 *
	 * @return The first object
	 */
	public T1 getT1() {
		return t1;
	}

	@Nullable
	@Override
	public Object get(int index) {
		return index == 0 ? t1 : null;
	}

	@Override
	public Object[] toArray() {
		return new Object[]{t1};
	}

	@Nonnull
	@Override
	public Iterator<?> iterator() {
		return Arrays.asList(t1).iterator();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof Tuple1)) return false;
		if (!super.equals(o)) return false;

		Tuple1 tuple1 = (Tuple1) o;

		return t1 != null ? t1.equals(tuple1.t1) : tuple1.t1 == null;

	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (t1 != null ? t1.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return t1 != null ? t1.toString() : "";
	}
}

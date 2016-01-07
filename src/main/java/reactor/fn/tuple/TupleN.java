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
 * A tuple that holds 9 or more values
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class TupleN extends Tuple8 {

	private static final long serialVersionUID = 666954435584703227L;

	public final Object[] entries;

	@SuppressWarnings("unchecked")
	TupleN(Object... values) {
		super(values.length, null, null, null, null, null, null, null, null);
		this.entries = Arrays.copyOf(values, values.length);
	}

	@Nullable
	@Override
	public Object get(int index) {
		return (size > 0 && size > index ? entries[index] : null);
	}

	@Override
	public Object[] toArray() {
		return entries;
	}

	@Override
	public Object getT8() {
		return get(7);
	}

	@Override
	public Object getT7() {
		return get(6);
	}

	@Override
	public Object getT6() {
		return get(5);
	}

	@Override
	public Object getT5() {
		return get(4);
	}

	@Override
	public Object getT4() {
		return get(3);
	}

	@Override
	public Object getT3() {
		return get(2);
	}

	@Override
	public Object getT2() {
		return get(1);
	}

	@Override
	public Object getT1() {
		return get(0);
	}

	@Nonnull
	@Override
	public Iterator<?> iterator() {
		return Arrays.asList(entries).iterator();
	}

	@Override
	public int hashCode() {
		if (this.size == 0) {
			return 0;
		} else if (this.size == 1) {
			return this.entries[0] == null ? 0 : this.entries[0].hashCode();
		} else {
			int hashCode = 1;
			for (Object entry : this.entries) {
				hashCode = hashCode ^ (entry == null ? 0 : entry.hashCode());
			}
			return hashCode;
		}
	}

	@Override
	public boolean equals(Object o) {
		if (o == null) return false;

		if (!(o instanceof TupleN)) return false;

		TupleN cast = (TupleN) o;

		if (this.size != cast.size) return false;

		for (int i = 0; i < this.size; i++) {
			if (null != this.entries[i] && !this.entries[i].equals(cast.entries[i])) {
				return false;
			}
		}

		return true;
	}

	@Override
	public String toString() {
		String formatted = "";
		for (int i = 0; i < size; i++) {
			formatted += entries[i] + ",";
		}

		return formatted.substring(0, formatted.length() - 1);
	}
}

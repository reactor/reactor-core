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

	private final Object[] entries;

	@SuppressWarnings("unchecked")
	TupleN(Object... values) {
		super(values.length, toT(0, values), toT(1, values), toT(2, values), toT(3, values), toT(4, values), toT(5,
				values), toT(6, values), toT(7, values));
		if(values.length > 8) {
			this.entries = new Object[values.length - 8];
			System.arraycopy(values, 8, entries, 0, entries.length);
		}
		else{
			this.entries = emptyArray;
		}
	}

	static Object toT(int index, Object[] array){
		return array.length > index ? array[index] : null;
	}

	@Nullable
	@Override
	public Object get(int index) {
		return (index > 8 && size > 8 ? entries[index - 8] : super.get(index));
	}

	@Override
	public Object[] toArray() {
		Object[] result = new Object[8+entries.length];
		System.arraycopy(entries, 0, result, 8, entries.length);
		result[0] = t1;
		result[1] = t2;
		result[2] = t3;
		result[3] = t4;
		result[4] = t5;
		result[5] = t6;
		result[6] = t7;
		result[7] = t8;
		return result;
	}

	@Nonnull
	@Override
	public Iterator<?> iterator() {
		return Arrays.asList(toArray()).iterator();
	}

	@Override
	public int hashCode() {
		if (this.size < 9) {
			return super.hashCode();
		} else {
			int hashCode = super.hashCode();
			for (Object entry : this.entries) {
				hashCode = hashCode ^ (entry == null ? 0 : entry.hashCode());
			}
			return hashCode;
		}
	}

	@Override
	public boolean equals(Object o) {
		if(size < 9){
			return super.equals(o);
		}

		if (o == null) return false;

		if (!(o instanceof TupleN)) return false;

		TupleN cast = (TupleN) o;

		if (this.entries.length != cast.entries.length) return false;

		for (int i = 0; i < entries.length; i++) {
			if (null != this.entries[i] && !this.entries[i].equals(cast.entries[i])) {
				return false;
			}
		}

		return true;
	}

	@Override
	public String toString() {
		String formatted = super.toString();
		for (int i = 0; i < entries.length; i++) {
			formatted += entries[i] + ",";
		}

		return formatted.substring(0, formatted.length() - 1);
	}
}

/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.util.context;

import java.util.AbstractMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Stream;

final class Context4 implements CoreContext {

	/**
	 * Checks for duplicate keys and null keys. This method is intended for a short space of keys in the
	 * 4-10 range. Shorter number of keys can easily be checked with direct equals comparison(s),
	 * saving on allocating a vararg array (although this method would still behave correctly).
	 *
	 * @param keys the keys to check for duplicates and nulls, by looping over the combinations
	 * @throws NullPointerException if any of the keys is null
	 * @throws IllegalArgumentException on the first key encountered twice
	 */
	static void checkKeys(Object... keys) {
		int size = keys.length;
		//NB: there is no sense in looking for duplicates when size < 2, but the loop below skips these cases anyway
		for (int i = 0; i < size - 1; i++) {
			Object key = Objects.requireNonNull(keys[i], "key" + (i+1));
			for (int j = i + 1; j < size; j++) {
				Object otherKey = keys[j];
				if (key.equals(otherKey)) {
					throw new IllegalArgumentException("Key #" + (i+1) + " (" + key + ") is duplicated");
				}
			}
		}
		//at the end of the loops, only the last key hasn't been checked for null
		if (size != 0) {
			Objects.requireNonNull(keys[size - 1], "key" + size);
		}
	}

	final Object key1;
	final Object value1;
	final Object key2;
	final Object value2;
	final Object key3;
	final Object value3;
	final Object key4;
	final Object value4;

	Context4(Object key1, Object value1,
			Object key2, Object value2,
			Object key3, Object value3,
			Object key4, Object value4) {
		//TODO merge null check and duplicate check in the util method
		Context4.checkKeys(key1, key2, key3, key4);
		this.key1 = Objects.requireNonNull(key1, "key1");
		this.value1 = Objects.requireNonNull(value1, "value1");
		this.key2 = Objects.requireNonNull(key2, "key2");
		this.value2 = Objects.requireNonNull(value2, "value2");
		this.key3 = Objects.requireNonNull(key3, "key3");
		this.value3 = Objects.requireNonNull(value3, "value3");
		this.key4 = Objects.requireNonNull(key4, "key4");
		this.value4 = Objects.requireNonNull(value4, "value4");
	}

	@Override
	public Context put(Object key, Object value) {
		Objects.requireNonNull(key, "key");
		Objects.requireNonNull(value, "value");

		if(this.key1.equals(key)){
			return new Context4(key, value, key2, value2, key3, value3, key4, value4);
		}

		if (this.key2.equals(key)) {
			return new Context4(key1, value1, key, value, key3, value3, key4, value4);
		}

		if (this.key3.equals(key)) {
			return new Context4(key1, value1, key2, value2, key, value, key4, value4);
		}

		if (this.key4.equals(key)) {
			return new Context4(key1, value1, key2, value2, key3, value3, key, value);
		}

		return new Context5(this.key1, this.value1, this.key2, this.value2, this.key3, this.value3,
				this.key4, this.value4, key, value);
	}

	@Override
	public Context delete(Object key) {
		Objects.requireNonNull(key, "key");

		if(this.key1.equals(key)){
			return new Context3(key2, value2, key3, value3, key4, value4);
		}

		if (this.key2.equals(key)) {
			return new Context3(key1, value1, key3, value3, key4, value4);
		}

		if (this.key3.equals(key)) {
			return new Context3(key1, value1, key2, value2, key4, value4);
		}

		if (this.key4.equals(key)) {
			return new Context3(key1, value1, key2, value2, key3, value3);
		}

		return this;
	}

	@Override
	public boolean hasKey(Object key) {
		return this.key1.equals(key) || this.key2.equals(key) || this.key3.equals(key) || this.key4.equals(key);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T get(Object key) {
		if (this.key1.equals(key)) {
			return (T)this.value1;
		}
		if (this.key2.equals(key)) {
			return (T)this.value2;
		}
		if (this.key3.equals(key)) {
			return (T)this.value3;
		}
		if (this.key4.equals(key)) {
			return (T)this.value4;
		}
		throw new NoSuchElementException("Context does not contain key: "+key);
	}

	@Override
	public int size() {
		return 4;
	}

	@Override
	public Stream<Map.Entry<Object, Object>> stream() {
		return Stream.of(
				new AbstractMap.SimpleImmutableEntry<>(key1, value1),
				new AbstractMap.SimpleImmutableEntry<>(key2, value2),
				new AbstractMap.SimpleImmutableEntry<>(key3, value3),
				new AbstractMap.SimpleImmutableEntry<>(key4, value4));
	}

	@Override
	public Context putAllInto(Context base) {
		return base
				.put(this.key1, this.value1)
				.put(this.key2, this.value2)
				.put(this.key3, this.value3)
				.put(this.key4, this.value4);
	}

	@Override
	public void unsafePutAllInto(ContextN other) {
		other.accept(key1, value1);
		other.accept(key2, value2);
		other.accept(key3, value3);
		other.accept(key4, value4);
	}

	@Override
	public String toString() {
		return "Context4{" + key1 + '='+ value1 + ", " + key2 + '=' + value2 + ", " +
				key3 + '=' + value3 + ", " + key4 + '=' + value4 + '}';
	}
}

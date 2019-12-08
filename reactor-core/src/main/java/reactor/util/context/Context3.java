/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
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

final class Context3 implements CoreContext {

	final Object key1;
	final Object value1;
	final Object key2;
	final Object value2;
	final Object key3;
	final Object value3;

	Context3(Object key1, Object value1,
			Object key2, Object value2,
			Object key3, Object value3) {
		if (Objects.requireNonNull(key1, "key1").equals(key2) || key1.equals(key3)) {
			throw new IllegalArgumentException("Key #1 (" + key1 + ") is duplicated");
		}
		else if (Objects.requireNonNull(key2, "key2").equals(key3)) {
			throw new IllegalArgumentException("Key #2 (" + key2 + ") is duplicated");
		}
		this.key1 = key1; //already checked for null above
		this.value1 = Objects.requireNonNull(value1, "value1");
		this.key2 = key2; //already checked for null above
		this.value2 = Objects.requireNonNull(value2, "value2");
		this.key3 = Objects.requireNonNull(key3, "key3");
		this.value3 = Objects.requireNonNull(value3, "value3");
	}

	@Override
	public Context put(Object key, Object value) {
		Objects.requireNonNull(key, "key");
		Objects.requireNonNull(value, "value");

		if(this.key1.equals(key)){
			return new Context3(key, value, key2, value2, key3, value3);
		}

		if (this.key2.equals(key)) {
			return new Context3(key1, value1, key, value, key3, value3);
		}

		if (this.key3.equals(key)) {
			return new Context3(key1, value1, key2, value2, key, value);
		}

		return new Context4(this.key1, this.value1, this.key2, this.value2, this.key3, this.value3, key, value);
	}

	@Override
	public Context delete(Object key) {
		Objects.requireNonNull(key, "key");

		if(this.key1.equals(key)){
			return new Context2(key2, value2, key3, value3);
		}

		if (this.key2.equals(key)) {
			return new Context2(key1, value1, key3, value3);
		}

		if (this.key3.equals(key)) {
			return new Context2(key1, value1, key2, value2);
		}

		return this;
	}

	@Override
	public boolean hasKey(Object key) {
		return this.key1.equals(key) || this.key2.equals(key) || this.key3.equals(key);
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
		throw new NoSuchElementException("Context does not contain key: "+key);
	}

	@Override
	public int size() {
		return 3;
	}

	@Override
	public Stream<Map.Entry<Object, Object>> stream() {
		return Stream.of(
				new AbstractMap.SimpleImmutableEntry<>(key1, value1),
				new AbstractMap.SimpleImmutableEntry<>(key2, value2),
				new AbstractMap.SimpleImmutableEntry<>(key3, value3));
	}

	@Override
	public Context putAllInto(Context base) {
		return base
				.put(this.key1, this.value1)
				.put(this.key2, this.value2)
				.put(this.key3, this.value3);
	}

	@Override
	public void unsafePutAllInto(ContextN other) {
		other.accept(key1, value1);
		other.accept(key2, value2);
		other.accept(key3, value3);
	}

	@Override
	public String toString() {
		return "Context3{" + key1 + '='+ value1 + ", " + key2 + '=' + value2 + ", " + key3 + '=' + value3 + '}';
	}
}

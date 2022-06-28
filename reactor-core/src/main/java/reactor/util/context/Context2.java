/*
 * Copyright (c) 2015-2022 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.function.BiConsumer;
import java.util.stream.Stream;

final class Context2 implements CoreContext {

	final Object key1;
	final Object value1;
	final Object key2;
	final Object value2;

	Context2(Object key1, Object value1, Object key2, Object value2) {
		if (Objects.requireNonNull(key1, "key1").equals(key2)) {
			throw new IllegalArgumentException("Key #1 (" + key1 + ") is duplicated");
		}
		this.key1 = key1; //checked for nulls above
		this.value1 = Objects.requireNonNull(value1, "value1");
		this.key2 = Objects.requireNonNull(key2, "key2");
		this.value2 = Objects.requireNonNull(value2, "value2");
	}

	@Override
	public Context put(Object key, Object value) {
		Objects.requireNonNull(key, "key");
		Objects.requireNonNull(value, "value");

		if(this.key1.equals(key)){
			return new Context2(key, value, key2, value2);
		}

		if (this.key2.equals(key)) {
			return new Context2(key1, value1, key, value);
		}

		return new Context3(this.key1, this.value1, this.key2, this.value2, key, value);
	}

	@Override
	public Context delete(Object key) {
		Objects.requireNonNull(key, "key");

		if(this.key1.equals(key)){
			return new Context1(key2, value2);
		}

		if (this.key2.equals(key)) {
			return new Context1(key1, value1);
		}

		return this;
	}

	@Override
	public boolean hasKey(Object key) {
		return this.key1.equals(key) || this.key2.equals(key);
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
		throw new NoSuchElementException("Context does not contain key: "+key);
	}

	@Override
	public int size() {
		return 2;
	}

	@Override
	public Stream<Map.Entry<Object, Object>> stream() {
		return Stream.of(
				new AbstractMap.SimpleImmutableEntry<>(key1, value1),
				new AbstractMap.SimpleImmutableEntry<>(key2, value2));
	}

	@Override
	public void forEach(BiConsumer<Object, Object> action) {
		action.accept(key1, value1);
		action.accept(key2, value2);
	}

	@Override
	public Context putAllInto(Context base) {
		return base
				.put(this.key1, this.value1)
				.put(this.key2, this.value2);
	}

	@Override
	public void unsafePutAllInto(ContextN other) {
		other.accept(key1, value1);
		other.accept(key2, value2);
	}

	@Override
	public String toString() {
		return "Context2{" + key1 + '='+ value1 + ", " + key2 + '=' + value2 + '}';
	}
}

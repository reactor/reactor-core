/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.util.context;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import reactor.util.annotation.Nullable;

@SuppressWarnings("unchecked")
final class ContextN extends HashMap<Object, Object>
		implements Context, Function<Entry<Object, Object>, Entry<Object, Object>>,
		           BiConsumer<Object, Object> {

	ContextN(Object key1, Object value1, Object key2, Object value2,
			Object key3, Object value3, Object key4, Object value4,
			Object key5, Object value5, Object key6, Object value6) {
		super(6, 1f);
		//accept below stands in for "inner put"
		accept(key1, value1);
		accept(key2, value2);
		accept(key3, value3);
		accept(key4, value4);
		accept(key5, value5);
		accept(key6, value6);
	}

	ContextN(Map<Object, Object> map, Object key, Object value) {
		super(Objects.requireNonNull(map, "map").size() + 1, 1f);
		map.forEach(this);
		accept(key, value); //innerPut
	}

	ContextN(Map<Object, Object> sourceMap, Map<?, ?> other) {
		super(Objects.requireNonNull(sourceMap, "sourceMap").size()
						+ Objects.requireNonNull(other, "other").size(),
				1f);
		sourceMap.forEach(this);
		other.forEach(this);
	}

	//this performs an inner put to the actual hashmap, and also allows passing `this` directly to
	//Map#forEach
	@Override
	public void accept(Object key, Object value) {
		super.put(Objects.requireNonNull(key, "key"),
				Objects.requireNonNull(value, "value"));
	}

	@Override
	public Context put(Object key, Object value) {
		Objects.requireNonNull(key, "key");
		Objects.requireNonNull(key, "value");
		return new ContextN(this, key, value);
	}

	@Override
	public Context delete(Object key) {
		Objects.requireNonNull(key, "key");
		if (!hasKey(key)) {
			return this;
		}

		int s = size() - 1;
		if (s == 5) {
			Entry<Object, Object>[] arr = new Entry[s];
			int idx = 0;
			for (Entry<Object, Object> entry : entrySet()) {
				if (!entry.getKey().equals(key)) {
					arr[idx] = entry;
					idx++;
				}
			}
			return new Context5(
					arr[0].getKey(), arr[0].getValue(),
					arr[1].getKey(), arr[1].getValue(),
					arr[2].getKey(), arr[2].getValue(),
					arr[3].getKey(), arr[3].getValue(),
					arr[4].getKey(), arr[4].getValue());
		}

		ContextN newInstance = new ContextN(this, Collections.emptyMap());
		newInstance.remove(key);
		return newInstance;
	}

	@Override
	public boolean hasKey(Object key) {
		return super.containsKey(key);
	}

	@Override
	public Object get(Object key) {
		Object o = super.get(key);
		if (o != null) {
			return o;
		}
		throw new NoSuchElementException("Context does not contain key: "+key);
	}

	@Override
	@Nullable
	public Object getOrDefault(Object key, @Nullable Object defaultValue) {
		Object o = super.get(key);
		if (o != null) {
			return o;
		}
		return defaultValue;
	}

	@Override
	public Stream<Entry<Object, Object>> stream() {
		return entrySet().stream().map(this);
	}

	@Override
	public Entry<Object, Object> apply(Entry<Object, Object> o) {
		return new Context1(o.getKey(), o.getValue());
	}

	@Override
	public Context putAll(Context other) {
		if (other.isEmpty()) return this;
		if (other instanceof ContextN) return new ContextN(this, ((ContextN) other));

		Map<?, ?> mapOther = other.stream()
		                          .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
		return new ContextN(this, mapOther);
	}

	@Override
	public String toString() {
		return "ContextN"+super.toString();
	}
}

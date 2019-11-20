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

import java.util.AbstractMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import reactor.util.annotation.Nullable;

final class ContextN extends AbstractContext {

	final LinkedHashMap<Object, Object> delegate;

	ContextN(Object key1, Object value1, Object key2, Object value2,
			Object key3, Object value3, Object key4, Object value4,
			Object key5, Object value5, Object key6, Object value6) {
		delegate = new LinkedHashMap<>(6, 1f);
		delegate.put(Objects.requireNonNull(key1, "key1"),
				Objects.requireNonNull(value1, "value1"));
		delegate.put(Objects.requireNonNull(key2, "key2"),
				Objects.requireNonNull(value2, "value2"));
		delegate.put(Objects.requireNonNull(key3, "key3"),
				Objects.requireNonNull(value3, "value3"));
		delegate.put(Objects.requireNonNull(key4, "key4"),
				Objects.requireNonNull(value4, "value4"));
		delegate.put(Objects.requireNonNull(key5, "key5"),
				Objects.requireNonNull(value5, "value5"));
		delegate.put(Objects.requireNonNull(key6, "key6"),
				Objects.requireNonNull(value6, "value6"));
	}

	ContextN(Map<Object, Object> map, Object key, Object value) {
		delegate = new LinkedHashMap<>(Objects.requireNonNull(map, "map").size() + 1, 1f);

		map.forEach((k, v) -> delegate.put(Objects.requireNonNull(k, "map contains null key"),
				Objects.requireNonNull(v, "map contains null value")));

		delegate.put(Objects.requireNonNull(key, "key"),
				Objects.requireNonNull(value, "value"));
	}

	ContextN(Map<Object, Object> sourceMap, Map<?, ?> other) {
		delegate = new LinkedHashMap<>(Objects.requireNonNull(sourceMap, "sourceMap").size()
						+ Objects.requireNonNull(other, "other").size(),
				1f);

		sourceMap.forEach((k, v) -> delegate.put(Objects.requireNonNull(k, "sourceMap contains null key"),
				Objects.requireNonNull(v, "sourceMap contains null value")));

		other.forEach((k, v) -> delegate.put(Objects.requireNonNull(k, "other map contains null key"),
				Objects.requireNonNull(v, "other map contains null value")));
	}

	@Override
	public boolean hasKey(Object key) {
		return delegate.containsKey(key);
	}

	@Override
	public AbstractContext put(Object key, Object value) {
		Objects.requireNonNull(key, "key");
		Objects.requireNonNull(key, "value");
		return new ContextN(this.delegate, key, value);
	}

	@Override
	public Context delete(Object key) {
		Objects.requireNonNull(key, "key");
		if (!hasKey(key)) {
			return this;
		}

		int s = size() - 1;
		if (s == 5) {
			@SuppressWarnings("unchecked")
			Entry<Object, Object>[] arr = new Entry[s];
			int idx = 0;
			for (Entry<Object, Object> entry : delegate.entrySet()) {
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

		ContextN newInstance = new ContextN(this.delegate, Collections.emptyMap());
		newInstance.delegate.remove(key);
		return newInstance;
	}

	@Override
	public int size() {
		return delegate.size();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Object get(Object key) {
		Object o = delegate.get(key);
		if (o != null) {
			return o;
		}
		throw new NoSuchElementException("Context does not contain key: "+key);
	}

	@SuppressWarnings("unchecked")
	@Override
	@Nullable
	public Object getOrDefault(Object key, @Nullable Object defaultValue) {
		Object o = delegate.get(key);
		if (o != null) {
			return o;
		}
		return defaultValue;
	}

	@Override
	public Stream<Entry<Object, Object>> stream() {
		return delegate.entrySet().stream().map(AbstractMap.SimpleImmutableEntry::new);
	}

@Override
protected AbstractContext putAllSelfInto(AbstractContext initial) {
	if (initial instanceof ContextN) return new ContextN(((ContextN) initial).delegate, this.delegate);

	AbstractContext[] holder = new AbstractContext[]{ initial };
	delegate.forEach((k, v) -> holder[0] = holder[0].put(k, v));
	return holder[0];
}

	@Override
	public Context putAll(Context other) {
		if (other.isEmpty()) return this;
		if (other instanceof ContextN) return new ContextN(this.delegate, ((ContextN) other).delegate);

		if (other instanceof AbstractContext) {
			AbstractContext abstractContext = (AbstractContext) other;
			return abstractContext.putAllSelfInto(this);
		}

		//slightly less wasteful implementation for non-core context: only collect the other since we already have a map for this
		return new ContextN(this.delegate,
				other.stream()
				     .collect(Collectors.toMap(
						     Map.Entry::getKey,
						     Map.Entry::getValue,
						     (value1, value2) -> value2, //key collisions shouldn't occur
						     LinkedHashMap::new)));
	}

	@Override
	public String toString() {
		return "ContextN"+delegate.toString();
	}
}

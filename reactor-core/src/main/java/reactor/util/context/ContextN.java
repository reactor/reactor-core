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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import reactor.util.annotation.Nullable;

@SuppressWarnings("unchecked")
final class ContextN extends LinkedHashMap<Object, Object>
		implements CoreContext, BiConsumer<Object, Object>, Consumer<Entry<Object, Object>> {

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

	/**
	 * Creates a new {@link ContextN} with values from the provided {@link Map}
	 *
	 * @param originalToCopy a {@link Map} to populate entries from. MUST NOT contain null keys/values
	 */
	ContextN(Map<Object, Object> originalToCopy) {
		super(Objects.requireNonNull(originalToCopy, "originalToCopy"));
	}

	ContextN(int initialCapacity) {
		super(initialCapacity, 1.0f);
	}

	//this performs an inner put to the actual map, and also allows passing `this` directly to
	//Map#forEach
	@Override
	public void accept(Object key, Object value) {
		super.put(Objects.requireNonNull(key, "key"),
				Objects.requireNonNull(value, "value"));
	}

	//this performs an inner put of the entry to the actual map
	@Override
	public void accept(Entry<Object, Object> entry) {
		accept(entry.getKey(), entry.getValue());
	}

	/**
	 * Note that this method overrides {@link LinkedHashMap#put(Object, Object)}.
	 * Consider using {@link #accept(Object, Object)} instead for putting items into the map.
	 *
	 * @param key the key to add/update in the new {@link Context}
	 * @param value the value to associate to the key in the new {@link Context}
	 */
	@Override
	public Context put(Object key, Object value) {
		ContextN newContext = new ContextN(this);
		newContext.accept(key, value);
		return newContext;
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

		ContextN newInstance = new ContextN(this);
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
		return entrySet().stream().map(AbstractMap.SimpleImmutableEntry::new);
	}

	@Override
	public Context putAllInto(Context base) {
		if (base instanceof ContextN) {
			ContextN newContext = new ContextN(base.size() + this.size());
			newContext.putAll((Map<Object, Object>) base);
			newContext.putAll((Map<Object, Object>) this);
			return newContext;
		}

		Context[] holder = new Context[]{base};
		forEach((k, v) -> holder[0] = holder[0].put(k, v));
		return holder[0];
	}

	@Override
	public void unsafePutAllInto(ContextN other) {
		other.putAll((Map<Object, Object>) this);
	}

	@Override
	public Context putAll(Context other) {
		if (other.isEmpty()) return this;

		// slightly less wasteful implementation for non-core context:
		// only collect the other since we already have a map for this.
		ContextN newContext = new ContextN(this);
		if (other instanceof CoreContext) {
			CoreContext coreContext = (CoreContext) other;
			coreContext.unsafePutAllInto(newContext);
		}
		else {
			// avoid Collector to reduce the allocations
			other.stream().sequential().forEach(newContext);
		}

		return newContext;
	}

	@Override
	public String toString() {
		return "ContextN" + super.toString();
	}
}

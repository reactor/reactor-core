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

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import reactor.util.annotation.Nullable;

/**
 * A key/value store that is propagated between components such as operators via the
 *  context protocol. Contexts are ideal to transport orthogonal information such as
 *  tracing or security tokens.
 * <p>
 * {@link Context} implementations are thread-safe and immutable: mutative operations like
 * {@link #put(Object, Object)} will in fact return a new {@link Context} instance.
 * <p>
 * Note that contexts are optimized for low cardinality key/value storage, and a user
 * might want to associate a dedicated mutable structure to a single key to represent his
 * own context instead of using multiple {@link #put}, which could be more costly.
 * Past five user key/value pair, the {@link Context} will use a copy-on-write
 * implementation backed by a new {@link java.util.Map} on each {@link #put}.
 *
 * @author Stephane Maldini
 */
public interface Context {

	/**
	 * Return an empty {@link Context}
	 *
	 * @return an empty {@link Context}
	 */
	static Context empty() {
		return Context0.INSTANCE;
	}

	/**
	 * Create a {@link Context} pre-initialized with one key-value pair.
	 *
	 * @param key the key to initialize.
	 * @param value the value for the key.
	 * @return a {@link Context} with a single entry.
	 * @throws NullPointerException if either key or value are null
	 */
	static Context of(Object key, Object value) {
		return new Context1(key, value);
	}

	/**
	 * Create a {@link Context} pre-initialized with two key-value pairs.
	 *
	 * @param key1 the first key to initialize.
	 * @param value1 the value for the first key.
	 * @param key2 the second key to initialize.
	 * @param value2 the value for the second key.
	 * @return a {@link Context} with two entries.
	 * @throws NullPointerException if any key or value is null
	 */
	static Context of(Object key1, Object value1,
			Object key2, Object value2) {
		return new Context2(key1, value1, key2, value2);
	}

	/**
	 * Create a {@link Context} pre-initialized with three key-value pairs.
	 *
	 * @param key1 the first key to initialize.
	 * @param value1 the value for the first key.
	 * @param key2 the second key to initialize.
	 * @param value2 the value for the second key.
	 * @param key3 the third key to initialize.
	 * @param value3 the value for the third key.
	 * @return a {@link Context} with three entries.
	 * @throws NullPointerException if any key or value is null
	 */
	static Context of(Object key1, Object value1,
			Object key2, Object value2,
			Object key3, Object value3) {
		return new Context3(key1, value1, key2, value2, key3, value3);
	}

	/**
	 * Create a {@link Context} pre-initialized with four key-value pairs.
	 *
	 * @param key1 the first key to initialize.
	 * @param value1 the value for the first key.
	 * @param key2 the second key to initialize.
	 * @param value2 the value for the second key.
	 * @param key3 the third key to initialize.
	 * @param value3 the value for the third key.
	 * @param key4 the fourth key to initialize.
	 * @param value4 the value for the fourth key.
	 * @return a {@link Context} with four entries.
	 * @throws NullPointerException if any key or value is null
	 */
	static Context of(Object key1, Object value1,
			Object key2, Object value2,
			Object key3, Object value3,
			Object key4, Object value4) {
		return new Context4(key1, value1, key2, value2, key3, value3, key4, value4);
	}

	/**
	 * Create a {@link Context} pre-initialized with five key-value pairs.
	 *
	 * @param key1 the first key to initialize.
	 * @param value1 the value for the first key.
	 * @param key2 the second key to initialize.
	 * @param value2 the value for the second key.
	 * @param key3 the third key to initialize.
	 * @param value3 the value for the third key.
	 * @param key4 the fourth key to initialize.
	 * @param value4 the value for the fourth key.
	 * @param key5 the fifth key to initialize.
	 * @param value5 the value for the fifth key.
	 * @return a {@link Context} with five entries.
	 * @throws NullPointerException if any key or value is null
	 */
	static Context of(Object key1, Object value1,
			Object key2, Object value2,
			Object key3, Object value3,
			Object key4, Object value4,
			Object key5, Object value5) {
		return new Context5(key1, value1, key2, value2, key3, value3, key4, value4, key5, value5);
	}

	/**
	 * Create a {@link Context} out of a {@link Map}. Prefer this method if you're somehow
	 * incapable of checking keys are all distinct in other {@link #of(Object, Object, Object, Object, Object, Object, Object, Object)}
	 * implementations.
	 *
	 * @implNote this method compacts smaller maps into a relevant fields-based implementation
	 * when map size is less than 6.
	 */
	static Context of(Map<?, ?> map) {
		int size = Objects.requireNonNull(map, "map").size();
		if (size == 0) return Context.empty();
		if (size <= 5) {
			Map.Entry[] entries = map.entrySet().toArray(new Map.Entry[size]);
			switch (size) {
				case 1:
					return new Context1(entries[0].getKey(), entries[0].getValue());
				case 2:
					return new Context2(entries[0].getKey(), entries[0].getValue(),
					entries[1].getKey(), entries[1].getValue());
				case 3:
					return new Context3(entries[0].getKey(), entries[0].getValue(),
					entries[1].getKey(), entries[1].getValue(),
					entries[2].getKey(), entries[2].getValue());
				case 4:
					return new Context4(entries[0].getKey(), entries[0].getValue(),
					entries[1].getKey(), entries[1].getValue(),
					entries[2].getKey(), entries[2].getValue(),
					entries[3].getKey(), entries[3].getValue());
				case 5:
					return new Context5(entries[0].getKey(), entries[0].getValue(),
					entries[1].getKey(), entries[1].getValue(),
					entries[2].getKey(), entries[2].getValue(),
					entries[3].getKey(), entries[3].getValue(),
					entries[4].getKey(), entries[4].getValue());
			}
		}
		// Since ContextN(Map) is a low level API that DOES NOT perform null checks,
		// we need to check every key/value before passing it to ContextN(Map)
		map.forEach((key, value) -> {
			Objects.requireNonNull(key, "null key found");
			if (value == null) {
				throw new NullPointerException("null value for key " + key);
			}
		});
		//noinspection unchecked
		return new ContextN((Map) map);
	}

	/**
	 * Resolve a value given a key that exists within the {@link Context}, or throw
	 * a {@link NoSuchElementException} if the key is not present.
	 *
	 * @param key a lookup key to resolve the value within the context
	 * @param <T> an unchecked casted generic for fluent typing convenience
	 *
	 * @return the value resolved for this key (throws if key not found)
	 * @throws NoSuchElementException when the given key is not present
	 * @see #getOrDefault(Object, Object)
	 * @see #getOrEmpty(Object)
	 * @see #hasKey(Object)
	 */
	<T> T get(Object key);

	/**
	 * Resolve a value given a type key within the {@link Context}.
	 *
	 * @param key a type key to resolve the value within the context
	 *
	 * @param <T> an unchecked casted generic for fluent typing convenience
	 *
	 * @return the value resolved for this type key (throws if key not found)
	 * @throws NoSuchElementException when the given type key is not present
	 * @see #getOrDefault(Object, Object)
	 * @see #getOrEmpty(Object)
	 */
	default <T> T get(Class<T> key){
		T v = get((Object)key);
		if(key.isInstance(v)){
			return v;
		}
		throw new NoSuchElementException("Context does not contain a value of type "+key
				.getName());
	}

	/**
	 * Resolve a value given a key within the {@link Context}. If unresolved return the
	 * passed default value.
	 *
	 * @param key a lookup key to resolve the value within the context
	 * @param defaultValue a fallback value if key doesn't resolve
	 *
	 * @return the value resolved for this key, or the given default if not present
	 */
	@Nullable
	default <T> T getOrDefault(Object key, @Nullable T defaultValue){
		if(!hasKey(key)){
			return defaultValue;
		}
		return get(key);
	}

	/**
	 * Resolve a value given a key within the {@link Context}.
	 *
	 * @param key a lookup key to resolve the value within the context
	 *
	 * @return an {@link Optional} of the value for that key.
	 */
	default <T> Optional<T> getOrEmpty(Object key){
		if(hasKey(key)) {
			return Optional.of(get(key));
		}
		return Optional.empty();
	}

	/**
	 * Return true if a particular key resolves to a value within the {@link Context}.
	 *
	 * @param key a lookup key to test for
	 *
	 * @return true if this context contains the given key
	 */
	boolean hasKey(Object key);

	/**
	 * Return true if the {@link Context} is empty.
	 *
	 * @return true if the {@link Context} is empty.
	 */
	default boolean isEmpty() {
		return size() == 0;
	}

	/**
	 * Create a new {@link Context} that contains all current key/value pairs plus the
	 * given key/value pair. If that key existed in the current Context, its associated
	 * value is replaced in the resulting {@link Context}.
	 *
	 * @param key the key to add/update in the new {@link Context}
	 * @param value the value to associate to the key in the new {@link Context}
	 *
	 * @return a new {@link Context} including the provided key/value
	 * @throws NullPointerException if either the key or value are null
	 */
	Context put(Object key, Object value);

	/**
	 * Create a new {@link Context} that contains all current key/value pairs plus the
	 * given key/value pair <strong>only if the value is not {@literal null}</strong>. If that key existed in the
	 * current Context, its associated value is replaced in the resulting {@link Context}.
	 *
	 * @param key the key to add/update in the new {@link Context}
	 * @param valueOrNull the value to associate to the key in the new {@link Context}, null to ignore the operation
	 *
	 * @return a new {@link Context} including the provided key/value, or the same {@link Context} if value is null
	 * @throws NullPointerException if the key is null
	 */
	default Context putNonNull(Object key, @Nullable Object valueOrNull) {
		if (valueOrNull != null) {
			return put(key, valueOrNull);
		}
		return this;
	}

	/**
	 * Return a new {@link Context} that will resolve all existing keys except the
	 * removed one, {@code key}.
	 * <p>
	 * Note that if this {@link Context} doesn't contain the key, this method simply
	 * returns this same instance.
	 *
	 * @param key the key to remove.
	 * @return a new {@link Context} that doesn't include the provided key
	 */
	Context delete(Object key);

	/**
	 * Return the size of this {@link Context}, the number of immutable key/value pairs stored inside it.
	 *
	 * @return the size of the {@link Context}
	 */
	int size();

	/**
	 * Stream key/value pairs from this {@link Context}
	 *
	 * @return a {@link Stream} of key/value pairs held by this context
	 */
	Stream<Map.Entry<Object,Object>> stream();

	/**
	 * Create a new {@link Context} by merging the content of this context and a given
	 * {@link Context}. If the other context is empty, the same {@link Context} instance
	 * is returned.
	 *
	 * @param other the other Context to get values from
	 * @return a new Context with a merge of the entries from this context and the given context.
	 */
	default Context putAll(Context other) {
		if (other.isEmpty()) return this;

		if (other instanceof CoreContext) {
			CoreContext coreContext = (CoreContext) other;
			return coreContext.putAllInto(this);
		}

		ContextN newContext = new ContextN(this.size() + other.size());
		this.stream().sequential().forEach(newContext);
		other.stream().sequential().forEach(newContext);
		if (newContext.size() <= 5) {
			// make it return Context{1-5}
			return Context.of(newContext);
		}
		return newContext;
	}
}

/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Stream;

import reactor.util.annotation.Nullable;

/**
 * A read-only view of a collection of key/value pairs that is propagated between components
 * such as operators via the context protocol. Contexts are ideal to transport orthogonal
 * information such as tracing or security tokens.
 * <p>
 * {@link Context} is an immutable variant of the same key/value pairs structure which exposes
 * a write API that returns new instances on each write.
 *
 * @author Simon Baslé
 */
public interface ContextView {

	/**
	 * Resolve a value given a key that exists within the {@link Context}, or throw
	 * a {@link NoSuchElementException} if the key is not present.
	 *
	 * @param key a lookup key to resolve the value within the context
	 * @param <T> an unchecked casted generic for fluent typing convenience
	 *
	 * @return the value resolved for this key (throws if key not found)
	 *
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
	 * @param <T> an unchecked casted generic for fluent typing convenience
	 *
	 * @return the value resolved for this type key (throws if key not found)
	 *
	 * @throws NoSuchElementException when the given type key is not present
	 * @see #getOrDefault(Object, Object)
	 * @see #getOrEmpty(Object)
	 */
	default <T> T get(Class<T> key) {
		T v = get((Object) key);
		if (key.isInstance(v)) {
			return v;
		}
		throw new NoSuchElementException("Context does not contain a value of type " + key.getName());
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
	default <T> T getOrDefault(Object key, @Nullable T defaultValue) {
		if (!hasKey(key)) {
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
	default <T> Optional<T> getOrEmpty(Object key) {
		if (hasKey(key)) {
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
	Stream<Map.Entry<Object, Object>> stream();
}

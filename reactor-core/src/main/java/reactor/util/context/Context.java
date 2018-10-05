/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

package reactor.util.context;

import java.util.Map;
import java.util.NoSuchElementException;
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

	@Deprecated
	static boolean isContextUnsupportedFatal() {
		return ContextUnsupported.isContextUnsupportedFatal;
	}

	@Deprecated
	static void reloadFeatureFlag() {
		ContextUnsupported.reload();
	}

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
	 */
	static Context of(Object key1, Object value1,
			Object key2, Object value2,
			Object key3, Object value3,
			Object key4, Object value4,
			Object key5, Object value5) {
		return new Context5(key1, value1, key2, value2, key3, value3, key4, value4, key5, value5);
	}

	/**
	 * Create a special empty {@link Context} that will reject reading operations with an
	 * {@link UnsupportedOperationException}. Write operations are permitted and return a
	 * new instance of {@link Context} that is now readable.
	 *
	 * @param reason a message describing why {@link Context} read are disallowed, used in
	 * the {@link Exception#getCause() cause message} for the read operation exceptions.
	 * @return a {@link Context} that explicitly doesn't support any read operation.
	 */
	static Context unsupported(String reason) {
		return new ContextUnsupported(reason, ContextUnsupported.isContextUnsupportedFatal);
	}

	/**
	 * Resolve a value given a key that exists within the {@link Context}, or throw
	 * a {@link NoSuchElementException} if the key is not present.
	 *
	 * @param key a lookup key to resolve the value within the context
	 *
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
		return this == Context0.INSTANCE || this instanceof Context0;
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
	 */
	Context put(Object key, Object value);

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

		return other.stream()
		            .reduce(this,
				            (c, e) -> c.put(e.getKey(), e.getValue()),
				            (c1, c2) -> { throw new UnsupportedOperationException("Context.putAll should not use a parallelized stream");}
		            );
	}

	String CONTEXT_UNSUPPORTED_PROPERTY = "reactor.context.unsupported.isFatal";
}

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

import javax.annotation.Nullable;

/**
 * A key/value store that is propagated between components such as operators via the
 *  context protocol. Contexts are ideal to transport orthogonal
 * infos such as tracing or security tokens.
 * <p>
 * {@link Context} implementations are thread-safe {@link #put(Object, Object)} will
 * usually return a safe new {@link Context} object.
 * <p>
 * Note that contexts are optimized for single key/value storage, and a user might want
 * to represent his own context instead of using more costly {@link #put}.
 * Past one user key/value pair, the context will use a copy-on-write {@link Context}
 * backed by
 * a new {@link java.util.Map} on each {@link #put}.
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
	 * Inject a key/value pair in a new {@link Context} inheriting current state.
	 * The returned {@link Context} will resolve the new key/value pair and any
	 * existing key/value pair.
	 *
	 * @param key a lookup key to reuse later for value resolution
	 * @param value the target object to store in the new {@link Context}
	 *
	 * @return a new {@link Context} including the user-provided key/value
	 */
	Context put(Object key, @Nullable Object value);

	/**
	 * Resolve a value given a key within the {@link Context}.
	 *
	 * @param key a lookup key to resolve the value within the context
	 *
	 * @param <T> an unchecked casted generic for fluent typing convenience
	 *
	 * @return the eventual value resolved by this key or null
	 */
	@Nullable
	<T> T get(Object key);

	/**
	 * Resolve a value given a type key within the {@link Context}.
	 *
	 * @param key a type key to resolve the value within the context
	 *
	 * @param <T> an unchecked casted generic for fluent typing convenience
	 *
	 * @return the eventual value resolved by this type key or null
	 */
	@Nullable
	default <T> T get(Class<T> key){
		T v = get((Object)key);
		if(key.isInstance(key)){
			return v;
		}
		return null;
	}

	/**
	 * Resolve a value given a key within the {@link Context}. If unresolved return the
	 * passed default value.
	 *
	 * @param key a lookup key to resolve the value within the context
	 * @param defaultValue a fallback value if key doesn't resolve
	 *
	 * @return an eventual value or the default passed
	 */
	@Nullable
	default <T> T getOrDefault(Object key, @Nullable T defaultValue){
		T v = get(key);
		if(v == null){
			return defaultValue;
		}
		return v;
	}

	/**
	 * Return true if {@link Context} is empty.
	 *
	 * @return true if {@link Context} is empty.
	 */
	default boolean isEmpty() {
		return this == empty();
	}
}

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

package reactor.core;

import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * An introspectable component
 *
 * @author Stephane Maldini
 */
@FunctionalInterface
public interface Scannable {

	/**
	 * A list of reserved keys for operator state scanning
	 */
	enum Attr {
		PARENT, DELAY_ERROR, DELAY_ERROR_END, PREFETCH, CAPACITY, ACTUAL, PUBSUB,
		ERROR, BUFFERED, EXPECTED_FROM_UPSTREAM, REQUESTED_FROM_DOWNSTREAM, CANCELLED, TERMINATED, LIMIT;

		static Stream<? extends Scannable> recurse(Scannable _s, Attr key){
			Scannable s = Scannable.from(_s.scan(key));
			if(s == null){
				return Stream.empty();
			}
			return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<Scannable>() {
				Scannable c = s;

				@Override
				public boolean hasNext() {
					return c != null;
				}

				@Override
				public Scannable next() {
					if(c != null){
						Scannable _c = c;
						c = Scannable.from(c.scan(key));
						return _c;
					}
					return null;
				}
			}, 0),false);
		}
	}

	/**
	 * A constant that represents {@link Scannable} returned via {@link #from(Object)}
	 * when the passed non-null reference is not a {@link Scannable}
	 *
	 */
	Scannable UNAVAILABLE_SCAN = key -> null;

	/**
	 * Return the casted {@link Scannable}, null or {@link #UNAVAILABLE_SCAN}
	 *
	 * @param o a reference to cast
	 *
	 * @return the casted {@link Scannable}, null or {@link #UNAVAILABLE_SCAN}
	 */
	@SuppressWarnings("unchecked")
	static Scannable from(Object o) {
		if (o == null) {
			return null;
		}
		if (o instanceof Scannable) {
			return ((Scannable) o);
		}
		return UNAVAILABLE_SCAN;
	}

	/**
	 * Return a {@link Stream} navigating the {@link org.reactivestreams.Subscriber}
	 * chain (downward).
	 *
	 * @return a {@link Stream} navigating the {@link org.reactivestreams.Subscriber}
	 * chain (downward)
	 */
	default Stream<? extends Scannable> actuals() {
		return Attr.recurse(this, Attr.ACTUAL);
	}

	/**
	 * Return a {@link Stream} of referenced inners (flatmap, multicast etc)
	 *
	 * @return a {@link Stream} of referenced inners (flatmap, multicast etc)
	 */
	default Stream<? extends Scannable> inners() {
		return Stream.empty();
	}

	/**
	 * Return a {@link Stream} navigating the {@link org.reactivestreams.Subscription}
	 * chain (upward).
	 *
	 * @return a {@link Stream} navigating the {@link org.reactivestreams.Subscription}
	 * chain (upward)
	 */
	default Stream<? extends Scannable> parents() {
		return Attr.recurse(this, Attr.PARENT);
	}

	/**
	 * Return an eventual value or null for a given component state {@link Attr}
	 *
	 * @param key an {@link Attr} to get the operator state
	 *
	 * @return an eventual value or null
	 */
	Object scan(Attr key);

	/**
	 * Resolve a value given an {@link Attr} within the {@link Scannable}. If unresolved
	 * return the
	 * passed default value.
	 *
	 * @param key a {@link Attr} to resolve the value within the context
	 * @param type the attribute type
	 *
	 * @return an eventual value or the default passed
	 */
	default <T> T scan(Attr key, Class<T> type) {
		Objects.requireNonNull(type, "type");
		Object v = scan(key);
		if (v == null || !type.isAssignableFrom(v.getClass())) {
			return null;
		}
		@SuppressWarnings("unchecked")
		T r = (T)v;
		return r;
	}

	/**
	 * Resolve a value given an {@link Attr} within the {@link Scannable}. If unresolved
	 * return the
	 * passed default value.
	 *
	 * @param key a {@link Attr} to resolve the value within the context
	 * @param defaultValue a fallback value if key doesn't resolve
	 *
	 * @return an eventual value or the default passed
	 */
	default <T> T scanOrDefault(Attr key, T defaultValue) {
		@SuppressWarnings("unchecked")
		T v = (T) scan(key);
		if (v == null) {
			return defaultValue;
		}
		return v;
	}

}

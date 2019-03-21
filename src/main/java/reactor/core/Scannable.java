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

package reactor.core;

import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A Scannable component exposes state in a non strictly memory consistent way and
 * results should be understood as best-effort hint of the underlying state. This is
 * useful to retro-engineer a component graph such as a flux operator chain via
 * {@link Stream} queries from
 * {@link #actuals()}, {@link #parents()} and {@link #inners()}. This allows for
 * visiting patterns and possibly enable serviceability features.
 * <p>
 *     Scannable is also a useful tool for the advanced user eager to learn which kind
 *     of state we usually manage in the package-scope schedulers or operators
 *     implementations.
 *
 * @author Stephane Maldini
 */
@FunctionalInterface
public interface Scannable {

	/**
	 * A list of reserved keys for component state scanning
	 */
	enum Attr {
		/**
		 * Parent key exposes the direct upstream relationship of the scanned component.
		 * It can be a Publisher source to an operator, a Subscription to a Subscriber
		 * (main flow if ambiguous with
		 * inner Subscriptions like flatMap), a Scheduler to a Worker.
		 * <p>
		 *     {@link #parents()} can be used to navigate the parent chain.
		 */
		PARENT,

		/**
		 * Delay_Error exposes a {@link Boolean} whether the scanned component
		 * actively supports error delaying if it manages a backlog instead of fast
		 * error-passing which might drop pending backlog.
		 * <p>
		 *     Note: This attribute usually resolves to a constant value
		 */
		DELAY_ERROR,

		/**
		 * Prefetch is an {@link Integer} attribute defining the rate of processing in a
		 * component
		 * which has capacity to request and hold a backlog of data. It
		 * usually maps to a component capacity when no arbitrary {@link #CAPACITY} is
		 * set. {@link Integer#MAX_VALUE} signal unlimited capacity and therefore
		 * unbounded demand.
		 * <p>
		 *     Note: This attribute usually resolves to a constant value
		 */
		PREFETCH,

		/**
		 * Return an an {@link Integer} capacity when no {@link #PREFETCH} is defined or
		 * when an arbitrary maximum limit is applied to the backlog capacity of the
		 * scanned component. {@link Integer#MAX_VALUE} signal unlimited capacity.
		 * <p>
		 *     Note: This attribute usually resolves to a constant value
		 */
		CAPACITY,

		/**
		 * The direct dependent component downstream reference if any. Operators in Flux/Mono for instance
		 *  delegate to a target Subscriber, which is going to be the actual chain
		 *  navigated with this reference key.
		 *  <p>
		 *      A reference chain downstream can be navigated via {@link #actuals()}.
		 */
		ACTUAL,

		/**
		 * a {@link Throwable} attribute which indicate an error state if the scanned
		 * component keeps track of it.
		 */
		ERROR,

		/**
		 * A {@link Integer} attribute implemented by components with a backlog
		 * capacity. It will expose current queue size or similar related to
		 * user-provided held data.
		 */
		BUFFERED,

		/**
		 * A {@link Long} attribute exposing the current pending demand of a downstream
		 * component. Note that {@link Long#MAX_VALUE} indicates an unbounded
		 * (push-style) demand as specified in
		 * {@link org.reactivestreams.Subscription#request(long)}.
		 */
		REQUESTED_FROM_DOWNSTREAM,

		/**
		 * A {@link Boolean} attribute indicating whether or not a downstream component
		 * has interrupted consuming this scanned component, e.g., a cancelled
		 * subscription. Note that it differs from {@link #TERMINATED} which is
		 * intended for "normal" shutdown cycles.
		 */
		CANCELLED,

		/**
		 * A {@link Boolean} attribute indicating whether or not an upstream component
		 * terminated this scanned component. e.g. a post onComplete/onError subscriber.
		 * By opposition to {@link #CANCELLED} which determines if a downstream
		 * component interrupted this scanned component.
		 */
		TERMINATED;

		/**
		 * A constant that represents {@link Scannable} returned via {@link #from(Object)}
		 * when the passed non-null reference is not a {@link Scannable}
		 */
		static final Scannable UNAVAILABLE_SCAN = new Scannable() {
			@Override
			public Object scan(Attr key) {
				return null;
			}

			@Override
			public boolean isScanAvailable() {
				return false;
			}
		};

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
					Scannable _c = c;
					c = Scannable.from(c.scan(key));
					return _c;
				}
			}, 0),false);
		}
	}

	/**
	 * Return the casted {@link Scannable}, null or a
	 * constant {@link Scannable} that return false on
	 * {@link Scannable#isScanAvailable}.
	 *
	 * @param o a reference to cast
	 *
	 * @return the casted {@link Scannable}, null or {@link Attr#UNAVAILABLE_SCAN}
	 */
	@SuppressWarnings("unchecked")
	static Scannable from(Object o) {
		if (o == null) {
			return null;
		}
		if (o instanceof Scannable) {
			return ((Scannable) o);
		}
		return Attr.UNAVAILABLE_SCAN;
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
	 * Return true whether the component is available for {@link #scan(Attr)} resolution.
	 *
	 * @return true whether the component is available for {@link #scan(Attr)} resolution.
	 */
	default boolean isScanAvailable(){
		return true;
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
	 * @return an eventual value or null if unmatched or unresolved
	 *
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

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

	@FunctionalInterface
	interface Attr<T> {

		T defaultValue();

		/**
		 * A constant that represents {@link Scannable} returned via {@link #from(Object)}
		 * when the passed non-null reference is not a {@link Scannable}
		 */
		static final Scannable UNAVAILABLE_SCAN = new Scannable() {
			@Override
			public Object scanUnsafe(Attr key) {
				return null;
			}

			@Override
			public boolean isScanAvailable() {
				return false;
			}
		};

		static Stream<? extends Scannable> recurse(Scannable _s, ScannableAttr key){
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

	enum ScannableAttr implements Attr<Scannable> {

		/**
		 * Parent key exposes the direct upstream relationship of the scanned component.
		 * It can be a Publisher source to an operator, a Subscription to a Subscriber
		 * (main flow if ambiguous with
		 * inner Subscriptions like flatMap), a Scheduler to a Worker.
		 * <p>
		 *     {@link #parents()} can be used to navigate the parent chain.
		 */
		PARENT(null),

		/**
		 * The direct dependent component downstream reference if any. Operators in Flux/Mono for instance
		 *  delegate to a target Subscriber, which is going to be the actual chain
		 *  navigated with this reference key.
		 *  <p>
		 *      A reference chain downstream can be navigated via {@link #actuals()}.
		 */
		ACTUAL(null);

		final Scannable defaultValue;

		ScannableAttr(Scannable defaultValue) {
			this.defaultValue = defaultValue;
		}

		@Override
		public Scannable defaultValue() {
			return defaultValue;
		}
	}

	enum IntAttr implements Attr<Integer> {

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
		PREFETCH(0),

		/**
		 * Return an an {@link Integer} capacity when no {@link #PREFETCH} is defined or
		 * when an arbitrary maximum limit is applied to the backlog capacity of the
		 * scanned component. {@link Integer#MAX_VALUE} signal unlimited capacity.
		 * <p>
		 *     Note: This attribute usually resolves to a constant value
		 */
		CAPACITY(0),

		/**
		 * A {@link Integer} attribute implemented by components with a backlog
		 * capacity. It will expose current queue size or similar related to
		 * user-provided held data.
		 */
		BUFFERED(0);

		final Integer defaultValue;

		IntAttr(Integer defaultValue) {
			this.defaultValue = defaultValue;
		}

		@Override
		public Integer defaultValue() {
			return defaultValue;
		}
	}

	enum LongAttr implements Attr<Long> {

		/**
		 * A {@link Long} attribute exposing the current pending demand of a downstream
		 * component. Note that {@link Long#MAX_VALUE} indicates an unbounded
		 * (push-style) demand as specified in
		 * {@link org.reactivestreams.Subscription#request(long)}.
		 */
		REQUESTED_FROM_DOWNSTREAM(0L);

		final Long defaultValue;

		LongAttr(Long defaultValue) {
			this.defaultValue = defaultValue;
		}

		@Override
		public Long defaultValue() {
			return defaultValue;
		}
	}

	enum ThrowableAttr implements Attr<Throwable> {

		/**
		 * a {@link Throwable} attribute which indicate an error state if the scanned
		 * component keeps track of it.
		 */
		ERROR(null);

		final Throwable defaultValue;

		ThrowableAttr(Throwable defaultValue) {
			this.defaultValue = defaultValue;
		}

		@Override
		public Throwable defaultValue() {
			return defaultValue;
		}
	}

	enum BooleanAttr implements Attr<Boolean> {

		/**
		 * Delay_Error exposes a {@link Boolean} whether the scanned component
		 * actively supports error delaying if it manages a backlog instead of fast
		 * error-passing which might drop pending backlog.
		 * <p>
		 *     Note: This attribute usually resolves to a constant value
		 */
		DELAY_ERROR(false),



		/**
		 * A {@link Boolean} attribute indicating whether or not a downstream component
		 * has interrupted consuming this scanned component, e.g., a cancelled
		 * subscription. Note that it differs from {@link #TERMINATED} which is
		 * intended for "normal" shutdown cycles.
		 */
		CANCELLED(null),

		/**
		 * A {@link Boolean} attribute indicating whether or not an upstream component
		 * terminated this scanned component. e.g. a post onComplete/onError subscriber.
		 * By opposition to {@link #CANCELLED} which determines if a downstream
		 * component interrupted this scanned component.
		 */
		TERMINATED(null);

		final Boolean defaultValue;

		BooleanAttr(Boolean defaultValue) {
			this.defaultValue = defaultValue;
		}

		@Override
		public Boolean defaultValue() {
			return defaultValue;
		}
	}

	enum StringAttr implements Attr<String> {

		/**
		 * a {@link String} attribute which indicates the sequence has been associated
		 * with a name.
		 */
		NAME(null);

		final String defaultValue;

		StringAttr(String defaultValue) {
			this.defaultValue = defaultValue;
		}

		@Override
		public String defaultValue() {
			return defaultValue;
		}
	}

	/**
	 * A list of reserved keys for component state scanning
	 */
	final class GenericAttr<T> implements Attr<T> {

		final T defaultValue;

		GenericAttr(T defaultValue) {
			this.defaultValue = defaultValue;
		}

		@Override
		public T defaultValue() {
			return defaultValue;
		}
	}

	/**
	 * Attempt to cast the Object to a {@link Scannable}. Return null if the
	 * value is null, or a default object if the value is not a {@link Scannable}.
	 * The default object is a constant {@link Scannable} that return false on
	 * {@link Scannable#isScanAvailable}.
	 *
	 * @param o a reference to cast
	 *
	 * @return the casted {@link Scannable}, null or a default {@link Scannable} instance
	 * that isn't actually scannable.
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
		return Attr.recurse(this, ScannableAttr.ACTUAL);
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
		return Attr.recurse(this, ScannableAttr.PARENT);
	}

	/**
	 * Introspect a component's specific state {@link Attr attribute}, returning an
	 * associated value specific to that component, or the default value associated with
	 * the key, or null if the attribute doesn't make sense for that particular component
	 * and has no sensible default.
	 *
	 * @param key a {@link Attr} to resolve the value within the context
	 * @return a value associated to the key or null if unmatched or unresolved
	 *
	 */
	default <T> T scan(Attr<T> key) {
		@SuppressWarnings("unchecked")
		T value = (T) scanUnsafe(key);
		if (value == null)
			return key.defaultValue();
		return value;
	}

	Object scanUnsafe(Attr key);

	/**
	 * Introspect a component's specific state {@link Attr attribute}, returning the
	 * provided default if the attribute maps to {@literal null} (which should mean it
	 * doesn't make sense for that particular component).
	 *
	 * @param key a {@link Attr} to resolve the value within the context
	 * @param defaultValue a fallback value if key resolves to {@literal null}
	 *
	 * @return an eventual value or the default passed
	 */
	default <T> T scanOrDefault(Attr<T> key, T defaultValue) {
		T v = scan(key);
		if (v == null) {
			return defaultValue;
		}
		return v;
	}

}

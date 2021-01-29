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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.reactivestreams.Subscriber;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.util.annotation.Nullable;
import reactor.util.function.Tuple2;

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
	 * The pattern for matching words unrelated to operator name.
	 * Used to strip an operator name of various prefixes and suffixes.
	 */
	Pattern OPERATOR_NAME_UNRELATED_WORDS_PATTERN =
		Pattern.compile("Parallel|Flux|Mono|Publisher|Subscriber|Fuseable|Operator|Conditional");

	/**
	 * Base class for {@link Scannable} attributes, which all can define a meaningful
	 * default.
	 *
	 * @param <T> the type of data associated with an attribute
	 */
	class Attr<T> {

		/* IMPLEMENTATION NOTE
		Note that some attributes define a object-to-T converter, which means their
		private {@link #tryConvert(Object)} method can safely be used by
		{@link Scannable#scan(Attr)}, making them resilient to class cast exceptions.
		 */

		/**
		 * The direct dependent component downstream reference if any. Operators in
		 * Flux/Mono for instance delegate to a target Subscriber, which is going to be
		 * the actual chain navigated with this reference key. Subscribers are not always
		 * {@link Scannable}, but this attribute will convert these raw results to an
		 * {@link Scannable#isScanAvailable() unavailable scan} object in this case.
		 * <p>
		 *  A reference chain downstream can be navigated via {@link Scannable#actuals()}.
		 */
		public static final Attr<Scannable> ACTUAL = new Attr<>(null,
				Scannable::from);

		/**
		 * Indicate that for some purposes a {@link Scannable} should be used as additional
		 * source of information about a contiguous {@link Scannable} in the chain.
		 * <p>
		 * For example {@link Scannable#steps()} uses this to collate the
		 * {@link Scannable#stepName() stepName} of an assembly trace to its
		 * wrapped operator (the one before it in the assembly chain).
		 */
		public static final Attr<Boolean> ACTUAL_METADATA = new Attr<>(false);

		/**
		 * A {@link Integer} attribute implemented by components with a backlog
		 * capacity. It will expose current queue size or similar related to
		 * user-provided held data. Note that some operators and processors CAN keep
		 * a backlog larger than {@code Integer.MAX_VALUE}, in which case
		 * the {@link Attr#LARGE_BUFFERED Attr} {@literal LARGE_BUFFERED}
		 * should be used instead. Such operators will attempt to serve a BUFFERED
		 * query but will return {@link Integer#MIN_VALUE} when actual buffer size is
		 * oversized for int.
		 */
		public static final Attr<Integer> BUFFERED = new Attr<>(0);

		/**
		 * Return an an {@link Integer} capacity when no {@link #PREFETCH} is defined or
		 * when an arbitrary maximum limit is applied to the backlog capacity of the
		 * scanned component. {@link Integer#MAX_VALUE} signal unlimited capacity.
		 * <p>
		 * Note: This attribute usually resolves to a constant value.
		 */
		public static final Attr<Integer> CAPACITY = new Attr<>(0);

		/**
		 * A {@link Boolean} attribute indicating whether or not a downstream component
		 * has interrupted consuming this scanned component, e.g., a cancelled
		 * subscription. Note that it differs from {@link #TERMINATED} which is
		 * intended for "normal" shutdown cycles.
		 */
		public static final Attr<Boolean> CANCELLED = new Attr<>(false);

		/**
		 * Delay_Error exposes a {@link Boolean} whether the scanned component
		 * actively supports error delaying if it manages a backlog instead of fast
		 * error-passing which might drop pending backlog.
		 * <p>
		 * Note: This attribute usually resolves to a constant value.
		 */
		public static final Attr<Boolean> DELAY_ERROR = new Attr<>(false);

		/**
		 * a {@link Throwable} attribute which indicate an error state if the scanned
		 * component keeps track of it.
		 */
		public static final Attr<Throwable> ERROR = new Attr<>(null);

		/**
		 * Similar to {@link Attr#BUFFERED}, but reserved for operators that can hold
		 * a backlog of items that can grow beyond {@literal Integer.MAX_VALUE}. These
		 * operators will also answer to a {@link Attr#BUFFERED} query up to the point
		 * where their buffer is actually too large, at which point they'll return
		 * {@literal Integer.MIN_VALUE}, which serves as a signal that this attribute
		 * should be used instead. Defaults to {@literal null}.
		 * <p>
		 * {@code Flux.flatMap}, {@code Flux.filterWhen}
		 * and {@code Flux.window} (with overlap) are known to use this attribute.
		 */
		public static final Attr<Long> LARGE_BUFFERED = new Attr<>(null);

		/**
		 * An arbitrary name given to the operator component. Defaults to {@literal null}.
		 */
		public static final Attr<String> NAME = new Attr<>(null);

		/**
		 * Parent key exposes the direct upstream relationship of the scanned component.
		 * It can be a Publisher source to an operator, a Subscription to a Subscriber
		 * (main flow if ambiguous with inner Subscriptions like flatMap), a Scheduler to
		 * a Worker. These types are not always {@link Scannable}, but this attribute
		 * will convert such raw results to an {@link Scannable#isScanAvailable() unavailable scan}
		 * object in this case.
		 * <p>
		 * {@link Scannable#parents()} can be used to navigate the parent chain.
		 */
		public static final Attr<Scannable> PARENT = new Attr<>(null,
				Scannable::from);

		/**
		 * A key that links a {@link Scannable} to another {@link Scannable} it runs on.
		 * Usually exposes a link between an operator/subscriber and its {@link Worker} or
		 * {@link Scheduler}, provided these are {@link Scannable}. Will return
		 * {@link Attr#UNAVAILABLE_SCAN} if the supporting execution is not Scannable or
		 * {@link Attr#NULL_SCAN} if the operator doesn't define a specific runtime.
		 */
		public static final Attr<Scannable> RUN_ON = new Attr<>(null, Scannable::from);

		/**
		 * Prefetch is an {@link Integer} attribute defining the rate of processing in a
		 * component which has capacity to request and hold a backlog of data. It
		 * usually maps to a component capacity when no arbitrary {@link #CAPACITY} is
		 * set. {@link Integer#MAX_VALUE} signal unlimited capacity and therefore
		 * unbounded demand.
		 * <p>
		 * Note: This attribute usually resolves to a constant value.
		 */
		public static final Attr<Integer> PREFETCH = new Attr<>(0);

		/**
		 * A {@link Long} attribute exposing the current pending demand of a downstream
		 * component. Note that {@link Long#MAX_VALUE} indicates an unbounded (push-style)
		 * demand as specified in {@link org.reactivestreams.Subscription#request(long)}.
		 */
		public static final Attr<Long> REQUESTED_FROM_DOWNSTREAM = new Attr<>(0L);

		/**
		 * A {@link Boolean} attribute indicating whether or not an upstream component
		 * terminated this scanned component. e.g. a post onComplete/onError subscriber.
		 * By opposition to {@link #CANCELLED} which determines if a downstream
		 * component interrupted this scanned component.
		 */
		public static final Attr<Boolean> TERMINATED = new Attr<>(false);

		/**
		 * A {@link Stream} of {@link reactor.util.function.Tuple2} representing key/value
		 * pairs for tagged components. Defaults to {@literal null}.
		 */
		public static final Attr<Stream<Tuple2<String, String>>> TAGS = new Attr<>(null);

		/**
		 * An {@link RunStyle} enum attribute indicating whether or not an operator continues to operate on the same thread.
		 * Each value provides a different degree of guarantee from weakest {@link RunStyle#UNKNOWN} to strongest {@link RunStyle#SYNC}.
		 *
		 * Defaults to {@link RunStyle#UNKNOWN}.
		 */
		public static final Attr<RunStyle> RUN_STYLE = new Attr<>(RunStyle.UNKNOWN);

		/**
		 * An {@link Enum} enumerating the different styles an operator can run : their {@link #ordinal()} reflects the level of confidence
		 * in their running mode
		 */
		public enum RunStyle {
			/**
				no guarantees can be given on the running mode (default value, weakest level of guarantee)
			 */
			UNKNOWN,

			/**
			 	the operator may change threads while running
			 */
			ASYNC,

			/**
			 	guarantees the operator doesn't change threads (strongest level of guarantee)
			 */
			SYNC;
		}

		/**
		 * Meaningful and always applicable default value for the attribute, returned
		 * instead of {@literal null} when a specific value hasn't been defined for a
		 * component. {@literal null} if no sensible generic default is available.
		 *
		 * @return the default value applicable to all components or null if none.
		 */
		@Nullable
		public T defaultValue(){
			return defaultValue;
		}

		/**
		 * Checks if this attribute is capable of safely converting any Object into
		 * a {@code T} via {@link #tryConvert(Object)} (potentially returning {@code null}
		 * or a Null Object for incompatible raw values).
		 * @return true if the attribute can safely convert any object, false if it can
		 * throw {@link ClassCastException}
		 */
		boolean isConversionSafe() {
			return safeConverter != null;
		}

		/**
		 * Attempt to convert any {@link Object} instance o into a {@code T}. By default
		 * unsafe attributes will just try forcing a cast, which can lead to {@link ClassCastException}.
		 * However, attributes for which {@link #isConversionSafe()} returns true are
		 * required to not throw an exception (but rather return {@code null} or a Null
		 * Object).
		 *
		 * @param o the instance to attempt conversion on
		 * @return the converted instance
		 */
		@Nullable
		T tryConvert(@Nullable Object o) {
			if (o == null) {
				return null;
			}
			if (safeConverter == null) {
				@SuppressWarnings("unchecked") T t = (T) o;
				return t;
			}
			return safeConverter.apply(o);
		}

		final T                             defaultValue;
		final Function<Object, ? extends T> safeConverter;

		protected Attr(@Nullable T defaultValue){
			this(defaultValue, null);
		}

		protected Attr(@Nullable T defaultValue,
				@Nullable Function<Object, ? extends T> safeConverter) {
			this.defaultValue = defaultValue;
			this.safeConverter = safeConverter;
		}

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

			@Override
			public String toString() {
				return "UNAVAILABLE_SCAN";
			}

			@Override
			public String stepName() {
				return "UNAVAILABLE_SCAN";
			}
		};

		/**
		 * A constant that represents {@link Scannable} returned via {@link #from(Object)}
		 * when the passed reference is null
		 */
		static final Scannable NULL_SCAN = new Scannable() {
			@Override
			public Object scanUnsafe(Attr key) {
				return null;
			}

			@Override
			public boolean isScanAvailable() {
				return false;
			}

			@Override
			public String toString() {
				return "NULL_SCAN";
			}

			@Override
			public String stepName() {
				return "NULL_SCAN";
			}
		};

		static Stream<? extends Scannable> recurse(Scannable _s,
				Attr<Scannable> key){
			Scannable s = Scannable.from(_s.scan(key));
			if(!s.isScanAvailable()) {
				return Stream.empty();
			}
			return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<Scannable>() {
				Scannable c = s;

				@Override
				public boolean hasNext() {
					return c != null && c.isScanAvailable();
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
	 * Attempt to cast the Object to a {@link Scannable}. Return {@link Attr#NULL_SCAN} if
	 * the value is null, or {@link Attr#UNAVAILABLE_SCAN} if the value is not a {@link Scannable}.
	 * Both are constant {@link Scannable} that return false on {@link Scannable#isScanAvailable}.
	 *
	 * @param o a reference to cast
	 *
	 * @return the casted {@link Scannable}, or one of two default {@link Scannable} instances
	 * that aren't actually scannable (one for nulls, one for non-scannable references)
	 */
	static Scannable from(@Nullable Object o) {
		if (o == null) {
			return Attr.NULL_SCAN;
		}
		if (o instanceof Scannable) {
			return ((Scannable) o);
		}
		return Attr.UNAVAILABLE_SCAN;
	}

	/**
	 * Return a {@link Stream} navigating the {@link org.reactivestreams.Subscriber}
	 * chain (downward). The current {@link Scannable} is not included.
	 *
	 * @return a {@link Stream} navigating the {@link org.reactivestreams.Subscriber}
	 * chain (downward, current {@link Scannable} not included).
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
	 * Check this {@link Scannable} and its {@link #parents()} for a user-defined name and
	 * return the first one that is reachable, or default to this {@link Scannable}
	 * {@link #stepName()} if none.
	 *
	 * @return the name of the first parent that has one defined (including this scannable)
	 */
	default String name() {
		String thisName = this.scan(Attr.NAME);
		if (thisName != null) {
			return thisName;
		}

		return parents()
				.map(s -> s.scan(Attr.NAME))
				.filter(Objects::nonNull)
				.findFirst()
				.orElse(stepName());
	}

	/**
	 * Return a meaningful {@link String} representation of this {@link Scannable} in
	 * its chain of {@link #parents()} and {@link #actuals()}.
	 */
	default String stepName() {
		/*
		 * Strip an operator name of various prefixes and suffixes.
		 * @param name the operator name, usually simpleClassName or fully-qualified classname.
		 * @return the stripped operator name
		 */
		String name = getClass().getName();
		int innerClassIndex = name.indexOf('$');
		if (innerClassIndex != -1) {
			name = name.substring(0, innerClassIndex);
		}
		int stripPackageIndex = name.lastIndexOf('.');
		if (stripPackageIndex != -1) {
			name = name.substring(stripPackageIndex + 1);
		}
		String stripped = OPERATOR_NAME_UNRELATED_WORDS_PATTERN
			.matcher(name)
			.replaceAll("");

		if (!stripped.isEmpty()) {
			return stripped.substring(0, 1).toLowerCase() + stripped.substring(1);
		}
		return stripped;
	}

	/**
	 * List the step names in the chain of {@link Scannable} (including the current element),
	 * in their assembly order. This traverses the chain of {@link Scannable} both upstream
	 * ({@link #parents()}) and downstream ({@link #actuals()}).
	 * <ol>
	 *     <li>if the current Scannable is a {@link Subscriber}, the chain can reach down to
	 *     the final subscriber, provided it is {@link Scannable} (eg. lambda subscriber)</li>
	 *     <li>if it is an operator the chain can reach up to the source, if it is a Reactor
	 *     source (that is {@link Scannable}).</li>
	 * </ol>
	 *
	 * @return a {@link Stream} of {@link #stepName()} for each discovered step in the {@link Scannable} chain
	 */
	default Stream<String> steps() {
		List<Scannable> chain = new ArrayList<>();
		chain.addAll(parents().collect(Collectors.toList()));
		Collections.reverse(chain);
		chain.add(this);
		chain.addAll(actuals().collect(Collectors.toList()));

		List<String> chainNames = new ArrayList<>(chain.size());
		for (int i = 0; i < chain.size(); i++) {
			Scannable step = chain.get(i);
			Scannable stepAfter = null;
			if (i < chain.size() - 1) {
				stepAfter = chain.get(i + 1);
			}
			//noinspection ConstantConditions
			if (stepAfter != null && stepAfter.scan(Attr.ACTUAL_METADATA)) {
				chainNames.add(stepAfter.stepName());
				i++;
			}
			else {
				chainNames.add(step.stepName());
			}
		}

		return chainNames.stream();
	}

	/**
	 * Return a {@link Stream} navigating the {@link org.reactivestreams.Subscription}
	 * chain (upward). The current {@link Scannable} is not included.
	 *
	 * @return a {@link Stream} navigating the {@link org.reactivestreams.Subscription}
	 * chain (upward, current {@link Scannable} not included).
	 */
	default Stream<? extends Scannable> parents() {
		return Attr.recurse(this, Attr.PARENT);
	}

	/**
	 * This method is used internally by components to define their key-value mappings
	 * in a single place. Although it is ignoring the generic type of the {@link Attr} key,
	 * implementors should take care to return values of the correct type, and return
	 * {@literal null} if no specific value is available.
	 * <p>
	 * For public consumption of attributes, prefer using {@link #scan(Attr)}, which will
	 * return a typed value and fall back to the key's default if the component didn't
	 * define any mapping.
	 *
	 * @param key a {@link Attr} to resolve for the component.
	 * @return the value associated to the key for that specific component, or null if none.
	 */
	@Nullable
	Object scanUnsafe(Attr key);

	/**
	 * Introspect a component's specific state {@link Attr attribute}, returning an
	 * associated value specific to that component, or the default value associated with
	 * the key, or null if the attribute doesn't make sense for that particular component
	 * and has no sensible default.
	 *
	 * @param key a {@link Attr} to resolve for the component.
	 * @return a value associated to the key or null if unmatched or unresolved
	 *
	 */
	@Nullable
	default <T> T scan(Attr<T> key) {
		//note tryConvert will just plain cast most of the time
		//except e.g. for Attr<Scannable>
		T value = key.tryConvert(scanUnsafe(key));
		if (value == null)
			return key.defaultValue();
		return value;
	}

	/**
	 * Introspect a component's specific state {@link Attr attribute}. If there's no
	 * specific value in the component for that key, fall back to returning the
	 * provided non null default.
	 *
	 * @param key a {@link Attr} to resolve for the component.
	 * @param defaultValue a fallback value if key resolve to {@literal null}
	 *
	 * @return a value associated to the key or the provided default if unmatched or unresolved
	 */
	default <T> T scanOrDefault(Attr<T> key, T defaultValue) {
		T v;
		//note tryConvert will just plain cast most of the time
		//except e.g. for Attr<Scannable>
		v = key.tryConvert(scanUnsafe(key));

		if (v == null) {
			return Objects.requireNonNull(defaultValue, "defaultValue");
		}
		return v;
	}

	/**
	 * Visit this {@link Scannable} and its {@link #parents()} and stream all the
	 * observed tags
	 *
	 * @return the stream of tags for this {@link Scannable} and its parents
	 */
	default Stream<Tuple2<String, String>> tags() {
		Stream<Tuple2<String, String>> parentTags =
				parents().flatMap(s -> s.scan(Attr.TAGS));

		Stream<Tuple2<String, String>> thisTags = scan(Attr.TAGS);

		if (thisTags == null) {
			return parentTags;
		}

		return Stream.concat(
				thisTags,
				parentTags
		);
	}

}

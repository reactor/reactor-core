/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.util.annotation.Nullable;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

/**
 * A domain representation of a Reactive Stream signal.
 * There are 4 distinct signals and their possible sequence is defined as such:
 * onError | (onSubscribe onNext* (onError | onComplete)?)
 *
 * @param <T> the value type
 *
 * @author Stephane Maldini
 */
public interface Signal<T> extends Supplier<T>, Consumer<Subscriber<? super T>> {

	/**
	 * Creates and returns a {@code Signal} of variety {@code Type.COMPLETE}.
	 * <p>
	 * Note that this variant associates an empty {@link Context} with the {@link Signal}.
	 *
	 * @param <T> the value type
	 *
	 * @return an {@code OnCompleted} variety of {@code Signal}
	 */
	static <T> Signal<T> complete() {
		return ImmutableSignal.onComplete();
	}

	/**
	 * Creates and returns a {@code Signal} of variety {@code Type.COMPLETE}, associated
	 * with a specific {@link Context}.
	 *
	 * @param <T> the value type
	 * @param context the {@link Context} associated with the completing source.
	 *
	 * @return an {@code OnCompleted} variety of {@code Signal}
	 */
	static <T> Signal<T> complete(Context context) {
		if (context.isEmpty()) {
			return ImmutableSignal.onComplete();
		}
		return new ImmutableSignal<>(context, SignalType.ON_COMPLETE, null, null, null);
	}

	/**
	 * Creates and returns a {@code Signal} of variety {@code Type.FAILED}, which holds
	 * the error.
	 * <p>
	 * Note that this variant associates an empty {@link Context} with the {@link Signal}.
	 *
	 * @param <T> the value type
	 * @param e the error associated to the signal
	 *
	 * @return an {@code OnError} variety of {@code Signal}
	 */
	static <T> Signal<T> error(Throwable e) {
		return error(e, Context.empty());
	}

	/**
	 * Creates and returns a {@code Signal} of variety {@code Type.FAILED}, which holds
	 * the error and the {@link Context} associated with the erroring source.
	 *
	 * @param <T> the value type
	 * @param e the error associated to the signal
	 * @param context the {@link Context} associated with the erroring source
	 *
	 * @return an {@code OnError} variety of {@code Signal}
	 */
	static <T> Signal<T> error(Throwable e, Context context) {
		return new ImmutableSignal<>(context, SignalType.ON_ERROR, null, e, null);
	}

	/**
	 * Creates and returns a {@code Signal} of variety {@code Type.NEXT}, which holds
	 * the value.
	 * <p>
	 * Note that this variant associates an empty {@link Context} with the {@link Signal}.
	 *
	 * @param <T> the value type
	 * @param t the value item associated to the signal
	 *
	 * @return an {@code OnNext} variety of {@code Signal}
	 */
	static <T> Signal<T> next(T t) {
		return next(t, Context.empty());
	}

	/**
	 * Creates and returns a {@code Signal} of variety {@code Type.NEXT}, which holds
	 * the value and the {@link Context} associated with the emitting source.
	 *
	 * @param <T> the value type
	 * @param t the value item associated to the signal
	 * @param context the {@link Context} associated with the emitting source
	 *
	 * @return an {@code OnNext} variety of {@code Signal}
	 */
	static <T> Signal<T> next(T t, Context context) {
		return new ImmutableSignal<>(context, SignalType.ON_NEXT, t, null, null);
	}

	/**
	 * Creates and returns a {@code Signal} of variety {@code Type.ON_SUBSCRIBE}.
	 * <p>
	 * Note that this variant associates an empty {@link Context} with the {@link Signal}.
	 *
	 * @param <T> the value type
	 * @param subscription the subscription
	 *
	 * @return an {@code OnSubscribe} variety of {@code Signal}
	 */
	static <T> Signal<T> subscribe(Subscription subscription) {
		return subscribe(subscription, Context.empty());
	}

	/**
	 * Creates and returns a {@code Signal} of variety {@code Type.ON_SUBSCRIBE}, that
	 * holds the {@link Context} associated with the subscribed source.
	 *
	 * @param <T> the value type
	 * @param subscription the subscription
	 * @param context the {@link Context} associated with the subscribed source
	 *
	 * @return an {@code OnSubscribe} variety of {@code Signal}
	 */
	static <T> Signal<T> subscribe(Subscription subscription, Context context) {
		return new ImmutableSignal<>(context, SignalType.ON_SUBSCRIBE, null, null, subscription);
	}

	/**
	 * Check if an arbitrary Object represents a COMPLETE {@link Signal}.
	 *
	 * @param o the object to check
	 * @return true if object represents the completion signal
	 */
	static boolean isComplete(Object o) {
		return o == ImmutableSignal.onComplete() ||
				(o instanceof Signal && ((Signal) o).getType() == SignalType.ON_COMPLETE);
	}

	/**
	 * Check if a arbitrary Object represents an ERROR {@link Signal}.
	 *
	 * @param o the object to check
	 * @return true if object represents the error signal
	 */
	static boolean isError(Object o) {
		return o instanceof Signal && ((Signal) o).getType() == SignalType.ON_ERROR;
	}

	/**
	 * Read the error associated with this (onError) signal.
	 *
	 * @return the Throwable associated with this (onError) signal, or null if not relevant
	 */
	@Nullable
	Throwable getThrowable();

	/**
	 * Read the subscription associated with this (onSubscribe) signal.
	 *
	 * @return the Subscription associated with this (onSubscribe) signal, or null if not
	 * relevant
	 */
	@Nullable
	Subscription getSubscription();

	/**
	 * Retrieves the item associated with this (onNext) signal.
	 *
	 * @return the item associated with this (onNext) signal, or null if not relevant
	 */
	@Override
	@Nullable
	T get();

	/**
	 * Has this signal an item associated with it ? (which only happens if it is an
	 * (onNext) signal)
	 *
	 * @return a boolean indicating whether or not this signal has an item associated with
	 * it
	 */
	default boolean hasValue() {
		return isOnNext() && get() != null;
	}

	/**
	 * Read whether this signal is on error and carries the cause.
	 *
	 * @return a boolean indicating whether this signal has an error
	 */
	default boolean hasError() {
		return isOnError() && getThrowable() != null;
	}

	/**
	 * Read the type of this signal: {@link SignalType#ON_SUBSCRIBE},
	 * {@link SignalType#ON_NEXT}, {@link SignalType#ON_ERROR}, or
	 * {@link SignalType#ON_COMPLETE}
	 *
	 * @return the type of the signal
	 */
	SignalType getType();

	/**
	 * Return the readonly {@link ContextView} that is accessible by the time this {@link Signal} was
	 * emitted.
	 *
	 * @return a readonly {@link ContextView}, or an empty one if no context is available.
	 */
	ContextView getContextView();

	/**
	 * Indicates whether this signal represents an {@code onError} event.
	 *
	 * @return a boolean indicating whether this signal represents an {@code onError}
	 * event
	 */
	default boolean isOnError() {
		return getType() == SignalType.ON_ERROR;
	}

	/**
	 * Indicates whether this signal represents an {@code onComplete} event.
	 *
	 * @return a boolean indicating whether this signal represents an {@code onComplete}
	 * event
	 */
	default boolean isOnComplete() {
		return getType() == SignalType.ON_COMPLETE;
	}

	/**
	 * Indicates whether this signal represents an {@code onSubscribe} event.
	 *
	 * @return a boolean indicating whether this signal represents an {@code onSubscribe}
	 * event
	 */
	default boolean isOnSubscribe() {
		return getType() == SignalType.ON_SUBSCRIBE;
	}

	/**
	 * Indicates whether this signal represents an {@code onNext} event.
	 *
	 * @return a boolean indicating whether this signal represents an {@code onNext} event
	 */
	default boolean isOnNext() {
		return getType() == SignalType.ON_NEXT;
	}

	/**
	 * Propagate the signal represented by this {@link Signal} instance to a
	 * given {@link Subscriber}.
	 *
	 * @param observer the {@link Subscriber} to play the {@link Signal} on
	 */
	@Override
	default void accept(Subscriber<? super T> observer) {
		if (isOnNext()) {
			observer.onNext(get());
		}
		else if (isOnComplete()) {
			observer.onComplete();
		}
		else if (isOnError()) {
			observer.onError(getThrowable());
		}
		else if (isOnSubscribe()) {
			observer.onSubscribe(getSubscription());
		}
	}
}

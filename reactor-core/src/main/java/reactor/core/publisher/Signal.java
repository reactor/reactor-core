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

package reactor.core.publisher;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import javax.annotation.Nullable;

/**
 * A domain representation of a Reactive Stream signal.
 * There are 4 distinct signals and their possible sequence is defined as such:
 * onError | (onSubscribe onNext* (onError | onComplete)?)
 *
 * @param <T> the value type
 *
 * @author Stephane Maldini
 */
public abstract class Signal<T> implements Supplier<T>, Consumer<Subscriber<? super T>> {

	//FIXME avoid loading subclass in superinterface?
	private static final Signal<Void> ON_COMPLETE =
			new ImmutableSignal<>(SignalType.ON_COMPLETE, null, null, null);

	/**
	 * Creates and returns a {@code Signal} of variety {@code Type.COMPLETE}.
	 *
	 * @param <T> the value type
	 *
	 * @return an {@code OnCompleted} variety of {@code Signal}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Signal<T> complete() {
		return (Signal<T>) ON_COMPLETE;
	}

	/**
	 * Creates and returns a {@code Signal} of variety {@code Type.FAILED}, which holds
	 * the error.
	 *
	 * @param <T> the value type
	 * @param e the error associated to the signal
	 *
	 * @return an {@code OnError} variety of {@code Signal}
	 */
	public static <T> Signal<T> error(Throwable e) {
		return new ImmutableSignal<>(SignalType.ON_ERROR, null, e, null);
	}

	/**
	 * Creates and returns a {@code Signal} of variety {@code Type.NEXT}, which holds
	 * the value.
	 *
	 * @param <T> the value type
	 * @param t the value item associated to the signal
	 *
	 * @return an {@code OnNext} variety of {@code Signal}
	 */
	public static <T> Signal<T> next(T t) {
		return new ImmutableSignal<>(SignalType.ON_NEXT, t, null, null);
	}

	/**
	 * Check if an arbitrary Object represents a COMPLETE {@link Signal}.
	 *
	 * @param o the object to check
	 * @return true if object represents the completion signal
	 */
	public static boolean isComplete(Object o) {
		return o == ON_COMPLETE;
	}

	/**
	 * Check if a arbitrary Object represents an ERROR {@link Signal}.
	 *
	 * @param o the object to check
	 * @return true if object represents the error signal
	 */
	public static boolean isError(Object o) {
		return o instanceof Signal && ((Signal) o).getType() == SignalType.ON_ERROR;
	}

	/**
	 * Creates and returns a {@code Signal} of variety {@code Type.ON_SUBSCRIBE}.
	 *
	 * @param <T> the value type
	 * @param subscription the subscription
	 *
	 * @return an {@code OnSubscribe} variety of {@code Signal}
	 */
	public static <T> Signal<T> subscribe(Subscription subscription) {
		return new ImmutableSignal<>(SignalType.ON_SUBSCRIBE, null, null, subscription);
	}

	/**
	 * Read the error associated with this (onError) signal.
	 *
	 * @return the Throwable associated with this (onError) signal, or null if not relevant
	 */
	@Nullable
	public abstract Throwable getThrowable();

	/**
	 * Read the subscription associated with this (onSubscribe) signal.
	 *
	 * @return the Subscription associated with this (onSubscribe) signal, or null if not
	 * relevant
	 */
	@Nullable
	public abstract Subscription getSubscription();

	/**
	 * Retrieves the item associated with this (onNext) signal.
	 *
	 * @return the item associated with this (onNext) signal, or null if not relevant
	 */
	@Override
	@Nullable
	public abstract T get();

	/**
	 * Has this signal an item associated with it ? (which only happens if it is an
	 * (onNext) signal)
	 *
	 * @return a boolean indicating whether or not this signal has an item associated with
	 * it
	 */
	public boolean hasValue() {
		return isOnNext() && get() != null;
	}

	/**
	 * Read whether this signal is on error and carries the cause.
	 *
	 * @return a boolean indicating whether this signal has an error
	 */
	public boolean hasError() {
		return isOnError() && getThrowable() != null;
	}

	/**
	 * Read the type of this signal: {@link SignalType#ON_SUBSCRIBE},
	 * {@link SignalType#ON_NEXT}, {@link SignalType#ON_ERROR}, or
	 * {@link SignalType#ON_COMPLETE}
	 *
	 * @return the type of the signal
	 */
	public abstract SignalType getType();

	/**
	 * Indicates whether this signal represents an {@code onError} event.
	 *
	 * @return a boolean indicating whether this signal represents an {@code onError}
	 * event
	 */
	public boolean isOnError() {
		return getType() == SignalType.ON_ERROR;
	}

	/**
	 * Indicates whether this signal represents an {@code onComplete} event.
	 *
	 * @return a boolean indicating whether this signal represents an {@code onSubscribe}
	 * event
	 */
	public boolean isOnComplete() {
		return getType() == SignalType.ON_COMPLETE;
	}

	/**
	 * Indicates whether this signal represents an {@code onSubscribe} event.
	 *
	 * @return a boolean indicating whether this signal represents an {@code onSubscribe}
	 * event
	 */
	public boolean isOnSubscribe() {
		return getType() == SignalType.ON_SUBSCRIBE;
	}

	/**
	 * Indicates whether this signal represents an {@code onNext} event.
	 *
	 * @return a boolean indicating whether this signal represents an {@code onNext} event
	 */
	public boolean isOnNext() {
		return getType() == SignalType.ON_NEXT;
	}

	/**
	 * Propagate the signal represented by this {@link Signal} instance to a
	 * given {@link Subscriber}.
	 *
	 * @param observer the {@link Subscriber} to play the {@link Signal} on
	 */
	@Override
	public void accept(Subscriber<? super T> observer) {
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

	//the base class defines equals and hashcode as final in order to allow
	//concrete implementations to be compared together, and discourage them
	//to implement additional state.
	@Override
	public final boolean equals(@Nullable Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || !(o instanceof Signal)) {
			return false;
		}

		Signal<?> signal = (Signal<?>) o;

		if (getType() != signal.getType()) {
			return false;
		}
		if (isOnComplete()) {
			return true;
		}
		if (isOnSubscribe()) {
			return Objects.equals(this.getSubscription(), signal.getSubscription());
		}
		if (isOnError()) {
			return Objects.equals(this.getThrowable(), signal.getThrowable());
		}
		if (isOnNext()) {
			return Objects.equals(this.get(), signal.get());
		}
		return false;
	}

	@Override
	public final int hashCode() {
		int result = getType().hashCode();
		if (isOnError()) {
			return  31 * result + (getThrowable() != null ? getThrowable().hashCode() :
					0);
		}
		if (isOnNext()) {
			//noinspection ConstantConditions
			return  31 * result + (get() != null ? get().hashCode() : 0);
		}
		if (isOnSubscribe()) {
			return  31 * result + (getSubscription() != null ?
					getSubscription().hashCode() : 0);
		}
		return result;
	}

	@Override
	public String toString() {
		switch (this.getType()) {
			case ON_SUBSCRIBE:
				return String.format("onSubscribe(%s)", this.getSubscription());
			case ON_NEXT:
				return String.format("onNext(%s)", this.get());
			case ON_ERROR:
				return String.format("onError(%s)", this.getThrowable());
			case ON_COMPLETE:
				return "onComplete()";
			default:
				return String.format("Signal type=%s", this.getType());
		}
	}
}

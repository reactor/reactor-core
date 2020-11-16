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

package reactor.core.publisher;

import java.io.Serializable;
import java.util.Objects;

import org.reactivestreams.Subscription;

import reactor.util.annotation.Nullable;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

/**
 * The common implementation of a {@link Signal} (serializable and immutable).
 * Use Signal factory methods to create an instance.
 * <p>
 * Associated {@link Context} are not serialized.
 *
 * @author Stephane Maldini
 * @author Simon Baslé
 */
final class ImmutableSignal<T> implements Signal<T>, Serializable {

	private static final long serialVersionUID = -2004454746525418508L;

	private final transient ContextView contextView;

	private final SignalType type;
	private final Throwable  throwable;

	private final T value;

	private transient final Subscription subscription;

	ImmutableSignal(ContextView contextView, SignalType type, @Nullable T value, @Nullable Throwable e, @Nullable Subscription subscription) {
		this.contextView = contextView;
		this.value = value;
		this.subscription = subscription;
		this.throwable = e;
		this.type = type;
	}

	@Override
	@Nullable
	public Throwable getThrowable() {
		return throwable;
	}

	@Override
	@Nullable
	public Subscription getSubscription() {
		return subscription;
	}

	@Override
	@Nullable
	public T get() {
		return value;
	}

	@Override
	public SignalType getType() {
		return type;
	}

	@Override
	public ContextView getContextView() {
		return contextView;
	}

	@Override
	public boolean equals(@Nullable Object o) {
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
	public int hashCode() {
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

	private static final Signal<?> ON_COMPLETE =
			new ImmutableSignal<>(Context.empty(), SignalType.ON_COMPLETE, null, null, null);

	/**
	 * @return a singleton signal to signify onComplete.
	 * As Signal is now associated with {@link Context}, prefer using per-subscription instances.
	 * This instance is used when context doesn't matter.
	 */
	@SuppressWarnings("unchecked")
	static <U> Signal<U> onComplete() {
		return (Signal<U>) ON_COMPLETE;
	}
}

/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.publisher;

import java.io.Serializable;

import org.reactivestreams.Subscription;

/**
 * The common implementation of a {@link Signal} (serializable and immutable).
 * Use Signal factory methods to create an instance.
 *
 * @author Stephane Maldini
 * @author Simon Baslé
 */
final class ImmutableSignal<T> extends Signal<T> implements Serializable {

	private static final long serialVersionUID = -2004454746525418508L;

	private final SignalType type;
	private final Throwable  throwable;

	private final T value;

	private transient final Subscription subscription;

	ImmutableSignal(SignalType type, T value, Throwable e, Subscription subscription) {
		this.value = value;
		this.subscription = subscription;
		this.throwable = e;
		this.type = type;
	}

	@Override
	public Throwable getThrowable() {
		return throwable;
	}

	@Override
	public Subscription getSubscription() {
		return subscription;
	}

	@Override
	public T get() {
		return value;
	}

	@Override
	public SignalType getType() {
		return type;
	}

}
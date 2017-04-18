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

import java.util.function.Consumer;

import reactor.core.Disposable;
import reactor.core.Fuseable;

/**
 * The abstract base class for connectable publishers that let subscribers pile up
 * before they connect to their data source.
 *
 * @see #publish
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 * @param <T> the input and output value type
 */
public abstract class ConnectableFlux<T> extends Flux<T> {

	/**
	 * Connects this {@link ConnectableFlux} to the upstream source when the first {@link org.reactivestreams.Subscriber}
	 * subscribes.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/autoconnect.png" alt="">
	 *
	 * @return a {@link Flux} that connects to the upstream source when the first {@link org.reactivestreams.Subscriber} subscribes
	 */
	public final Flux<T> autoConnect() {
		return autoConnect(1);
	}

	/**
	 * Connects this {@link ConnectableFlux} to the upstream source when the specified amount of
	 * {@link org.reactivestreams.Subscriber} subscribes.
	 * <p>
	 * Subscribing and immediately unsubscribing still contributes to the counter that
	 * triggers the connection.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/autoconnect.png" alt="">
	 *
	 * @param minSubscribers the minimum number of subscribers
	 *
	 * @return a {@link Flux} that connects to the upstream source when the given amount of Subscribers subscribed
	 */
	public final Flux<T> autoConnect(int minSubscribers) {
		return autoConnect(minSubscribers, NOOP_DISCONNECT);
	}

	/**
	 * Connects this {@link ConnectableFlux} to the upstream source when the specified amount of
	 * {@link org.reactivestreams.Subscriber} subscribes and calls the supplied consumer with a runnable that allows disconnecting.
	 * @param minSubscribers the minimum number of subscribers
	 * @param cancelSupport the consumer that will receive the {@link Disposable} that allows disconnecting
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/autoconnect.png" alt="">
	 *
	 * @return a {@link Flux} that connects to the upstream source when the given amount of subscribers subscribed
	 */
	public final Flux<T> autoConnect(int minSubscribers, Consumer<? super Disposable> cancelSupport) {
		if (minSubscribers == 0) {
			connect(cancelSupport);
			return this;
		}
		if(this instanceof Fuseable){
			return onAssembly(new FluxAutoConnectFuseable<>(this,
					minSubscribers,
					cancelSupport));
		}
		return onAssembly(new FluxAutoConnect<>(this, minSubscribers,
				cancelSupport));
	}

	/**
	 * Connect this {@link ConnectableFlux} to its source and return a {@link Runnable} that
	 * can be used for disconnecting.
	 * @return the {@link Disposable} that allows disconnecting the connection after.
	 */
	public final Disposable connect() {
		final Disposable[] out = { null };
		connect(r -> out[0] = r);
		return out[0];
	}

	/**
	 * Connects this {@link ConnectableFlux} to its source and sends a {@link Disposable} to a callback that
	 * can be used for disconnecting.
	 *
	 * <p>The call should be idempotent in respect of connecting the first
	 * and subsequent times. In addition the disconnection should be also tied
	 * to a particular connection (so two different connection can't disconnect the other).
	 *
	 * @param cancelSupport the callback is called with a Disposable instance that can
	 * be called to disconnect the source, even synchronously.
	 */
	public abstract void connect(Consumer<? super Disposable> cancelSupport);

	/**
	 * Connects to the upstream source when the first {@link org.reactivestreams.Subscriber} subscribes and disconnects
	 * when all Subscribers cancelled or the upstream source completed.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/refCount.png" alt="">
	 *
	 * @return a reference counting {@link Flux}
	 */
	public final Flux<T> refCount() {
		return refCount(1);
	}

	/**
	 * Connects to the upstream source when the given number of {@link org.reactivestreams.Subscriber} subscribes and disconnects
	 * when all Subscribers cancelled or the upstream source completed.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/refCount.png" alt="">
	 *
	 * @param minSubscribers the number of subscribers expected to subscribe before connection
	 *
	 * @return a reference counting {@link Flux}
	 */
	public final Flux<T> refCount(int minSubscribers) {
		return onAssembly(new FluxRefCount<>(this, minSubscribers));
	}

	static final Consumer<Disposable> NOOP_DISCONNECT = runnable -> {

	};
}

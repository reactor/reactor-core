/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.time.Duration;
import java.util.function.Consumer;

import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

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
	 * <img class="marble" src="doc-files/marbles/autoConnect.svg" alt="">
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
	 * <img class="marble" src="doc-files/marbles/autoConnectWithMinSubscribers.svg" alt="">
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
	 * {@link org.reactivestreams.Subscriber} subscribes and calls the supplied consumer with a {@link Disposable}
	 * that allows disconnecting.
	 * <p>
	 * <img class="marble" src="doc-files/marbles/autoConnectWithMinSubscribers.svg" alt="">
	 *
	 * @param minSubscribers the minimum number of subscribers
	 * @param cancelSupport the consumer that will receive the {@link Disposable} that allows disconnecting
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
	 * Connect this {@link ConnectableFlux} to its source and return a {@link Disposable} that
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
	 * and subsequent times. In addition, the disconnection should be tied
	 * to a particular connection (so two different connections can't disconnect the
	 * other).
	 *
	 * @param cancelSupport the callback is called with a Disposable instance that can
	 * be called to disconnect the source, even synchronously.
	 */
	public abstract void connect(Consumer<? super Disposable> cancelSupport);

	@Override
	public final ConnectableFlux<T> hide() {
		return new ConnectableFluxHide<>(this);
	}

	/**
	 * Connects to the upstream source when the first {@link org.reactivestreams.Subscriber} subscribes and disconnects
	 * when all Subscribers cancelled or the upstream source completed.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/refCount.svg" alt="">
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
	 * <img class="marble" src="doc-files/marbles/refCountWithMinSubscribers.svg" alt="">
	 *
	 * @param minSubscribers the number of subscribers expected to subscribe before connection
	 *
	 * @return a reference counting {@link Flux}
	 */
	public final Flux<T> refCount(int minSubscribers) {
		return onAssembly(new FluxRefCount<>(this, minSubscribers));
	}

	/**
	 * Connects to the upstream source when the given number of {@link org.reactivestreams.Subscriber} subscribes.
	 * Disconnection can happen in two scenarios: when the upstream source completes (or errors) then
	 * there is an immediate disconnection. However, when all subscribers have cancelled,
	 * a <strong>deferred</strong> disconnection is scheduled. If any new subscriber comes
	 * in during the {@code gracePeriod} that follows, the disconnection is cancelled.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/refCountWithMinSubscribersAndGracePeriod.svg" alt="">
	 *
	 * @param minSubscribers the number of subscribers expected to subscribe before connection
	 * @param gracePeriod the {@link Duration} for which to wait for new subscribers before actually
	 * disconnecting when all subscribers have cancelled.
	 *
	 * @return a reference counting {@link Flux} with a grace period for disconnection
	 */
	public final Flux<T> refCount(int minSubscribers, Duration gracePeriod) {
		return refCount(minSubscribers, gracePeriod, Schedulers.parallel());
	}

	/**
	 * Connects to the upstream source when the given number of {@link org.reactivestreams.Subscriber} subscribes.
	 * Disconnection can happen in two scenarios: when the upstream source completes (or errors) then
	 * there is an immediate disconnection. However, when all subscribers have cancelled,
	 * a <strong>deferred</strong> disconnection is scheduled. If any new subscriber comes
	 * in during the {@code gracePeriod} that follows, the disconnection is cancelled.
	 *
	 * <p>
	 * <img class="marble" src="doc-files/marbles/refCountWithMinSubscribersAndGracePeriod.svg" alt="">
	 *
	 * @param minSubscribers the number of subscribers expected to subscribe before connection
	 * @param gracePeriod the {@link Duration} for which to wait for new subscribers before actually
	 * disconnecting when all subscribers have cancelled.
	 * @param scheduler the {@link Scheduler} on which to run timeouts
	 *
	 * @return a reference counting {@link Flux} with a grace period for disconnection
	 */
	public final Flux<T> refCount(int minSubscribers, Duration gracePeriod, Scheduler scheduler) {
		return onAssembly(new FluxRefCountGrace<>(this, minSubscribers, gracePeriod, scheduler));
	}

	static final Consumer<Disposable> NOOP_DISCONNECT = runnable -> {

	};
}

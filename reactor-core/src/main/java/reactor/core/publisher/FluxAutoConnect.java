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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * Connects to the underlying Flux once the given amount of Subscribers
 * subscribed.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxAutoConnect<T> extends Flux<T>
		implements Scannable {

	final ConnectableFlux<? extends T> source;

	final Consumer<? super Disposable> cancelSupport;

	volatile int remaining;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<FluxAutoConnect> REMAINING =
			AtomicIntegerFieldUpdater.newUpdater(FluxAutoConnect.class, "remaining");


	FluxAutoConnect(ConnectableFlux<? extends T> source,
			int n, Consumer<? super Disposable> cancelSupport) {
		if (n <= 0) {
			throw new IllegalArgumentException("n > required but it was " + n);
		}
		this.source = Objects.requireNonNull(source, "source");
		this.cancelSupport = Objects.requireNonNull(cancelSupport, "cancelSupport");
		REMAINING.lazySet(this, n);
	}
	
	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		source.subscribe(actual);
		if (remaining > 0 && REMAINING.decrementAndGet(this) == 0) {
			source.connect(cancelSupport);
		}
	}

	@Override
	public int getPrefetch() {
		return source.getPrefetch();
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.PARENT) return source;
		if (key == Attr.CAPACITY) return remaining;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}
}

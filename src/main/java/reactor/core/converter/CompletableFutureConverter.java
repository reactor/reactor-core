/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.converter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.Exceptions;

/**
 * Convert a Java 8+ {@link CompletableFuture} to/from a Reactive Streams {@link Publisher}.
 *
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 * @since 2.5
 */
public final class CompletableFutureConverter{

	static final CompletableFutureConverter INSTANCE = new CompletableFutureConverter();

	@SuppressWarnings("unchecked")
	static public <T> CompletableFuture<T> from(Publisher<T> o) {
		return INSTANCE.fromPublisher(o);
	}

	@SuppressWarnings("unchecked")
	static public <T> CompletableFuture<T> fromSingle(Publisher<T> o) {
		return INSTANCE.fromSinglePublisher(o);
	}

	<T> CompletableFuture<T> completableFuture(final AtomicReference<Subscription> ref) {
		return new CompletableFuture<T>() {
			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				boolean cancelled = super.cancel(mayInterruptIfRunning);
				if (cancelled) {
					Subscription s = ref.getAndSet(null);
					if (s != null) {
						s.cancel();
					}
				}
				return cancelled;
			}
		};
	}

	CompletableFuture fromPublisher(Publisher<?> pub) {
		final AtomicReference<Subscription> ref = new AtomicReference<>();
		final CompletableFuture<List<Object>> future = completableFuture(ref);

		pub.subscribe(new Subscriber<Object>() {
			private List<Object> values = null;

			@Override
			public void onSubscribe(Subscription s) {
				if (BackpressureUtils.validate(ref.getAndSet(s), s)) {
					s.request(Long.MAX_VALUE);
				}
				else {
					s.cancel();
				}
			}

			@Override
			public void onNext(Object t) {
				if (values == null) {
					values = new ArrayList<>();
				}
				values.add(t);
			}

			@Override
			public void onError(Throwable t) {
				if (ref.getAndSet(null) != null) {
					future.completeExceptionally(t);
				}
			}

			@Override
			@SuppressWarnings("unchecked")
			public void onComplete() {
				if (ref.getAndSet(null) != null) {
					future.complete(values);
				}
			}
		});
		return future;
	}

	CompletableFuture fromSinglePublisher(Publisher<?> pub) {
		final AtomicReference<Subscription> ref = new AtomicReference<>();
		final CompletableFuture<Object> future = completableFuture(ref);

		pub.subscribe(new Subscriber<Object>() {
			@Override
			public void onSubscribe(Subscription s) {
				if (BackpressureUtils.validate(ref.getAndSet(s), s)) {
					s.request(Long.MAX_VALUE);
				}
				else {
					s.cancel();
				}
			}

			@Override
			public void onNext(Object t) {
				Subscription s = ref.getAndSet(null);
				if (s != null) {
					future.complete(t);
					s.cancel();
				}
				else {
					Exceptions.onNextDropped(t);
				}
			}

			@Override
			public void onError(Throwable t) {
				if (ref.getAndSet(null) != null) {
					future.completeExceptionally(t);
				}
			}

			@Override
			@SuppressWarnings("unchecked")
			public void onComplete() {
				if (ref.getAndSet(null) != null) {
					future.complete(null);
				}
			}
		});
		return future;
	}

}
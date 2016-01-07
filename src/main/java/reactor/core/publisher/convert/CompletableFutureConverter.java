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

package reactor.core.publisher.convert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Flux;
import reactor.Mono;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.publisher.MonoJust;
import reactor.core.subscriber.SubscriberWithContext;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.BackpressureUtils;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;

/**
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 */
public final class CompletableFutureConverter extends PublisherConverter<CompletableFuture> {

	static final CompletableFutureConverter INSTANCE = new CompletableFutureConverter();

	@SuppressWarnings("unchecked")
	static public <T> CompletableFuture<T> from(Publisher<T> o) {
		return INSTANCE.fromPublisher(o);
	}

	@SuppressWarnings("unchecked")
	static public <T> CompletableFuture<T> fromSingle(Publisher<T> o) {
		return INSTANCE.fromSinglePublisher(o);
	}

	@SuppressWarnings("unchecked")
	static public <T> Publisher<T> from(CompletableFuture<T> o) {
		return INSTANCE.toPublisher(o);
	}

	@Override
	public CompletableFuture fromPublisher(Publisher<?> pub) {
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

	private <T> CompletableFuture<T> completableFuture(final AtomicReference<Subscription> ref) {
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

	public CompletableFuture fromSinglePublisher(Publisher<?> pub) {
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

	@Override
	@SuppressWarnings("unchecked")
	public Mono toPublisher(Object future) {
		return new MonoCompletableFuture<>((CompletableFuture<?>) future);
	}

	@Override
	public Class<CompletableFuture> get() {
		return CompletableFuture.class;
	}

	private static class MonoCompletableFuture<T> extends Mono<T>
			implements Consumer<Void>, BiConsumer<Long, SubscriberWithContext<T, Void>> {

		private final CompletableFuture<? extends T> future;
		private final Publisher<? extends T>         futurePublisher;

		@SuppressWarnings("unused")
		private volatile long requested;
		private static final AtomicLongFieldUpdater<MonoCompletableFuture> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(MonoCompletableFuture.class, "requested");

		public MonoCompletableFuture(CompletableFuture<? extends T> future) {
			this.future = future;
			this.futurePublisher = Flux.generate(this, null, this);
		}

		@Override
		public void accept(Long n, final SubscriberWithContext<T, Void> sub) {
			if (!BackpressureUtils.checkRequest(n, sub)) {
				return;
			}

			if (BackpressureUtils.getAndAdd(REQUESTED, MonoCompletableFuture.this, n) > 0) {
				return;
			}

			future.whenComplete(new java.util.function.BiConsumer<T, Throwable>() {
				@Override
				public void accept(T result, Throwable error) {
					if (error != null) {
						sub.onError(error);
					}
					else {
						sub.onNext(result);
						sub.onComplete();
					}
				}
			});
		}

		@Override
		public void accept(Void aVoid) {
			if (!future.isDone()) {
				future.cancel(true);
			}
		}

		@Override
		public void subscribe(final Subscriber<? super T> subscriber) {
			try {
				if (future.isDone()) {
					new MonoJust<>(future.get()).subscribe(subscriber);
				}
				else if (future.isCancelled()) {
					EmptySubscription.error(subscriber, CancelException.get());
				}
				else {
					futurePublisher.subscribe(subscriber);
				}
			}
			catch (Throwable throwable) {
				EmptySubscription.error(subscriber, throwable);
			}
		}
	}
}
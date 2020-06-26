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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.util.annotation.Nullable;

/**
 * Subscribes to the source Publisher asynchronously through a scheduler function or
 * ExecutorService.
 * 
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxSubscribeOn<T> extends InternalFluxOperator<T, T> {

	final Scheduler scheduler;
	final boolean requestOnSeparateThread;

	FluxSubscribeOn(
			Flux<? extends T> source,
			Scheduler scheduler,
			boolean requestOnSeparateThread) {
		super(source);
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
		this.requestOnSeparateThread = requestOnSeparateThread;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		Worker worker = Objects.requireNonNull(scheduler.createWorker(),
				"The scheduler returned a null Function");

		SubscribeOnSubscriber<T> parent = new SubscribeOnSubscriber<>(source,
				actual, worker, requestOnSeparateThread);
		actual.onSubscribe(parent);

		try {
			worker.schedule(parent);
		}
		catch (RejectedExecutionException ree) {
			if (parent.s != Operators.cancelledSubscription()) {
				actual.onError(Operators.onRejectedExecution(ree, parent, null, null,
						actual.currentContext()));
			}
		}
		return null;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_ON) return scheduler;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;
		return super.scanUnsafe(key);
	}

	static final class SubscribeOnSubscriber<T> implements InnerOperator<T, T>, Runnable {

		final CoreSubscriber<? super T> actual;

		final CorePublisher<? extends T> source;

		final Worker  worker;
		final boolean requestOnSeparateThread;

		volatile Subscription s;
		static final AtomicReferenceFieldUpdater<SubscribeOnSubscriber, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(SubscribeOnSubscriber.class,
						Subscription.class,
						"s");


		volatile long requested;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<SubscribeOnSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(SubscribeOnSubscriber.class,
						"requested");

		volatile Thread thread;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SubscribeOnSubscriber, Thread> THREAD =
				AtomicReferenceFieldUpdater.newUpdater(SubscribeOnSubscriber.class,
						Thread.class,
						"thread");

		SubscribeOnSubscriber(CorePublisher<? extends T> source, CoreSubscriber<? super T> actual,
				Worker worker, boolean requestOnSeparateThread) {
			this.actual = actual;
			this.worker = worker;
			this.source = source;
			this.requestOnSeparateThread = requestOnSeparateThread;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				long r = REQUESTED.getAndSet(this, 0L);
				if (r != 0L) {
					requestUpstream(r, s);
				}
			}
		}

		void requestUpstream(final long n, final Subscription s) {
			if (!requestOnSeparateThread || Thread.currentThread() == THREAD.get(this)) {
				s.request(n);
			}
			else {
				try {
					worker.schedule(() -> s.request(n));
				}
				catch (RejectedExecutionException ree) {
					if(!worker.isDisposed()) {
						//FIXME should not throw but if we implement strict
						// serialization like in StrictSubscriber, onNext will carry an
						// extra cost
						throw Operators.onRejectedExecution(ree, this, null, null,
								actual.currentContext());
					}
				}
			}
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			try {
				actual.onError(t);
			}
			finally {
				worker.dispose();
			}
		}

		@Override
		public void onComplete() {
			actual.onComplete();
			worker.dispose();
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Subscription s = S.get(this);
				if (s != null) {
					requestUpstream(n, s);
				}
				else {
					Operators.addCap(REQUESTED, this, n);
					s = S.get(this);
					if (s != null) {
						long r = REQUESTED.getAndSet(this, 0L);
						if (r != 0L) {
							requestUpstream(r, s);
						}
					}

				}
			}
		}

		@Override
		public void run() {
			THREAD.lazySet(this, Thread.currentThread());
			source.subscribe(this);
		}

		@Override
		public void cancel() {
			Subscription a = s;
			if (a != Operators.cancelledSubscription()) {
				a = S.getAndSet(this, Operators.cancelledSubscription());
				if (a != null && a != Operators.cancelledSubscription()) {
					a.cancel();
				}
			}
			worker.dispose();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.RUN_ON) return worker;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

	}

}

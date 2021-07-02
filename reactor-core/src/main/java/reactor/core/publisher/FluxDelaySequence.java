/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.scheduler.Scheduler;

/**
 * @author Simon Baslé
 */
//adapted from RxJava2 FlowableDelay: https://github.com/ReactiveX/RxJava/blob/2.x/src/main/java/io/reactivex/internal/operators/flowable/FlowableDelay.java
final class FluxDelaySequence<T> extends InternalFluxOperator<T, T> {

	final Duration  delay;
	final Scheduler scheduler;

	FluxDelaySequence(Flux<T> source, Duration delay, Scheduler scheduler) {
		super(source);
		this.delay = delay;
		this.scheduler = scheduler;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		Scheduler.Worker w = scheduler.createWorker();

		return new DelaySubscriber<T>(actual, delay, w);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_ON) return scheduler;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

		return super.scanUnsafe(key);
	}

	static final class DelaySubscriber<T> implements InnerOperator<T, T> {

		final CoreSubscriber<? super T> actual;
		final long delay;
		final TimeUnit timeUnit;
		final Scheduler.Worker w;

		Subscription s;

		volatile boolean done;

		volatile long delayed;
		static final AtomicLongFieldUpdater<DelaySubscriber> DELAYED =
				AtomicLongFieldUpdater.newUpdater(DelaySubscriber.class, "delayed");


		DelaySubscriber(CoreSubscriber<? super T> actual, Duration delay, Scheduler.Worker w) {
			super();
			this.actual = Operators.serialize(actual);
			this.w = w;
			this.delay = delay.toNanos();
			this.timeUnit = TimeUnit.NANOSECONDS;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(final T t) {
			if (done || delayed < 0) {
				Operators.onNextDropped(t, currentContext());
				return;
			}
			//keep track of the number of delayed onNext so that
			//we can also delay onError/onComplete when an onNext
			//is "in flight"
			DELAYED.incrementAndGet(this);
			w.schedule(() -> delayedNext(t), delay, timeUnit);
		}

		private void delayedNext(T t) {
			//this onNext has been delayed and now processed
			DELAYED.decrementAndGet(this);
			actual.onNext(t);
		}

		@Override
		public void onError(final Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, currentContext());
				return;
			}
			done = true;
			//if no currently delayed onNext (eg. empty source),
			// we can immediately error
			if (DELAYED.compareAndSet(this, 0, -1)) {
				actual.onError(t);
			}
			else {
				w.schedule(new OnError(t), delay, timeUnit);
			}
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			//if no currently delayed onNext (eg. empty source),
			// we can immediately complete
			if (DELAYED.compareAndSet(this, 0, -1)) {
				actual.onComplete();
			}
			else {
				w.schedule(new OnComplete(), delay, timeUnit);
			}
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
			w.dispose();
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.RUN_ON) return w;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.CANCELLED) return w.isDisposed() && !done;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		final class OnError implements Runnable {
			private final Throwable t;

			OnError(Throwable t) {
				this.t = t;
			}

			@Override
			public void run() {
				try {
					actual.onError(t);
				} finally {
					w.dispose();
				}
			}
		}

		final class OnComplete implements Runnable {
			@Override
			public void run() {
				try {
					actual.onComplete();
				} finally {
					w.dispose();
				}
			}
		}
	}
}
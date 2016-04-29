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
package reactor.core.publisher;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.DeferredSubscription;
import reactor.core.util.Exceptions;

/**
 * Takes a value from upstream then uses the duration provided by a 
 * generated Publisher to skip other values until that other Publisher signals.
 *
 * @param <T> the source and output value type
 * @param <U> the value type of the publisher signalling the end of the throttling duration
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
final class FluxThrottleFirst<T, U> extends FluxSource<T, T> {

	final Function<? super T, ? extends Publisher<U>> throttler;

	public FluxThrottleFirst(Publisher<? extends T> source,
			Function<? super T, ? extends Publisher<U>> throttler) {
		super(source);
		this.throttler = Objects.requireNonNull(throttler, "throttler");
	}
	
	@Override
	public void subscribe(Subscriber<? super T> s) {
		ThrottleFirstMain<T, U> main = new ThrottleFirstMain<>(s, throttler);
		
		s.onSubscribe(main);
		
		source.subscribe(main);
	}
	
	static final class ThrottleFirstMain<T, U> 
	implements Subscriber<T>, Subscription {

		final Subscriber<? super T> actual;
		
		final Function<? super T, ? extends Publisher<U>> throttler;
		
		volatile boolean gate;
		
		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ThrottleFirstMain, Subscription> S =
			AtomicReferenceFieldUpdater.newUpdater(ThrottleFirstMain.class, Subscription.class, "s");

		volatile Subscription other;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ThrottleFirstMain, Subscription> OTHER =
			AtomicReferenceFieldUpdater.newUpdater(ThrottleFirstMain.class, Subscription.class, "other");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ThrottleFirstMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(ThrottleFirstMain.class, "requested");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ThrottleFirstMain> WIP =
				AtomicIntegerFieldUpdater.newUpdater(ThrottleFirstMain.class, "wip");

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ThrottleFirstMain, Throwable> ERROR =
			AtomicReferenceFieldUpdater.newUpdater(ThrottleFirstMain.class, Throwable.class, "error");

		public ThrottleFirstMain(Subscriber<? super T> actual,
				Function<? super T, ? extends Publisher<U>> throttler) {
			this.actual = actual;
			this.throttler = throttler;
		}

		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				BackpressureUtils.getAndAddCap(REQUESTED, this, n);
			}
		}

		@Override
		public void cancel() {
			BackpressureUtils.terminate(S, this);
			BackpressureUtils.terminate(OTHER, this);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			if (!gate) {
				gate = true;
				
				if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
					actual.onNext(t);
					if (WIP.decrementAndGet(this) != 0) {
						handleTermination();
						return;
					}
				} else {
					return;
				}
				
				Publisher<U> p;
				
				try {
					p = throttler.apply(t);
				} catch (Throwable e) {
					BackpressureUtils.terminate(S, this);
					Exceptions.throwIfFatal(e);
					error(Exceptions.unwrap(e));
					return;
				}
				
				if (p == null) {
					BackpressureUtils.terminate(S, this);
					
					error(new NullPointerException("The throttler returned a null publisher"));
					return;
				}
				
				ThrottleFirstOther<U> other = new ThrottleFirstOther<>(this);
				
				if (BackpressureUtils.replace(OTHER, this, other)) {
					p.subscribe(other);
				}
			}
		}

		void handleTermination() {
			Throwable e = Exceptions.terminate(ERROR, this);
			if (e != null && e != Exceptions.TERMINATED) {
				actual.onError(e);
			} else {
				actual.onComplete();
			}
		}
		
		void error(Throwable e) {
			if (Exceptions.addThrowable(ERROR, this, e)) {
				if (WIP.getAndIncrement(this) == 0) {
					handleTermination();
				}
			} else {
				Exceptions.onErrorDropped(e);
			}
		}
		
		@Override
		public void onError(Throwable t) {
			BackpressureUtils.terminate(OTHER, this);
			
			error(t);
		}

		@Override
		public void onComplete() {
			BackpressureUtils.terminate(OTHER, this);
			
			if (WIP.getAndIncrement(this) == 0) {
				handleTermination();
			}
		}
		
		void otherNext() {
			gate = false;
		}
		
		void otherError(Throwable e) {
			BackpressureUtils.terminate(S, this);
			
			error(e);
		}
	}
	
	static final class ThrottleFirstOther<U>
	extends DeferredSubscription
	implements Subscriber<U> {

		final ThrottleFirstMain<?, U> main;
		
		public ThrottleFirstOther(ThrottleFirstMain<?, U> main) {
			this.main = main;
		}
		
		@Override
		public void onSubscribe(Subscription s) {
			if (set(s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(U t) {
			cancel();
			
			main.otherNext();
		}

		@Override
		public void onError(Throwable t) {
			main.otherError(t);
		}

		@Override
		public void onComplete() {
			main.otherNext();
		}
		
	}
}

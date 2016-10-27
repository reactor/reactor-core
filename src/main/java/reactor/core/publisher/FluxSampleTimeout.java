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
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;

/**
 * Emits the last value from upstream only if there were no newer values emitted
 * during the time window provided by a publisher for that particular last value.
 *
 * @param <T> the source value type
 * @param <U> the value type of the duration publisher
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxSampleTimeout<T, U> extends FluxSource<T, T> {

	final Function<? super T, ? extends Publisher<U>> throttler;
	
	final Supplier<Queue<Object>> queueSupplier;

	public FluxSampleTimeout(Publisher<? extends T> source,
			Function<? super T, ? extends Publisher<U>> throttler,
					Supplier<Queue<Object>> queueSupplier) {
		super(source);
		this.throttler = Objects.requireNonNull(throttler, "throttler");
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
	}

	@Override
	public long getPrefetch() {
		return Long.MAX_VALUE;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void subscribe(Subscriber<? super T> s) {
		
		Queue<ThrottleTimeoutOther<T, U>> q;
		
		try {
			q = (Queue)queueSupplier.get();
		} catch (Throwable e) {
			Operators.error(s, Operators.onOperatorError(e));
			return;
		}
		
		if (q == null) {
			Operators.error(s, Operators.onOperatorError(new
					NullPointerException("The queueSupplier returned a null queue")));
			return;
		}
		
		ThrottleTimeoutMain<T, U> main = new ThrottleTimeoutMain<>(s, throttler, q);
		
		s.onSubscribe(main);
		
		source.subscribe(main);
	}
	
	static final class ThrottleTimeoutMain<T, U>
	implements Subscriber<T>, Subscription {
		
		final Subscriber<? super T> actual;
		
		final Function<? super T, ? extends Publisher<U>> throttler;
		
		final Queue<ThrottleTimeoutOther<T, U>> queue;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ThrottleTimeoutMain, Subscription> S =
			AtomicReferenceFieldUpdater.newUpdater(ThrottleTimeoutMain.class, Subscription.class, "s");

		volatile Subscription other;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ThrottleTimeoutMain, Subscription> OTHER =
			AtomicReferenceFieldUpdater.newUpdater(ThrottleTimeoutMain.class, Subscription.class, "other");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ThrottleTimeoutMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(ThrottleTimeoutMain.class, "requested");

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ThrottleTimeoutMain> WIP =
				AtomicIntegerFieldUpdater.newUpdater(ThrottleTimeoutMain.class, "wip");

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ThrottleTimeoutMain, Throwable> ERROR =
			AtomicReferenceFieldUpdater.newUpdater(ThrottleTimeoutMain.class, Throwable.class, "error");

		volatile boolean done;
		
		volatile boolean cancelled;
		
		volatile long index;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<ThrottleTimeoutMain> INDEX =
				AtomicLongFieldUpdater.newUpdater(ThrottleTimeoutMain.class, "index");

		public ThrottleTimeoutMain(Subscriber<? super T> actual,
				Function<? super T, ? extends Publisher<U>> throttler,
						Queue<ThrottleTimeoutOther<T, U>> queue) {
			this.actual = actual;
			this.throttler = throttler;
			this.queue = queue;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;
				Operators.terminate(S, this);
				Operators.terminate(OTHER, this);
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			long idx = INDEX.incrementAndGet(this);

			if (!Operators.set(OTHER, this, Operators.emptySubscription())) {
				return;
			}
			
			Publisher<U> p;
			
			try {
				p = throttler.apply(t);
			} catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));
				return;
			}

			if (p == null) {
				onError(Operators.onOperatorError(s, new NullPointerException("The " +
						"throttler returned a null publisher"), t));
				return;
			}
			
			ThrottleTimeoutOther<T, U> os = new ThrottleTimeoutOther<>(this, t, idx);
			
			if (Operators.replace(OTHER, this, os)) {
				p.subscribe(os);
			}
		}

		void error(Throwable t) {
			if (Exceptions.addThrowable(ERROR, this, t)) {
				done = true;
				drain();
			} else {
				Operators.onErrorDropped(t);
			}
		}
		
		@Override
		public void onError(Throwable t) {
			Operators.terminate(OTHER, this);
			
			error(t);
		}

		@Override
		public void onComplete() {
			Subscription o = other;
			if (o instanceof ThrottleTimeoutOther) {
				ThrottleTimeoutOther<?, ?> os = (ThrottleTimeoutOther<?, ?>) o;
				os.cancel();
				os.onComplete();
			}
			done = true;
			drain();
		}
		
		void otherNext(ThrottleTimeoutOther<T, U> other) {
			queue.offer(other);
			drain();
		}
		
		void otherError(long idx, Throwable e) {
			if (idx == index) {
				Operators.terminate(S, this);
				
				error(e);
			} else {
				Operators.onErrorDropped(e);
			}
		}
		
		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}
			
			final Subscriber<? super T> a = actual;
			final Queue<ThrottleTimeoutOther<T, U>> q = queue;
			
			int missed = 1;
			
			for (;;) {
				
				for (;;) {
					boolean d = done;
					
					ThrottleTimeoutOther<T, U> o = q.poll();
					
					boolean empty = o == null;
					
					if (checkTerminated(d, empty, a, q)) {
						return;
					}
					
					if (empty) {
						break;
					}
					
					if (o.index == index) {
						long r = requested;
						if (r != 0) {
							a.onNext(o.value);
							if (r != Long.MAX_VALUE) {
								REQUESTED.decrementAndGet(this);
							}
						} else {
							cancel();
							
							q.clear();
							
							Throwable e = new IllegalStateException("Could not emit value due to lack of requests");
							Exceptions.addThrowable(ERROR, this, e);
							e = Exceptions.terminate(ERROR, this);
							
							a.onError(e);
							return;
						}
					}
				}
				
				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}
		
		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<?> q) {
			if (cancelled) {
				q.clear();
				return true;
			}
			if (d) {
				Throwable e = Exceptions.terminate(ERROR, this);
				if (e != null && e != Exceptions.TERMINATED) {
					cancel();
					
					q.clear();
					
					a.onError(e);
					return true;
				} else
				if (empty) {
					
					a.onComplete();
					return true;
				}
			}
			return false;
		}
	}
	
	static final class ThrottleTimeoutOther<T, U> extends Operators.DeferredSubscription
	implements Subscriber<U> {
		final ThrottleTimeoutMain<T, U> main;
		
		final T value;
		
		final long index;
		
		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ThrottleTimeoutOther> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(ThrottleTimeoutOther.class, "once");
		

		public ThrottleTimeoutOther(ThrottleTimeoutMain<T, U> main, T value, long index) {
			this.main = main;
			this.value = value;
			this.index = index;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (set(s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(U t) {
			if (ONCE.compareAndSet(this, 0, 1)) {
				cancel();
				
				main.otherNext(this);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (ONCE.compareAndSet(this, 0, 1)) {
				main.otherError(index, t);
			} else {
				Operators.onErrorDropped(t);
			}
		}

		@Override
		public void onComplete() {
			if (ONCE.compareAndSet(this, 0, 1)) {
				main.otherNext(this);
			}
		}
	}
}

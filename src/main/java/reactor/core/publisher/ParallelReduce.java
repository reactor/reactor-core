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

import java.util.function.*;

import org.reactivestreams.*;

import reactor.core.Exceptions;

/**
 * Reduce the sequence of values in each 'rail' to a single value.
 *
 * @param <T> the input value type
 * @param <R> the result value type
 */
final class ParallelReduce<T, R> extends ParallelFlux<R> {
	
	final ParallelFlux<? extends T> source;
	
	final Supplier<R> initialSupplier;
	
	final BiFunction<R, T, R> reducer;
	
	public ParallelReduce(ParallelFlux<? extends T> source, Supplier<R> initialSupplier, BiFunction<R, T, R> reducer) {
		this.source = source;
		this.initialSupplier = initialSupplier;
		this.reducer = reducer;
	}

	@Override
	public long getPrefetch() {
		return Long.MAX_VALUE;
	}

	@Override
	public void subscribe(Subscriber<? super R>[] subscribers) {
		if (!validate(subscribers)) {
			return;
		}
		
		int n = subscribers.length;
		@SuppressWarnings("unchecked")
		Subscriber<T>[] parents = new Subscriber[n];
		
		for (int i = 0; i < n; i++) {
			
			R initialValue;
			
			try {
				initialValue = initialSupplier.get();
			} catch (Throwable ex) {
				reportError(subscribers, Exceptions.onOperatorError(ex));
				return;
			}
			
			if (initialValue == null) {
				reportError(subscribers, new NullPointerException("The initialSupplier returned a null value"));
				return;
			}
			
			parents[i] = new ParallelReduceSubscriber<>(subscribers[i], initialValue, reducer);
		}
		
		source.subscribe(parents);
	}
	
	void reportError(Subscriber<?>[] subscribers, Throwable ex) {
		for (Subscriber<?> s : subscribers) {
			Operators.error(s, ex);
		}
	}

	@Override
	public int parallelism() {
		return source.parallelism();
	}

	@Override
	public boolean isOrdered() {
		return false;
	}

	static final class ParallelReduceSubscriber<T, R> extends
	                                                  Operators.DeferredScalarSubscriber<T, R> {

		final BiFunction<R, T, R> reducer;

		R accumulator;
		
		Subscription s;

		boolean done;
		
		public ParallelReduceSubscriber(Subscriber<? super R> subscriber, R initialValue, BiFunction<R, T, R> reducer) {
			super(subscriber);
			this.accumulator = initialValue;
			this.reducer = reducer;
		}
		
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				
				subscriber.onSubscribe(this);
				
				s.request(Long.MAX_VALUE);
			}
		}
		
		@Override
		public void onNext(T t) {
			if (done) {
				return;
			}
			
			R v;
			
			try {
				v = reducer.apply(accumulator, t);
			} catch (Throwable ex) {
				onError(Exceptions.onOperatorError(this, ex, t));
				return;
			}
			
			if (v == null) {
				onError(Exceptions.onOperatorError(this, new NullPointerException("The" +
						" reducer returned a null value"), t));
				return;
			}
			
			accumulator = v;
		}
		
		@Override
		public void onError(Throwable t) {
			if (done) {
				Exceptions.onErrorDropped(t);
				return;
			}
			done = true;
			accumulator = null;
			subscriber.onError(t);
		}
		
		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			
			R a = accumulator;
			accumulator = null;
			complete(a);
		}
		
		@Override
		public void cancel() {
			super.cancel();
			s.cancel();
		}
	}
}

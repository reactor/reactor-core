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
 * @param <C> the collection type
 */
final class ParallelCollect<T, C> extends ParallelFlux<C> {
	
	final ParallelFlux<? extends T> source;
	
	final Supplier<C> initialCollection;
	
	final BiConsumer<C, T> collector;
	
	public ParallelCollect(ParallelFlux<? extends T> source, 
			Supplier<C> initialCollection, BiConsumer<C, T> collector) {
		this.source = source;
		this.initialCollection = initialCollection;
		this.collector = collector;
	}

	@Override
	public long getPrefetch() {
		return Long.MAX_VALUE;
	}

	@Override
	public void subscribe(Subscriber<? super C>[] subscribers) {
		if (!validate(subscribers)) {
			return;
		}
		
		int n = subscribers.length;
		@SuppressWarnings("unchecked")
		Subscriber<T>[] parents = new Subscriber[n];
		
		for (int i = 0; i < n; i++) {
			
			C initialValue;
			
			try {
				initialValue = initialCollection.get();
			} catch (Throwable ex) {
				reportError(subscribers, Exceptions.onOperatorError(ex));
				return;
			}
			
			if (initialValue == null) {
				reportError(subscribers, new NullPointerException("The initialSupplier returned a null value"));
				return;
			}
			
			parents[i] = new ParallelCollectSubscriber<>(subscribers[i], initialValue, collector);
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

	static final class ParallelCollectSubscriber<T, C> extends
	                                                   Operators.DeferredScalarSubscriber<T, C> {

		final BiConsumer<C, T> collector;

		C collection;
		
		Subscription s;

		boolean done;
		
		public ParallelCollectSubscriber(Subscriber<? super C> subscriber, 
				C initialValue, BiConsumer<C, T> collector) {
			super(subscriber);
			this.collection = initialValue;
			this.collector = collector;
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
			
			try {
				collector.accept(collection, t);
			} catch (Throwable ex) {
				onError(Exceptions.onOperatorError(this, ex, t));
			}
		}
		
		@Override
		public void onError(Throwable t) {
			if (done) {
				Exceptions.onErrorDropped(t);
				return;
			}
			done = true;
			collection = null;
			subscriber.onError(t);
		}
		
		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			C c = collection;
			collection = null;
			complete(c);
		}
		
		@Override
		public void cancel() {
			super.cancel();
			s.cancel();
		}
	}
}

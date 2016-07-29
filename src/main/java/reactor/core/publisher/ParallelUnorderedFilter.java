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
 * Filters each 'rail' of the source ParallelFlux with a predicate function.
 *
 * @param <T> the input value type
 */
final class ParallelUnorderedFilter<T> extends ParallelFlux<T> {

	final ParallelFlux<T> source;
	
	final Predicate<? super T> predicate;
	
	public ParallelUnorderedFilter(ParallelFlux<T> source, Predicate<? super T> predicate) {
		this.source = source;
		this.predicate = predicate;
	}

	@Override
	public void subscribe(Subscriber<? super T>[] subscribers) {
		if (!validate(subscribers)) {
			return;
		}
		
		int n = subscribers.length;
		@SuppressWarnings("unchecked")
		Subscriber<? super T>[] parents = new Subscriber[n];
		
		for (int i = 0; i < n; i++) {
			parents[i] = new ParallelFilterSubscriber<>(subscribers[i], predicate);
		}
		
		source.subscribe(parents);
	}

	@Override
	public int parallelism() {
		return source.parallelism();
	}

	@Override
	public boolean isOrdered() {
		return false;
	}
	
	static final class ParallelFilterSubscriber<T> implements Subscriber<T>, Subscription {

		final Subscriber<? super T> actual;
		
		final Predicate<? super T> predicate;
		
		Subscription s;
		
		boolean done;
		
		public ParallelFilterSubscriber(Subscriber<? super T> actual, Predicate<? super T> predicate) {
			this.actual = actual;
			this.predicate = predicate;
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				return;
			}
			boolean b;
			
			try {
				b = predicate.test(t);
			} catch (Throwable ex) {
				onError(Exceptions.mapOperatorError(s, ex));
				return;
			}
			
			if (b) {
				actual.onNext(t);
			} else {
				s.request(1);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Exceptions.onErrorDropped(t);
				return;
			}
			done = true;
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			actual.onComplete();
		}
		
	}
}

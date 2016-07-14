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
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Fuseable;
import reactor.core.subscriber.DeferredScalarSubscriber;
import reactor.core.subscriber.SubscriptionHelper;
import reactor.core.util.Exceptions;

/**
 * Collects the values from the source sequence into a {@link java.util.stream.Collector}
 * instance.
 *
 * @param <T> the source value type
 * @param <A> an intermediate value type
 * @param <R> the output value type
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoStreamCollector<T, A, R> extends MonoSource<T, R> implements Fuseable {
	
	final Collector<T, A, R> collector;

	public MonoStreamCollector(Publisher<? extends T> source, 
			Collector<T, A, R> collector) {
		super(source);
		this.collector = Objects.requireNonNull(collector, "collector");
	}

	@Override
	public void subscribe(Subscriber<? super R> s) {
		A container;
		BiConsumer<A, T> accumulator;
		Function<A, R> finisher;

		try {
			container = collector.supplier().get();
			
			accumulator = collector.accumulator();
			
			finisher = collector.finisher();
		} catch (Throwable ex) {
			Exceptions.throwIfFatal(ex);
			SubscriptionHelper.error(s, ex);
			return;
		}
		
		source.subscribe(new StreamCollectorSubscriber<>(s, container, accumulator, finisher));
	}
	
	static final class StreamCollectorSubscriber<T, A, R>
	extends DeferredScalarSubscriber<T, R> {
		final BiConsumer<A, T> accumulator;
		
		final Function<A, R> finisher;
		
		A container;
		
		Subscription s;
		
		boolean done;

		public StreamCollectorSubscriber(Subscriber<? super R> actual,
				A container, BiConsumer<A, T> accumulator, Function<A, R> finisher) {
			super(actual);
			this.container = container;
			this.accumulator = accumulator;
			this.finisher = finisher;
		}
		
		@Override
		public void onSubscribe(Subscription s) {
			if (SubscriptionHelper.validate(this.s, s)) {
				this.s = s;
				
				subscriber.onSubscribe(this);
				
				s.request(Long.MAX_VALUE);
			}
		}
		
		@Override
		public void onNext(T t) {
			if (done) {
				Exceptions.onNextDropped(t);
				return;
			}
			try {
				accumulator.accept(container, t);
			} catch (Throwable ex) {
				Exceptions.throwIfFatal(ex);
				s.cancel();
				onError(ex);
				return;
			}
		}
		
		@Override
		public void onError(Throwable t) {
			if (done) {
				Exceptions.onErrorDropped(t);
				return;
			}
			done = true;
			container = null;
			subscriber.onError(t);
		}
		
		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			
			A a = container;
			container = null;
			
			R r;
			
			try {
				r = finisher.apply(a);
			} catch (Throwable ex) {
				Exceptions.throwIfFatal(ex);
				subscriber.onError(ex);
				return;
			}
			
			complete(r);
		}
		
		@Override
		public void cancel() {
			super.cancel();
			s.cancel();
		}
	}
}

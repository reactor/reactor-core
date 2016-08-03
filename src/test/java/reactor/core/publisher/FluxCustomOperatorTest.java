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

import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.function.Function;

import reactor.test.TestSubscriber;

import static reactor.core.Exceptions.throwIfFatal;
import static reactor.core.publisher.Flux.just;

/**
 * Runnable test to backup issue described in https://github.com/reactor/reactor-core/issues/131
 */
public class FluxCustomOperatorTest {

	@Test
	public void fluxSource() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		Flux<Integer> intFlux = just(1);
		// 1) Cannot chain custom operator as publisher needs to be passed in constructor
		new FluxNullSafeMap<>(intFlux, v -> v).map(v -> v + 1).subscribe(ts);

		ts.assertValues(2).assertComplete().assertNoError();
	}

	@Test
	public void transformLamda() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		// 1) Nested lambda aren't ideal as not very intuative
		// 2) Not ideal that custom operator needs to be passed instance of subscriber. Means new operator instance is
		// needed for each use.
		Flux<Integer> intFlux = just(1).transform(flux -> s -> flux.subscribe(new NullSafeMapOperatorAdapter<>(s, v -> v)));
		// 3) Cannot chain after custum operator as generic type isn't propagated
		intFlux.map(v -> v + 1).subscribe(ts);

		ts.assertValues(2).assertComplete().assertNoError();
	}

	@Test
	public void transformAnonymous() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		// 1) Chaining can be achieved with transform(), but only if a anonymous class with generic type is used
		just(1).transform(flux -> new Publisher<Integer>() {
				@Override
				public void subscribe(Subscriber<? super Integer> s) {
					flux.subscribe(new NullSafeMapOperatorAdapter<>(s, v -> v));
				}
		}).map(v -> v + 1).subscribe(ts);

		ts.assertValues(2).assertComplete().assertNoError();
	}

	@Test
	public void oldLiftLamda() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		// 1) Not ideal that custom operator needs to be passed instance of subscriber. Means new operator instance is
		// needed for each use.
		Flux<Integer> intFlux = just(1).liftOld(s -> new NullSafeMapOperatorAdapter<>(s, v -> v));
		// 2) Cannot chain after custum operator as generic type isn't propagated
		intFlux.map(v -> v + 1).subscribe(ts);

		ts.assertValues(2).assertComplete().assertNoError();
	}

	@Test
	public void oldLiftAnonymous() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		// 1) Chaining can be used, but only if a anonymous function with generic types is used
		just(1).liftOld(new Function<Subscriber<? super Integer>, Subscriber<? super Integer>>() {
				@Override
				public Subscriber<? super Integer> apply(Subscriber<? super Integer> s) {
					return new NullSafeMapOperatorAdapter<>(s, v -> v);
				}
		}).map(v -> v + 1).subscribe(ts);

		ts.assertValues(2).assertComplete().assertNoError();
	}

	@Test
	public void rxJavaStyleliftOperator() {
		TestSubscriber<Integer> ts = TestSubscriber.create();

		// 1) Forward chaining works
		// 2) Custom operator does not need new instance per usage.
		// 3) No need for the user of custom operation to do anything with flux or subscriber instances
		// 4) Concise as no anonymous classes
		just(1).lift(new NullSafeMapOperator<>(v -> v)).map(v -> v + 1).subscribe(ts);
		ts.assertValues(2).assertComplete().assertNoError();
	}

	class FluxNullSafeMap<T, R> extends FluxSource<T, R> {

		private Function<T, R> mapper;

		public FluxNullSafeMap(Publisher<T> publisher, Function<T, R> mapper) {
			super(publisher);
			this.mapper = mapper;
		}

		@Override
		public void subscribe(Subscriber<? super R> s) {
			source.subscribe(new NullSafeMapOperatorAdapter<>(s, mapper));
		}
	}

	public class NullSafeMapOperator<R, T> implements Flux.Operator<R, T> {
		private Function<T, R> mapper;

		public NullSafeMapOperator(Function<T, R> mapper) {
			this.mapper = mapper;
		}

		@Override
		public Subscriber<? super T> apply(Subscriber<? super R> subscriber) {
			return new NullSafeMapOperatorAdapter<>(subscriber, mapper);
		}
	}

	class NullSafeMapOperatorAdapter<T, R> extends OperatorAdapter<T, R> {
		private Function<T, R> mapper;

		public NullSafeMapOperatorAdapter(Subscriber<? super R> subscriber, Function<T, R> mapper) {
			super(subscriber);
			this.mapper = mapper;
		}

		@Override
		protected void doNext(T event) {
			R result;

			try {
				result = mapper.apply(event);
			} catch (Throwable e) {
				throwIfFatal(e);
				subscription.cancel();
				return;
			}

			if (result != null) {
				subscriber.onNext(result);
			} else {
				subscription.request(1);
			}
		}
	}

}

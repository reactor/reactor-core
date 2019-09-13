/*
 * Copyright (c) 2019-Present Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.util.context;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.*;

@RunWith(JUnitParamsRunner.class)
public class ContextPropagationTest {

	public static List<TransformDeferredSourceFactory> transformDeferredSources() {
		return Arrays.asList(
				new TransformDeferredSourceFactory("Flux") {
					@Override
					public CorePublisher<Context> apply(Function<CorePublisher<Context>, Publisher<Context>> f) {
						return Flux.deferWithContext(Flux::just).transformDeferred(f);
					}
				},
				new TransformDeferredSourceFactory("Mono") {
					@Override
					public CorePublisher<Context> apply(Function<CorePublisher<Context>, Publisher<Context>> f) {
						return Mono.subscriberContext().transformDeferred(f);
					}
				}
		);
	}

	@Test
	@Parameters(method = "transformDeferredSources")
	public void transformDeferredPreservesContext(TransformDeferredSourceFactory fn) {
		Function<CorePublisher<Context>, Publisher<Context>> lifter = source -> {
			return actual -> source.subscribe(new ForwardingSubscriber<>(actual));
		};

		Context c = Context.of("foo", "bar");
		Context ctx = Flux.from(fn.apply(lifter))
		                  .subscriberContext(c)
		                  .blockLast();

		assertThat(ctx.getOrEmpty("foo")).hasValue("bar");
	}

	/**
	 * TODO consider throwing an error when the context is overridden instead of changing the current one
	 */
	@Test
	@Parameters(method = "transformDeferredSources")
	public void transformDeferredDoesNotOverrideContext(
			Function<Function<CorePublisher<Context>, Publisher<Context>>, CorePublisher<Context>> fn
	) {
		Function<CorePublisher<Context>, Publisher<Context>> lifter = source -> {
			return actual -> source.subscribe(new ForwardingCoreSubscriber<Context>(actual) {
				@Override
				public Context currentContext() {
					return Context.of("foo", "baz");
				}
			});
		};

		Context c = Context.of("foo", "bar");
		Context ctx = Flux.from(fn.apply(lifter)).subscriberContext(c).blockLast();

		assertThat(ctx.getOrEmpty("foo")).hasValue("baz");
	}

	/**
	 * TODO consider throwing an error when the context is reset
	 *  to the default value of {@link CoreSubscriber#currentContext()}
	 */
	@Test
	@Parameters(method = "transformDeferredSources")
	public void transformDeferredDoesRespectsEmptyContext(
			Function<Function<CorePublisher<Context>, Publisher<Context>>, CorePublisher<Context>> fn
	) {
		Function<CorePublisher<Context>, Publisher<Context>> lifter = source -> {
			return actual -> source.subscribe(new ForwardingCoreSubscriber<Context>(actual) {
				@Override
				public Context currentContext() {
					return Context.empty();
				}
			});
		};

		Context c = Context.of("foo", "bar");
		Context ctx = Flux.from(fn.apply(lifter)).subscriberContext(c).blockLast();

		assertThat(ctx.isEmpty()).as("context is empty").isTrue();
	}

	static abstract class TransformDeferredSourceFactory implements Function<Function<CorePublisher<Context>, Publisher<Context>>, CorePublisher<Context>> {

		final String name;

		TransformDeferredSourceFactory(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return name;
		}
	}

	static class ForwardingSubscriber<T> implements Subscriber<T> {

		final Subscriber<? super T> actual;

		ForwardingSubscriber(Subscriber<? super T> actual) {
			this.actual = actual;
		}

		@Override
		public void onComplete() {
			actual.onComplete();
		}

		@Override
		public void onError(Throwable throwable) {
			actual.onError(throwable);
		}

		@Override
		public void onNext(T context) {
			actual.onNext(context);
		}

		@Override
		public void onSubscribe(Subscription subscription) {
			actual.onSubscribe(subscription);
		}
	}

	static class ForwardingCoreSubscriber<T> extends ForwardingSubscriber<T> implements CoreSubscriber<T> {

		ForwardingCoreSubscriber(Subscriber<? super T> actual) {
			super(actual);
		}
	}

}

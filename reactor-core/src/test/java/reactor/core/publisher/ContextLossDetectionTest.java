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

package reactor.core.publisher;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

@RunWith(JUnitParamsRunner.class)
public class ContextLossDetectionTest {

	public static List<SourceFactory> sources() {
		return Arrays.asList(
				new SourceFactory("Flux#transform") {
					@Override
					public CorePublisher<ContextView> apply(TestTransformer f) {
						return Flux.deferWithContext(Flux::just).transform(f);
					}
				},
				new SourceFactory("Flux#transformDeferred") {
					@Override
					public CorePublisher<ContextView> apply(TestTransformer f) {
						return Flux.deferWithContext(Flux::just).transformDeferred(f);
					}
				},
				new SourceFactory("Flux#transformDeferred(BiFunction)") {
					@Override
					public CorePublisher<ContextView> apply(TestTransformer f) {
						return Flux.deferWithContext(Flux::just).transformDeferred(f.adaptToBiFunction());
					}
				},
				new SourceFactory("Flux#transformDeferred(BiFunction) direct") {
					@Override
					public CorePublisher<ContextView> apply(TestTransformer f) {
						return Flux.<ContextView>empty().transformDeferred(f.adaptToBiFunction());
					}
				},

				new SourceFactory("Mono#transform") {
					@Override
					public CorePublisher<ContextView> apply(TestTransformer f) {
						return Mono.deferWithContext(Mono::just).transform(f);
					}
				},
				new SourceFactory("Mono#transformDeferred") {
					@Override
					public CorePublisher<ContextView> apply(TestTransformer f) {
						return Mono.deferWithContext(Mono::just).transformDeferred(f);
					}
				},
				new SourceFactory("Mono#transformDeferred(BiFunction)") {
					@Override
					public CorePublisher<ContextView> apply(TestTransformer f) {
						return Mono.deferWithContext(Mono::just).transformDeferred(f.adaptToBiFunction());
					}
				},
				new SourceFactory("Mono#transformDeferred(BiFunction) direct") {
					@Override
					public CorePublisher<ContextView> apply(TestTransformer f) {
						return Mono.<ContextView>empty().transformDeferred(f.adaptToBiFunction());
					}
				}
		);
	}

	@BeforeClass
	public static void beforeClass() {
		Hooks.enableContextLossTracking();
	}

	@AfterClass
	public static void afterClass() {
		Hooks.disableContextLossTracking();
	}

	@Test
	@Parameters(method = "sources")
	public void transformDeferredDetectsContextLoss(SourceFactory fn) {
		TestTransformer lifter = new TestTransformer(fn + "'s badTransformer") {
			@Override
			public Publisher<ContextView> apply(CorePublisher<ContextView> source) {
				return actual ->
						source.subscribe(new ForwardingCoreSubscriber<ContextView>(actual) {
							@Override
							public Context currentContext() {
								return Context.of("foo", "baz");
							}
						});
			}
		};

		assertThatIllegalStateException()
				.isThrownBy(() -> {
					Flux.from(fn.apply(lifter))
					    .subscriberContext(Context.of("foo", "bar"))
					    .blockLast();
				})
				.withMessage("Context loss after applying " + fn + "'s badTransformer");
	}

	@Test
	@Parameters(method = "sources")
	public void transformDeferredDetectsContextLossWithEmptyContext(SourceFactory fn) {
		TestTransformer lifter = new TestTransformer(fn + "'s badTransformer") {
			@Override
			public Publisher<ContextView> apply(CorePublisher<ContextView> source) {
				return actual -> source.subscribe(new ForwardingCoreSubscriber<ContextView>(actual) {
					@Override
					public Context currentContext() {
						return Context.empty();
					}
				});
			}
		};

		assertThatIllegalStateException()
				.isThrownBy(() -> {
					Flux.from(fn.apply(lifter))
					    .subscriberContext(Context.of("foo", "bar"))
					    .blockLast();
				})
				.withMessage("Context loss after applying " + fn + "'s badTransformer");
	}

	@Test
	@Parameters(method = "sources")
	public void transformDeferredDetectsContextLossWithDefaultContext(SourceFactory fn) {
		TestTransformer lifter = new TestTransformer(fn + "'s badTransformer") {
			@Override
			public Publisher<ContextView> apply(CorePublisher<ContextView> source) {
				return actual -> source.subscribe(new ForwardingCoreSubscriber<>(actual));
			}
		};

		assertThatIllegalStateException()
				.isThrownBy(() -> {
					Flux.from(fn.apply(lifter))
					    .subscriberContext(Context.of("foo", "bar"))
					    .blockLast();
				})
				.withMessage("Context loss after applying " + fn + "'s badTransformer");
	}

	@Test
	@Parameters(method = "sources")
	public void transformDeferredDetectsContextLossWithRSSubscriber(SourceFactory fn) {
		TestTransformer lifter = new TestTransformer(fn + "'s badTransformer") {
			@Override
			public Publisher<ContextView> apply(CorePublisher<ContextView> source) {
				return actual -> source.subscribe(new ForwardingCoreSubscriber<>(actual));
			}
		};

		assertThatIllegalStateException()
				.isThrownBy(() -> {
					FutureSubscriber<ContextView> subscriber = new FutureSubscriber<ContextView>() {
						@Override
						public void onSubscribe(Subscription subscription) {
							subscription.cancel();
						}
					};
					Flux.from(fn.apply(lifter))
					    .subscriberContext(Context.of("foo", "bar"))
					    .subscribe(subscriber);

					try {
						subscriber.get(1, TimeUnit.SECONDS);
					}
					catch (ExecutionException e) {
						throw e.getCause();
					}
				})
				.withMessage("Context loss after applying " + fn + "'s badTransformer");
	}

	static abstract class TestTransformer implements Function<CorePublisher<ContextView>, Publisher<ContextView>> {

		final String description;

		protected TestTransformer(String description) {
			this.description = description;
		}

		public TestTransformerAdapter adaptToBiFunction() {
			return new TestTransformerAdapter(this);
		}

		@Override
		public abstract Publisher<ContextView> apply(CorePublisher<ContextView> publisher);

		@Override
		public String toString() {
			return description;
		}
	}

	static class TestTransformerAdapter implements BiFunction<ContextView, CorePublisher<ContextView>, Publisher<ContextView>> {

		final TestTransformer delegate;

		protected TestTransformerAdapter(TestTransformer delegate) {
			this.delegate = delegate;
		}

		@Override
		public Publisher<ContextView> apply(ContextView contextView,
				CorePublisher<ContextView> publisher) {
			return delegate.apply(publisher);
		}

		@Override
		public String toString() {
			return delegate.toString();
		}
	}

	static abstract class SourceFactory implements Function<TestTransformer, CorePublisher<ContextView>> {

		final String name;

		SourceFactory(String name) {
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

	static abstract class FutureSubscriber<T> extends CompletableFuture<T> implements Subscriber<T> {

		@Override
		public void onNext(T t) {
			complete(t);
		}

		@Override
		public void onError(Throwable throwable) {
			completeExceptionally(throwable);
		}

		@Override
		public void onComplete() {
			complete(null);
		}
	}

}

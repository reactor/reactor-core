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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

public class ContextLossDetectionTest {

	public static List<ContextTestCase> sources() {
		return Arrays.asList(
				new ContextTestCase("Flux#transform") {
					@Override
					public CorePublisher<ContextView> apply(LossyTransformer f) {
						return Flux.deferContextual(Flux::just).transform(f);
					}
				},
				new ContextTestCase("Flux#transformDeferred") {
					@Override
					public CorePublisher<ContextView> apply(LossyTransformer f) {
						return Flux.deferContextual(Flux::just).transformDeferred(f);
					}
				},
				new ContextTestCase("Flux#transformDeferredContextual") {
					@Override
					public CorePublisher<ContextView> apply(LossyTransformer f) {
						return Flux.deferContextual(Flux::just).transformDeferredContextual(f.adaptToBiFunction());
					}
				},
				new ContextTestCase("Flux#transformDeferredContextual direct") {
					@Override
					public CorePublisher<ContextView> apply(LossyTransformer f) {
						return Flux.<ContextView>empty().transformDeferredContextual(f.adaptToBiFunction());
					}
				},

				new ContextTestCase("Mono#transform") {
					@Override
					public CorePublisher<ContextView> apply(LossyTransformer f) {
						return Mono.deferContextual(Mono::just).transform(f);
					}
				},
				new ContextTestCase("Mono#transformDeferred") {
					@Override
					public CorePublisher<ContextView> apply(LossyTransformer f) {
						return Mono.deferContextual(Mono::just).transformDeferred(f);
					}
				},
				new ContextTestCase("Mono#transformDeferredContextual") {
					@Override
					public CorePublisher<ContextView> apply(LossyTransformer f) {
						return Mono.deferContextual(Mono::just).transformDeferredContextual(f.adaptToBiFunction());
					}
				},
				new ContextTestCase("Mono#transformDeferredContextual direct") {
					@Override
					public CorePublisher<ContextView> apply(LossyTransformer f) {
						return Mono.<ContextView>empty().transformDeferredContextual(f.adaptToBiFunction());
					}
				}
		);
	}

	@BeforeAll
	public static void beforeClass() {
		Hooks.enableContextLossTracking();
	}

	@AfterAll
	public static void afterClass() {
		Hooks.disableContextLossTracking();
	}

	@ParameterizedTest
	@MethodSource("sources")
	public void transformDeferredDetectsContextLoss(ContextTestCase fn) {
		LossyTransformer transformer = new LossyTransformer(fn + "'s badTransformer",
				Context.of("foo", "baz"));

		assertThatIllegalStateException()
				.isThrownBy(() -> fn.assembleWithDownstreamContext(transformer, Context.of("foo", "bar"))
				                    .blockLast())
				.withMessage("Context loss after applying " + fn + "'s badTransformer");
	}

	@ParameterizedTest
	@MethodSource("sources")
	public void transformDeferredDetectsContextLossWithEmptyContext(ContextTestCase fn) {
		LossyTransformer transformer = new LossyTransformer(fn + "'s badTransformer",
				Context.empty());

		assertThatIllegalStateException()
				.isThrownBy(() -> fn.assembleWithDownstreamContext(transformer, Context.of("foo", "bar"))
				                    .blockLast())
				.withMessage("Context loss after applying " + fn + "'s badTransformer");
	}

	@ParameterizedTest
	@MethodSource("sources")
	public void transformDeferredDetectsContextLossWithDefaultContext(ContextTestCase fn) {
		LossyTransformer transformer = new LossyTransformer(fn + "'s badTransformer", true);

		assertThatIllegalStateException()
				.isThrownBy(() -> fn.assembleWithDownstreamContext(transformer, Context.of("foo", "bar"))
						.blockLast())
				.withMessage("Context loss after applying " + fn + "'s badTransformer");
	}

	@ParameterizedTest
	@MethodSource("sources")
	public void transformDeferredDetectsContextLossWithRSSubscriber(ContextTestCase fn) {
		LossyTransformer transformer = new LossyTransformer(fn + "'s badTransformer", false);

		assertThatIllegalStateException()
				.isThrownBy(() -> {
					FutureSubscriber<ContextView> subscriber = new FutureSubscriber<ContextView>() {
						@Override
						public void onSubscribe(Subscription subscription) {
							subscription.cancel();
						}
					};
					fn.assembleWithDownstreamContext(transformer, Context.of("foo", "bar"))
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

	static class LossyTransformer implements Function<CorePublisher<ContextView>, Publisher<ContextView>> {

		final String description;
		final boolean useCoreSubscriber;
		@Nullable
		final Context contextOfSubscriber;

		protected LossyTransformer(String description, Context contextOfSubscriber) {
			this.description = description;
			this.useCoreSubscriber = true;
			this.contextOfSubscriber = contextOfSubscriber;
		}

		LossyTransformer(String description, boolean useCoreSubscriber) {
			this.description = description;
			this.useCoreSubscriber = useCoreSubscriber;
			this.contextOfSubscriber = null;
		}

		LossyBiTransformer adaptToBiFunction() {
			return new LossyBiTransformer(this);
		}

		@Override
		public Publisher<ContextView> apply(CorePublisher<ContextView> publisher) {
			if (contextOfSubscriber == null) {
				return new ContextLossyPublisher<>(publisher, useCoreSubscriber);
			}
			return new ContextLossyPublisher<>(publisher, contextOfSubscriber);
		}

		@Override
		public String toString() {
			return description;
		}
	}

	static class LossyBiTransformer implements BiFunction<CorePublisher<ContextView>, ContextView, Publisher<ContextView>> {

		final LossyTransformer delegate;

		protected LossyBiTransformer(LossyTransformer delegate) {
			this.delegate = delegate;
		}

		@Override
		public Publisher<ContextView> apply(CorePublisher<ContextView> publisher, ContextView contextView) {
			return delegate.apply(publisher);
		}

		@Override
		public String toString() {
			return delegate.toString();
		}
	}

	static abstract class ContextTestCase implements Function<LossyTransformer, CorePublisher<ContextView>> {

		final String name;

		ContextTestCase(String name) {
			this.name = name;
		}

		Flux<ContextView> assembleWithDownstreamContext(LossyTransformer lossyTransformer,
				Context downstreamContext) {
			return Flux.from(apply(lossyTransformer))
			           .contextWrite(downstreamContext);
		}

		@Override
		public String toString() {
			return name;
		}
	}

	static class ContextLossyPublisher<T> implements Publisher<T> {

		final Publisher<T> source;
		@Nullable
		final Context      lossyContext;
		final boolean      useCoreSubscriber;

		ContextLossyPublisher(Publisher<T> source, Context lossyContext) {
			this.source = source;
			this.lossyContext = lossyContext;
			this.useCoreSubscriber = false;
		}

		ContextLossyPublisher(Publisher<T> source, boolean useCoreSubscriber) {
			this.source = source;
			this.lossyContext = null;
			this.useCoreSubscriber = useCoreSubscriber;
		}

		@Override
		public void subscribe(Subscriber<? super T> subscriber) {
			if (lossyContext == null && !useCoreSubscriber) {
				source.subscribe(new ForeignOperator<>(subscriber));
			}
			else {
				source.subscribe(new CoreLossyOperator<>(subscriber, lossyContext));
			}
		}

		static class ForeignOperator<T> implements Subscriber<T>, Subscription {

			private final Subscriber<? super T> downstream;
			private Subscription upstream;

			ForeignOperator(Subscriber<? super T> subscriber) {
				this.downstream = subscriber;
			}

			@Override
			public void onSubscribe(Subscription subscription) {
				this.upstream = subscription;
				this.downstream.onSubscribe(this);
			}

			@Override
			public void request(long l) {
				this.upstream.request(l);
			}

			@Override
			public void cancel() {
				this.upstream.cancel();
			}

			@Override
			public void onNext(T t) {
				this.downstream.onNext(t);
			}

			@Override
			public void onComplete() {
				this.downstream.onComplete();
			}

			@Override
			public void onError(Throwable throwable) {
				this.downstream.onError(throwable);
			}
		}

		static class CoreLossyOperator<T> extends ForeignOperator<T> implements CoreSubscriber<T> {

			@Nullable
			private final Context lossyContext;

			CoreLossyOperator(Subscriber<? super T> subscriber, @Nullable Context lossyContext) {
				super(subscriber);
				this.lossyContext = lossyContext;
			}

			@Override
			public Context currentContext() {
				if (this.lossyContext == null) {
					return CoreSubscriber.super.currentContext();
				}
				return this.lossyContext;
			}
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

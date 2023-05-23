/*
 * Copyright (c) 2016-2023 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Objects;

import io.micrometer.context.ContextSnapshot;
import org.reactivestreams.Publisher;

import org.reactivestreams.Subscription;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * A connecting {@link Flux} Publisher (right-to-left from a composition chain perspective)
 *
 * @param <I> Upstream type
 */
final class FluxSource<I> extends Flux<I> implements SourceProducer<I>,
                                                     OptimizableOperator<I, I> {


	final Publisher<? extends I> source;

	@Nullable
	final OptimizableOperator<?, I> optimizableOperator;

	/**
	 * Build a {@link FluxSource} wrapper around the passed parent {@link Publisher}
	 *
	 * @param source the {@link Publisher} to decorate
	 */
	FluxSource(Publisher<? extends I> source) {
		this.source = Objects.requireNonNull(source);
		if (source instanceof OptimizableOperator) {
			@SuppressWarnings("unchecked")
			OptimizableOperator<?, I> optimSource = (OptimizableOperator<?, I>) source;
			this.optimizableOperator = optimSource;
		}
		else {
			this.optimizableOperator = null;
		}
	}

	/**
	 * Default is simply delegating and decorating with {@link Flux} API. Note this
	 * assumes an identity between input and output types.
	 * @param actual
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(CoreSubscriber<? super I> actual) {
		if (ContextPropagationSupport.shouldPropagateContextToThreadLocals()) {
			source.subscribe(new FluxSourceRestoringThreadLocalsSubscriber<>(actual));
		} else {
			source.subscribe(actual);
		}
	}

	@Override
	public final CorePublisher<? extends I> source() {
		return this;
	}

	@Override
	public final OptimizableOperator<?, ? extends I> nextOptimizableSource() {
		return optimizableOperator;
	}

	@Override
	public CoreSubscriber<? super I> subscribeOrReturn(CoreSubscriber<? super I> actual) {
		return actual;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.PARENT) return source;
		if (key == Attr.RUN_STYLE) return Scannable.from(source).scanUnsafe(key);
		return null;
	}

	static final class FluxSourceRestoringThreadLocalsSubscriber<T>
			implements Fuseable.ConditionalSubscriber<T>, InnerConsumer<T> {

		final CoreSubscriber<? super T>                 actual;
		final Fuseable.ConditionalSubscriber<? super T> actualConditional;

		Subscription s;

		@SuppressWarnings("unchecked")
		FluxSourceRestoringThreadLocalsSubscriber(CoreSubscriber<? super T> actual) {
			this.actual = actual;
			if (actual instanceof Fuseable.ConditionalSubscriber) {
				this.actualConditional = (Fuseable.ConditionalSubscriber<? super T>) actual;
			}
			else {
				this.actualConditional = null;
			}
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) {
				return s;
			}
			if (key == Attr.RUN_STYLE) {
				return Attr.RunStyle.SYNC;
			}
			if (key == Attr.ACTUAL) {
				return actual;
			}
			return null;
		}

		@Override
		public Context currentContext() {
			return actual.currentContext();
		}

		@SuppressWarnings("try")
		@Override
		public void onSubscribe(Subscription s) {
			// This is needed, as the downstream can then switch threads,
			// continue the subscription using different primitives and omit this operator
			try (ContextSnapshot.Scope ignored =
					     ContextPropagation.setThreadLocals(actual.currentContext())) {
				actual.onSubscribe(s);
			}
		}

		@SuppressWarnings("try")
		@Override
		public void onNext(T t) {
			try (ContextSnapshot.Scope ignored =
					     ContextPropagation.setThreadLocals(actual.currentContext())) {
				actual.onNext(t);
			}
		}

		@SuppressWarnings("try")
		@Override
		public boolean tryOnNext(T t) {
			try (ContextSnapshot.Scope ignored =
					     ContextPropagation.setThreadLocals(actual.currentContext())) {
				if (actualConditional != null) {
					return actualConditional.tryOnNext(t);
				}
				actual.onNext(t);
				return true;
			}
		}

		@SuppressWarnings("try")
		@Override
		public void onError(Throwable t) {
			try (ContextSnapshot.Scope ignored =
					     ContextPropagation.setThreadLocals(actual.currentContext())) {
				actual.onError(t);
			}
		}

		@SuppressWarnings("try")
		@Override
		public void onComplete() {
			try (ContextSnapshot.Scope ignored =
					     ContextPropagation.setThreadLocals(actual.currentContext())) {
				actual.onComplete();
			}
		}
	}
}

/*
 * Copyright (c) 2016-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;

import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;

/**
 * A decorating {@link Flux} {@link Publisher} that exposes {@link Flux} API over an
 * arbitrary {@link Publisher}. Useful to create operators which return a {@link Flux}.
 *
 * @param <I> delegate {@link Publisher} type
 * @param <O> produced type
 */
abstract class FluxFromMonoOperator<I, O> extends Flux<O> implements Scannable,
                                                                     OptimizableOperator<O, I> {

	protected final Mono<? extends I> source;

	final @Nullable OptimizableOperator<?, I> optimizableOperator;

	/**
	 * Build a {@link FluxFromMonoOperator} wrapper around the passed parent {@link Publisher}
	 *
	 * @param source the {@link Publisher} to decorate
	 */
	@SuppressWarnings("unchecked")
	protected FluxFromMonoOperator(Mono<? extends I> source) {
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

	@Override
	public @Nullable Object scanUnsafe(Attr key) {
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.PARENT) return source;
		if (key == InternalProducerAttr.INSTANCE) return true;
		return null;
	}


	@Override
	@SuppressWarnings("unchecked")
	public final void subscribe(CoreSubscriber<? super O> subscriber) {
		OptimizableOperator operator = this;
		try {
			while (true) {
				subscriber = operator.subscribeOrReturn(subscriber);
				if (subscriber == null) {
					// null means "I will subscribe myself", returning...
					return;
				}
				OptimizableOperator newSource = operator.nextOptimizableSource();
				if (newSource == null) {
					subscriber = Operators.restoreContextOnSubscriberIfPublisherNonInternal(operator.source(), subscriber);
					operator.source().subscribe(subscriber);
					return;
				}
				operator = newSource;
			}
		}
		catch (Throwable e) {
			Operators.reportThrowInSubscribe(subscriber, e);
			return;
		}
	}

	@Override
	public abstract @Nullable CoreSubscriber<? super I> subscribeOrReturn(CoreSubscriber<? super O> actual) throws Throwable;

	@Override
	public final CorePublisher<? extends I> source() {
		return source;
	}

	@Override
	public final @Nullable OptimizableOperator<?, ? extends I> nextOptimizableSource() {
		return optimizableOperator;
	}

}

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

import org.reactivestreams.Publisher;

import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

abstract class InternalFluxOperator<I, O> extends FluxOperator<I, O> implements Scannable,
                                                                                OptimizableOperator<O, I> {

	@Nullable
	final OptimizableOperator<?, I> optimizableOperator;

	/**
	 * Build a {@link InternalFluxOperator} wrapper around the passed parent {@link Publisher}
	 *
	 * @param source the {@link Publisher} to decorate
	 */
	protected InternalFluxOperator(Flux<? extends I> source) {
		super(source);
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
	@SuppressWarnings("unchecked")
	public final void subscribe(CoreSubscriber<? super O> subscriber) {
		OptimizableOperator operator = this;
		try {
			while (true) {
				subscriber = operator.subscribeOrReturn(subscriber);
				if (subscriber == null) {
					// if internally subscribed, it means the optimized operator is
					// already within the internals and subscribing up the chain will
					// at some point need to consider the source and wrap it

					// null means "I will subscribe myself", returning...
					return;
				}
				OptimizableOperator newSource = operator.nextOptimizableSource();
				if (newSource == null) {
					CorePublisher operatorSource = operator.source();
					subscriber = Operators.restoreContextOnSubscriberIfPublisherNonInternal(operatorSource, subscriber);
					operatorSource.subscribe(subscriber);
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

	@Nullable
	public abstract CoreSubscriber<? super I> subscribeOrReturn(CoreSubscriber<? super O> actual) throws Throwable;

	@Override
	public final CorePublisher<? extends I> source() {
		return source;
	}

	@Override
	public final OptimizableOperator<?, ? extends I> nextOptimizableSource() {
		return optimizableOperator;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.PARENT) return source;
		if (key == InternalProducerAttr.INSTANCE) return true;
		return super.scanUnsafe(key);
	}

}

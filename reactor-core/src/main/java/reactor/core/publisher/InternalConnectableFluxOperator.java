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

import org.jspecify.annotations.Nullable;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;

abstract class InternalConnectableFluxOperator<I, O> extends ConnectableFlux<O> implements Scannable,
                                                                                           OptimizableOperator<O, I> {

	final ConnectableFlux<I> source;

	@Nullable
	final OptimizableOperator<?, I> optimizableOperator;

	/**
	 * Build an {@link InternalConnectableFluxOperator} wrapper around the passed parent {@link ConnectableFlux}
	 *
	 * @param source the {@link ConnectableFlux} to decorate
	 */
	public InternalConnectableFluxOperator(ConnectableFlux<I> source) {
		this.source = source;
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

	@Override
	public @Nullable Object scanUnsafe(Scannable.Attr key) {
		if (key == Scannable.Attr.PREFETCH) return getPrefetch();
		if (key == Scannable.Attr.PARENT) return source;
		if (key == InternalProducerAttr.INSTANCE) return true;
		return null;
	}
}

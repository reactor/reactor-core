/*
 * Copyright (c) 2019-Present Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

abstract class InternalFluxOperator<I, O> extends FluxOperator<I, O> implements Scannable, CoreOperator<O, I> {

	@Nullable
	final CoreOperator<?, I> coreOperator;

	/**
	 * Build a {@link InternalFluxOperator} wrapper around the passed parent {@link Publisher}
	 *
	 * @param source the {@link Publisher} to decorate
	 */
	protected InternalFluxOperator(Flux<? extends I> source) {
		super(source);
		this.coreOperator = source instanceof CoreOperator ? (CoreOperator) source : null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public final void subscribe(CoreSubscriber<? super O> subscriber) {
		CoreOperator operator = this;
		while (true) {
			subscriber = operator.subscribeOrReturn(subscriber);
			if (subscriber == null) {
				// null means "I will subscribe myself", returning...
				return;
			}
			CoreOperator newSource = operator.source();
			if (newSource == null) {
				throw new NullPointerException("Operator's " + operator.getClass() + " source is 'null'");
			}
			operator = newSource;
		}
	}

	@Override
	public final CoreSubscriber<? super I> subscribeOrReturn(CoreSubscriber<? super O> actual) {
		CoreSubscriber<? super I> subscriber = internalSubscribeOrReturn(actual);
		if (subscriber == null) {
			return null;
		}

		if (coreOperator == null) {
			source.subscribe(subscriber);
			return null;
		}

		return subscriber;
	}

	@Nullable
	abstract CoreSubscriber<? super I> internalSubscribeOrReturn(CoreSubscriber<? super O> actual);

	@Override
	public final CoreOperator<?, ? extends I> source() {
		return coreOperator;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PREFETCH) return getPrefetch();
		if (key == Attr.PARENT) return source;
		return null;
	}

}

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
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

abstract class InternalConnectableFluxOperator<I, O> extends ConnectableFlux<O> implements Scannable, CoreOperator<O, I> {

	final ConnectableFlux<I> source;

	/**
	 * Build an {@link InternalConnectableFluxOperator} wrapper around the passed parent {@link ConnectableFlux}
	 *
	 * @param source the {@link ConnectableFlux} to decorate
	 */
	public InternalConnectableFluxOperator(ConnectableFlux<I> source) {
		this.source = source;
	}

	@Override
	public final void subscribe(CoreSubscriber<? super O> subscriber) {
		Publisher publisher = this;

		// do-while since `this` already implements `CoreOperator`
		do {
			CoreOperator operator = (CoreOperator) publisher;

			subscriber = operator.subscribeOrReturn(subscriber);
			if (subscriber == null) {
				// null means "I will subscribe myself", returning...
				return;
			}
			publisher = operator.source();
		}
		while (publisher instanceof CoreOperator);

		if (publisher instanceof CorePublisher) {
			((CorePublisher) publisher).subscribe(subscriber);
		}
		else {
			publisher.subscribe(subscriber);
		}
	}

	@Override
	public abstract CoreSubscriber<? super I> subscribeOrReturn(CoreSubscriber<? super O> actual);

	@Override
	public final Publisher<? extends I> source() {
		return source;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Scannable.Attr key) {
		if (key == Scannable.Attr.PREFETCH) return getPrefetch();
		if (key == Scannable.Attr.PARENT) return source;
		return null;
	}
}

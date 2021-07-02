/*
 * Copyright (c) 2017-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.function.Function;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;

/**
 * A {@link Flux} that emits the item from a {@link Mono} and recursively expand it into
 * inner sequences that are also replayed. The type of recursion is driven by the
 * {@code breadthFirst} parameter.
 *
 * @author David Karnok
 * @author Simon Baslé
 */
final class MonoExpand<T> extends FluxFromMonoOperator<T, T> {

	final boolean                                               breadthFirst;
	final Function<? super T, ? extends Publisher<? extends T>> expander;
	final int                                                   capacityHint;

	MonoExpand(Mono<T> source,
			Function<? super T, ? extends Publisher<? extends T>> expander,
			boolean breadthFirst, int capacityHint) {
		super(source);
		this.expander = expander;
		this.breadthFirst = breadthFirst;
		this.capacityHint = capacityHint;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> s) {
		if (breadthFirst) {
			FluxExpand.ExpandBreathSubscriber<T> parent =
					new FluxExpand.ExpandBreathSubscriber<>(s, expander, capacityHint);
			parent.queue.offer(source);
			s.onSubscribe(parent);
			parent.drainQueue();
		}
		else {
			FluxExpand.ExpandDepthSubscription<T> parent =
					new FluxExpand.ExpandDepthSubscription<>(s, expander, capacityHint);
			parent.source = source;
			s.onSubscribe(parent);
		}
		return null;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}
}

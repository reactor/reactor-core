/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.util.Objects;
import java.util.function.BiFunction;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * @author Stephane Maldini
 */
final class ParallelLift<I, O> extends ParallelFlux<O> implements Scannable {

	final BiFunction<? super Publisher<I>, ? super CoreSubscriber<? super O>, ? extends CoreSubscriber<? super I>>
			lifter;

	final ParallelFlux<I> source;

	ParallelLift(ParallelFlux<I> p,
			BiFunction<? super Publisher<I>, ? super CoreSubscriber<? super O>, ? extends CoreSubscriber<? super I>> lifter) {
		this.source = Objects.requireNonNull(p, "source");
		this.lifter = lifter;
	}

	@Override
	public int getPrefetch() {
		return source.getPrefetch();
	}

	@Override
	public int parallelism() {
		return source.parallelism();
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) {
			return source;
		}
		if (key == Attr.PREFETCH) {
			return getPrefetch();
		}

		return null;
	}

	@Override
	protected void subscribe(CoreSubscriber<? super O>[] s) {
		@SuppressWarnings("unchecked") CoreSubscriber<? super I>[] subscribers =
				new CoreSubscriber[parallelism()];

		int i = 0;
		while (i < subscribers.length) {
			subscribers[i++] =
					Objects.requireNonNull(lifter.apply(source, s[i-1]),
							"Lifted subscriber MUST NOT be null");
		}

		source.subscribe(subscribers);
	}
}

/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * @author Stephane Maldini
 * @author Simon Baslé
 */
final class ParallelLiftFuseable<I, O> extends ParallelFlux<O>
		implements Scannable, Fuseable {

	final Operators.LiftFunction<I, O> liftFunction;

	final ParallelFlux<I> source;

	ParallelLiftFuseable(ParallelFlux<I> p,
			Operators.LiftFunction<I, O> liftFunction) {
		this.source = Objects.requireNonNull(p, "source");
		this.liftFunction = liftFunction;
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
		if (key == Attr.RUN_STYLE) {
			return Scannable.from(source).scanUnsafe(key);
		}
		if (key == Attr.LIFTER) {
			return liftFunction.name;
		}

		return null;
	}

	@Override
	public String stepName() {
		if (source instanceof Scannable) {
			return Scannable.from(source).stepName();
		}
		return Scannable.super.stepName();
	}

	@Override
	public void subscribe(CoreSubscriber<? super O>[] s) {
		@SuppressWarnings("unchecked") CoreSubscriber<? super I>[] subscribers =
				new CoreSubscriber[parallelism()];

		int i = 0;
		while (i < subscribers.length) {
			CoreSubscriber<? super O> actual = s[i];
			CoreSubscriber<? super I> converted =
					Objects.requireNonNull(liftFunction.lifter.apply(source, actual),
							"Lifted subscriber MUST NOT be null");

			Objects.requireNonNull(converted, "Lifted subscriber MUST NOT be null");

			if (actual instanceof Fuseable.QueueSubscription
					&& !(converted instanceof QueueSubscription)) {
				//user didn't produce a QueueSubscription, original was one
				converted = new FluxHide.SuppressFuseableSubscriber<>(converted);
			}
			//otherwise QS is not required or user already made a compatible conversion
			subscribers[i] = converted;
			i++;
		}

		source.subscribe(subscribers);
	}


}

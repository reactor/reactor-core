/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

/**
 * Counts the number of values in the source sequence.
 *
 * @param <T> the source value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 *
 */
final class MonoCount<T> extends MonoFromFluxOperator<T, Long> implements Fuseable {

	MonoCount(Flux<? extends T> source) {
		super(source);
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super Long> actual) {
		return new CountSubscriber<>(actual);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class CountSubscriber<T> extends Operators.BaseFluxToMonoOperator<T, Long> {

		boolean done;

		long counter;

		CountSubscriber(CoreSubscriber<? super Long> actual) {
			super(actual);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;

			return super.scanUnsafe(key);
		}

		@Override
		public void onNext(T t) {
			Operators.onDiscard(t, currentContext());
			counter++;
		}

		@Override
		public void onError(Throwable t) {
			this.actual.onError(t);
		}

		@Override
		public void onComplete() {
			completePossiblyEmpty();
		}

		@Override
		Long accumulatedValue() {
			return counter;
		}
	}
}

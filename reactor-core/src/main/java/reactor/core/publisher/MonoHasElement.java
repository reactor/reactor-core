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
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoHasElement<T> extends InternalMonoOperator<T, Boolean> implements Fuseable {

	MonoHasElement(Mono<? extends T> source) {
		super(source);
	}

	@Override
	public @Nullable Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return super.scanUnsafe(key);
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super Boolean> actual) {
		return new HasElementSubscriber<>(actual);
	}

	static final class HasElementSubscriber<T>
			extends Operators.BaseFluxToMonoOperator<T, Boolean> {

		boolean done;

		HasElementSubscriber(CoreSubscriber<? super Boolean> actual) {
			super(actual);
		}

		@Override
		public @Nullable Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;

			return super.scanUnsafe(key);
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, currentContext());
				return;
			}

			this.done = true;

			Operators.onDiscard(t, currentContext());

			this.actual.onNext(true);
			this.actual.onComplete();
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, currentContext());
				return;
			}

			this.done = true;

			this.actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}

			this.done = true;

			completePossiblyEmpty();
		}

		@Override
		Boolean accumulatedValue() {
			return false;
		}
	}
}

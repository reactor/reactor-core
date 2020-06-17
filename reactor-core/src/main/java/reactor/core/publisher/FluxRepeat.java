/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;

/**
 * Repeatedly subscribes to the source and relays its values either
 * indefinitely or a fixed number of times.
 * <p>
 * The times == Long.MAX_VALUE is treated as infinite repeat. Times = 1 mirrors the source
 * (the "original" run) and then repeats it once more. Callers should take care of times =
 * 0 and times = -1 cases, which should map to replaying the source and an empty sequence,
 * respectively.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxRepeat<T> extends InternalFluxOperator<T, T> {

	final long times;

	FluxRepeat(Flux<? extends T> source, long times) {
		super(source);
		if (times <= 0L) {
			throw new IllegalArgumentException("times > 0 required");
		}
		this.times = times;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		RepeatSubscriber<T> parent = new RepeatSubscriber<>(source, actual, times  + 1);

		actual.onSubscribe(parent);

		if (!parent.isCancelled()) {
			parent.onComplete();
		}
		return null;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class RepeatSubscriber<T>
			extends Operators.MultiSubscriptionSubscriber<T, T> {

		final CorePublisher<? extends T> source;

		long remaining;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<RepeatSubscriber> WIP =
		  AtomicIntegerFieldUpdater.newUpdater(RepeatSubscriber.class, "wip");

		long produced;

		RepeatSubscriber(CorePublisher<? extends T> source, CoreSubscriber<? super T> actual, long remaining) {
			super(actual);
			this.source = source;
			this.remaining = remaining;
		}

		@Override
		public void onNext(T t) {
			produced++;

			actual.onNext(t);
		}

		@Override
		public void onComplete() {
			long r = remaining;
			if (r != Long.MAX_VALUE) {
				if (r == 0) {
					actual.onComplete();
					return;
				}
				remaining = r - 1;
			}

			resubscribe();
		}

		void resubscribe() {
			if (WIP.getAndIncrement(this) == 0) {
				do {
					if (isCancelled()) {
						return;
					}

					long c = produced;
					if (c != 0L) {
						produced = 0L;
						produced(c);
					}

					source.subscribe(this);

				} while (WIP.decrementAndGet(this) != 0);
			}
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
			return super.scanUnsafe(key);
		}
	}
}

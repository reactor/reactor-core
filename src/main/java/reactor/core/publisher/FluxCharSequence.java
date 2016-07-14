/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscriber;
import reactor.core.flow.Fuseable;
import reactor.core.subscriber.SubscriptionHelper;

/**
 * Streams the characters of a string.
 */

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 * @since 2.5
 */
final class FluxCharSequence extends Flux<Integer> {

	final CharSequence string;

	public FluxCharSequence(CharSequence string) {
		this.string = string;
	}

	@Override
	public void subscribe(Subscriber<? super Integer> s) {
		s.onSubscribe(new CharSequenceSubscription(s, string));
	}

	static final class CharSequenceSubscription
			implements Fuseable.QueueSubscription<Integer> {

		final Subscriber<? super Integer> actual;

		final CharSequence string;

		final int end;

		int index;

		volatile boolean cancelled;

		volatile long requested;
		static final AtomicLongFieldUpdater<CharSequenceSubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(CharSequenceSubscription.class,
						"requested");

		public CharSequenceSubscription(Subscriber<? super Integer> actual,
				CharSequence string) {
			this.actual = actual;
			this.string = string;
			this.end = string.length();
		}

		@Override
		public void cancel() {
			cancelled = true;
		}

		@Override
		public void request(long n) {
			if (SubscriptionHelper.validate(n)) {
				if (SubscriptionHelper.getAndAddCap(REQUESTED, this, n) == 0) {
					if (n == Long.MAX_VALUE) {
						fastPath();
					}
					else {
						slowPath(n);
					}
				}
			}
		}

		void fastPath() {
			int e = end;
			CharSequence s = string;
			Subscriber<? super Integer> a = actual;

			for (int i = index; i != e; i++) {
				if (cancelled) {
					return;
				}

				a.onNext((int) s.charAt(i));
			}

			if (!cancelled) {
				a.onComplete();
			}
		}

		void slowPath(long r) {
			long e = 0L;
			int i = index;
			int f = end;
			CharSequence s = string;
			Subscriber<? super Integer> a = actual;

			for (; ; ) {

				while (e != r && i != f) {
					if (cancelled) {
						return;
					}

					a.onNext((int) s.charAt(i));

					i++;
					e++;
				}

				if (i == f) {
					if (!cancelled) {
						a.onComplete();
					}
					return;
				}

				r = requested;
				if (e == r) {
					index = i;
					r = REQUESTED.addAndGet(this, -e);
					if (r == 0L) {
						break;
					}
				}
			}
		}

		@Override
		public int requestFusion(int requestedMode) {
			return requestedMode & Fuseable.SYNC;
		}

		@Override
		public Integer poll() {
			int i = index;
			if (i != end) {
				index = i + 1;
				return (int) string.charAt(i);
			}
			return null;
		}

		@Override
		public boolean isEmpty() {
			return index != end;
		}

		@Override
		public int size() {
			return end - index;
		}

		@Override
		public void clear() {
			index = end;
		}
	}

}

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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Subscriber;
import reactor.util.context.Context;
import javax.annotation.Nullable;

/**
 * Emits a constant or generated Throwable instance to Subscribers.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxError<T> extends Flux<T> {

	final Throwable error;

	final boolean whenRequested;

	FluxError(Throwable error, boolean whenRequested) {
		this.error = Objects.requireNonNull(error);
		this.whenRequested = whenRequested;
	}

	@Override
	public void subscribe(Subscriber<? super T> s, Context context) {
		if (whenRequested) {
			s.onSubscribe(new ErrorSubscription(s, error));
		}
		else {
			Operators.error(s, Operators.onOperatorError(error));
		}
	}

	static final class ErrorSubscription implements InnerProducer {

		final Subscriber<?> actual;

		final Throwable error;

		volatile int once;
		static final AtomicIntegerFieldUpdater<ErrorSubscription> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(ErrorSubscription.class, "once");

		ErrorSubscription(Subscriber<?> actual, Throwable error) {
			this.actual = actual;
			this.error = error;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				if (ONCE.compareAndSet(this, 0, 1)) {
					actual.onError(error);
				}
			}
		}

		@Override
		public void cancel() {
			once = 1;
		}

		@Override
		public Subscriber<?> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ThrowableAttr.ERROR) return error;
			if (key == BooleanAttr.CANCELLED || key == BooleanAttr.TERMINATED)
				return once == 1;

			return InnerProducer.super.scanUnsafe(key);
		}
	}
}

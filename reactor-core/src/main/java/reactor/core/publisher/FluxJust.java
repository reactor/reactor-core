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

import java.util.Objects;

import org.reactivestreams.Subscriber;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

/**
 * A Stream that emits only one value and then complete.
 * <p>
 * Since the flux retains the value in a final field, any
 * {@link this#subscribe(Subscriber)} will
 * replay the value. This is a "Cold" fluxion.
 * <p>
 * Create such flux with the provided factory, E.g.:
 * <pre>
 * {@code
 * Flux.just(1).subscribe(
 *    log::info,
 *    log::error,
 *    ()-> log.info("complete")
 * )
 * }
 * </pre>
 * Will log:
 * <pre>
 * {@code
 * 1
 * complete
 * }
 * </pre>
 *
 * @author Stephane Maldini
 */
final class FluxJust<T> extends Flux<T>
		implements Fuseable.ScalarCallable<T>, Fuseable,
		           SourceProducer<T> {

	final T value;

	FluxJust(T value) {
		this.value = Objects.requireNonNull(value, "value");
	}

	@Override
	public T call() throws Exception {
		return value;
	}

	@Override
	public void subscribe(final CoreSubscriber<? super T> actual) {
		actual.onSubscribe(new WeakScalarSubscription<>(value, actual));
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.BUFFERED) return 1;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}

	static final class WeakScalarSubscription<T> implements QueueSubscription<T>,
	                                                        InnerProducer<T>{

		boolean terminado;
		final T                     value;
		final CoreSubscriber<? super T> actual;

		WeakScalarSubscription(@Nullable T value, CoreSubscriber<? super T> actual) {
			this.value = value;
			this.actual = actual;
		}

		@Override
		public void request(long elements) {
			if (terminado) {
				return;
			}

			terminado = true;
			if (value != null) {
				actual.onNext(value);
			}
			actual.onComplete();
		}

		@Override
		public void cancel() {
			terminado = true;
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & Fuseable.SYNC) != 0) {
				return Fuseable.SYNC;
			}
			return 0;
		}

		@Override
		@Nullable
		public T poll() {
			if (!terminado) {
				terminado = true;
				return value;
			}
			return null;
		}

		@Override
		public boolean isEmpty() {
			return terminado;
		}

		@Override
		public int size() {
			return isEmpty() ? 0 : 1;
		}

		@Override
		public void clear() {
			terminado = true;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED || key == Attr.CANCELLED) return terminado;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerProducer.super.scanUnsafe(key);
		}
	}
}

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

import java.util.Collection;
import java.util.Objects;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import reactor.core.scheduler.Scheduler;

/**
 * @author Stephane Maldini
 */
final class FluxBufferTimeOrSize<T, C extends Collection<? super T>> extends FluxBatch<T, C> {

	final Supplier<C> bufferSupplier;

	FluxBufferTimeOrSize(Flux<T> source,
			int maxSize,
			long timespan,
			Scheduler timer,
			Supplier<C> bufferSupplier) {
		super(source, maxSize, timespan, timer);
		this.bufferSupplier = Objects.requireNonNull(bufferSupplier, "bufferSupplier");
	}

	@Override
	public void subscribe(Subscriber<? super C> subscriber) {
		source.subscribe(new BufferTimeoutSubscriber<>(prepareSub(subscriber),
				batchSize,
				timespan,
				timer.createWorker(),
				bufferSupplier));
	}

	final static class BufferTimeoutSubscriber<T, C extends Collection<? super T>> extends
	                                                                               BatchSubscriber<T, C> {

		final Supplier<C> bufferSupplier;
		volatile C values;

		BufferTimeoutSubscriber(Subscriber<? super C> actual,
				int maxSize,
				long timespan,
				Scheduler.Worker timer,
				Supplier<C> bufferSupplier) {
			super(actual, maxSize, false, timespan, timer);
			this.bufferSupplier = bufferSupplier;
		}

		@Override
		protected void doOnSubscribe() {
			values = bufferSupplier.get();
		}

		@Override
		protected void checkedError(Throwable ev) {
			synchronized (this) {
				C v = values;
				if(v != null) {
					v.clear();
					values = null;
				}
			}
			actual.onError(ev);
		}

		@Override
		public void nextCallback(T value) {
			synchronized (this) {
				C v = values;
				if(v == null) {
					v = Objects.requireNonNull(bufferSupplier.get(),
							"The bufferSupplier returned a null buffer");
					values = v;
				}
				v.add(value);
			}
		}

		@Override
		public void flushCallback(T ev) {
			C v = values;
			boolean flush = false;
			synchronized (this) {
				if (v != null && !v.isEmpty()) {
					values = bufferSupplier.get();
					flush = true;
				}
			}

			if (flush) {
				actual.onNext(v);
			}
		}
	}
}


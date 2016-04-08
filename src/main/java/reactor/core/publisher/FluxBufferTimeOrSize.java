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

import java.util.ArrayList;
import java.util.List;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.scheduler.Timer;

/**
 * @author Stephane Maldini
 * @since 1.1, 2.5
 */
final class FluxBufferTimeOrSize<T> extends FluxBatch<T, List<T>> {

	public FluxBufferTimeOrSize(Publisher<T> source, int maxSize, long timespan, Timer timer) {
		super(source, maxSize, true, false, true, timespan, timer);
	}

	@Override
	public void subscribe(Subscriber<? super List<T>> subscriber) {
		source.subscribe(new BufferAction<>(prepareSub(subscriber), batchSize, timespan, timer));
	}

	final static class BufferAction<T> extends BatchAction<T, List<T>> {

		private final List<T> values = new ArrayList<T>();

		public BufferAction(Subscriber<? super List<T>> actual,
				int maxSize,
				long timespan,
				Timer timer) {

			super(actual, maxSize, true, false, true, timespan, timer);
		}

		@Override
		protected void checkedError(Throwable ev) {
			if (timer != null) {
				synchronized (timer) {
					values.clear();
				}
			}
			else {
				values.clear();
			}
			subscriber.onError(ev);
		}

		@Override
		public void nextCallback(T value) {
			if (timer != null) {
				synchronized (timer) {
					values.add(value);
				}
			}
			else {
				values.add(value);
			}
		}

		@Override
		public void flushCallback(T ev) {
			final List<T> toSend;
			if (timer != null) {
				synchronized (timer) {
					if (values.isEmpty()) {
						return;
					}
					toSend = new ArrayList<T>(values);
					values.clear();
				}
			}
			else {
				if (values.isEmpty()) {
					return;
				}
				toSend = new ArrayList<T>(values);
				values.clear();
			}
			subscriber.onNext(toSend);
		}
	}
}


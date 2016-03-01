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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.timer.Timer;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.5
 */
final class FluxEvery<T> extends FluxBatch<T, T> {

	public FluxEvery(Publisher<T> source, int maxSize) {
		this(source, maxSize, false);
	}

	public FluxEvery(Publisher<T> source, int maxSize, boolean first) {
		super(source, maxSize, !first, first, true);
	}

	@Override
	public void subscribe(Subscriber<? super T> subscriber) {
		source.subscribe(new SampleAction<T>(prepareSub(subscriber), first, batchSize, timespan, timer));
	}

	final static class SampleAction<T> extends BatchAction<T, T> {

		private T sample;

		public SampleAction(Subscriber<? super T> actual,
				boolean first,
				int maxSize,
				long timespan,
				Timer timer) {

			super(actual, maxSize, !first, first, true, timespan, timer);
		}

		@Override
		protected void firstCallback(T event) {
			sample = event;
		}

		@Override
		protected void nextCallback(T event) {
			sample = event;
		}

		@Override
		protected void flushCallback(T event) {
			if (sample != null) {
				T _last = sample;
				sample = null;
				subscriber.onNext(_last);
			}
		}

		@Override
		public String toString() {
			return super.toString() + " every=" + sample;
		}
	}
}

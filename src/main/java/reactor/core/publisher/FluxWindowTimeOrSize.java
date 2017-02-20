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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.scheduler.Scheduler;



/**
 * WindowTimeoutSubscriber is forwarding events on a steam until {@code backlog} is reached, after that streams collected events
 * further, complete it and create a fresh new fluxion.
 * @author Stephane Maldini
 */
final class FluxWindowTimeOrSize<T> extends FluxBatch<T, Flux<T>> {

	FluxWindowTimeOrSize(Flux<T> source, int backlog, long timespan, Scheduler timer) {
		super(source, backlog, timespan, timer);
	}

	@Override
	public void subscribe(Subscriber<? super Flux<T>> subscriber) {
		source.subscribe(new WindowTimeoutSubscriber<>(prepareSub(subscriber),
						batchSize, timespan, timer));
	}

	final static class Window<T> extends Flux<T> implements InnerOperator<T, T> {

		final UnicastProcessor<T> processor;
		final Scheduler      timer;

		int count = 0;

		Window(Scheduler timer) {
			this.processor = UnicastProcessor.create();
			this.timer = timer;
		}

		@Override
		public void onSubscribe(Subscription s) {
		}

		@Override
		public void onNext(T t) {
			count++;
			processor.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			processor.onError(t);
		}

		@Override
		public void onComplete() {
			processor.onComplete();
		}

		@Override
		public void subscribe(Subscriber<? super T> s) {
			processor.subscribe(s);
		}

		@Override
		public void request(long n) {

		}

		@Override
		public void cancel() {

		}

		@Override
		public Subscriber<? super T> actual() {
			return processor;
		}
	}

	final static class WindowTimeoutSubscriber<T> extends BatchSubscriber<T, Flux<T>> {

		final Scheduler timer;

		Window<T> currentWindow;

		WindowTimeoutSubscriber(Subscriber<? super Flux<T>> actual,
				int backlog,
				long timespan,
				Scheduler timer) {
			super(actual, backlog, true, timespan, timer.createWorker());
			this.timer = timer;

		}

		@Override
		void doOnSubscribe() {

		}

		Flux<T> createWindowStream() {
			Window<T> _currentWindow = new Window<>(timer);
			currentWindow = _currentWindow;
			return _currentWindow;
		}

		@Override
		protected void checkedError(Throwable ev) {
			if (currentWindow != null) {
				currentWindow.onError(ev);
			}
			super.checkedError(ev);
		}

		@Override
		protected void checkedComplete() {
			try {
				if (currentWindow != null) {
					currentWindow.onComplete();
					currentWindow = null;
				}
			}
			finally {
				super.checkedComplete();
			}
		}

		@Override
		protected void firstCallback(T event) {
			actual.onNext(createWindowStream());
		}

		@Override
		protected void nextCallback(T event) {
			if (currentWindow != null) {
				currentWindow.onNext(event);
			}
		}

		@Override
		protected void flushCallback(T event) {
			if (currentWindow != null) {
				currentWindow.onComplete();
				//currentWindow = null;
			}
		}

	}


}

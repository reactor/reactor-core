/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.function.Supplier;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.scheduler.Scheduler;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

/**
 * @author Simon Baslé
 */
final class FluxPublishOnCaptured<T> extends FluxOperator<T, T> {

	final Supplier<Scheduler> alternative;

	FluxPublishOnCaptured(Flux<? extends T> source, Supplier<Scheduler> alternative) {
		super(source);
		this.alternative = alternative;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		source.subscribe(new PublishOnCapturedSubscriber<>(actual, alternative));
	}

	static final class PublishOnCapturedSubscriber<T>
		extends FluxPublishOn.PublishOnSubscriber<T> {

		final Context             context;
		final Supplier<Scheduler> alternativeSupplier;

		PublishOnCapturedSubscriber(CoreSubscriber<? super T> actual, Supplier<Scheduler> alternativeSupplier) {
			super(actual,
					null,
					null,
					true,
					Queues.SMALL_BUFFER_SIZE,
					Queues.SMALL_BUFFER_SIZE,
					Queues.get(Queues.SMALL_BUFFER_SIZE));
			this.context = actual.currentContext()
			                     .put(PublishOnCapturedSubscriber.class, this);
			this.alternativeSupplier = alternativeSupplier;
		}

		@Override
		public Context currentContext() {
			return this.context;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (scheduler == null && !done && !cancelled) {
				Scheduler alternative = alternativeSupplier.get();
				this.scheduler = alternative;
				this.worker = alternative.createWorker();
			}
			super.onSubscribe(s);
		}

		void set(Scheduler scheduler) {
			if (this.scheduler != null || done || cancelled) {
				return;
			}
			this.scheduler = scheduler;
			this.worker = scheduler.createWorker();
		}
	}
}

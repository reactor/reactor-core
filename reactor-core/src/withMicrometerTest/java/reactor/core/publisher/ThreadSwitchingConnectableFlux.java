/*
 * Copyright (c) 2024 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;

public class ThreadSwitchingConnectableFlux<T> extends ConnectableFlux<T>
		implements Subscription {

	private final ExecutorService           executorService;
	private final T                         item;
	private final Throwable                 error;
	private       CoreSubscriber<? super T> actual;
	AtomicBoolean done = new AtomicBoolean();

	public ThreadSwitchingConnectableFlux(T item, ExecutorService executorService) {
		this.executorService = executorService;
		this.item = item;
		this.error = null;
	}

	@Override
	public void connect(Consumer<? super Disposable> cancelSupport) {
		// Assuming just one Subscriber
		if (!done.get()) {
			this.executorService.submit(this::deliver);
		}
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		this.actual = actual;
		this.actual.onSubscribe(this);
	}

	private void deliver() {
		if (done.compareAndSet(false, true)) {
			if (this.item != null) {
				this.actual.onNext(this.item);
			}
			if (this.error != null) {
				this.actual.onError(this.error);
			}
			this.executorService.submit(this.actual::onComplete);
		}
	}

	@Override
	public void request(long n) {
		// ignore, assume there's always request
		Operators.validate(n);
	}

	@Override
	public void cancel() {
		done.set(true);
	}
}

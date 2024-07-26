/*
 * Copyright (c) 2023 VMware Inc. or its affiliates, All Rights Reserved.
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

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;

public class ThreadSwitchingParallelFlux<T> extends ParallelFlux<T> implements
                                                                    Subscription, Runnable {

	private final T item;
	private final ExecutorService executorService;
	AtomicBoolean done = new AtomicBoolean();
	CoreSubscriber<? super T>[] actual;

	public ThreadSwitchingParallelFlux(T item, ExecutorService executorService) {
		this.item = item;
		this.executorService = executorService;
	}

	@Override
	public int parallelism() {
		return 1;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T>[] subscribers) {
		if (!validate(subscribers)) {
			return;
		}

		this.actual = subscribers;
		executorService.submit(this);
	}

	@Override
	public void run() {
		actual[0].onSubscribe(this);
	}

	private void deliver() {
		if (done.compareAndSet(false, true)) {
			this.actual[0].onNext(this.item);
			this.executorService.submit(this.actual[0]::onComplete);
		}
	}

	@Override
	public void request(long n) {
		if (Operators.validate(n)) {
			if (!done.get()) {
				this.executorService.submit(this::deliver);
			}
		}
	}

	@Override
	public void cancel() {
		done.set(true);
	}
}

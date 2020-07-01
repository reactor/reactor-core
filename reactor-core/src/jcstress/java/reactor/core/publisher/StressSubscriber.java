/*
 * Copyright (c) 2020-Present Pivotal Software Inc, All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class StressSubscriber extends BaseSubscriber<Object> {

	enum Operation {
		ON_NEXT,
		ON_ERROR,
		ON_COMPLETE,
	}

	public AtomicReference<Operation> guard = new AtomicReference<>(null);

	public AtomicBoolean concurrentOnNext = new AtomicBoolean(false);

	public AtomicBoolean concurrentOnError = new AtomicBoolean(false);

	public AtomicBoolean concurrentOnComplete = new AtomicBoolean(false);

	public final AtomicInteger onNextCalls = new AtomicInteger();

	public final AtomicInteger onErrorCalls = new AtomicInteger();

	public final AtomicInteger onCompleteCalls = new AtomicInteger();

	@Override
	protected void hookOnNext(Object value) {
		if (!guard.compareAndSet(null, Operation.ON_NEXT)) {
			concurrentOnNext.set(true);
		} else {
			guard.compareAndSet(Operation.ON_NEXT, null);
		}
		onNextCalls.incrementAndGet();
	}

	@Override
	protected void hookOnError(Throwable throwable) {
		if (!guard.compareAndSet(null, Operation.ON_ERROR)) {
			concurrentOnError.set(true);
		} else {
			guard.compareAndSet(Operation.ON_ERROR, null);
		}
		onErrorCalls.incrementAndGet();
	}

	@Override
	protected void hookOnComplete() {
		if (!guard.compareAndSet(null, Operation.ON_COMPLETE)) {
			concurrentOnComplete.set(true);
		} else {
			guard.compareAndSet(Operation.ON_COMPLETE, null);
		}
		onCompleteCalls.incrementAndGet();
	}
}

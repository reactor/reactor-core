/*
 * Copyright (c) 2020-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import reactor.core.publisher.Sinks.One;

public class SinkOneSerialized<T> extends SinkEmptySerialized<T> implements InternalOneSink<T>, ContextHolder {

	final One<T> sinkOne;

	public SinkOneSerialized(One<T> sinkOne, ContextHolder contextHolder) {
		super(sinkOne, contextHolder);
		this.sinkOne = sinkOne;
	}

	@Override
	public Sinks.EmitResult tryEmitValue(T t) {
		Thread currentThread = Thread.currentThread();
		if (!tryAcquire(currentThread)) {
			return Sinks.EmitResult.FAIL_NON_SERIALIZED;
		}

		try {
			return sinkOne.tryEmitValue(t);
		}
		finally {
			if (WIP.decrementAndGet(this) == 0) {
				LOCKED_AT.compareAndSet(this, currentThread, null);
			}
		}
	}
}

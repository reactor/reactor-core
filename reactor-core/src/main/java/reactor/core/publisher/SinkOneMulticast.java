/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.time.Duration;
import java.util.Objects;

import reactor.core.CoreSubscriber;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.util.annotation.Nullable;

final class SinkOneMulticast<O> extends SinkEmptyMulticast<O> implements InternalOneSink<O> {

	@SuppressWarnings("rawtypes")
	static final Inner[] TERMINATED_VALUE = new Inner[0];

	static final int STATE_VALUE = 1;

	@Nullable
	O value;

	@Override
	boolean isTerminated(Inner<?>[] array) {
		return array == TERMINATED_VALUE || super.isTerminated(array);
	}

	@Override
	public EmitResult tryEmitValue(@Nullable O value) {
		Inner<O>[] prevSubscribers = this.subscribers;

		if (isTerminated(prevSubscribers)) {
			return EmitResult.FAIL_TERMINATED;
		}

		if (value == null) {
			return tryEmitEmpty();
		}

		this.value = value;

		for (;;) {
			if (SUBSCRIBERS.compareAndSet(this, prevSubscribers, TERMINATED_VALUE)) {
				break;
			}

			prevSubscribers = this.subscribers;
			if (isTerminated(prevSubscribers)) {
				return EmitResult.FAIL_TERMINATED;
			}
		}

		for (Inner<O> as : prevSubscribers) {
			as.complete(value);
		}

		return EmitResult.OK;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.TERMINATED) return isTerminated(subscribers);
		if (key == Attr.ERROR) return subscribers == TERMINATED_ERROR ? error : null;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

	@Override
	public void subscribe(final CoreSubscriber<? super O> actual) {
		NextInner<O> as = new NextInner<>(actual, this);
		actual.onSubscribe(as);
		final int addState = add(as);
		if (addState == STATE_ADDED) {
			if (as.isCancelled()) {
				remove(as);
			}
		}
		else if (addState == STATE_ERROR) {
			actual.onError(error);
		}
		else if (addState == STATE_EMPTY) {
			as.complete();
		}
		else {
			as.complete(value);
		}
	}

	@Override
	int add(Inner<O> ps) {
		for (; ; ) {
			Inner<O>[] a = subscribers;

			if (a == TERMINATED_EMPTY) {
				return STATE_EMPTY;
			}

			if (a == TERMINATED_ERROR) {
				return STATE_ERROR;
			}

			if (a == TERMINATED_VALUE) {
				return STATE_VALUE;
			}

			int n = a.length;
			@SuppressWarnings("unchecked") Inner<O>[] b = new Inner[n + 1];
			System.arraycopy(a, 0, b, 0, n);
			b[n] = ps;

			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				return STATE_ADDED;
			}
		}
	}

	@Nullable
	@Override
	public O block(Duration timeout) {
		if (timeout.isNegative()) {
			return super.block(Duration.ZERO);
		}
		return super.block(timeout);
	}

	final static class NextInner<T> extends Operators.MonoInnerProducerBase<T> implements Inner<T> {

		final SinkOneMulticast<T> parent;

		NextInner(CoreSubscriber<? super T> actual, SinkOneMulticast<T> parent) {
			super(actual);
			this.parent = parent;
		}

		@Override
		protected void doOnCancel() {
			parent.remove(this);
		}

		@Override
		public void error(Throwable t) {
			if (!isCancelled()) {
				actual().onError(t);
			}
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.RUN_STYLE) {
				return Attr.RunStyle.SYNC;
			}
			return super.scanUnsafe(key);
		}
	}
}

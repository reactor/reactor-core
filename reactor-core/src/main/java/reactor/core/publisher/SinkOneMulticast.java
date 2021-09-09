/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

	@Nullable
	O value;

	@Override
	public EmitResult tryEmitEmpty() {
		return tryEmitValue(null);
	}

	@Override
	@SuppressWarnings("unchecked")
	public EmitResult tryEmitError(Throwable cause) {
		Objects.requireNonNull(cause, "onError cannot be null");

		Inner<O>[] prevSubscribers = SUBSCRIBERS.getAndSet(this, TERMINATED);
		if (prevSubscribers == TERMINATED) {
			return EmitResult.FAIL_TERMINATED;
		}

		error = cause;
		value = null;

		for (Inner<O> as : prevSubscribers) {
			as.error(cause);
		}
		return EmitResult.OK;
	}

	@Override
	public EmitResult tryEmitValue(@Nullable O value) {
		@SuppressWarnings("unchecked") Inner<O>[] array = SUBSCRIBERS.getAndSet(this, TERMINATED);
		if (array == TERMINATED) {
			return EmitResult.FAIL_TERMINATED;
		}

		this.value = value;
		if (value == null) {
			for (Inner<O> as : array) {
				as.complete();
			}
		}
		else {
			for (Inner<O> as : array) {
				as.complete(value);
			}
		}
		return EmitResult.OK;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.TERMINATED) return subscribers == TERMINATED;
		if (key == Attr.ERROR) return error;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

	@Override
	public void subscribe(final CoreSubscriber<? super O> actual) {
		NextInner<O> as = new NextInner<>(actual, this);
		actual.onSubscribe(as);
		if (add(as)) {
			if (as.isCancelled()) {
				remove(as);
			}
		}
		else {
			Throwable ex = error;
			if (ex != null) {
				actual.onError(ex);
			}
			else {
				O v = value;
				if (v != null) {
					as.complete(v);
				}
				else {
					as.complete();
				}
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

/*
 * Copyright (c) 2020-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

//intentionally not final
class SinkEmptyMulticast<T> extends Mono<T> implements InternalEmptySink<T> {

	volatile Inner<T>[]                                                   subscribers;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<SinkEmptyMulticast, Inner[]> SUBSCRIBERS =
		AtomicReferenceFieldUpdater.newUpdater(SinkEmptyMulticast.class, Inner[].class, "subscribers");

	@SuppressWarnings("rawtypes")
	static final Inner[] EMPTY = new Inner[0];
	@SuppressWarnings("rawtypes")
	static final Inner[] TERMINATED_EMPTY = new Inner[0];
	@SuppressWarnings("rawtypes")
	static final Inner[] TERMINATED_ERROR = new Inner[0];

	static final int STATE_ADDED = 0;
	static final int STATE_ERROR = -1;
	static final int STATE_EMPTY = -2;

	@Nullable
	Throwable error;

	SinkEmptyMulticast() {
		SUBSCRIBERS.lazySet(this, EMPTY);
	}

	@Override
	public int currentSubscriberCount() {
		return subscribers.length;
	}

	@Override
	public Mono<T> asMono() {
		return this;
	}

	boolean isTerminated(Inner<?>[] array) {
		return array == TERMINATED_EMPTY || array == TERMINATED_ERROR;
	}

	@Override
	public EmitResult tryEmitEmpty() {
		Inner<T>[] array;
		for (;;) {
			array = this.subscribers;

			if (isTerminated(array)) {
				return EmitResult.FAIL_TERMINATED;
			}

			if (SUBSCRIBERS.compareAndSet(this, array, TERMINATED_EMPTY)) {
				break;
			}
		}

		for (Inner<?> as : array) {
			as.complete();
		}
		return EmitResult.OK;
	}

	@Override
	@SuppressWarnings("unchecked")
	public EmitResult tryEmitError(Throwable cause) {
		Objects.requireNonNull(cause, "onError cannot be null");

		Inner<T>[] prevSubscribers = this.subscribers;

		if (isTerminated(prevSubscribers)) {
			return EmitResult.FAIL_TERMINATED;
		}

		error = cause;

		for (;;) {
			if (SUBSCRIBERS.compareAndSet(this, prevSubscribers, TERMINATED_ERROR)) {
				break;
			}

			prevSubscribers = this.subscribers;
			if (isTerminated(prevSubscribers)) {
				return EmitResult.FAIL_TERMINATED;
			}
		}

		for (Inner<T> as : prevSubscribers) {
			as.error(cause);
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
	public Context currentContext() {
		return Operators.multiSubscribersContext(subscribers);
	}

	int add(Inner<T> ps) {
		for (; ; ) {
			Inner<T>[] a = subscribers;

			if (a == TERMINATED_EMPTY) {
				return STATE_EMPTY;
			}

			if (a == TERMINATED_ERROR) {
				return STATE_ERROR;
			}

			int n = a.length;
			@SuppressWarnings("unchecked") Inner<T>[] b = new Inner[n + 1];
			System.arraycopy(a, 0, b, 0, n);
			b[n] = ps;

			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				return STATE_ADDED;
			}
		}
	}

	@SuppressWarnings("unchecked")
	void remove(Inner<T> ps) {
		for (; ; ) {
			Inner<T>[] a = subscribers;
			int n = a.length;
			if (n == 0) {
				return;
			}

			int j = -1;
			for (int i = 0; i < n; i++) {
				if (a[i] == ps) {
					j = i;
					break;
				}
			}

			if (j < 0) {
				return;
			}

			Inner<T>[] b;

			if (n == 1) {
				b = EMPTY;
			}
			else {
				b = new Inner[n - 1];
				System.arraycopy(a, 0, b, 0, j);
				System.arraycopy(a, j + 1, b, j, n - j - 1);
			}
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				return;
			}
		}
	}

	//redefined in SinkOneMulticast
	@Override
	public void subscribe(final CoreSubscriber<? super T> actual) {
		Inner<T> as = new VoidInner<>(actual, this);
		actual.onSubscribe(as);
		final int addedState = add(as);
		if (addedState == STATE_ADDED) {
			if (as.isCancelled()) {
				remove(as);
			}
		}
		else if (addedState == STATE_ERROR) {
			Throwable ex = error;

			actual.onError(ex);
		}
		else {
			as.complete();
		}

	}

	@Override
	public Stream<? extends Scannable> inners() {
		return Stream.of(subscribers);
	}

	static interface Inner<T> extends InnerProducer<T> {
		//API must be compatible with Operators.MonoInnerProducerBase

		void error(Throwable t);
		void complete(T value);
		void complete();
		boolean isCancelled();
	}

	//VoidInner is optimized for not storing request / value
	final static class VoidInner<T> extends AtomicBoolean implements Inner<T> {

		final SinkEmptyMulticast<T> parent;
		final CoreSubscriber<? super T> actual;

		VoidInner(CoreSubscriber<? super T> actual, SinkEmptyMulticast<T> parent) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public void cancel() {
			if (getAndSet(true)) {
				return;
			}

			parent.remove(this);
		}

		@Override
		public boolean isCancelled() {
			return get();
		}

		@Override
		public void request(long l) {
			Operators.validate(l);
		}

		@Override
		public void complete(T value) {
			//NO-OP
		}

		@Override
		public void complete() {
			if (get()) {
				return;
			}
			actual.onComplete();
		}

		@Override
		public void error(Throwable t) {
			if (get()) {
				Operators.onOperatorError(t, actual.currentContext());
				return;
			}
			actual.onError(t);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) {
				return parent;
			}
			if (key == Attr.CANCELLED) {
				return get();
			}
			if (key == Attr.RUN_STYLE) {
				return Attr.RunStyle.SYNC;
			}
			return Inner.super.scanUnsafe(key);
		}
	}
}

/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.util.context.Context;

final class SinkEmptyMulticast<T> extends Mono<T> implements InternalEmptySink<T> {

	volatile VoidInner<T>[] subscribers;

	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<SinkEmptyMulticast, VoidInner[]> SUBSCRIBERS =
			AtomicReferenceFieldUpdater.newUpdater(SinkEmptyMulticast.class, VoidInner[].class, "subscribers");

	@SuppressWarnings("rawtypes")
	static final VoidInner[] EMPTY = new VoidInner[0];

	@SuppressWarnings("rawtypes")
	static final VoidInner[] TERMINATED = new VoidInner[0];

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

	@Override
	public EmitResult tryEmitEmpty() {
		VoidInner<?>[] array = SUBSCRIBERS.getAndSet(this, TERMINATED);

		if (array == TERMINATED) {
			return Sinks.EmitResult.FAIL_TERMINATED;
		}

		for (VoidInner<?> as : array) {
			as.onComplete();
		}
		return EmitResult.OK;
	}

	@Override
	public Sinks.EmitResult tryEmitError(Throwable cause) {
		Objects.requireNonNull(cause, "onError cannot be null");

		if (subscribers == TERMINATED) {
			return Sinks.EmitResult.FAIL_TERMINATED;
		}

		//guarded by a read memory barrier (isTerminated) and a subsequent write with getAndSet
		error = cause;

		for (VoidInner<?> as : SUBSCRIBERS.getAndSet(this, TERMINATED)) {
			as.onError(cause);
		}
		return Sinks.EmitResult.OK;
	}

	@Override
	public Context currentContext() {
		return Operators.multiSubscribersContext(subscribers);
	}

	boolean add(VoidInner<T> ps) {
		for (; ; ) {
			VoidInner<T>[] a = subscribers;

			if (a == TERMINATED) {
				return false;
			}

			int n = a.length;
			@SuppressWarnings("unchecked")
			VoidInner<T>[] b = new VoidInner[n + 1];
			System.arraycopy(a, 0, b, 0, n);
			b[n] = ps;

			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				return true;
			}
		}
	}

	void remove(VoidInner<T> ps) {
		for (; ; ) {
			VoidInner<T>[] a = subscribers;
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

			VoidInner<?>[] b;

			if (n == 1) {
				b = EMPTY;
			}
			else {
				b = new VoidInner[n - 1];
				System.arraycopy(a, 0, b, 0, j);
				System.arraycopy(a, j + 1, b, j, n - j - 1);
			}
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				return;
			}
		}
	}

	@Override
	public void subscribe(final CoreSubscriber<? super T> actual) {
		VoidInner<T> as = new VoidInner<T>(actual, this);
		actual.onSubscribe(as);
		if (add(as)) {
			if (as.get()) {
				remove(as);
			}
		}
		else {
			if (as.get()) {
				return;
			}
			Throwable ex = error;
			if (ex != null) {
				actual.onError(ex);
			}
			else {
				actual.onComplete();
			}
		}
	}

	@Override
	public Stream<? extends Scannable> inners() {
		return Stream.of(subscribers);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.TERMINATED) return subscribers == TERMINATED;
		if (key == Attr.ERROR) return error;

		return null;
	}

	final static class VoidInner<T> extends AtomicBoolean implements InnerOperator<Void, T> {
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
		public void request(long l) {
			Operators.validate(l);
		}

		@Override
		public void onSubscribe(Subscription s) {
			Objects.requireNonNull(s);
		}

		@Override
		public void onNext(Void aVoid) {

		}

		@Override
		public void onComplete() {
			if (get()) {
				return;
			}
			actual.onComplete();
		}

		@Override
		public void onError(Throwable t) {
			if (get()) {
				Operators.onOperatorError(t, currentContext());
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
			return InnerOperator.super.scanUnsafe(key);
		}
	}
}

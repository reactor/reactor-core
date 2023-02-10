/*
 * Copyright (c) 2016-2023 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.function.Function;

import io.micrometer.context.ContextSnapshot;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

final class MonoContextWriteRestoringThreadLocals<T> extends MonoOperator<T, T> {

	final Function<Context, Context> doOnContext;

	MonoContextWriteRestoringThreadLocals(Mono<? extends T> source,
			Function<Context, Context> doOnContext) {
		super(source);
		this.doOnContext = Objects.requireNonNull(doOnContext, "doOnContext");
	}

	@SuppressWarnings("try")
	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		final Context c = doOnContext.apply(actual.currentContext());

		final ContextWriteRestoringThreadLocalsSubscriber<T> threadLocalsSubscriber =
				new ContextWriteRestoringThreadLocalsSubscriber<>(actual, c);

		try (ContextSnapshot.Scope ignored = ContextSnapshot.setThreadLocals(c)) {
			source.subscribe(threadLocalsSubscriber);
		}

		// The onSubscribe signal is delivered outside the ThreadLocal scope
		// associated with the augmented Context. The corresponding onSubscribe
		// in ContextWriteRestoringThreadLocalsSubscriber implementation doesn't deliver
		// the signal, but we do it here to avoid unnecessary restoration.
		actual.onSubscribe(threadLocalsSubscriber);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class ContextWriteRestoringThreadLocalsSubscriber<T>
			implements InnerOperator<T, T>, Fuseable.QueueSubscription<T> {

		final CoreSubscriber<? super T> actual;
		final Context                   context;

		Subscription s;
		boolean      done;

		ContextWriteRestoringThreadLocalsSubscriber(CoreSubscriber<? super T> actual, Context context) {
			this.actual = actual;
			this.context = context;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) {
				return s;
			}
			if (key == Attr.RUN_STYLE) {
				return Attr.RunStyle.SYNC;
			}
			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Context currentContext() {
			return this.context;
		}

		@Override
		public void onSubscribe(Subscription s) {
			// The signal to downstream subscriber is delivered by the operator's
			// subscribe method to prevent additional ThreadLocal context restoration.
			if (Operators.validate(this.s, s)) {
				this.s = s;
			}
		}

		@SuppressWarnings("try")
		@Override
		public void onNext(T t) {
			this.done = true;
			// We probably ended up here from a request, which set thread locals to
			// current context, but we need to clean up and restore thread locals for
			// the actual subscriber downstream, as it can expect TLs to match the
			// different context.
			try (ContextSnapshot.Scope ignored =
					     ContextSnapshot.setThreadLocals(actual.currentContext())) {
				actual.onNext(t);
				actual.onComplete();
			}
		}

		@SuppressWarnings("try")
		@Override
		public void onError(Throwable t) {
			if (this.done) {
				Operators.onErrorDropped(t, context);
				return;
			}

			this.done = true;

			try (ContextSnapshot.Scope ignored =
					     ContextSnapshot.setThreadLocals(actual.currentContext())) {
				actual.onError(t);
			}
		}

		@SuppressWarnings("try")
		@Override
		public void onComplete() {
			if (this.done) {
				return;
			}

			this.done = true;

			try (ContextSnapshot.Scope ignored =
					     ContextSnapshot.setThreadLocals(actual.currentContext())) {
				actual.onComplete();
			}
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@SuppressWarnings("try")
		@Override
		public void request(long n) {
			try (ContextSnapshot.Scope ignored =
					     ContextSnapshot.setThreadLocals(context)) {
				s.request(n);
			}
		}

		@SuppressWarnings("try")
		@Override
		public void cancel() {
			try (ContextSnapshot.Scope ignored =
					     ContextSnapshot.setThreadLocals(context)) {
				s.cancel();
			}
		}

		@Override
		public int requestFusion(int requestedMode) {
			return Fuseable.NONE;
		}

		@Override
		@Nullable
		public T poll() {
			throw new UnsupportedOperationException("Operator does not support fusion");
		}

		@Override
		public boolean isEmpty() {
			throw new UnsupportedOperationException("Operator does not support fusion");
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException("Operator does not support fusion");
		}

		@Override
		public int size() {
			throw new UnsupportedOperationException("Operator does not support fusion");
		}
	}
}

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
import reactor.core.Fuseable.ConditionalSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

final class FluxContextWriteRestoringThreadLocals<T> extends FluxOperator<T, T> {

	final Function<Context, Context> doOnContext;

	FluxContextWriteRestoringThreadLocals(Flux<? extends T> source,
			Function<Context, Context> doOnContext) {
		super(source);
		this.doOnContext = Objects.requireNonNull(doOnContext, "doOnContext");
	}

	@SuppressWarnings("try")
	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Context c = doOnContext.apply(actual.currentContext());

		try (ContextSnapshot.Scope ignored = ContextPropagation.setThreadLocals(c)) {
			source.subscribe(new ContextWriteRestoringThreadLocalsSubscriber<>(actual, c));
		}
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class ContextWriteRestoringThreadLocalsSubscriber<T>
			implements ConditionalSubscriber<T>, InnerOperator<T, T> {

		final CoreSubscriber<? super T>        actual;
		final ConditionalSubscriber<? super T> actualConditional;
		final Context                          context;

		Subscription s;

		@SuppressWarnings("unchecked")
		ContextWriteRestoringThreadLocalsSubscriber(CoreSubscriber<? super T> actual, Context context) {
			this.actual = actual;
			this.context = context;
			if (actual instanceof ConditionalSubscriber) {
				this.actualConditional = (ConditionalSubscriber<? super T>) actual;
			}
			else {
				this.actualConditional = null;
			}
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

		@SuppressWarnings("try")
		@Override
		public void onSubscribe(Subscription s) {
			// This is needed, as the downstream can then switch threads,
			// continue the subscription using different primitives and omit this operator
			try (ContextSnapshot.Scope ignored =
					     ContextPropagation.setThreadLocals(actual.currentContext())) {
				if (Operators.validate(this.s, s)) {
					this.s = s;
					actual.onSubscribe(this);
				}
			}
		}

		@SuppressWarnings("try")
		@Override
		public void onNext(T t) {
			// We probably ended up here from a request, which set thread locals to
			// current context, but we need to clean up and restore thread locals for
			// the actual subscriber downstream, as it can expect TLs to match the
			// different context.
			try (ContextSnapshot.Scope ignored =
					     ContextPropagation.setThreadLocals(actual.currentContext())) {
				actual.onNext(t);
			}
		}

		@SuppressWarnings("try")
		@Override
		public boolean tryOnNext(T t) {
			try (ContextSnapshot.Scope ignored =
					     ContextPropagation.setThreadLocals(actual.currentContext())) {
				if (actualConditional != null) {
					return actualConditional.tryOnNext(t);
				}
				actual.onNext(t);
				return true;
			}
		}

		@SuppressWarnings("try")
		@Override
		public void onError(Throwable t) {
			try (ContextSnapshot.Scope ignored =
					     ContextPropagation.setThreadLocals(actual.currentContext())) {
				actual.onError(t);
			}
		}

		@SuppressWarnings("try")
		@Override
		public void onComplete() {
			try (ContextSnapshot.Scope ignored =
					     ContextPropagation.setThreadLocals(actual.currentContext())) {
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
					     ContextPropagation.setThreadLocals(context)) {
				s.request(n);
			}
		}

		@SuppressWarnings("try")
		@Override
		public void cancel() {
			try (ContextSnapshot.Scope ignored =
					     ContextPropagation.setThreadLocals(context)) {
				s.cancel();
			}
		}
	}
}

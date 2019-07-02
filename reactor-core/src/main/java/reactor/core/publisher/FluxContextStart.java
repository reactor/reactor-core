/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import java.util.Objects;
import java.util.function.Function;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

final class FluxContextStart<T> extends InternalFluxOperator<T, T> implements Fuseable {

	final Function<Context, Context> doOnContext;

	FluxContextStart(Flux<? extends T> source,
			Function<Context, Context> doOnContext) {
		super(source);
		this.doOnContext = Objects.requireNonNull(doOnContext, "doOnContext");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		Context c;
		try {
			c = doOnContext.apply(actual.currentContext());
		}
		catch (Throwable t) {
			Operators.error(actual, Operators.onOperatorError(t, actual.currentContext()));
			return null;
		}

		return new ContextStartSubscriber<>(actual, c);
	}

	static final class ContextStartSubscriber<T>
			implements ConditionalSubscriber<T>, InnerOperator<T, T>,
			           QueueSubscription<T> {

		final CoreSubscriber<? super T>        actual;
		final ConditionalSubscriber<? super T> actualConditional;
		final Context                          context;

		QueueSubscription<T> qs;
		Subscription         s;

		@SuppressWarnings("unchecked")
		ContextStartSubscriber(CoreSubscriber<? super T> actual, Context context) {
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
			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Context currentContext() {
			return this.context;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				if (s instanceof QueueSubscription) {
					this.qs = (QueueSubscription<T>) s;
				}
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public boolean tryOnNext(T t) {
			if (actualConditional != null) {
				return actualConditional.tryOnNext(t);
			}
			actual.onNext(t);
			return true;
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			actual.onComplete();
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		public int requestFusion(int requestedMode) {
			if (qs == null) {
				return Fuseable.NONE;
			}
			return qs.requestFusion(requestedMode);
		}

		@Override
		@Nullable
		public T poll() {
			if (qs != null) {
				return qs.poll();
			}
			return null;
		}

		@Override
		public boolean isEmpty() {
			return qs == null || qs.isEmpty();
		}

		@Override
		public void clear() {
			if (qs != null) {
				qs.clear();
			}
		}

		@Override
		public int size() {
			return qs != null ? qs.size() : 0;
		}
	}
}

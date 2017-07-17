/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Objects;
import java.util.function.BiFunction;
import javax.annotation.Nullable;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.context.Context;

final class FluxContextGet<T, V> extends FluxOperator<T, V> implements Fuseable {

	final BiFunction<? super T, Context, ? extends V> doOnContext;

	FluxContextGet(Flux<? extends T> source, BiFunction<? super T, Context, ? extends V> doOnContext) {
		super(source);
		this.doOnContext = Objects.requireNonNull(doOnContext, "doOnContext");
	}

	@Override
	public void subscribe(CoreSubscriber<? super V> s) {
		source.subscribe(new ContextGetSubscriber<>(s, doOnContext));
	}

	static final class ContextGetSubscriber<T, V>
			implements ConditionalSubscriber<T>, InnerOperator<T, V>,
			           QueueSubscription<V> {

		final CoreSubscriber<? super V>                   actual;
		final ConditionalSubscriber<? super V>            actualConditional;
		final BiFunction<? super T, Context, ? extends V> doOnContext;

		volatile Context context;

		QueueSubscription<T> qs;
		Subscription         s;
		int mode;

		@SuppressWarnings("unchecked")
		ContextGetSubscriber(CoreSubscriber<? super V> actual,
				BiFunction<? super T, Context, ? extends V> doOnContext) {
			this.actual = actual;
			this.context = actual.currentContext();
			this.doOnContext = doOnContext;
			if (actual instanceof ConditionalSubscriber) {
				this.actualConditional = (ConditionalSubscriber<? super V>) actual;
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
			if (mode == ASYNC) {
				actual.onNext(null);
			}
			else {
				V v;

				try {
					v = Objects.requireNonNull(doOnContext.apply(t, context),
							"The mapper returned a null value.");
				}
				catch (Throwable e) {
					onError(Operators.onOperatorError(s, e, t));
					return;
				}

				actual.onNext(v);
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			V v;
			try {
				v = Objects.requireNonNull(doOnContext.apply(t, context),
						"The mapper returned a null value.");
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(s, e, t));
				return true;
			}

			if (actualConditional != null) {
				return actualConditional.tryOnNext(v);
			}
			actual.onNext(v);
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
		public CoreSubscriber<? super V> actual() {
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
			int m;
			if ((requestedMode & Fuseable.THREAD_BARRIER) != 0) {
				return Fuseable.NONE;
			}
			else {
				m = qs.requestFusion(requestedMode);
			}
			mode = m;
			return m;
		}

		@Override
		@Nullable
		public V poll() {
			if (qs != null) {
				T v = qs.poll();
				if (v != null) {
					V u = doOnContext.apply(v, context);
					if (u == null) {
						throw new NullPointerException();
					}
					return u;
				}
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

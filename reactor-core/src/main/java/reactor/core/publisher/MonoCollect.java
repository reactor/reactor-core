/*
 * Copyright (c) 2011-Present Pivotal Software Inc, All Rights Reserved.
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

import java.util.Collection;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Collects the values of the source sequence into a container returned by
 * a supplier and a collector action working on the container and the current source
 * value.
 *
 * @param <T> the source value type
 * @param <R> the container value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoCollect<T, R> extends MonoFromFluxOperator<T, R>
		implements Fuseable {

	final Supplier<R> supplier;

	final BiConsumer<? super R, ? super T> action;

	MonoCollect(Flux<? extends T> source,
			Supplier<R> supplier,
			BiConsumer<? super R, ? super T> action) {
		super(source);
		this.supplier = Objects.requireNonNull(supplier, "supplier");
		this.action = Objects.requireNonNull(action);
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super R> actual) {
		R container = Objects.requireNonNull(supplier.get(),
				"The supplier returned a null container");

		return new CollectSubscriber<>(actual, action, container);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class CollectSubscriber<T, R> extends Operators.MonoSubscriber<T, R>  {

		final BiConsumer<? super R, ? super T> action;

		R container;

		Subscription s;

		boolean done;

		CollectSubscriber(CoreSubscriber<? super R> actual,
				BiConsumer<? super R, ? super T> action,
				R container) {
			super(actual);
			this.action = action;
			this.container = container;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
			return super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);

				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}
			R c;
			synchronized (this) {
				c = container;
				if (c != null) {
					try {
						action.accept(c, t);
					}
					catch (Throwable e) {
						Context ctx = actual.currentContext();
						Operators.onDiscard(t, ctx);
						onError(Operators.onOperatorError(this, e, t, ctx));
					}
					return;
				}
			}
			Operators.onDiscard(t, actual.currentContext());
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;
			R c;
			synchronized (this) {
				c = container;
				container = null;
			}
			discard(c);
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			R c;
			synchronized (this) {
				c = container;
				container = null;
			}
			if (c != null) {
				complete(c);
			}
		}

		@Override
		protected void discard(R v) {
			if (v instanceof Collection) {
				Collection<?> c = (Collection<?>) v;
				Operators.onDiscardMultiple(c, actual.currentContext());
			}
			else {
				super.discard(v);
			}
		}

		@Override
		public void cancel() {
			int state;
			R c;
			synchronized (this) {
				state = STATE.getAndSet(this, CANCELLED);
				if (state != CANCELLED) {
					s.cancel();
				}
				if (state <= HAS_REQUEST_NO_VALUE) {
					c = container;
					this.value = null;
					container = null;
				}
				else {
					c = null;
				}
			}
			if (c != null) {
				discard(c);
			}
		}
	}
}

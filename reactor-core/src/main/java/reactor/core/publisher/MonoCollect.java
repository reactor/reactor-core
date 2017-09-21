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
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;

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
	public void subscribe(CoreSubscriber<? super R> actual) {
		R container;

		try {
			container = Objects.requireNonNull(supplier.get(),
					"The supplier returned a null container");
		}
		catch (Throwable e) {
			Operators.error(actual, Operators.onOperatorError(e, actual.currentContext()));
			return;
		}

		source.subscribe(new CollectSubscriber<>(actual, action, container));
	}

	static final class CollectSubscriber<T, R> extends Operators.MonoSubscriber<T, R>  {

		final BiConsumer<? super R, ? super T> action;

		Subscription s;

		boolean done;

		CollectSubscriber(CoreSubscriber<? super R> actual,
				BiConsumer<? super R, ? super T> action,
				R container) {
			super(actual);
			this.action = action;
			this.value = container;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.PARENT) return s;

			return super.scanUnsafe(key);
		}

		@Override
		public void cancel() {
			super.cancel();
			s.cancel();
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

			try {
				action.accept(value, t);
			}
			catch (Throwable e) {
				onError(Operators.onOperatorError(this, e, t, actual.currentContext()));
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			complete(value);
		}

		@Override
		public void setValue(R value) {
			// value is constant
		}

	}
}

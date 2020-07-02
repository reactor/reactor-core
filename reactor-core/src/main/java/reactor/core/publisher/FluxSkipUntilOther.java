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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Skips values from the main publisher until the other publisher signals
 * an onNext or onComplete.
 *
 * @param <T> the value type of the main Publisher
 * @param <U> the value type of the other Publisher
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 */
final class FluxSkipUntilOther<T, U> extends InternalFluxOperator<T, T> {

	final Publisher<U> other;

	FluxSkipUntilOther(Flux<? extends T> source, Publisher<U> other) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		SkipUntilMainSubscriber<T> mainSubscriber = new SkipUntilMainSubscriber<>(actual);

		SkipUntilOtherSubscriber<U> otherSubscriber = new SkipUntilOtherSubscriber<>(mainSubscriber);

		other.subscribe(otherSubscriber);

		return mainSubscriber;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class SkipUntilOtherSubscriber<U> implements InnerConsumer<U> {

		final SkipUntilMainSubscriber<?> main;

		SkipUntilOtherSubscriber(SkipUntilMainSubscriber<?> main) {
			this.main = main;
		}

		@Override
		public Context currentContext() {
			return main.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return main.other == Operators.cancelledSubscription();
			if (key == Attr.PARENT) return main.other;
			if (key == Attr.ACTUAL) return main;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return null;
		}

		@Override
		public void onSubscribe(Subscription s) {
			main.setOther(s);

			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(U t) {
			if (main.gate) {
				return;
			}
			SkipUntilMainSubscriber<?> m = main;
			m.other.cancel();
			m.gate = true;
			m.other = Operators.cancelledSubscription();
		}

		@Override
		public void onError(Throwable t) {
			SkipUntilMainSubscriber<?> m = main;
			if (m.gate) {
				Operators.onErrorDropped(t, main.currentContext());
				return;
			}
			m.onError(t);
		}

		@Override
		public void onComplete() {
			SkipUntilMainSubscriber<?> m = main;
			if (m.gate) {
				return;
			}
			m.gate = true;
			m.other = Operators.cancelledSubscription();
		}


	}

	static final class SkipUntilMainSubscriber<T>
			implements InnerOperator<T, T> {

		final CoreSubscriber<? super T> actual;
		final Context ctx;

		volatile Subscription       main;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SkipUntilMainSubscriber, Subscription>
				MAIN =
				AtomicReferenceFieldUpdater.newUpdater(SkipUntilMainSubscriber.class,
						Subscription.class,
						"main");

		volatile Subscription other;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<SkipUntilMainSubscriber, Subscription>
				OTHER =
				AtomicReferenceFieldUpdater.newUpdater(SkipUntilMainSubscriber.class,
						Subscription.class,
						"other");

		volatile boolean gate;

		SkipUntilMainSubscriber(CoreSubscriber<? super T> actual) {
			this.actual = Operators.serialize(actual);
			this.ctx = actual.currentContext();
		}

		@Override
		public final CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return main;
			if (key == Attr.CANCELLED) return main == Operators.cancelledSubscription();
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(Scannable.from(other));
		}

		void setOther(Subscription s) {
			if (!OTHER.compareAndSet(this, null, s)) {
				s.cancel();
				if (other != Operators.cancelledSubscription()) {
					Operators.reportSubscriptionSet();
				}
			}
		}

		@Override
		public void request(long n) {
			main.request(n);
		}

		@Override
		public void cancel() {
			Operators.terminate(MAIN, this);
			Operators.terminate(OTHER, this);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (!MAIN.compareAndSet(this, null, s)) {
				s.cancel();
				if (main != Operators.cancelledSubscription()) {
					Operators.reportSubscriptionSet();
				}
			}
			else {
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (gate) {
				actual.onNext(t);
			}
			else {
				Operators.onDiscard(t, ctx);
				main.request(1);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (MAIN.compareAndSet(this, null, Operators.cancelledSubscription())) {
					Operators.error(actual, t);
					return;
			}
			else if (main == Operators.cancelledSubscription()){
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			cancel();

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			Operators.terminate(OTHER, this);

			actual.onComplete();
		}
	}
}

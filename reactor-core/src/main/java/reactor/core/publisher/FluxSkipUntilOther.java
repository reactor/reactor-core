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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import reactor.util.context.Context;
import javax.annotation.Nullable;

/**
 * Skips values from the main publisher until the other publisher signals
 * an onNext or onComplete.
 *
 * @param <T> the value type of the main Publisher
 * @param <U> the value type of the other Publisher
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 */
final class FluxSkipUntilOther<T, U> extends FluxOperator<T, T> {

	final Publisher<U> other;

	FluxSkipUntilOther(Flux<? extends T> source, Publisher<U> other) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
	}

	@Override
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		SkipUntilMainSubscriber<T> mainSubscriber = new SkipUntilMainSubscriber<>(s);

		SkipUntilOtherSubscriber<U> otherSubscriber = new SkipUntilOtherSubscriber<>(mainSubscriber);

		other.subscribe(otherSubscriber);

		source.subscribe(mainSubscriber, ctx);
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
			if (key == BooleanAttr.CANCELLED) return main.other == Operators.cancelledSubscription();
			if (key == ScannableAttr.PARENT) return main.other;
			if (key == ScannableAttr.ACTUAL) return main;

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
				Operators.onErrorDropped(t);
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
			extends CachedContextProducer<T>
			implements InnerOperator<T, T> {

		volatile Subscription main;
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

		SkipUntilMainSubscriber(Subscriber<? super T> actual) {
			super(Operators.serialize(actual));
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return main;
			if (key == BooleanAttr.CANCELLED) return main == Operators.cancelledSubscription();

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
				Operators.onErrorDropped(t);
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

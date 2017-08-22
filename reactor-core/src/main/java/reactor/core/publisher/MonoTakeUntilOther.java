/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
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
import javax.annotation.Nullable;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.context.Context;

/**
 * @author Simon Baslé
 */
final class MonoTakeUntilOther<T, U> extends MonoOperator<T, T> {

	private final Publisher<U> other;

	MonoTakeUntilOther(Mono<? extends T> source, Publisher<U> other) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> s) {
		MainSubscriber<T> mainSubscriber = new MainSubscriber<>(s);

		OtherSubscriber<U> otherSubscriber = new OtherSubscriber<>(mainSubscriber);

		other.subscribe(otherSubscriber);
		source.subscribe(mainSubscriber);
	}

	static final class OtherSubscriber<U> implements InnerConsumer<U> {

		final MainSubscriber<?> main;

		boolean once;

		OtherSubscriber(MainSubscriber<?> main) {
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

			return null;
		}

		@Override
		public void onSubscribe(Subscription s) {
			main.setOther(s);

			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(U t) {
			onComplete();
		}

		@Override
		public void onError(Throwable t) {
			if (once) {
				return;
			}
			once = true;
			main.onError(t);
		}

		@Override
		public void onComplete() {
			if (once) {
				return;
			}
			once = true;
			main.onComplete();
		}
	}

	static final class MainSubscriber<T> implements InnerOperator<T, T> {

		final CoreSubscriber<? super T> actual;

		volatile Subscription main;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MainSubscriber, Subscription> MAIN =
				AtomicReferenceFieldUpdater.newUpdater(MainSubscriber.class, Subscription.class, "main");

		volatile Subscription other;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MainSubscriber, Subscription> OTHER =
				AtomicReferenceFieldUpdater.newUpdater(MainSubscriber.class, Subscription.class, "other");

		MainSubscriber(CoreSubscriber<? super T> actual) {
			this.actual = Operators.serialize(actual);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return main;
			if (key == Attr.CANCELLED) return main == Operators.cancelledSubscription();

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public final CoreSubscriber<? super T> actual() {
			return actual;
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

		void cancelMain() {
			Subscription s = main;
			if (s != Operators.cancelledSubscription()) {
				s = MAIN.getAndSet(this, Operators.cancelledSubscription());
				if (s != null && s != Operators.cancelledSubscription()) {
					s.cancel();
				}
			}
		}

		void cancelOther() {
			Subscription s = other;
			if (s != Operators.cancelledSubscription()) {
				s = OTHER.getAndSet(this, Operators.cancelledSubscription());
				if (s != null && s != Operators.cancelledSubscription()) {
					s.cancel();
				}
			}
		}

		@Override
		public void cancel() {
			cancelMain();
			cancelOther();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (!MAIN.compareAndSet(this, null, s)) {
				s.cancel();
				if (main != Operators.cancelledSubscription()) {
					Operators.reportSubscriptionSet();
				}
			} else {
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			actual.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			if (main == null) {
				if (MAIN.compareAndSet(this, null, Operators.cancelledSubscription())) {
					Operators.error(actual, t);
					return;
				}
			}
			cancel();

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (main == null) {
				if (MAIN.compareAndSet(this, null, Operators.cancelledSubscription())) {
					cancelOther();
					Operators.complete(actual);
					return;
				}
			}
			cancel();

			actual.onComplete();
		}
	}
}

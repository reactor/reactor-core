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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * @author Simon Baslé
 */
final class SinksSubscribers {

	private SinksSubscribers() {}

	static abstract class AbstractSubscriberAdapter<T, SINK>
			implements CoreSubscriber<T>, Disposable, BiConsumer<Long, Long> {

		final SINK sink;
		final Context context;
		final Sinks.EmitFailureHandler emitFailureHandler;

		volatile Subscription                                                         s;
		@SuppressWarnings("rawtypes")
		static   AtomicReferenceFieldUpdater<AbstractSubscriberAdapter, Subscription> S
				= AtomicReferenceFieldUpdater.newUpdater(AbstractSubscriberAdapter.class, Subscription.class, "s");

		AbstractSubscriberAdapter(SINK sink) {
			this.sink = sink;
			this.context = sink instanceof ContextHolder ? ((ContextHolder) sink).currentContext() : Context.empty();
			this.emitFailureHandler = Sinks.EmitFailureHandler.FAIL_FAST;
		}

		protected abstract void doneSubscribed();

		protected abstract void doneDisposed();

		@Override
		public Context currentContext() {
			return context;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				doneSubscribed();
			}
		}

		@Override
		public void accept(Long lowestCommon, Long highestServiceable) {
			if (!isDisposed()) {
				s.request(highestServiceable);
			}
		}

		@Override
		public boolean isDisposed() {
			return s == Operators.cancelledSubscription();
		}

		@Override
		public void dispose() {
			if (Operators.terminate(S, this)) {
				doneDisposed();
			}
		}
	}

	static final class ManySubscriberAdapter<T> extends AbstractSubscriberAdapter<T, Sinks.Many<T>> {

		@Nullable
		BiConsumer<Long, Long> oldRequestHandler;

		ManySubscriberAdapter(Sinks.Many<T> many) {
			super(many);
		}

		@Override
		public void onNext(T t) {
			sink.emitNext(t, emitFailureHandler);
		}

		@Override
		public void onError(Throwable throwable) {
			Subscription s = S.getAndSet(this, Operators.cancelledSubscription());
			if (s == Operators.cancelledSubscription()) {
				Operators.onErrorDropped(throwable, this.context);
				return;
			}
			sink.emitError(throwable, emitFailureHandler);
		}

		@Override
		public void onComplete() {
			Subscription s = S.getAndSet(this, Operators.cancelledSubscription());
			if (s == Operators.cancelledSubscription()) {
				return;
			}
			sink.emitComplete(emitFailureHandler);
		}

		@Override
		protected void doneSubscribed() {
			oldRequestHandler = sink.setRequestHandler(this);
			sink.requestSnapshot();
		}

		@Override
		protected void doneDisposed() {
			sink.setRequestHandler(oldRequestHandler);
		}
	}

	static final class EmptySubscriberAdapter<T> extends AbstractSubscriberAdapter<Void, Sinks.Empty<T>> {

		EmptySubscriberAdapter(Sinks.Empty<T> sink) {
			super(sink);
		}

		@Override
		public void onNext(Void t) {
			//should never be called, but no-op by nature anyway
		}

		@Override
		public void onError(Throwable throwable) {
			Subscription s = S.getAndSet(this, Operators.cancelledSubscription());
			if (s == Operators.cancelledSubscription()) {
				Operators.onErrorDropped(throwable, this.context);
				return;
			}
			sink.emitError(throwable, emitFailureHandler);
		}

		@Override
		public void onComplete() {
			Subscription s = S.getAndSet(this, Operators.cancelledSubscription());
			if (s == Operators.cancelledSubscription()) {
				return;
			}
			sink.emitEmpty(emitFailureHandler);
		}

		@Override
		protected void doneSubscribed() { }

		@Override
		protected void doneDisposed() { }
	}

	/**
	 * This class assumes that it will be subscribed to a Mono or Mono-compatible source
	 * (only onNext+onComplete, onComplete or onError allowed).
	 * @param <T>
	 */
	static final class OneSubscriberAdapter<T> extends AbstractSubscriberAdapter<T, Sinks.One<T>> {

		OneSubscriberAdapter(Sinks.One<T> sink) {
			super(sink);
		}

		@Override
		public void onNext(T t) {
			Subscription s = S.getAndSet(this, Operators.cancelledSubscription());
			if (s == Operators.cancelledSubscription()) {
				Operators.onDiscard(t, this.context);
				//TODO differentiate between onNext+onNext and onComplete+onNext, so that first case cancels upstream?
				return;
			}
			sink.emitValue(t, emitFailureHandler);
		}

		@Override
		public void onError(Throwable throwable) {
			Subscription s = S.getAndSet(this, Operators.cancelledSubscription());
			if (s == Operators.cancelledSubscription()) {
				Operators.onErrorDropped(throwable, this.context);
				return;
			}
			sink.emitError(throwable, emitFailureHandler);
		}

		@Override
		public void onComplete() {
			Subscription s = S.getAndSet(this, Operators.cancelledSubscription());
			if (s == Operators.cancelledSubscription()) {
				return; //either complete empty or already completed as part of onNext
			}
			sink.emitEmpty(emitFailureHandler);
		}

		@Override
		protected void doneSubscribed() { }

		@Override
		protected void doneDisposed() { }
	}
}

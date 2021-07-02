/*
 * Copyright (c) 2019-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Observe termination from all 'rails' into a single Mono.
 */
final class ParallelThen extends Mono<Void> implements Scannable, Fuseable {

	final ParallelFlux<?> source;

	ParallelThen(ParallelFlux<?> source) {
		this.source = source;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) return source;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return null;
	}

	@Override
	public void subscribe(CoreSubscriber<? super Void> actual) {
		ThenMain parent = new ThenMain(actual, source.parallelism());
		actual.onSubscribe(parent);

		source.subscribe(parent.subscribers);
	}

	static final class ThenMain
			extends Operators.MonoSubscriber<Object, Void> {

		final ThenInner[] subscribers;

		volatile int remaining;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ThenMain>
		             REMAINING = AtomicIntegerFieldUpdater.newUpdater(
				ThenMain.class,
				"remaining");

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ThenMain, Throwable>
				ERROR = AtomicReferenceFieldUpdater.newUpdater(
				ThenMain.class,
				Throwable.class,
				"error");

		ThenMain(CoreSubscriber<? super Void> subscriber, int n) {
			super(subscriber);
			ThenInner[] a = new ThenInner[n];
			for (int i = 0; i < n; i++) {
				a[i] = new ThenInner(this);
			}
			this.subscribers = a;
			REMAINING.lazySet(this, n);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.ERROR) return error;
			if (key == Attr.TERMINATED) return REMAINING.get(this) == 0;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return super.scanUnsafe(key);
		}

		@Override
		public void cancel() {
			for (ThenInner inner : subscribers) {
				inner.cancel();
			}
			super.cancel();
		}

		void innerError(Throwable ex) {
			if(ERROR.compareAndSet(this, null, ex)){
				cancel();
				actual.onError(ex);
			}
			else if(error != ex) {
				Operators.onErrorDropped(ex, actual.currentContext());
			}
		}

		void innerComplete() {
			if (REMAINING.decrementAndGet(this) == 0) {
				actual.onComplete();
			}
		}
	}

	static final class ThenInner implements InnerConsumer<Object> {

		final ThenMain parent;

		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ThenInner, Subscription>
				S = AtomicReferenceFieldUpdater.newUpdater(
				ThenInner.class,
				Subscription.class,
				"s");

		ThenInner(ThenMain parent) {
			this.parent = parent;
		}

		@Override
		public Context currentContext() {
			return parent.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return s == Operators.cancelledSubscription();
			if (key == Attr.PARENT) return s;
			if (key == Attr.ACTUAL) return parent;
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return null;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(Object t) {
			//ignored
			Operators.onDiscard(t, parent.currentContext());
		}

		@Override
		public void onError(Throwable t) {
			parent.innerError(t);
		}

		@Override
		public void onComplete() {
			parent.innerComplete();
		}

		void cancel() {
			Operators.terminate(S, this);
		}
	}
}

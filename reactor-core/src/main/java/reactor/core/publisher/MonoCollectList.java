/*
 * Copyright (c) 2016-2022 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.ArrayList;
import java.util.List;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

/**
 * Buffers all values from the source Publisher and emits it as a single List.
 *
 * @param <T> the source value type
 */
final class MonoCollectList<T> extends MonoFromFluxOperator<T, List<T>> implements Fuseable {

	MonoCollectList(Flux<? extends T> source) {
		super(source);
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super List<T>> actual) {
		return new MonoCollectListSubscriber<>(actual);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class MonoCollectListSubscriber<T> implements InnerOperator<T, List<T>>,
	                                                           Fuseable, //for constants only
	                                                           QueueSubscription<List<T>> {

		final CoreSubscriber<? super List<T>> actual;

		List<T> list;

		Subscription s;

		boolean done;

		MonoCollectListSubscriber(CoreSubscriber<? super List<T>> actual) {
			this.actual = actual;
			//not this is not thread safe so concurrent discarding multiple + add might fail with ConcurrentModificationException
			this.list = new ArrayList<>();
		}

		@Override
		public CoreSubscriber<? super List<T>> actual() {
			return this.actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.PREFETCH) return 0;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			final List<T> l;
			synchronized (this) {
				l = list;
				if (l != null) {
					l.add(t);
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

			final List<T> l;
			synchronized (this) {
				l = list;
				list = null;
			}
			Operators.onDiscardMultiple(l, actual.currentContext());

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if(done) {
				return;
			}
			done = true;

			final List<T> l;
			synchronized (this) {
				l = list;
				list = null;
			}

			if (l != null) {
				actual.onNext(l);
				actual.onComplete();
			}
		}

		@Override
		public void cancel() {
			s.cancel();

			final List<?> l;
			synchronized (this) {
				l = list;
				list = null;
			}

			if (l != null) {
				Operators.onDiscardMultiple(l, actual.currentContext());
			}
		}

		@Override
		public void request(long n) {
			s.request(Long.MAX_VALUE);
		}

		@Override
		public int requestFusion(int requestedMode) {
			return Fuseable.NONE;
		}

		@Override
		public List<T> poll() {
			return null;
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public boolean isEmpty() {
			return false;
		}

		@Override
		public void clear() {

		}
	}
}

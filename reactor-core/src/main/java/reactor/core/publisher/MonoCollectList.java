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

	static final class MonoCollectListSubscriber<T> extends Operators.MonoSubscriber<T, List<T>> {

		Subscription s;

		List<T> list;

		boolean done;

		MonoCollectListSubscriber(CoreSubscriber<? super List<T>> actual) {
			super(actual);
			//not this is not thread safe so concurrent discarding multiple + add might fail with ConcurrentModificationException
			this.list = new ArrayList<>();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
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
			List<T> l;
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
			if(done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			done = true;
			List<T> l;
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
			List<T> l;
			synchronized (this) {
				l = list;
				list = null;
			}
			if (l != null) {
				complete(l);
			}
		}

		@Override
		protected void discard(List<T> v) {
			Operators.onDiscardMultiple(v, actual.currentContext());
		}

		@Override
		public void cancel() {
			int state;
			List<T> l;
			synchronized (this) {
				state = STATE.getAndSet(this, CANCELLED);
				if (state <= HAS_REQUEST_NO_VALUE) {
					l = list;
					value = null;
					list = null;
				}
				else {
					l = null;
				}
			}
			if (l != null) {
				s.cancel();
				discard(l);
			}
		}
	}
}

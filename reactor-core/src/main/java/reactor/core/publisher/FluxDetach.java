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


import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Detaches both the child Subscriber and the Subscription on
 * termination or cancellation.
 * <p>This should help with odd retention scenarios when running
 * with non Rx mentality based Publishers.
 * 
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxDetach<T> extends InternalFluxOperator<T, T> {

	FluxDetach(Flux<? extends T> source) {
		super(source);
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new DetachSubscriber<>(actual);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class DetachSubscriber<T> implements InnerOperator<T, T> {

		CoreSubscriber<? super T> actual;
		
		Subscription s;

		DetachSubscriber(CoreSubscriber<? super T> actual) {
			this.actual = actual;
		}

		@Override
		public Context currentContext() {
			return actual == null ? Context.empty() : actual.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return actual == null;
			if (key == Attr.CANCELLED) return actual == null && s == null;
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
			Subscriber<? super T> a = actual;
			if (a != null) {
				a.onNext(t);
			}
		}
		
		@Override
		public void onError(Throwable t) {
			Subscriber<? super T> a = actual;
			if (a != null) {
				actual = null;
				s = null;
				
				a.onError(t);
			}
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void onComplete() {
			Subscriber<? super T> a = actual;
			if (a != null) {
				actual = null;
				s = null;
				
				a.onComplete();
			}
		}
		
		@Override
		public void request(long n) {
			Subscription a = s;
			if (a != null) {
				a.request(n);
			}
		}
		
		@Override
		public void cancel() {
			Subscription a = s;
			if (a != null) {
				actual = null;
				s = null;
				
				a.cancel();
			}
		}
	}
}

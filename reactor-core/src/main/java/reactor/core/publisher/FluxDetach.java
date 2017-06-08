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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import javax.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Detaches the both the child Subscriber and the Subscription on
 * termination or cancellation.
 * <p>This should help with odd retention scenarios when running
 * wit non Rx mentality based Publishers.
 * 
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxDetach<T> extends FluxOperator<T, T> {

	FluxDetach(Flux<? extends T> source) {
		super(source);
	}

	@Override
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		source.subscribe(new DetachSubscriber<>(s), ctx);
	}
	
	static final class DetachSubscriber<T> implements InnerOperator<T, T> {
		
		Subscriber<? super T> actual;
		
		Subscription s;

		DetachSubscriber(Subscriber<? super T> actual) {
			this.actual = actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.TERMINATED) return actual == null;
			if (key == BooleanAttr.CANCELLED) return actual == null && s == null;

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
		public Subscriber<? super T> actual() {
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

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
import reactor.core.Fuseable;
import reactor.util.context.Context;
import reactor.core.Scannable;
import javax.annotation.Nullable;

/**
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoHasElements<T> extends MonoOperator<T, Boolean> implements Fuseable {

	MonoHasElements(ContextualPublisher<? extends T> source) {
		super(source);
	}

	@Override
	public void subscribe(Subscriber<? super Boolean> s, Context ctx) {
		if (source instanceof Mono) {
			source.subscribe(new HasElementSubscriber<>(s), ctx);
		}
		else {
			source.subscribe(new HasElementsSubscriber<>(s), ctx);
		}
	}

	static final class HasElementsSubscriber<T> extends Operators.MonoSubscriber<T, Boolean> {
		Subscription s;

		HasElementsSubscriber(Subscriber<? super Boolean> actual) {
			super(actual);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;

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
			s.cancel();

			complete(true);
		}

		@Override
		public void onComplete() {
			complete(false);
		}

	}

	static final class HasElementSubscriber<T>
			extends Operators.MonoSubscriber<T, Boolean> {
		Subscription s;

		HasElementSubscriber(Subscriber<? super Boolean> actual) {
			super(actual);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) {
				return s;
			}
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
			//here we avoid the cancel because the source is assumed to be a Mono
			complete(true);
		}

		@Override
		public void onComplete() {
			complete(false);
		}

	}
}

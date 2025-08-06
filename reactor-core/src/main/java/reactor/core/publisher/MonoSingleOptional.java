/*
 * Copyright (c) 2016-2025 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Optional;

import org.jspecify.annotations.Nullable;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.util.context.Context;

/**
 * Emits a single item from the source wrapped into an Optional, emits
 * an empty Optional instead for empty source.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoSingleOptional<T> extends InternalMonoOperator<T, Optional<T>> {

    MonoSingleOptional(Mono<? extends T> source) {
        super(source);
    }

    @Override
    public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super Optional<T>> actual) {
        return new MonoSingleOptional.SingleOptionalSubscriber<>(actual);
    }

    @Override
    public Object scanUnsafe(Attr key) {
        if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
        return super.scanUnsafe(key);
    }

	static final class SingleOptionalSubscriber<T> extends Operators.MonoInnerProducerBase<Optional<T>> implements InnerConsumer<T> {

		Subscription s;

		boolean done;

		@Override
		public @Nullable Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.PARENT) return s;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return super.scanUnsafe(key);
		}

		@Override
		public Context currentContext() {
			return actual().currentContext();
		}

		SingleOptionalSubscriber(CoreSubscriber<? super Optional<T>> actual) {
			super(actual);
		}

		@Override
		public void doOnRequest(long n) {
			s.request(Long.MAX_VALUE);
		}

		@Override
		public void doOnCancel() {
			s.cancel();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				actual().onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual().currentContext());
				return;
			}
			done = true;
			complete(Optional.of(t));
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual().currentContext());
				return;
			}
			done = true;
			actual().onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			complete(Optional.empty());
		}

	}
}

/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.Mono;
import reactor.core.error.Exceptions;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.core.support.Assert;
import reactor.core.support.ReactiveState;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;

/**
 * @author Stephane Maldini
 * @since 2.5
 */
public final class MonoSuccess<I> extends Mono.MonoBarrier<I, I> implements ReactiveState.FeedbackLoop {

	private final Consumer<? super I>              onSuccess;
	private final BiConsumer<? super I, Throwable> onSuccessOrFailure;
	private final BiConsumer<? super I, Throwable> afterSuccessOrFailure;

	public MonoSuccess(Publisher<? extends I> source,
			Consumer<? super I> onSuccess,
			BiConsumer<? super I, Throwable> onSuccessOrFailure,
			BiConsumer<? super I, Throwable> afterSuccessOrFailure) {
		super(source);
		Assert.isTrue(onSuccess != null || onSuccessOrFailure != null || afterSuccessOrFailure != null, "Callback not" +
				" registered (null argument ?)");
		this.onSuccess = onSuccess;
		this.afterSuccessOrFailure = afterSuccessOrFailure;
		this.onSuccessOrFailure = onSuccessOrFailure;
	}

	@Override
	public void subscribe(Subscriber<? super I> s) {
		if (onSuccessOrFailure != null) {
			source.subscribe(new MonoSuccessBarrier<>(s, null, onSuccessOrFailure, null));
		}
		else if (afterSuccessOrFailure != null) {
			source.subscribe(new MonoSuccessBarrier<>(s, null, null, afterSuccessOrFailure));
		}
		else {
			source.subscribe(new MonoSuccessBarrier<>(s, onSuccess, null, null));
		}
	}

	@Override
	public Object delegateInput() {
		return onSuccess;
	}

	@Override
	public Object delegateOutput() {
		return afterSuccessOrFailure == null ? onSuccessOrFailure : afterSuccessOrFailure;
	}

	private static final class MonoSuccessBarrier<I> extends SubscriberBarrier<I, I> {

		private final Consumer<? super I>              onSuccess;
		private final BiConsumer<? super I, Throwable> onSuccessOrFailure;
		private final BiConsumer<? super I, Throwable> afterSuccessOrFailure;

		public MonoSuccessBarrier(Subscriber<? super I> s,
				Consumer<? super I> onSuccess,
				BiConsumer<? super I, Throwable> onSuccessOrFailure,
				BiConsumer<? super I, Throwable> afterSuccessOrFailure) {
			super(s);
			this.onSuccess = onSuccess;
			this.onSuccessOrFailure = onSuccessOrFailure;
			this.afterSuccessOrFailure = afterSuccessOrFailure;
		}

		@Override
		protected void doComplete() {
			if (upstream() == null) {
				return;
			}
			if (onSuccess != null) {
				onSuccess.accept(null);
				subscriber.onComplete();
				return;
			}

			if (onSuccessOrFailure != null) {
				onSuccessOrFailure.accept(null, null);
				subscriber.onComplete();
				return;
			}

			try {
				subscriber.onComplete();
			}
			catch (Throwable t) {
				Exceptions.throwIfFatal(t);
				afterSuccessOrFailure.accept(null, Exceptions.unwrap(t));
			}
		}

		@Override
		protected void doNext(I t) {
			if (upstream() == null) {
				Exceptions.onNextDropped(t);
				return;
			}
			cancel();
			if (onSuccess != null) {
				onSuccess.accept(t);
				subscriber.onNext(t);
				subscriber.onComplete();
				return;
			}

			if (onSuccessOrFailure != null) {
				onSuccessOrFailure.accept(t, null);
				subscriber.onNext(t);
				subscriber.onComplete();
				return;
			}

			try {
				subscriber.onNext(t);
				subscriber.onComplete();
			}
			catch (Throwable x) {
				Exceptions.throwIfFatal(x);
				afterSuccessOrFailure.accept(t, Exceptions.unwrap(x));
			}
		}

		@Override
		protected void doError(Throwable throwable) {
			if (onSuccessOrFailure != null) {
				onSuccessOrFailure.accept(null, throwable);
				return;
			}

			if (afterSuccessOrFailure == null) {
				subscriber.onError(throwable);
				return;
			}

			try {
				subscriber.onError(throwable);
			}
			catch (Throwable t) {
				Throwable _t = Exceptions.unwrap(t);
				_t.addSuppressed(throwable);
				Exceptions.throwIfFatal(t);
				afterSuccessOrFailure.accept(null, _t);
			}
		}
	}
}

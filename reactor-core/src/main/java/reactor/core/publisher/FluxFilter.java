/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Objects;
import java.util.function.Predicate;

import org.reactivestreams.Subscription;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Fuseable.ConditionalSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Filters out values that make a filter function return false.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxFilter<T> extends InternalFluxOperator<T, T> {

	final Predicate<? super T> predicate;

	FluxFilter(Flux<? extends T> source, Predicate<? super T> predicate) {
		super(source);
		this.predicate = Objects.requireNonNull(predicate, "predicate");
	}

	@Override
	@SuppressWarnings("unchecked")
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		if (actual instanceof ConditionalSubscriber) {
			return new FilterConditionalSubscriber<>((ConditionalSubscriber<? super T>) actual,
					predicate);
		}
		return new FilterSubscriber<>(actual, predicate);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static final class FilterSubscriber<T>
			implements InnerOperator<T, T>,
			           Fuseable.ConditionalSubscriber<T> {

		final CoreSubscriber<? super T> actual;
		final Context ctx;

		final Predicate<? super T> predicate;

		Subscription s;

		boolean done;

		FilterSubscriber(CoreSubscriber<? super T> actual, Predicate<? super T> predicate) {
			this.actual = actual;
			this.ctx = actual.currentContext();
			this.predicate = predicate;
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
				Operators.onNextDropped(t,  this.ctx);
				return;
			}

			boolean b;

			try {
				b = predicate.test(t);
			}
			catch (Throwable e) {
				Throwable e_ = Operators.onNextError(t, e,  this.ctx, s);
				if (e_ != null) {
					onError(e_);
				}
				else {
					s.request(1);
				}
				Operators.onDiscard(t,  this.ctx);
				return;
			}
			if (b) {
				actual.onNext(t);
			}
			else {
				Operators.onDiscard(t,  this.ctx);
				s.request(1);
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t,  this.ctx);
				return false;
			}

			boolean b;

			try {
				b = predicate.test(t);
			}
			catch (Throwable e) {
				Throwable e_ = Operators.onNextError(t, e,  this.ctx, s);
				if (e_ != null) {
					onError(e_);
				}
				Operators.onDiscard(t,  this.ctx);
				return false;
			}
			if (b) {
				actual.onNext(t);
			}
			else {
				Operators.onDiscard(t,  this.ctx);
			}
			return b;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t,  this.ctx);
				return;
			}
			done = true;
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			actual.onComplete();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
		}
	}

	static final class FilterConditionalSubscriber<T>
			implements InnerOperator<T, T>,
			           Fuseable.ConditionalSubscriber<T> {

		final Fuseable.ConditionalSubscriber<? super T> actual;
		final Context ctx;

		final Predicate<? super T> predicate;

		Subscription s;

		boolean done;

		FilterConditionalSubscriber(Fuseable.ConditionalSubscriber<? super T> actual,
				Predicate<? super T> predicate) {
			this.actual = actual;
			this.ctx = actual.currentContext();
			this.predicate = predicate;
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
				Operators.onNextDropped(t,  this.ctx);
				return;
			}

			boolean b;

			try {
				b = predicate.test(t);
			}
			catch (Throwable e) {
				Throwable e_ = Operators.onNextError(t, e,  this.ctx, s);
				if (e_ != null) {
					onError(e_);
				}
				else {
					s.request(1);
				}
				Operators.onDiscard(t,  this.ctx);
				return;
			}
			if (b) {
				actual.onNext(t);
			}
			else {
				s.request(1);
				Operators.onDiscard(t,  this.ctx);
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t,  this.ctx);
				return false;
			}

			boolean b;

			try {
				b = predicate.test(t);
			}
			catch (Throwable e) {
				Throwable e_ = Operators.onNextError(t, e,  this.ctx, s);
				if (e_ != null) {
					onError(e_);
				}
				Operators.onDiscard(t,  this.ctx);
				return false;
			}
			if (b) {
				return actual.tryOnNext(t);
			}
			else {
				Operators.onDiscard(t,  this.ctx);
				return false;
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t,  this.ctx);
				return;
			}
			done = true;
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			actual.onComplete();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}


		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
		}
	}

}

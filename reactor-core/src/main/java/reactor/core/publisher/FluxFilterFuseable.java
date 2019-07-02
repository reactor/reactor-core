/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

import java.util.Objects;
import java.util.function.Predicate;

import org.reactivestreams.Subscription;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Filters out values that make a filter function return false.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxFilterFuseable<T> extends InternalFluxOperator<T, T> implements Fuseable {

	final Predicate<? super T> predicate;

	FluxFilterFuseable(Flux<? extends T> source, Predicate<? super T> predicate) {
		super(source);
		this.predicate = Objects.requireNonNull(predicate, "predicate");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		if (actual instanceof ConditionalSubscriber) {
			return new FilterFuseableConditionalSubscriber<>((ConditionalSubscriber<? super T>) actual,
					predicate);
		}
		return new FilterFuseableSubscriber<>(actual, predicate);
	}

	static final class FilterFuseableSubscriber<T>
			implements InnerOperator<T, T>, QueueSubscription<T>,
			           ConditionalSubscriber<T> {

		final CoreSubscriber<? super T> actual;
		final Context                   ctx;

		final Predicate<? super T> predicate;

		QueueSubscription<T> s;

		boolean done;

		int sourceMode;

		FilterFuseableSubscriber(CoreSubscriber<? super T> actual,
				Predicate<? super T> predicate) {
			this.actual = actual;
			this.ctx = actual.currentContext();
			this.predicate = predicate;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = (QueueSubscription<T>) s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (sourceMode == ASYNC) {
				actual.onNext(null);
			}
			else {
				if (done) {
					Operators.onNextDropped(t, this.ctx);
					return;
				}
				boolean b;

				try {
					b = predicate.test(t);
				}
				catch (Throwable e) {
					Throwable e_ = Operators.onNextError(t, e, this.ctx, s);
					if (e_ != null) {
						onError(e_);
					}
					else {
						s.request(1);
					}
					Operators.onDiscard(t, this.ctx);
					return;
				}
				if (b) {
					actual.onNext(t);
				}
				else {
					s.request(1);
					Operators.onDiscard(t, this.ctx);
				}
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t, this.ctx);
				return false;
			}

			boolean b;

			try {
				b = predicate.test(t);
			}
			catch (Throwable e) {
				Throwable e_ = Operators.onNextError(t, e, this.ctx, s);
				if (e_ != null) {
					onError(e_);
				}
				Operators.onDiscard(t, this.ctx);
				return false;
			}
			if (b) {
				actual.onNext(t);
				return true;
			}
			Operators.onDiscard(t, this.ctx);
			return false;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, this.ctx);
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

		@Override
		@Nullable
		public T poll() {
			if (sourceMode == ASYNC) {
				long dropped = 0;
				for (; ; ) {
					T v = s.poll();

					try {
						if (v == null || predicate.test(v)) {
							if (dropped != 0) {
								request(dropped);
							}
							return v;
						}
						Operators.onDiscard(v, this.ctx);
						dropped++;
					}
					catch (Throwable e) {
						RuntimeException e_ = Operators.onNextPollError(v, e, currentContext());
						Operators.onDiscard(v, this.ctx);
						if (e_ != null) {
							throw e_;
						}
						//else continue
					}
				}
			}
			else {
				for (; ; ) {
					T v = s.poll();

					try {
						if (v == null || predicate.test(v)) {
							return v;
						}
						Operators.onDiscard(v, this.ctx);
					}
					catch (Throwable e) {
						RuntimeException e_ = Operators.onNextPollError(v, e, currentContext());
						Operators.onDiscard(v, this.ctx);
						if (e_ != null) {
							throw e_;
						}
						// else continue
					}
				}
			}
		}

		@Override
		public boolean isEmpty() {
			return s.isEmpty();
		}

		@Override
		public void clear() {
			s.clear();
		}

		@Override
		public int requestFusion(int requestedMode) {
			int m;
			if ((requestedMode & Fuseable.THREAD_BARRIER) != 0) {
				return Fuseable.NONE;
			}
			else {
				m = s.requestFusion(requestedMode);
			}
			sourceMode = m;
			return m;
		}

		@Override
		public int size() {
			return s.size();
		}
	}

	static final class FilterFuseableConditionalSubscriber<T>
			implements InnerOperator<T, T>, ConditionalSubscriber<T>,
			           QueueSubscription<T> {

		final ConditionalSubscriber<? super T> actual;
		final Context ctx;

		final Predicate<? super T> predicate;

		QueueSubscription<T> s;

		boolean done;

		int sourceMode;

		FilterFuseableConditionalSubscriber(ConditionalSubscriber<? super T> actual,
				Predicate<? super T> predicate) {
			this.actual = actual;
			this.ctx = actual.currentContext();
			this.predicate = predicate;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = (QueueSubscription<T>) s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {

			if (sourceMode == ASYNC) {
				actual.onNext(null);
			}
			else {
				if (done) {
					Operators.onNextDropped(t, this.ctx);
					return;
				}
				boolean b;

				try {
					b = predicate.test(t);
				}
				catch (Throwable e) {
					Throwable e_ = Operators.onNextError(t, e, this.ctx, s);
					if (e_ != null) {
						onError(e_);
					}
					else {
						s.request(1);
					}
					Operators.onDiscard(t, this.ctx);
					return;
				}
				if (b) {
					actual.onNext(t);
				}
				else {
					s.request(1);
					Operators.onDiscard(t, this.ctx);
				}
			}
		}

		@Override
		public boolean tryOnNext(T t) {
			if (done) {
				Operators.onNextDropped(t, this.ctx);
				return false;
			}

			boolean b;

			try {
				b = predicate.test(t);
			}
			catch (Throwable e) {
				Throwable e_ = Operators.onNextError(t, e, this.ctx, s);
				if (e_ != null) {
					onError(e_);
				}
				Operators.onDiscard(t, this.ctx);
				return false;
			}
			if (b) {
				return actual.tryOnNext(t);
			}
			else {
				Operators.onDiscard(t, this.ctx);
				return false;
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, this.ctx);
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

		@Override
		@Nullable
		public T poll() {
			if (sourceMode == ASYNC) {
				long dropped = 0;
				for (; ; ) {
					T v = s.poll();
					try {
						if (v == null || predicate.test(v)) {
							if (dropped != 0) {
								request(dropped);
							}
							return v;
						}
						Operators.onDiscard(v, this.ctx);
						dropped++;
					}
					catch (Throwable e) {
						RuntimeException e_ = Operators.onNextPollError(v, e, this.ctx);
						Operators.onDiscard(v, this.ctx);
						if (e_ != null) {
							throw e_;
						}
						// else continue
					}
				}
			}
			else {
				for (; ; ) {
					T v = s.poll();

					try {
						if (v == null || predicate.test(v)) {
							return v;
						}
						Operators.onDiscard(v, this.ctx);
					}
					catch (Throwable e) {
						RuntimeException e_ = Operators.onNextPollError(v, e, this.ctx);
						Operators.onDiscard(v, this.ctx);
						if (e_ != null) {
							throw e_;
						}
						// else continue
					}
				}
			}
		}

		@Override
		public boolean isEmpty() {
			return s.isEmpty();
		}

		@Override
		public void clear() {
			s.clear();
		}

		@Override
		public int size() {
			return s.size();
		}

		@Override
		public int requestFusion(int requestedMode) {
			int m;
			if ((requestedMode & Fuseable.THREAD_BARRIER) != 0) {
				return Fuseable.NONE;
			}
			else {
				m = s.requestFusion(requestedMode);
			}
			sourceMode = m;
			return m;
		}
	}

}

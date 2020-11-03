/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;

/**
 * @author Simon Baslé
 */
final class FluxTimed<T> extends InternalFluxOperator<T, Timed<T>> {

	final Scheduler clock;

	FluxTimed(Flux<? extends T> source, Scheduler clock) {
		super(source);
		this.clock = clock;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super Timed<T>> actual) {
		return new TimedSubscriber<>(actual, this.clock);
	}

	@Nullable
	@Override
	public Object scanUnsafe(Attr key) {
		//FIXME
		return super.scanUnsafe(key);
	}

	/**
	 * Immutable version of {@link Timed}. This is preferable to the subscriber implementing
	 * Timed interface, as timestamps are likely to be collected for later use (so flyweight
	 * would get in the way).
	 *
	 * @param <T>
	 */
	static final class ImmutableTimed<T> implements Timed<T> {

		final long eventElapsedSinceSubscriptionNanos;
		final long eventElapsedNanos;
		final long eventTimestampEpochMillis;
		final T    event;

		ImmutableTimed(long eventElapsedSinceSubscriptionNanos,
				long eventElapsedNanos,
				long eventTimestampEpochMillis,
				T event) {
			this.eventElapsedSinceSubscriptionNanos = eventElapsedSinceSubscriptionNanos;
			this.eventElapsedNanos = eventElapsedNanos;
			this.eventTimestampEpochMillis = eventTimestampEpochMillis;
			this.event = event;
		}

		@Override
		public T get() {
			return this.event;
		}

		@Override
		public Duration elapsed() {
			return Duration.ofNanos(eventElapsedNanos);
		}

		@Override
		public Duration elapsedSinceSubscription() {
			return Duration.ofNanos(eventElapsedSinceSubscriptionNanos);
		}

		@Override
		public Instant timestamp() {
			return Instant.ofEpochMilli(eventTimestampEpochMillis);
		}

		@Override
		public String toString() {
			return "Timed(" + event + "){eventElapsedNanos=" + eventElapsedNanos + ", eventElapsedSinceSubscriptionNanos=" + eventElapsedSinceSubscriptionNanos + ",  eventTimestampEpochMillis=" + eventTimestampEpochMillis + '}';
		}
	}

	static final class TimedSubscriber<T> implements InnerOperator<T, Timed<T>> {

		final CoreSubscriber<? super Timed<T>> actual;
		final Scheduler clock;

		long subscriptionNanos;
		long lastEventNanos;

		Subscription s;

		TimedSubscriber(CoreSubscriber<? super Timed<T>> actual, Scheduler clock) {
			this.actual = actual;
			this.clock = clock;
		}

		@Override
		public CoreSubscriber<? super Timed<T>> actual() {
			return this.actual;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				this.subscriptionNanos = clock.now(TimeUnit.NANOSECONDS);
				this.lastEventNanos = subscriptionNanos;

				actual.onSubscribe(this);
			}
		}

		//TODO protect against malformed publishers

		@Override
		public void onNext(T t) {
			long nowNanos = clock.now(TimeUnit.NANOSECONDS);
			long timestamp = clock.now(TimeUnit.MILLISECONDS);
			Timed<T> timed = new ImmutableTimed<>(nowNanos - this.subscriptionNanos, nowNanos - this.lastEventNanos, timestamp, t);
			this.lastEventNanos = nowNanos;
			actual.onNext(timed);
		}

		@Override
		public void onError(Throwable throwable) {
			actual.onError(throwable);
		}

		@Override
		public void onComplete() {
			actual.onComplete();
		}

		@Override
		public void request(long l) {
			if (Operators.validate(l)) {
				s.request(l);
			}
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			//FIXME
			return null;
		}
	}
}

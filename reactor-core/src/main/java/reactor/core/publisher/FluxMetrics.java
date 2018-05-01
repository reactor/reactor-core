/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicLong;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;

/** @author Simon Baslé */
final class FluxMetrics<T> extends FluxOperator<T, T> {

	FluxMetrics(Flux<? extends T> flux) {
		super(flux);
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		InnerOperator<T,T> metricsOperator;
		try {
			metricsOperator = new MicrometerMetricsSubscriber<>(actual, Metrics.globalRegistry);
		}
		catch (Throwable e) {
			metricsOperator = new SimplisticMetricsSubscriber<>(actual);
		}
		source.subscribe(metricsOperator);
	}

	static final class SimplisticMetricsSubscriber<T> implements InnerOperator<T,T> {

		final CoreSubscriber<? super T> actual;

		boolean done;
		Subscription s;

		public SimplisticMetricsSubscriber(CoreSubscriber<? super T> actual) {
			this.actual = actual;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}
			//TODO
		}

		@Override
		public void onError(Throwable e) {
			if (done) {
				Operators.onErrorDropped(e, actual.currentContext());
				return;
			}
			done = true;
			//TODO
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			//TODO
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public void request(long l) {
			if (Operators.validate(l)) {
				s.request(l);
			}

			//TODO
		}

		@Override
		public void cancel() {
			s.cancel();
			//TODO
		}
	}

	static final class MicrometerMetricsSubscriber<T> implements InnerOperator<T,T> {

		final CoreSubscriber<? super T> actual;
		final MeterRegistry             registry;

		final Counter                   onErrorCounter;
		final Counter                   onCompleteCounter;
		final Counter                   malformedSourceCounter;
		final Counter                   cancelCounter;

		final AtomicLong requested;

		Timer.Sample subscribeToTerminateTimer;
		Timer.Sample onNextIntervalTimer;

		boolean done;
		Subscription s;


		public MicrometerMetricsSubscriber(CoreSubscriber<? super T> actual,
				MeterRegistry registry) {
			this.actual = actual;
			this.registry = registry;

			//find the global meters
			this.onErrorCounter = registry.counter(METER_ON_ERROR);
			this.onCompleteCounter = registry.counter(METER_ON_COMPLETE);
			this.malformedSourceCounter = registry.counter(METER_MALFORMED);
			this.cancelCounter = registry.counter(METER_CANCEL);

			this.requested = registry.gauge(METER_BACKPRESSURE, new AtomicLong());
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void onNext(T t) {
			if (done) {
				this.malformedSourceCounter.increment();
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}
			requested.decrementAndGet();

			//record the delay since previous onNext/onSubscribe. This also records the count.
			this.onNextIntervalTimer.stop(registry.timer(METER_ON_NEXT_DELAY));
			//reset the timer sample
			this.onNextIntervalTimer = Timer.start(registry);

			actual.onNext(t);
		}

		@Override
		public void onError(Throwable e) {
			if (done) {
				this.malformedSourceCounter.increment();
				Operators.onErrorDropped(e, actual.currentContext());
				return;
			}
			done = true;
			this.onNextIntervalTimer.stop(registry.timer(METER_ON_NEXT_DELAY));
			this.onErrorCounter.increment();
			this.subscribeToTerminateTimer.stop(registry.timer(METER_SUBSCRIBE_TO_TERMINATE,
					TAG_SUBSCRIBE_TO_TERMINATE, "ERROR"));

			actual.onError(e);
		}

		@Override
		public void onComplete() {
			if (done) {
				this.malformedSourceCounter.increment();
				return;
			}
			done = true;
			this.onNextIntervalTimer.stop(registry.timer(METER_ON_NEXT_DELAY));
			this.onCompleteCounter.increment();
			this.subscribeToTerminateTimer.stop(registry.timer(METER_SUBSCRIBE_TO_TERMINATE,
					TAG_SUBSCRIBE_TO_TERMINATE, "COMPLETE"));

			actual.onComplete();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.subscribeToTerminateTimer = Timer.start(registry);
				this.onNextIntervalTimer = Timer.start(registry);

				this.s = s;
				actual.onSubscribe(this);

			}
		}

		@Override
		public void request(long l) {
			if (Operators.validate(l)) {
				//reproduces the Operators.addCap(AtomicLongFieldUpdater) on AtomicLong
				long r, u;
				for (;;) {
					r = requested.get();
					if (r == Long.MAX_VALUE) {
						break;
					}
					u = Operators.addCap(r, l);
					if (requested.compareAndSet(r, u)) {
						break;
					}
				}
				s.request(l);
			}

			//TODO
		}

		@Override
		public void cancel() {
			this.cancelCounter.increment();
			this.subscribeToTerminateTimer.stop(registry.timer(METER_SUBSCRIBE_TO_TERMINATE,
					TAG_SUBSCRIBE_TO_TERMINATE, "CANCEL"));
			//TODO
			s.cancel();
		}

		static final String METER_ON_ERROR = "reactor.onError";
		static final String METER_ON_COMPLETE = "reactor.onComplete";
		static final String METER_CANCEL = "reactor.cancel";
		static final String METER_MALFORMED = "reactor.malformedSource";
		static final String METER_SUBSCRIBE_TO_TERMINATE = "reactor.subscribeToTerminate";
		static final String METER_ON_NEXT_DELAY = "reactor.onNextDelay";
		static final String METER_BACKPRESSURE = "reactor.backpressure";

		static final String TAG_SUBSCRIBE_TO_TERMINATE   = "reactor.subscribeToTerminate.type";
	}
}

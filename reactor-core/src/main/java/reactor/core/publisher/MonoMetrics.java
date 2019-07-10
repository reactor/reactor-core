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

import java.util.function.Function;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

import static reactor.core.publisher.FluxMetrics.*;

/**
 * Activate metrics gathering on a {@link Mono}, assumes Micrometer is on the classpath.
 *
 * @implNote Metrics.isInstrumentationAvailable() test should be performed BEFORE instantiating
 * or referencing this class, otherwise a {@link NoClassDefFoundError} will be thrown if
 * Micrometer is not there.
 *
 * @author Simon Baslé
 * @author Stephane Maldini
 */
final class MonoMetrics<T> extends MonoOperator<T, T> {

	final String        name;
	final Tags          tags;
	@Nullable
	final MeterRegistry meterRegistry;

	MonoMetrics(Mono<? extends T> mono) {
		this(mono, null);
	}

	/**
	 * For testing purposes.
	 *
	 * @param meterRegistry the registry to use
	 */
	MonoMetrics(Mono<? extends T> mono, @Nullable MeterRegistry meterRegistry) {
		super(mono);

		this.name = resolveName(mono);
		this.tags = resolveTags(mono, FluxMetrics.DEFAULT_TAGS_MONO, this.name);

		if (meterRegistry == null) {
			this.meterRegistry = Metrics.globalRegistry;
		}
		else {
			this.meterRegistry = meterRegistry;
		}
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		source.subscribe(new MetricsSubscriber<>(actual, meterRegistry, Clock.SYSTEM, this.tags));
	}

	static class MetricsSubscriber<T> implements InnerOperator<T, T> {

		final CoreSubscriber<? super T>  actual;
		final Clock                      clock;
		final Counter                    malformedSourceCounter;
		final Counter                    subscribedCounter;
		final Timer                      subscribeToCompleteTimer;
		final Timer                      subscribeToCancelTimer;
		final Function<Throwable, Timer> subscribeToErrorTimerFactory;

		Timer.Sample subscribeToTerminateSample;

		boolean done;
		Subscription s;

		MetricsSubscriber(CoreSubscriber<? super T> actual,
				MeterRegistry registry, Clock clock, Tags commonTags) {
			this.actual = actual;
			this.clock = clock;


			this.subscribeToCompleteTimer = Timer
					.builder(FluxMetrics.METER_FLOW_DURATION)
					.tags(commonTags.and(FluxMetrics.TAG_ON_COMPLETE))
					.description("Times the duration elapsed between a subscription and the onComplete termination of the sequence")
					.register(registry);
			this.subscribeToCancelTimer = Timer
					.builder(FluxMetrics.METER_FLOW_DURATION)
					.tags(commonTags.and(FluxMetrics.TAG_CANCEL))
					.description("Times the duration elapsed between a subscription and the cancellation of the sequence")
					.register(registry);

			//note that Builder ISN'T TRULY IMMUTABLE. This is ok though as there will only ever be one usage.
			Timer.Builder subscribeToErrorTimerBuilder = Timer
					.builder(FluxMetrics.METER_FLOW_DURATION)
					.tags(commonTags.and(TAG_ON_ERROR))
					.description("Times the duration elapsed between a subscription and the onError termination of the sequence, with the exception name as a tag.");
			this.subscribeToErrorTimerFactory = e -> subscribeToErrorTimerBuilder.tag(FluxMetrics.TAG_KEY_EXCEPTION,
					e.getClass()
					 .getName())
			                                                                     .register(registry);

			this.subscribedCounter = Counter
					.builder(FluxMetrics.METER_SUBSCRIBED)
					.tags(commonTags)
					.baseUnit("subscribers")
					.description("Counts how many Reactor sequences have been subscribed to")
					.register(registry);

			this.malformedSourceCounter = registry.counter(FluxMetrics.METER_MALFORMED, commonTags);
		}

		@Override
		final public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		final public void cancel() {
			this.subscribeToTerminateSample.stop(subscribeToCancelTimer);

			s.cancel();
		}

		@Override
		final public void onComplete() {
			if (done) {
				this.malformedSourceCounter.increment();
				return;
			}
			done = true;
			this.subscribeToTerminateSample.stop(subscribeToCompleteTimer);

			actual.onComplete();
		}

		@Override
		final public void onError(Throwable e) {
			if (done) {
				this.malformedSourceCounter.increment();
				Operators.onErrorDropped(e, actual.currentContext());
				return;
			}
			done = true;
			//register a timer for that particular exception
			Timer timer = subscribeToErrorTimerFactory.apply(e);
			//record error termination
			this.subscribeToTerminateSample.stop(timer);

			actual.onError(e);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.subscribedCounter.increment();
				this.subscribeToTerminateSample = Timer.start(clock);
				this.s = s;
				actual.onSubscribe(this);

			}
		}

		@Override
		final public void onNext(T t) {
			if (done) {
				this.malformedSourceCounter.increment();
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			actual.onNext(t);
		}

		@Override
		final public void request(long l) {
			if (Operators.validate(l)) {
				s.request(l);
			}
		}
	}

}

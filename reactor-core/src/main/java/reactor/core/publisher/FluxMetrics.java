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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static reactor.util.Metrics.*;

/**
 * Activate metrics gathering on a {@link Flux} if Micrometer is on the classpath.
 *
 * @author Simon Baslé
 */
final class FluxMetrics<T> extends FluxOperator<T, T> {

	private static final Logger LOGGER = Loggers.getLogger(FluxMetrics.class);

	/**
	 * Extract the name and tags from the upstream, and detect if there was an actual name
	 * (ie. distinct from {@link Scannable#stepName()}) set by the user.
	 *
	 * @param source the upstream
	 * @return a {@link Tuple2} of name and list of {@link Tag}
	 */
	static Tuple2<String, List<Tag>> resolveNameAndTags(Publisher<?> source) {
		//resolve the tags and names at instantiation
		String name;
		List<Tag> tags;

		Scannable scannable = Scannable.from(source);
		if (scannable.isScanAvailable()) {
			String nameOrDefault = scannable.name();
			if (scannable.stepName().equals(nameOrDefault)) {
				name = REACTOR_DEFAULT_NAME;
			}
			else {
				name = nameOrDefault;
			}
			tags = scannable.tags()
			                .map(tuple -> Tag.of(tuple.getT1(), tuple.getT2()))
			                .collect(Collectors.toList());
		}
		else {
			LOGGER.warn("Attempting to activate metrics but the upstream is not Scannable. " +
					"You might want to use `name()` (and optionally `tags()`) right before `metrics()`");
			name = reactor.util.Metrics.REACTOR_DEFAULT_NAME;
			tags = Collections.emptyList();
		}

		return Tuples.of(name, tags);
	}

	final String    name;
	final List<Tag> tags;

	FluxMetrics(Flux<? extends T> flux) {
		super(flux);

		Tuple2<String, List<Tag>> nameAndTags = resolveNameAndTags(flux);
		this.name = nameAndTags.getT1();
		this.tags = nameAndTags.getT2();
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		CoreSubscriber<? super T> metricsOperator;
		if (reactor.util.Metrics.isMicrometerAvailable()) {
			metricsOperator = new MicrometerMetricsSubscriber<>(actual, Metrics.globalRegistry,
					Clock.SYSTEM, this.name, this.tags, false);
		}
		else {
			metricsOperator = actual;
		}
		source.subscribe(metricsOperator);
	}

	static final class MicrometerMetricsSubscriber<T> implements InnerOperator<T,T> {

		final CoreSubscriber<? super T> actual;
		final MeterRegistry             registry;
		final Clock                     clock;

		final Counter             malformedSourceCounter;
		final Counter             subscribedCounter;
		final DistributionSummary requestedCounter;

		Timer.Sample subscribeToTerminateSample;
		long lastNextEventNanos = -1L;

		boolean done;
		Subscription s;

		final Timer onNextIntervalTimer;
		final Timer subscribeToCompleteTimer;
		final Timer subscribeToErrorTimer;
		final Timer subscribeToCancelTimer;

		MicrometerMetricsSubscriber(CoreSubscriber<? super T> actual,
				MeterRegistry registry,
				Clock clock,
				String sequenceName,
				List<Tag> sequenceTags,
				boolean monoSource) {
			this.actual = actual;
			this.registry = registry;
			this.clock = clock;

			List<Tag> commonTags = new ArrayList<>();
			commonTags.add(Tag.of(TAG_SEQUENCE_NAME, sequenceName));
			commonTags.add(Tag.of(TAG_SEQUENCE_TYPE, monoSource ? TAGVALUE_MONO : TAGVALUE_FLUX));
			commonTags.addAll(sequenceTags);

			this.subscribeToCompleteTimer = Timer
					.builder(METER_SUBSCRIBE_TO_TERMINATE)
					.tags(commonTags)
					.tag(TAG_TERMINATION_TYPE, TAGVALUE_ON_COMPLETE)
					.description("Times the duration elapsed between a subscription and the onComplete termination of the sequence")
					.register(registry);
			this.subscribeToErrorTimer = Timer
					.builder(METER_SUBSCRIBE_TO_TERMINATE)
					.tags(commonTags)
					.tag(TAG_TERMINATION_TYPE, TAGVALUE_ON_ERROR)
					.description("Times the duration elapsed between a subscription and the onError termination of the sequence")
					.register(registry);
			this.subscribeToCancelTimer = Timer
					.builder(METER_SUBSCRIBE_TO_TERMINATE)
					.tags(commonTags)
					.tag(TAG_TERMINATION_TYPE, TAGVALUE_CANCEL)
					.description("Times the duration elapsed between a subscription and the cancellation of the sequence")
					.register(registry);

			this.onNextIntervalTimer = Timer
					.builder(METER_ON_NEXT_DELAY)
					.tags(commonTags)
					.description("Measures delays between onNext signals (or between onSubscribe and first onNext)")
					.register(registry);

			this.subscribedCounter = Counter
					.builder(METER_SUBSCRIBED)
					.tags(commonTags)
					.baseUnit("subscribers")
					.description("Counts how many Reactor sequences have been subscribed to")
					.register(registry);

			this.malformedSourceCounter = registry.counter(METER_MALFORMED, commonTags);

			if (!REACTOR_DEFAULT_NAME.equals(sequenceName)) {
				this.requestedCounter = DistributionSummary
						.builder(METER_REQUESTED)
						.tags(commonTags)
						.description("Counts the amount requested to a named Flux by all subscribers, until at least one requests an unbounded amount")
						.baseUnit("requested amount")
						.register(registry);
			}
			else {
				requestedCounter = null;
			}
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

			//record the delay since previous onNext/onSubscribe. This also records the count.
			long last = this.lastNextEventNanos;
			this.lastNextEventNanos = clock.monotonicTime();
			this.onNextIntervalTimer.record(lastNextEventNanos - last, TimeUnit.NANOSECONDS);

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
			//we don't record the time between last onNext and onError,
			// because it would skew the onNext count by one
			this.subscribeToTerminateSample.stop(subscribeToErrorTimer);

			actual.onError(e);
		}

		@Override
		public void onComplete() {
			if (done) {
				this.malformedSourceCounter.increment();
				return;
			}
			done = true;
			//we don't record the time between last onNext and onComplete,
			// because it would skew the onNext count by one
			this.subscribeToTerminateSample.stop(subscribeToCompleteTimer);

			actual.onComplete();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.subscribedCounter.increment();
				this.subscribeToTerminateSample = Timer.start(registry);
				this.lastNextEventNanos = clock.monotonicTime();

				this.s = s;
				actual.onSubscribe(this);

			}
		}

		@Override
		public void request(long l) {
			if (Operators.validate(l)) {
				if (requestedCounter != null) {
					requestedCounter.record(l);
				}
				s.request(l);
			}
		}

		@Override
		public void cancel() {
			//we don't record the time between last onNext and cancel,
			// because it would skew the onNext count by one
			this.subscribeToTerminateSample.stop(subscribeToCancelTimer);
			
			s.cancel();
		}
	}
}

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

import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.function.Tuple2;

/**
 * Activate metrics gathering on a {@link Flux}, assuming Micrometer is on the classpath.
 *
 * @implNote Metrics.isInstrumentationAvailable() test should be performed BEFORE instantiating or referencing this
 * class, otherwise a {@link NoClassDefFoundError} will be thrown if Micrometer is not there.
 *
 * @author Simon Baslé
 * @author Stephane Maldini
 */
final class FluxMetrics<T> extends FluxOperator<T, T> {

	final String name;
	final Tags tags;

	//Note: meters and tag names are normalized by micrometer on the basis that the word
	// separator is the dot, not camelCase...
	final MeterRegistry registryCandidate;

	FluxMetrics(Flux<? extends T> flux) {
		this(flux, null);
	}

	/**
	 * For testing purposes.
	 *
	 * @param registry the registry to use, or null for global one
	 */
	FluxMetrics(Flux<? extends T> flux, @Nullable MeterRegistry registry) {
		super(flux);

		this.name = resolveName(flux);
		this.tags = resolveTags(flux, DEFAULT_TAGS_FLUX, this.name);

		if (registry == null) {
			this.registryCandidate = Metrics.globalRegistry;
		}
		else {
			this.registryCandidate = registry;
		}

	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		source.subscribe(new MetricsSubscriber<>(actual, registryCandidate, Clock.SYSTEM, this.name, this.tags));
	}

	static class MetricsSubscriber<T> implements InnerOperator<T, T> {

		final CoreSubscriber<? super T>  actual;
		final Clock                      clock;
		final Counter                    malformedSourceCounter;
		final Counter                    subscribedCounter;
		final DistributionSummary        requestedCounter;
		final Timer                      onNextIntervalTimer;
		final Timer                      subscribeToCompleteTimer;
		final Function<Throwable, Timer> subscribeToErrorTimerFactory;
		final Timer                      subscribeToCancelTimer;

		Timer.Sample subscribeToTerminateSample;
		long         lastNextEventNanos = -1L;
		boolean      done;
		Subscription s;

		MetricsSubscriber(CoreSubscriber<? super T> actual,
				MeterRegistry registry,
				Clock clock,
				String sequenceName,
				Tags commonTags) {
			this.actual = actual;
			this.clock = clock;

			this.subscribeToCompleteTimer = Timer.builder(METER_FLOW_DURATION)
			                                     .tags(commonTags.and(TAG_ON_COMPLETE))
			                                     .description(
					                                     "Times the duration elapsed between a subscription and the onComplete termination of the sequence")
			                                     .register(registry);
			this.subscribeToCancelTimer = Timer.builder(METER_FLOW_DURATION)
			                                   .tags(commonTags.and(TAG_CANCEL))
			                                   .description(
					                                   "Times the duration elapsed between a subscription and the cancellation of the sequence")
			                                   .register(registry);

			//note that Builder ISN'T TRULY IMMUTABLE. This is ok though as there will only ever be one usage.
			Timer.Builder subscribeToErrorTimerBuilder = Timer.builder(METER_FLOW_DURATION)
			                                                  .tags(commonTags.and(TAG_ON_ERROR))
			                                                  .description(
					                                                  "Times the duration elapsed between a subscription and the onError termination of the sequence, with the exception name as a tag");
			this.subscribeToErrorTimerFactory = e ->
				subscribeToErrorTimerBuilder.tag(TAG_KEY_EXCEPTION, e.getClass().getName())
				                            .register(registry);

			this.onNextIntervalTimer = Timer.builder(METER_ON_NEXT_DELAY)
			                                .tags(commonTags)
			                                .description(
					                                "Measures delays between onNext signals (or between onSubscribe and first onNext)")
			                                .register(registry);

			this.subscribedCounter = Counter.builder(METER_SUBSCRIBED)
			                                .tags(commonTags)
			                                .baseUnit("subscribers")
			                                .description("Counts how many Reactor sequences have been subscribed to")
			                                .register(registry);

			this.malformedSourceCounter = registry.counter(METER_MALFORMED, commonTags);

			if (!REACTOR_DEFAULT_NAME.equals(sequenceName)) {
				this.requestedCounter = DistributionSummary.builder(METER_REQUESTED)
				                                           .tags(commonTags)
				                                           .description(
						                                           "Counts the amount requested to a named Flux by all subscribers, until at least one requests an unbounded amount")
				                                           .baseUnit("requested amount")
				                                           .register(registry);
			}
			else {
				requestedCounter = null;
			}
		}

		@Override
		final public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		final public void cancel() {
			//we don't record the time between last onNext and cancel,
			// because it would skew the onNext count by one
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
			//we don't record the time between last onNext and onComplete,
			// because it would skew the onNext count by one
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
			//we don't record the time between last onNext and onError,
			// because it would skew the onNext count by one

			//register a timer for that particular exception
			Timer timer = subscribeToErrorTimerFactory.apply(e);
			//record error termination
			this.subscribeToTerminateSample.stop(timer);

			actual.onError(e);
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
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.subscribedCounter.increment();
				this.subscribeToTerminateSample = Timer.start(clock);
				this.lastNextEventNanos = clock.monotonicTime();
				this.s = s;
				actual.onSubscribe(this);

			}
		}

		@Override
		final public void request(long l) {
			if (Operators.validate(l)) {
				if (requestedCounter != null) {
					requestedCounter.record(l);
				}
				s.request(l);
			}
		}
	}

	/**
	 * The default sequence name that will be used for instrumented {@link Flux} and {@link Mono} that don't have a
	 * {@link Flux#name(String) name}.
	 *
	 * @see #TAG_SEQUENCE_NAME
	 */
	static final String REACTOR_DEFAULT_NAME = "reactor";
	/**
	 * Meter that counts the number of events received from a malformed source (ie an onNext after an onComplete).
	 */
	static final String METER_MALFORMED      = "reactor.malformed.source";
	/**
	 * Meter that counts the number of subscriptions to a sequence.
	 */
	static final String METER_SUBSCRIBED     = "reactor.subscribed";
	/**
	 * Meter that times the duration between the subscription and the sequence's terminal event. The timer is also using
	 * the {@link #TAG_KEY_STATUS} tag to determine which kind of event terminated the sequence.
	 */
	static final String METER_FLOW_DURATION  = "reactor.flow.duration";
	/**
	 * Meter that times the delays between each onNext (or between the first onNext and the onSubscribe event).
	 */
	static final String METER_ON_NEXT_DELAY  = "reactor.onNext.delay";
	/**
	 * Meter that tracks the request amount, in {@link Flux#name(String) named} sequences only.
	 */
	static final String METER_REQUESTED   = "reactor.requested";
	/**
	 * Tag used by {@link #METER_FLOW_DURATION} to mark what kind of terminating event occurred: {@link
	 * #TAG_ON_COMPLETE}, {@link #TAG_ON_ERROR} or {@link #TAG_CANCEL}.
	 */
	static final String TAG_KEY_STATUS    = "status";
	/**
	 * Tag used by {@link #METER_FLOW_DURATION} when {@link #TAG_KEY_STATUS} is {@link #TAG_ON_ERROR}, to store the
	 * exception that occurred.
	 */
	static final String TAG_KEY_EXCEPTION = "exception";
	/**
	 * Tag bearing the sequence's name, as given by the {@link Flux#name(String)} operator.
	 */
	static final String TAG_SEQUENCE_NAME = "flow";
	static final Tags   DEFAULT_TAGS_FLUX = Tags.of(Tag.of("type", "Flux"));
	static final Tags   DEFAULT_TAGS_MONO = Tags.of(Tag.of("type", "Mono"));

	// === Operator ===
	static final Tag TAG_ON_ERROR     = Tag.of(TAG_KEY_STATUS, "error");
	static final Tags TAG_ON_COMPLETE = Tags.of(TAG_KEY_STATUS, "completed", TAG_KEY_EXCEPTION, "");
	static final Tags TAG_CANCEL      = Tags.of(TAG_KEY_STATUS, "cancelled", TAG_KEY_EXCEPTION, "");

	static final Logger log = Loggers.getLogger(FluxMetrics.class);

	static final BiFunction<Tags, Tuple2<String, String>, Tags> TAG_ACCUMULATOR =
			(prev, tuple) -> prev.and(Tag.of(tuple.getT1(), tuple.getT2()));
	static final BinaryOperator<Tags> TAG_COMBINER = (a, b) -> b;

	/**
	 * Extract the name from the upstream, and detect if there was an actual name (ie. distinct from {@link
	 * Scannable#stepName()}) set by the user.
	 *
	 * @param source the upstream
	 *
	 * @return a name
	 */
	static String resolveName(Publisher<?> source) {
		Scannable scannable = Scannable.from(source);
		if (scannable.isScanAvailable()) {
			String nameOrDefault = scannable.name();
			if (scannable.stepName()
			             .equals(nameOrDefault)) {
				return REACTOR_DEFAULT_NAME;
			}
			else {
				return nameOrDefault;
			}
		}
		else {
			log.warn("Attempting to activate metrics but the upstream is not Scannable. " + "You might want to use `name()` (and optionally `tags()`) right before `metrics()`");
			return REACTOR_DEFAULT_NAME;
		}

	}

	/**
	 * Extract the tags from the upstream
	 *
	 * @param source the upstream
	 *
	 * @return a {@link Tags} of {@link Tag}
	 */
	static Tags resolveTags(Publisher<?> source, Tags tags, String sequenceName) {
		Scannable scannable = Scannable.from(source);
		tags = tags.and(Tag.of(FluxMetrics.TAG_SEQUENCE_NAME, sequenceName));

		if (scannable.isScanAvailable()) {
			return scannable.tags()
			                .reduce(tags, TAG_ACCUMULATOR, TAG_COMBINER);
		}

		return tags;
	}

}

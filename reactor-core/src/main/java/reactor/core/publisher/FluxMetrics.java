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

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.Metrics.MicrometerConfiguration;
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
final class FluxMetrics<T> extends InternalFluxOperator<T, T> {

	final String name;
	final Tags tags;

	//Note: meters and tag names are normalized by micrometer on the basis that the word
	// separator is the dot, not camelCase...
	final MeterRegistry registryCandidate;

	FluxMetrics(Flux<? extends T> flux) {
		super(flux);

		this.name = resolveName(flux);
		this.tags = resolveTags(flux, DEFAULT_TAGS_FLUX);

		this.registryCandidate = MicrometerConfiguration.getRegistry();
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new MetricsSubscriber<>(actual, registryCandidate, Clock.SYSTEM, this.name, this.tags);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	static class MetricsSubscriber<T> implements InnerOperator<T, T> {

		final CoreSubscriber<? super T> actual;
		final Clock                     clock;
		final String                    sequenceName;
		final Tags                      commonTags;
		final MeterRegistry             registry;
		final DistributionSummary       requestedCounter;
		final Timer                     onNextIntervalTimer;

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
			this.sequenceName = sequenceName;
			this.commonTags = commonTags;
			this.registry = registry;


			this.onNextIntervalTimer = Timer.builder(sequenceName + METER_ON_NEXT_DELAY)
			                                .tags(commonTags)
			                                .description(
					                                "Measures delays between onNext signals (or between onSubscribe and first onNext)")
			                                .register(registry);

			if (!REACTOR_DEFAULT_NAME.equals(sequenceName)) {
				this.requestedCounter = DistributionSummary.builder(sequenceName + METER_REQUESTED)
				                                           .tags(commonTags)
				                                           .description(
						                                           "Counts the amount requested to a named Flux by all subscribers, until at least one requests an unbounded amount")
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
			recordCancel(sequenceName, commonTags, registry, subscribeToTerminateSample);

			s.cancel();
		}

		@Override
		final public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			//we don't record the time between last onNext and onComplete,
			// because it would skew the onNext count by one.
			// We differentiate between empty completion and value completion, however, via tags.
			if (this.onNextIntervalTimer.count() == 0) {
				recordOnCompleteEmpty(sequenceName, commonTags, registry, subscribeToTerminateSample);
			} else {
				recordOnComplete(sequenceName, commonTags, registry, subscribeToTerminateSample);
			}

			actual.onComplete();
		}

		@Override
		final public void onError(Throwable e) {
			if (done) {
				recordMalformed(sequenceName, commonTags, registry);
				Operators.onErrorDropped(e, actual.currentContext());
				return;
			}
			done = true;
			//we don't record the time between last onNext and onError,
			// because it would skew the onNext count by one
			recordOnError(sequenceName, commonTags, registry, subscribeToTerminateSample, e);
			actual.onError(e);
		}

		@Override
		public void onNext(T t) {
			if (done) {
				recordMalformed(sequenceName, commonTags, registry);
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
				recordOnSubscribe(sequenceName, commonTags, registry);
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

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
			return InnerOperator.super.scanUnsafe(key);
		}
	}

	/**
	 * The default sequence name that will be used for instrumented {@link Flux} and {@link Mono} that don't have a
	 * {@link Flux#name(String) name}.
	 */
	static final String REACTOR_DEFAULT_NAME = "reactor";
	/**
	 * Meter that counts the number of events received from a malformed source (ie an onNext after an onComplete).
	 */
	static final String METER_MALFORMED      = ".malformed.source";
	/**
	 * Meter that counts the number of subscriptions to a sequence.
	 */
	static final String METER_SUBSCRIBED     = ".subscribed";
	/**
	 * Meter that times the duration elapsed between a subscription and the termination or cancellation of the sequence.
	 * A status tag is added to specify what event caused the timer to end (completed, completedEmpty, error, cancelled).
	 */
	static final String METER_FLOW_DURATION  = ".flow.duration";
	/**
	 * Meter that times the delays between each onNext (or between the first onNext and the onSubscribe event).
	 */
	static final String METER_ON_NEXT_DELAY  = ".onNext.delay";
	/**
	 * Meter that tracks the request amount, in {@link Flux#name(String) named} sequences only.
	 */
	static final String METER_REQUESTED   = ".requested";
	/**
	 * Tag used by {@link #METER_FLOW_DURATION} when "status" is {@link #TAG_ON_ERROR}, to store the
	 * exception that occurred.
	 */
	static final String TAG_KEY_EXCEPTION = "exception";
	/**
	 * Tag bearing the sequence's name, as given by the {@link Flux#name(String)} operator.
	 */
	static final Tags   DEFAULT_TAGS_FLUX = Tags.of("type", "Flux");
	static final Tags   DEFAULT_TAGS_MONO = Tags.of("type", "Mono");

	// === Operator ===
	static final Tag  TAG_ON_ERROR    = Tag.of("status", "error");
	static final Tags TAG_ON_COMPLETE = Tags.of("status", "completed", TAG_KEY_EXCEPTION, "");
	static final Tags TAG_ON_COMPLETE_EMPTY = Tags.of("status", "completedEmpty", TAG_KEY_EXCEPTION, "");
	static final Tags TAG_CANCEL      = Tags.of("status", "cancelled", TAG_KEY_EXCEPTION, "");

	static final Logger log = Loggers.getLogger(FluxMetrics.class);

	static final BiFunction<Tags, Tuple2<String, String>, Tags> TAG_ACCUMULATOR =
			(prev, tuple) -> prev.and(Tag.of(tuple.getT1(), tuple.getT2()));
	static final BinaryOperator<Tags> TAG_COMBINER = Tags::and;

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
	static Tags resolveTags(Publisher<?> source, Tags tags) {
		Scannable scannable = Scannable.from(source);

		if (scannable.isScanAvailable()) {
			LinkedList<Tuple2<String, String>> scannableTags = new LinkedList<>();
			scannable.tags().forEach(scannableTags::push);
			return scannableTags.stream()
			                    //Note the combiner below is for parallel streams, which won't be used
			                    //For the identity, `commonTags` should be ok (even if reduce uses it multiple times)
			                    //since it deduplicates
			                    .reduce(tags, TAG_ACCUMULATOR, TAG_COMBINER);
		}

		return tags;
	}

	/*
	 * This method calls the registry, which can be costly. However the cancel signal is only expected
	 * once per Subscriber. So the net effect should be that the registry is only called once, which
	 * is equivalent to registering the meter as a final field, with the added benefit of paying that
	 * cost only in case of cancellation.
	 */
	static void recordCancel(String name, Tags commonTags, MeterRegistry registry, Timer.Sample flowDuration) {
		Timer timer = Timer.builder(name + METER_FLOW_DURATION)
		                   .tags(commonTags.and(TAG_CANCEL))
		                   .description(
				                   "Times the duration elapsed between a subscription and the cancellation of the sequence")
		                   .register(registry);

		flowDuration.stop(timer);
	}

	/*
	 * This method calls the registry, which can be costly. However a malformed signal is generally
	 * not expected, or at most once per Subscriber. So the net effect should be that the registry
	 * is only called once, which is equivalent to registering the meter as a final field,
	 * with the added benefit of paying that cost only in case of onNext/onError after termination.
	 */
	static void recordMalformed(String name, Tags commonTags, MeterRegistry registry) {
		registry.counter(name + FluxMetrics.METER_MALFORMED, commonTags)
		        .increment();
	}

	/*
	 * This method calls the registry, which can be costly. However the onError signal is expected
	 * at most once per Subscriber. So the net effect should be that the registry is only called once,
	 * which is equivalent to registering the meter as a final field, with the added benefit of paying
	 * that cost only in case of error.
	 */
	static void recordOnError(String name, Tags commonTags, MeterRegistry registry, Timer.Sample flowDuration, Throwable e) {
		Timer timer = Timer.builder(name + METER_FLOW_DURATION)
		                   .tags(commonTags.and(TAG_ON_ERROR))
		                   .tag(TAG_KEY_EXCEPTION,
				                   e.getClass()
				                    .getName())
		                   .description(
				                   "Times the duration elapsed between a subscription and the onError termination of the sequence, with the exception name as a tag.")
		                   .register(registry);

		flowDuration.stop(timer);
	}

	/*
	 * This method calls the registry, which can be costly. However the onComplete signal is expected
	 * at most once per Subscriber. So the net effect should be that the registry is only called once,
	 * which is equivalent to registering the meter as a final field, with the added benefit of paying
	 * that cost only in case of completion (which is not always occurring).
	 */
	static void recordOnComplete(String name, Tags commonTags, MeterRegistry registry, Timer.Sample flowDuration) {
		Timer timer = Timer.builder(name + METER_FLOW_DURATION)
		                   .tags(commonTags.and(TAG_ON_COMPLETE))
		                   .description(
				                   "Times the duration elapsed between a subscription and the onComplete termination of a sequence that did emit some elements")
		                   .register(registry);

		flowDuration.stop(timer);
	}

	/*
	 * This method calls the registry, which can be costly. However the onComplete signal is expected
	 * at most once per Subscriber. So the net effect should be that the registry is only called once,
	 * which is equivalent to registering the meter as a final field, with the added benefit of paying
	 * that cost only in case of completion (which is not always occurring).
	 */
	static void recordOnCompleteEmpty(String name, Tags commonTags, MeterRegistry registry, Timer.Sample flowDuration) {
		Timer timer = Timer.builder(name + METER_FLOW_DURATION)
				.tags(commonTags.and(TAG_ON_COMPLETE_EMPTY))
				.description(
						"Times the duration elapsed between a subscription and the onComplete termination of a sequence that didn't emit any element")
				.register(registry);

		flowDuration.stop(timer);
	}

	/*
	 * This method calls the registry, which can be costly. However the onSubscribe signal is expected
	 * at most once per Subscriber. So the net effect should be that the registry is only called once,
	 * which is equivalent to registering the meter as a final field, with the added benefit of paying
	 * that cost only in case of subscription.
	 */
	static void recordOnSubscribe(String name, Tags commonTags, MeterRegistry registry) {
		Counter.builder(name + METER_SUBSCRIBED)
		       .tags(commonTags)
		       .description("Counts how many Reactor sequences have been subscribed to")
		       .register(registry)
		       .increment();
	}

}

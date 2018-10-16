/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * Activate metrics gathering on a {@link Flux}, assuming Micrometer is on the classpath.
 *
 * @implNote Metrics.isMicrometerAvailable() test should be performed BEFORE instantiating
 * or referencing this class, otherwise a {@link NoClassDefFoundError} will be thrown if
 * Micrometer is not there.
 *
 * @author Simon Baslé
 */
final class FluxMetrics<T> extends FluxOperator<T, T> {

	private static final Logger LOGGER = Loggers.getLogger(FluxMetrics.class);

	//=== Constants ===

	/**
	 * The default sequence name that will be used for instrumented {@link Flux} and
	 * {@link Mono} that don't have a {@link Flux#name(String) name}.
	 *
	 * @see #TAG_SEQUENCE_NAME
	 */
	static final String REACTOR_DEFAULT_NAME = "reactor";

	//Note: meters and tag names are normalized by micrometer on the basis that the word
	// separator is the dot, not camelCase...
	/**
	 * Meter that counts the number of events received from a malformed source (ie an
	 * onNext after an onComplete).
	 */
	static final String METER_MALFORMED     = "reactor.malformed.source";
	/**
	 * Meter that counts the number of subscriptions to a sequence.
	 */
	static final String METER_SUBSCRIBED    = "reactor.subscribed";
	/**
	 * Meter that times the duration between the subscription and the sequence's terminal
	 * event. The timer is also using the {@link #TAG_STATUS} tag to determine
	 * which kind of event terminated the sequence.
	 */
	static final String METER_FLOW_DURATION = "reactor.flow.duration";
	/**
	 * Meter that times the delays between each onNext (or between the first onNext and
	 * the onSubscribe event).
	 */
	static final String METER_ON_NEXT_DELAY = "reactor.onNext.delay";
	/**
	 * Meter that tracks the request amount, in {@link Flux#name(String) named}
	 * sequences only.
	 */
	static final String METER_REQUESTED     = "reactor.requested";

	/**
	 * Tag used by {@link #METER_FLOW_DURATION} to mark what kind of terminating
	 * event occurred: {@link #TAGVALUE_ON_COMPLETE}, {@link #TAGVALUE_ON_ERROR} or
	 * {@link #TAGVALUE_CANCEL}.
	 */
	static final String TAG_STATUS = "status";

	/**
	 * Tag used by {@link #METER_FLOW_DURATION} when {@link #TAG_STATUS}
	 * is {@link #TAGVALUE_ON_ERROR}, to store the exception that occurred.
	 */
	static final String TAG_EXCEPTION = "exception";

	/**
	 * Tag bearing the sequence's name, as given by the {@link Flux#name(String)} operator.
	 */
	static final String TAG_SEQUENCE_NAME    = "flow";
	/**
	 * Tag bearing the sequence's type, {@link Flux} or {@link Mono}.
	 * @see #TAGVALUE_FLUX
	 * @see #TAGVALUE_MONO
	 */
	static final String TAG_SEQUENCE_TYPE    = "type";

	//... tag values are free-for-all
	static final String TAGVALUE_ON_ERROR    = "error";
	static final String TAGVALUE_ON_COMPLETE = "completed";
	static final String TAGVALUE_CANCEL      = "cancelled";
	static final String TAGVALUE_FLUX        = "Flux";
	static final String TAGVALUE_MONO        = "Mono";

	// === Utility Methods ===

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
			name = REACTOR_DEFAULT_NAME;
			tags = Collections.emptyList();
		}

		return Tuples.of(name, tags);
	}

	// === Operator ===

	final String    name;
	final List<Tag> tags;

	@Nullable
	final MeterRegistry    registryCandidate;

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

		Tuple2<String, List<Tag>> nameAndTags = resolveNameAndTags(flux);
		this.name = nameAndTags.getT1();
		this.tags = nameAndTags.getT2();

		if (registry == null) {
			registry = (MeterRegistry) reactor.core.Metrics.getRegistryCandidate();
		}

		this.registryCandidate = registry;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		MeterRegistry registry = Metrics.globalRegistry;
		if (registryCandidate != null) {
			registry = registryCandidate;
		}
		source.subscribe(new MicrometerFluxMetricsSubscriber<>(actual, registry,
				Clock.SYSTEM, this.name, this.tags));
	}

	static class MicrometerFluxMetricsSubscriber<T> implements InnerOperator<T,T> {

		final CoreSubscriber<? super T> actual;
		final MeterRegistry             registry;
		final Clock                     clock;

		final Counter             malformedSourceCounter;
		final Counter             subscribedCounter;
		final DistributionSummary requestedCounter;

		Timer.Sample subscribeToTerminateSample;
		long lastNextEventNanos = -1L;

		boolean done;
		@Nullable
		Fuseable.QueueSubscription<T> qs;
		Subscription s;

		final Timer         onNextIntervalTimer;
		final Timer         subscribeToCompleteTimer;
		final Timer.Builder subscribeToErrorTimerBuilder;
		final Timer         subscribeToCancelTimer;

		MicrometerFluxMetricsSubscriber(CoreSubscriber<? super T> actual,
				MeterRegistry registry,
				Clock clock,
				String sequenceName,
				List<Tag> sequenceTags) {
			this.actual = actual;
			this.registry = registry;
			this.clock = clock;

			List<Tag> commonTags = new ArrayList<>();
			commonTags.add(Tag.of(TAG_SEQUENCE_NAME, sequenceName));
			commonTags.add(Tag.of(TAG_SEQUENCE_TYPE, TAGVALUE_FLUX));
			commonTags.addAll(sequenceTags);

			this.subscribeToCompleteTimer = Timer
					.builder(METER_FLOW_DURATION)
					.tags(commonTags)
					.tag(TAG_STATUS, TAGVALUE_ON_COMPLETE)
					.description("Times the duration elapsed between a subscription and the onComplete termination of the sequence")
					.register(registry);
			this.subscribeToCancelTimer = Timer
					.builder(METER_FLOW_DURATION)
					.tags(commonTags)
					.tag(TAG_STATUS, TAGVALUE_CANCEL)
					.description("Times the duration elapsed between a subscription and the cancellation of the sequence")
					.register(registry);

			//note that Builder ISN'T TRULY IMMUTABLE. This is ok though as there will only ever be one usage.
			this.subscribeToErrorTimerBuilder = Timer
					.builder(METER_FLOW_DURATION)
					.tags(commonTags)
					.tag(TAG_STATUS, TAGVALUE_ON_ERROR)
					.description("Times the duration elapsed between a subscription and the onError termination of the sequence, with the exception name as a tag");

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

			//register a timer for that particular exception
			Timer timer = subscribeToErrorTimerBuilder
					.tag(FluxMetrics.TAG_EXCEPTION, e.getClass().getName())
					.register(registry);
			//record error termination
			this.subscribeToTerminateSample.stop(timer);

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

				if (s instanceof Fuseable.QueueSubscription) {
					//noinspection unchecked
					this.qs = (Fuseable.QueueSubscription<T>) s;
				}
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

	/**
	 * @implNote we don't want to particularly track fusion-specific calls, as this
	 * subscriber would only detect fused subsequences immediately upstream of it, so
	 * Counters would be a bit irrelevant. We however want to instrument onNext counts.
	 *
	 * @param <T>
	 */
	static final class MicrometerFluxMetricsFuseableSubscriber<T> extends MicrometerFluxMetricsSubscriber<T>
			implements Fuseable, Fuseable.QueueSubscription<T> {

		private int fusionMode;

		MicrometerFluxMetricsFuseableSubscriber(CoreSubscriber<? super T> actual,
				MeterRegistry registry, Clock clock, String sequenceName, List<Tag> sequenceTags) {
			super(actual, registry, clock, sequenceName, sequenceTags);
		}

		@Override
		public void onNext(T t) {
			if (this.fusionMode == Fuseable.ASYNC) {
				actual.onNext(null);
				return;
			}

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
		public int requestFusion(int mode) {
			//Simply negotiate the fusion by delegating:
			if (qs != null) {
				this.fusionMode = qs.requestFusion(mode);
				return fusionMode;
			}
			return Fuseable.NONE; //should not happen unless requestFusion called before subscribe
		}

		@Override
		@Nullable
		public T poll() {
			if (qs == null) {
				return null;
			}
			try {
				T v = qs.poll();

				if (v == null && fusionMode == SYNC) {
					//this is also a complete event
					this.subscribeToTerminateSample.stop(subscribeToCompleteTimer);
				}
				if (v != null) {
					//this is an onNext event
					//record the delay since previous onNext/onSubscribe. This also records the count.
					long last = this.lastNextEventNanos;
					this.lastNextEventNanos = clock.monotonicTime();
					this.onNextIntervalTimer.record(lastNextEventNanos - last, TimeUnit.NANOSECONDS);
				}
				return v;
			} catch (Throwable e) {
				//register a timer for that particular exception
				Timer timer = subscribeToErrorTimerBuilder
						.tag(FluxMetrics.TAG_EXCEPTION, e.getClass().getName())
						.register(registry);
				//record error termination
				this.subscribeToTerminateSample.stop(timer);
				throw e;
			}
		}

		@Override
		public void clear() {
			if (qs != null) {
				qs.clear();
			}
		}

		@Override
		public boolean isEmpty() {
			return qs == null || qs.isEmpty();
		}

		@Override
		public int size() {
			return qs == null ? 0 : qs.size();
		}
	}
}

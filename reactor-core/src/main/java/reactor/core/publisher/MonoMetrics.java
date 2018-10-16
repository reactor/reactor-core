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
import java.util.List;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;
import reactor.util.function.Tuple2;

/**
 * Activate metrics gathering on a {@link Mono}, assumes Micrometer is on the classpath.
 *
 * @implNote Metrics.isMicrometerAvailable() test should be performed BEFORE instantiating
 * or referencing this class, otherwise a {@link NoClassDefFoundError} will be thrown if
 * Micrometer is not there.
 *
 * @author Simon Baslé
 */
final class MonoMetrics<T> extends MonoOperator<T, T> {

	final String    name;
	final List<Tag> tags;

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

		Tuple2<String, List<Tag>> nameAndTags = FluxMetrics.resolveNameAndTags(mono);
		this.name = nameAndTags.getT1();
		this.tags = nameAndTags.getT2();

		if (meterRegistry == null) {
			meterRegistry = (MeterRegistry) reactor.core.Metrics.getRegistryCandidate();
		}

		this.meterRegistry = meterRegistry;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		MeterRegistry registry = Metrics.globalRegistry;
		if (meterRegistry != null) {
			registry = meterRegistry;
		}
		source.subscribe(new MicrometerMonoMetricsSubscriber<>(actual, registry,
				Clock.SYSTEM, this.name, this.tags));
	}

	static class MicrometerMonoMetricsSubscriber<T> implements InnerOperator<T,T> {

		final CoreSubscriber<? super T> actual;
		final MeterRegistry             registry;
		final Clock                     clock;

		final Counter             malformedSourceCounter;
		final Counter             subscribedCounter;

		Timer.Sample subscribeToTerminateSample;

		boolean done;
		@Nullable
		Fuseable.QueueSubscription<T> qs;
		Subscription s;

		final Timer         subscribeToCompleteTimer;
		final Timer.Builder subscribeToErrorTimerBuilder;
		final Timer         subscribeToCancelTimer;

		MicrometerMonoMetricsSubscriber(CoreSubscriber<? super T> actual,
				MeterRegistry registry,
				Clock clock,
				String sequenceName,
				List<Tag> sequenceTags) {
			this.actual = actual;
			this.registry = registry;
			this.clock = clock;

			List<Tag> commonTags = new ArrayList<>();
			commonTags.add(Tag.of(FluxMetrics.TAG_SEQUENCE_NAME, sequenceName));
			commonTags.add(Tag.of(FluxMetrics.TAG_SEQUENCE_TYPE, FluxMetrics.TAGVALUE_MONO));
			commonTags.addAll(sequenceTags);

			this.subscribeToCompleteTimer = Timer
					.builder(FluxMetrics.METER_FLOW_DURATION)
					.tags(commonTags)
					.tag(FluxMetrics.TAG_STATUS, FluxMetrics.TAGVALUE_ON_COMPLETE)
					.description("Times the duration elapsed between a subscription and the onComplete termination of the sequence")
					.register(registry);
			this.subscribeToCancelTimer = Timer
					.builder(FluxMetrics.METER_FLOW_DURATION)
					.tags(commonTags)
					.tag(FluxMetrics.TAG_STATUS, FluxMetrics.TAGVALUE_CANCEL)
					.description("Times the duration elapsed between a subscription and the cancellation of the sequence")
					.register(registry);

			//note that Builder ISN'T TRULY IMMUTABLE. This is ok though as there will only ever be one usage.
			this.subscribeToErrorTimerBuilder = Timer
					.builder(FluxMetrics.METER_FLOW_DURATION)
					.tags(commonTags)
					.tag(FluxMetrics.TAG_STATUS, FluxMetrics.TAGVALUE_ON_ERROR)
					.description("Times the duration elapsed between a subscription and the onError termination of the sequence, with the exception name as a tag.");

			this.subscribedCounter = Counter
					.builder(FluxMetrics.METER_SUBSCRIBED)
					.tags(commonTags)
					.baseUnit("subscribers")
					.description("Counts how many Reactor sequences have been subscribed to")
					.register(registry);

			this.malformedSourceCounter = registry.counter(FluxMetrics.METER_MALFORMED, commonTags);
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
			this.subscribeToTerminateSample.stop(subscribeToCompleteTimer);

			actual.onComplete();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.subscribedCounter.increment();
				this.subscribeToTerminateSample = Timer.start(registry);

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
				s.request(l);
			}
		}

		@Override
		public void cancel() {
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
	static final class MicrometerMonoMetricsFuseableSubscriber<T> extends MicrometerMonoMetricsSubscriber<T>
			implements Fuseable, Fuseable.QueueSubscription<T> {

		private int mode;

		MicrometerMonoMetricsFuseableSubscriber(CoreSubscriber<? super T> actual,
				MeterRegistry registry, Clock clock, String sequenceName, List<Tag> sequenceTags) {
			super(actual, registry, clock, sequenceName, sequenceTags);
		}

		@Override
		public void onNext(T t) {
//			if (this.mode == ASYNC) {
//				actual.onNext(null);
//			}
//			else {
				super.onNext(t);
//			}
		}

		@Override
		public int requestFusion(int mode) {
			//Simply negotiate the fusion by delegating:
			if (qs != null) {
				this.mode = qs.requestFusion(mode);
				return this.mode;
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

				if (v == null && this.mode == SYNC) {
					//this is also a complete event
					this.subscribeToTerminateSample.stop(subscribeToCompleteTimer);
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

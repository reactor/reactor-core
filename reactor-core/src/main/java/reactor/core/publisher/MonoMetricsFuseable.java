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

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

import static reactor.core.publisher.FluxMetrics.resolveName;
import static reactor.core.publisher.FluxMetrics.resolveTags;

/**
 * Activate metrics gathering on a {@link Mono} (Fuseable version), assumes Micrometer is on the classpath.

 * @implNote Metrics.isInstrumentationAvailable() test should be performed BEFORE instantiating or referencing this
 * class, otherwise a {@link NoClassDefFoundError} will be thrown if Micrometer is not there.
 *
 * @author Simon Baslé
 * @author Stephane Maldini
 */
final class MonoMetricsFuseable<T> extends InternalMonoOperator<T, T> implements Fuseable {

	final String name;
	final Tags   tags;

	final MeterRegistry registryCandidate;

	MonoMetricsFuseable(Mono<? extends T> mono) {
		this(mono, null);
	}

	/**
	 * For testing purposes.
	 *
	 * @param registryCandidate the registry to use, as a plain {@link Object} to avoid leaking dependency
	 */
	MonoMetricsFuseable(Mono<? extends T> mono, @Nullable MeterRegistry registryCandidate) {
		super(mono);

		this.name = resolveName(mono);
		this.tags = resolveTags(mono, FluxMetrics.DEFAULT_TAGS_MONO, this.name);

		if (registryCandidate == null) {
			this.registryCandidate = Metrics.globalRegistry;
		}
		else {
			this.registryCandidate = registryCandidate;
		}
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new MetricsFuseableSubscriber<>(actual, registryCandidate, Clock.SYSTEM, this.tags);
	}

	/**
	 * @param <T>
	 *
	 * @implNote we don't want to particularly track fusion-specific calls, as this subscriber would only detect fused
	 * subsequences immediately upstream of it, so Counters would be a bit irrelevant. We however want to instrument
	 * onNext counts.
	 */
	static final class MetricsFuseableSubscriber<T> extends MonoMetrics.MetricsSubscriber<T>
			implements Fuseable, QueueSubscription<T> {

		int mode;

		@Nullable
		Fuseable.QueueSubscription<T> qs;

		MetricsFuseableSubscriber(CoreSubscriber<? super T> actual,
				MeterRegistry registry,
				Clock clock,
				Tags sequenceTags) {
			super(actual, registry, clock, sequenceTags);
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
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				FluxMetrics.recordOnSubscribe(commonTags, registry);
				this.subscribeToTerminateSample = Timer.start(clock);
				this.qs = Operators.as(s);
				this.s = s;
				actual.onSubscribe(this);

			}
		}

		@Override
		@Nullable
		public T poll() {
			if (qs == null) {
				return null;
			}
			try {
				T v = qs.poll();
				if (!done && (v != null || mode == SYNC)) {
					FluxMetrics.recordOnComplete(commonTags, registry, subscribeToTerminateSample);
				}
				done = true;
				return v;
			}
			catch (Throwable e) {
				FluxMetrics.recordOnError(commonTags, registry, subscribeToTerminateSample, e);
				throw e;
			}
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
		public int size() {
			return qs == null ? 0 : qs.size();
		}
	}
}

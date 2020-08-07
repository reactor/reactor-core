/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.Metrics;
import reactor.util.annotation.Nullable;

import static reactor.core.publisher.FluxMetrics.*;

/**
 * Activate metrics gathering on a {@link Flux} (Fuseable version), assumes Micrometer is on the classpath.

 * @implNote Metrics.isInstrumentationAvailable() test should be performed BEFORE instantiating or referencing this
 * class, otherwise a {@link NoClassDefFoundError} will be thrown if Micrometer is not there.
 *
 * @author Simon Baslé
 * @author Stephane Maldini
 */
final class FluxMetricsFuseable<T> extends InternalFluxOperator<T, T> implements Fuseable {

	final String        name;
	final Tags          tags;
	final MeterRegistry registryCandidate;

	FluxMetricsFuseable(Flux<? extends T> flux) {
		super(flux);

		this.name = resolveName(flux);
		this.tags = resolveTags(flux, FluxMetrics.DEFAULT_TAGS_FLUX);
		this.registryCandidate = Metrics.MicrometerConfiguration.getRegistry();
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		return new MetricsFuseableSubscriber<>(actual, registryCandidate, Clock.SYSTEM, this.name, this.tags);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}

	/**
	 * @param <T>
	 *
	 * @implNote we don't want to particularly track fusion-specific calls, as this subscriber would only detect fused
	 * subsequences immediately upstream of it, so Counters would be a bit irrelevant. We however want to instrument
	 * onNext counts.
	 */
	static final class MetricsFuseableSubscriber<T> extends FluxMetrics.MetricsSubscriber<T>
			implements Fuseable, QueueSubscription<T> {

		int mode;

		@Nullable
		Fuseable.QueueSubscription<T> qs;

		MetricsFuseableSubscriber(CoreSubscriber<? super T> actual,
				MeterRegistry registry,
				Clock clock,
				String sequenceName,
				Tags sequenceTags) {
			super(actual, registry, clock, sequenceName, sequenceTags);
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
		public void onNext(T t) {
			if (this.mode == Fuseable.ASYNC) {
				actual.onNext(null);
				return;
			}

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
		@Nullable
		public T poll() {
			if (qs == null) {
				return null;
			}
			try {
				T v = qs.poll();

				if (v == null && mode == SYNC) {
					if (this.onNextIntervalTimer.count() == 0) {
						recordOnCompleteEmpty(sequenceName, commonTags, registry, subscribeToTerminateSample);
					} else {
						recordOnComplete(sequenceName, commonTags, registry, subscribeToTerminateSample);
					}
				}
				if (v != null) {
					//this is an onNext event
					//record the delay since previous onNext/onSubscribe. This also records the count.
					long last = this.lastNextEventNanos;
					this.lastNextEventNanos = clock.monotonicTime();
					this.onNextIntervalTimer.record(lastNextEventNanos - last, TimeUnit.NANOSECONDS);
				}
				return v;
			}
			catch (Throwable e) {
				recordOnError(sequenceName, commonTags, registry, subscribeToTerminateSample, e);
				throw e;
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				recordOnSubscribe(sequenceName, commonTags, registry);
				this.subscribeToTerminateSample = Timer.start(clock);
				this.lastNextEventNanos = clock.monotonicTime();
				this.qs = Operators.as(s);
				this.s = s;
				actual.onSubscribe(this);
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

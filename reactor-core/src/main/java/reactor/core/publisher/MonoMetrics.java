/*
 * Copyright (c) 2018-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import io.micrometer.core.instrument.*;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.util.Metrics;
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
final class MonoMetrics<T> extends InternalMonoOperator<T, T> {

	final String        name;
	final Tags          tags;

	final MeterRegistry registryCandidate;

	MonoMetrics(Mono<? extends T> mono) {
		super(mono);

		this.name = resolveName(mono);
		this.tags = resolveTags(mono, DEFAULT_TAGS_MONO);

		this.registryCandidate = Metrics.MicrometerConfiguration.getRegistry();
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
		final String 					sequenceName;
		final Tags                      commonTags;
		final MeterRegistry             registry;

		Timer.Sample subscribeToTerminateSample;
		boolean done;
		Subscription s;

		MetricsSubscriber(CoreSubscriber<? super T> actual,
				MeterRegistry registry, Clock clock, String sequenceName, Tags commonTags) {
			this.actual = actual;
			this.clock = clock;
			this.sequenceName = sequenceName;
			this.commonTags = commonTags;
			this.registry = registry;
		}

		@Override
		final public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		final public void cancel() {
			recordCancel(sequenceName, commonTags, registry, subscribeToTerminateSample);
			s.cancel();
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			recordOnCompleteEmpty(sequenceName, commonTags, registry, subscribeToTerminateSample);
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
			done = true;
			recordOnComplete(sequenceName, commonTags, registry, subscribeToTerminateSample);
			actual.onNext(t);
			actual.onComplete();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				recordOnSubscribe(sequenceName, commonTags, registry);
				this.subscribeToTerminateSample = Timer.start(clock);
				this.s = s;
				actual.onSubscribe(this);
			}
		}

		@Override
		final public void request(long l) {
			if (Operators.validate(l)) {
				s.request(l);
			}
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
			return InnerOperator.super.scanUnsafe(key);
		}

	}

}

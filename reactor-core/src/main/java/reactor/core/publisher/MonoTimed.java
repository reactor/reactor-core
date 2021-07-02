/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;

/**
 * @author Simon Baslé
 */
final class MonoTimed<T> extends InternalMonoOperator<T, Timed<T>> {

	final Scheduler clock;

	MonoTimed(Mono<? extends T> source, Scheduler clock) {
		super(source);
		this.clock = clock;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super Timed<T>> actual) {
		return new FluxTimed.TimedSubscriber<>(actual, this.clock);
	}

	@Nullable
	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PREFETCH) return 0;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return super.scanUnsafe(key);
	}
}

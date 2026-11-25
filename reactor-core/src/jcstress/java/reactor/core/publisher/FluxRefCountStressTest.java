/*
 * Copyright (c) 2026 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.concurrent.atomic.AtomicInteger;

import org.jspecify.annotations.Nullable;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;

public abstract class FluxRefCountStressTest {

	// see https://github.com/reactor/reactor-core/issues/4289
	// The only subscriber cancels from another thread while it is being subscribed, so the
	// cancel can land before, during or after the source is connected. In every case the
	// upstream must either never be subscribed or end up cancelled.
	public abstract static class SubscribeCancelRaceBaseStressTest {

		final AtomicInteger upstreamSubscriptions = new AtomicInteger();
		final AtomicInteger upstreamCancellations = new AtomicInteger();

		final StressSubscriber<Integer> subscriber = new StressSubscriber<>();

		final Flux<Integer> sharedSource;

		SubscribeCancelRaceBaseStressTest(@Nullable Duration gracePeriod) {
			ConnectableFlux<Integer> published =
					Flux.<Integer>never()
					    .doOnSubscribe(s -> upstreamSubscriptions.incrementAndGet())
					    .doOnCancel(upstreamCancellations::incrementAndGet)
					    .publish();
			this.sharedSource = gracePeriod == null ? published.refCount(1) : published.refCount(1, gracePeriod);
		}
	}

	@JCStressTest
	@Outcome(id = {"0, 0"}, expect = ACCEPTABLE, desc = "cancelled before connecting, upstream never subscribed")
	@Outcome(id = {"1, 1"}, expect = ACCEPTABLE, desc = "cancelled after connecting, upstream disconnected")
	@State
	public static class RefCountSubscribeCancelRaceStressTest extends SubscribeCancelRaceBaseStressTest {

		public RefCountSubscribeCancelRaceStressTest() {
			super(null);
		}

		@Actor
		public void subscribe() {
			sharedSource.subscribe(subscriber);
		}

		@Actor
		public void cancel() {
			subscriber.cancel();
		}

		@Arbiter
		public void arbiter(II_Result r) {
			r.r1 = upstreamSubscriptions.get();
			r.r2 = upstreamCancellations.get();
		}
	}

	@JCStressTest
	@Outcome(id = {"0, 0"}, expect = ACCEPTABLE, desc = "cancelled before connecting, upstream never subscribed")
	@Outcome(id = {"1, 1"}, expect = ACCEPTABLE, desc = "cancelled after connecting, upstream disconnected")
	@State
	public static class RefCountGraceSubscribeCancelRaceStressTest extends SubscribeCancelRaceBaseStressTest {

		// a zero grace period disconnects synchronously, keeping the outcome deterministic
		public RefCountGraceSubscribeCancelRaceStressTest() {
			super(Duration.ZERO);
		}

		@Actor
		public void subscribe() {
			sharedSource.subscribe(subscriber);
		}

		@Actor
		public void cancel() {
			subscriber.cancel();
		}

		@Arbiter
		public void arbiter(II_Result r) {
			r.r1 = upstreamSubscriptions.get();
			r.r2 = upstreamCancellations.get();
		}
	}
}

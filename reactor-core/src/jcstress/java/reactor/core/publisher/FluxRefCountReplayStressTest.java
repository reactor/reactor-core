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
import org.openjdk.jcstress.infra.results.III_Result;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE_INTERESTING;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

public abstract class FluxRefCountReplayStressTest {

	// see https://github.com/reactor/reactor-core/issues/3639
	// Subscribers of a replayed source that emits and completes synchronously keep coming
	// and going, so the source is connected and disconnected over and over. Every
	// subscriber must receive the replayed value, whichever connection it ends up with.
	public abstract static class ReplayBaseStressTest {

		final AtomicInteger values      = new AtomicInteger();
		final AtomicInteger errors      = new AtomicInteger();
		final AtomicInteger completions = new AtomicInteger();

		final Flux<String> shared;

		ReplayBaseStressTest(@Nullable Duration gracePeriod) {
			ConnectableFlux<String> replayed =
					Flux.<String>create(sink -> sink.next("Test").complete()).replay(1);
			this.shared = gracePeriod == null ? replayed.refCount(1) : replayed.refCount(1, gracePeriod);
		}

		// takes the first value and cancels, which disconnects the source
		void takeFirst(int times) {
			for (int i = 0; i < times; i++) {
				shared.next()
				      .subscribe(v -> values.incrementAndGet(), e -> errors.incrementAndGet(),
						      completions::incrementAndGet);
			}
		}

		// consumes until the source terminates, which ends the connection without
		// disconnecting it
		void consumeAll(int times) {
			for (int i = 0; i < times; i++) {
				shared.subscribe(v -> values.incrementAndGet(), e -> errors.incrementAndGet(),
						completions::incrementAndGet);
			}
		}

		void arbiter(III_Result r) {
			r.r1 = values.get();
			r.r2 = errors.get();
			r.r3 = completions.get();
		}
	}

	@JCStressTest
	@Outcome(id = {"8, 0, 8"}, expect = ACCEPTABLE, desc = "every subscriber received the value")
	@Outcome(expect = FORBIDDEN, desc = "values, errors, completions")
	@State
	public static class RefCountTakeFirstStressTest extends ReplayBaseStressTest {

		public RefCountTakeFirstStressTest() {
			super(null);
		}

		@Actor
		public void first() {
			takeFirst(4);
		}

		@Actor
		public void second() {
			takeFirst(4);
		}

		@Arbiter
		public void arbiter(III_Result r) {
			super.arbiter(r);
		}
	}

	@JCStressTest
	@Outcome(id = {"8, 0, 8"}, expect = ACCEPTABLE, desc = "every subscriber received the value")
	@Outcome(expect = FORBIDDEN, desc = "values, errors, completions")
	@State
	public static class RefCountGraceTakeFirstStressTest extends ReplayBaseStressTest {

		// a zero grace period disconnects synchronously, keeping the outcome deterministic
		public RefCountGraceTakeFirstStressTest() {
			super(Duration.ZERO);
		}

		@Actor
		public void first() {
			takeFirst(4);
		}

		@Actor
		public void second() {
			takeFirst(4);
		}

		@Arbiter
		public void arbiter(III_Result r) {
			super.arbiter(r);
		}
	}

	@JCStressTest
	@Outcome(id = {"4, 0, 4"}, expect = ACCEPTABLE, desc = "every subscriber received the value")
	@Outcome(id = {"4, 1, 3", "4, 2, 2"}, expect = ACCEPTABLE_INTERESTING,
			desc = "every subscriber received the value, but a subscriber that attached to the source before it was counted got disconnected")
	@Outcome(expect = FORBIDDEN, desc = "values, errors, completions")
	@State
	public static class RefCountTakeFirstOrConsumeAllStressTest extends ReplayBaseStressTest {

		public RefCountTakeFirstOrConsumeAllStressTest() {
			super(null);
		}

		@Actor
		public void first() {
			takeFirst(2);
		}

		@Actor
		public void second() {
			consumeAll(2);
		}

		@Arbiter
		public void arbiter(III_Result r) {
			super.arbiter(r);
		}
	}

	@JCStressTest
	@Outcome(id = {"4, 0, 4"}, expect = ACCEPTABLE, desc = "every subscriber received the value")
	@Outcome(id = {"4, 1, 3", "4, 2, 2"}, expect = ACCEPTABLE_INTERESTING,
			desc = "every subscriber received the value, but a subscriber that attached to the source before it was counted got disconnected")
	@Outcome(expect = FORBIDDEN, desc = "values, errors, completions")
	@State
	public static class RefCountGraceTakeFirstOrConsumeAllStressTest extends ReplayBaseStressTest {

		// a zero grace period disconnects synchronously, keeping the outcome deterministic
		public RefCountGraceTakeFirstOrConsumeAllStressTest() {
			super(Duration.ZERO);
		}

		@Actor
		public void first() {
			takeFirst(2);
		}

		@Actor
		public void second() {
			consumeAll(2);
		}

		@Arbiter
		public void arbiter(III_Result r) {
			super.arbiter(r);
		}
	}
}

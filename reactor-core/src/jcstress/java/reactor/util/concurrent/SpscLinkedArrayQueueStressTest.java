/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.util.concurrent;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.LLLLLL_Result;
import org.openjdk.jcstress.infra.results.LLL_Result;

public abstract class SpscLinkedArrayQueueStressTest {

	// We initialize the queue to the minimum link size which is 8 and our tests must
	// cover adding more elements than the underlying array can initially contain to
	// evaluate concurrency at the growth boundary.
	private static final int LINK_SIZE = 8;

	@JCStressTest
	@Outcome(id = {"2, v1, v2"}, expect = Expect.ACCEPTABLE, desc = "Pair not consumed")
	@Outcome(id = {"0, v1, v2"}, expect = Expect.ACCEPTABLE, desc = "Pair consumed")
	@State
	public static class BiPredicateAndPollStressTest {

		final SpscLinkedArrayQueue<String> queue = new SpscLinkedArrayQueue<>(LINK_SIZE);

		@Actor
		public void offerPair() {
			queue.test("v1", "v2");
		}

		@Actor
		public void pollPair(LLL_Result r) {
			String v = queue.poll();
			r.r2 = v;
			if (v != null) {
				r.r3 = queue.poll();
			}
		}

		@Arbiter
		public void arbiter(LLL_Result r) {
			int size = queue.size();
			r.r1 = size;
			if (size > 0) {
				r.r2 = queue.poll();
				r.r3 = queue.poll();
			}
		}
	}

	@JCStressTest
	@Outcome(id = {"0, a1, a2, a3, a4, a5, a6, a7, a8, b1, b2"},
			expect = Expect.ACCEPTABLE, desc = "5 pairs consumed")
	@Outcome(id = {"2, a1, a2, a3, a4, a5, a6, a7, a8, b1, b2"},
			expect = Expect.ACCEPTABLE, desc = "4 pairs consumed")
	@Outcome(id = {"4, a1, a2, a3, a4, a5, a6, a7, a8, b1, b2"},
			expect = Expect.ACCEPTABLE, desc = "3 pairs consumed")
	@Outcome(id = {"6, a1, a2, a3, a4, a5, a6, a7, a8, b1, b2"},
			expect = Expect.ACCEPTABLE, desc = "2 pairs consumed")
	@Outcome(id = {"8, a1, a2, a3, a4, a5, a6, a7, a8, b1, b2"},
			expect = Expect.ACCEPTABLE, desc = "1 pair consumed")
	@Outcome(id = {"10, a1, a2, a3, a4, a5, a6, a7, a8, b1, b2"},
			expect = Expect.ACCEPTABLE, desc = "0 pairs consumed")
	@State
	public static class BiPredicateAndPollAtGrowthBoundaryStressTest {

		final SpscLinkedArrayQueue<String> queue = new SpscLinkedArrayQueue<>(8);

		{
			queue.test("p1", "p2");
			queue.test("p3", "p4");
		}

		@Actor
		public void offerPairs() {
			queue.test("a1", "a2");
			queue.test("a3", "a4");
			queue.test("a5", "a6");
			queue.test("a7", "a8");
			queue.test("b1", "b2");
		}

		@Actor
		public void pollPairs(LLLLLL_Result r) {
			// Consume prefix.
			queue.poll();
			queue.poll();
			queue.poll();
			queue.poll();

			String v;

			v = queue.poll();
			if (v != null) {
				r.r2 = v + ", " + queue.poll();
			}
			else {
				return;
			}

			v = queue.poll();
			if (v != null) {
				r.r3 = v + ", " + queue.poll();
			}
			else {
				return;
			}

			v = queue.poll();
			if (v != null) {
				r.r4 = v + ", " + queue.poll();
			}
			else {
				return;
			}

			v = queue.poll();
			if (v != null) {
				r.r5 = v + ", " + queue.poll();
			}
			else {
				return;
			}

			v = queue.poll();
			if (v != null) {
				r.r6 = v + ", " + queue.poll();
			}
		}

		@Arbiter
		public void arbiter(LLLLLL_Result r) {
			int size = queue.size();

			r.r1 = size;
			switch (size) {
				case 10:
					r.r2 = queue.poll() + ", " + queue.poll();
					// fall through to consume the rest
				case 8:
					r.r3 = queue.poll() + ", " + queue.poll();
					// fall through to consume the rest
				case 6:
					r.r4 = queue.poll() + ", " + queue.poll();
					// fall through to consume the rest
				case 4:
					r.r5 = queue.poll() + ", " + queue.poll();
					// fall through to consume the rest
				case 2:
					r.r6 = queue.poll() + ", " + queue.poll();
			}
		}
	}
}

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
import org.openjdk.jcstress.infra.results.LLLLL_Result;
import org.openjdk.jcstress.infra.results.LLLL_Result;
import org.openjdk.jcstress.infra.results.LLL_Result;

public abstract class MpscLinkedQueueStressTest {

	@JCStressTest
	@Outcome(id = {"3, v1, v2, v3"}, expect = Expect.ACCEPTABLE, desc = "Pair, then single")
	@Outcome(id = {"3, v3, v1, v2"}, expect = Expect.ACCEPTABLE, desc = "Single, then pair")
	@State
	public static class BiPredicateAndOfferStressTest {

		final MpscLinkedQueue<String> queue = new MpscLinkedQueue<>();

		@Actor
		public void offerPair() {
			queue.test("v1", "v2");
		}

		@Actor
		public void offerSingle() {
			queue.offer("v3");
		}

		@Arbiter
		public void arbiter(LLLL_Result r) {
			r.r1 = queue.size();
			r.r2 = queue.poll();
			r.r3 = queue.poll();
			r.r4 = queue.poll();
		}
	}

	@JCStressTest
	@Outcome(id = {"4, a1, a2, b1, b2"}, expect = Expect.ACCEPTABLE, desc = "Pair A, then pair B")
	@Outcome(id = {"4, b1, b2, a1, a2"}, expect = Expect.ACCEPTABLE, desc = "Pair B, then pair A")
	@State
	public static class BiPredicateTwoActorsStressTest {

		final MpscLinkedQueue<String> queue = new MpscLinkedQueue<>();

		@Actor
		public void offerPairA() {
			queue.test("a1", "a2");
		}

		@Actor
		public void offerPairB() {
			queue.test("b1", "b2");
		}

		@Arbiter
		public void arbiter(LLLLL_Result r) {
			r.r1 = queue.size();
			r.r2 = queue.poll();
			r.r3 = queue.poll();
			r.r4 = queue.poll();
			r.r5 = queue.poll();
		}
	}

	@JCStressTest
	@Outcome(id = {"0, v1, v2"}, expect = Expect.ACCEPTABLE, desc = "Value consumed")
	@Outcome(id = {"2, v1, v2"}, expect = Expect.ACCEPTABLE, desc = "Value not consumed")
	@State
	public static class BiPredicateAndPollStressTest {

		final MpscLinkedQueue<String> queue = new MpscLinkedQueue<>();

		@Actor
		public void offerPair() {
			queue.test("v1", "v2");
		}

		@Actor
		public void pollPair(LLL_Result r) {
			String v = queue.poll();
			if (null != v) {
				r.r2 = v;
				r.r3 = queue.poll();
			}
		}

		@Arbiter
		public void arbiter(LLL_Result r) {
			int size = queue.size();
			r.r1 = size;
			if (size != 0) {
				r.r2 = queue.poll();
				r.r3 = queue.poll();
			}
		}
	}

	@JCStressTest
	@Outcome(id = {"4, a1, a2, b1, b2"}, expect = Expect.ACCEPTABLE,
			desc = "Consumed empty queue, pair A, then pair B present")
	@Outcome(id = {"4, b1, b2, a1, a2"}, expect = Expect.ACCEPTABLE,
			desc = "Consumed empty queue, pair B, then pair A present")
	@Outcome(id = {"2, a1, a2, b1, b2"}, expect = Expect.ACCEPTABLE,
			desc = "Consumed pair A, then pair B left")
	@Outcome(id = {"2, b1, b2, a1, a2"}, expect = Expect.ACCEPTABLE,
			desc = "Consumed pair B, then pair A left")
	@State
	public static class BiPredicateTwoProducersOneConsumerStressTest {

		final MpscLinkedQueue<String> queue = new MpscLinkedQueue<>();

		@Actor
		public void offerPairA() {
			queue.test("a1", "a2");
		}

		@Actor
		public void offerPairB() {
			queue.test("b1", "b2");
		}

		@Actor
		public void pollPair(LLLLL_Result r) {
			String item = queue.poll();

			if (null != item) {
				r.r2 = item;
				r.r3 = queue.poll();
			}
		}

		@Arbiter
		public void arbiter(LLLLL_Result r) {
			r.r1 = queue.size();
			if (queue.size() > 2) {
				r.r2 = queue.poll();
				r.r3 = queue.poll();
			}
			r.r4 = queue.poll();
			r.r5 = queue.poll();
		}
	}
}

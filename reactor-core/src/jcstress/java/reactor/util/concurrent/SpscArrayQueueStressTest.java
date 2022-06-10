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

public abstract class SpscArrayQueueStressTest {

	@JCStressTest
	@Outcome(id = {"5, 1, 2, 3, 4, 5"}, expect = Expect.ACCEPTABLE, desc = "None consumed, all in order")
	@Outcome(id = {"4, 1, 2, 3, 4, 5"}, expect = Expect.ACCEPTABLE, desc = "1 consumed, all in order")
	@Outcome(id = {"3, 1, 2, 3, 4, 5"}, expect = Expect.ACCEPTABLE, desc = "2 consumed, all in order")
	@Outcome(id = {"2, 1, 2, 3, 4, 5"}, expect = Expect.ACCEPTABLE, desc = "3 consumed, all in order")
	@Outcome(id = {"1, 1, 2, 3, 4, 5"}, expect = Expect.ACCEPTABLE, desc = "4 consumed, all in order")
	@Outcome(id = {"0, 1, 2, 3, 4, 5"}, expect = Expect.ACCEPTABLE, desc = "All consumed, all in order")
	@State
	public static class OfferAndPollStressTest {

		final SpscArrayQueue<Integer> queue = new SpscArrayQueue<>(8);

		@Actor
		public void offer() {
			queue.offer(1);
			queue.offer(2);
			queue.offer(3);
			queue.offer(4);
			queue.offer(5);
		}

		@Actor
		public void poll(LLLLLL_Result r) {
			int elementNum = 0;
			for (int i = 0; i < 5; i++) {
				Integer e = queue.poll();
				if (e != null) {
					switch (++elementNum) {
						case 1:
							r.r2 = e;
							break;
						case 2:
							r.r3 = e;
							break;
						case 3:
							r.r4 = e;
							break;
						case 4:
							r.r5 = e;
							break;
						case 5:
							r.r6 = e;
							break;
					}
				}
			}
		}

		@Arbiter
		public void arbiter(LLLLLL_Result r) {
			int size = queue.size();
			r.r1 = size;

			int elementNum = 5 - size;
			for (int i = 0; i < size; i++) {
				Integer e = queue.poll();
				switch (++elementNum) {
					case 1:
						r.r2 = e;
						break;
					case 2:
						r.r3 = e;
						break;
					case 3:
						r.r4 = e;
						break;
					case 4:
						r.r5 = e;
						break;
					case 5:
						r.r6 = e;
						break;
					default:
						break;
				}
			}
		}
	}

}

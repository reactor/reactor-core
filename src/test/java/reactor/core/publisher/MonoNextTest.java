/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import org.junit.Test;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;

public class MonoNextTest {

	@Test
	public void normal() {
		Flux.range(1, 1_000_000)
		    .next()
		    .subscribeWith(AssertSubscriber.create())
		    .assertValues(1)
		    .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);
		Flux.range(1, 1_000_000)
		    .next()
		    .subscribeWith(ts)
		    .assertNoValues()
		    .assertNotComplete();

		ts.request(1);
		ts.assertValues(1)
		  .assertComplete();
	}

	@Test
	public void cancel() {
		TestPublisher<String> cancelTester = TestPublisher.create();

		MonoProcessor<String> processor = cancelTester.flux()
		                                              .next()
		                                              .toProcessor();
		processor.subscribe();
		processor.cancel();

		cancelTester.assertCancelled();
	}
}
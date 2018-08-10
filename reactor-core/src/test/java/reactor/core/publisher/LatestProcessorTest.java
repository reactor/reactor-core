/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
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

public class LatestProcessorTest {

	@Test
	public void earlySubscriberThenValue() {
		AssertSubscriber<String> ts = AssertSubscriber.create();

		LatestProcessor<String> processor = new LatestProcessor<>();
		processor.subscribe(ts);

		Flux.just("A", "B").subscribe(processor);

		ts.assertValues("A");
		ts.assertComplete();
	}

	@Test
	public void earlySubscriberThenValueBounded() {
		AssertSubscriber<String> ts = AssertSubscriber.create(0);

		LatestProcessor<String> processor = new LatestProcessor<>();
		processor.subscribe(ts);

		Flux.just("A", "B").subscribe(processor);

		ts.assertNoValues();
		ts.assertNotComplete();

		ts.request(2);

		ts.assertValues("A");
		ts.assertComplete();
	}

	@Test
	public void earlySubscriberThenCompletion() {
		AssertSubscriber<String> ts = AssertSubscriber.create();

		LatestProcessor<String> processor = new LatestProcessor<>();
		processor.subscribe(ts);

		Flux.<String>empty().subscribe(processor);

		ts.assertNoValues();
		ts.assertComplete();
	}

	@Test
	public void earlySubscriberThenCompletionWithNoRequest() {
		AssertSubscriber<String> ts = AssertSubscriber.create(0);

		LatestProcessor<String> processor = new LatestProcessor<>();
		processor.subscribe(ts);

		Flux.<String>empty().subscribe(processor);

		ts.assertNoValues();
		ts.assertComplete();
	}

	@Test
	public void earlySubscriberThenError() {
		AssertSubscriber<String> ts = AssertSubscriber.create();

		LatestProcessor<String> processor = new LatestProcessor<>();
		processor.subscribe(ts);

		Flux.<String>error(new IllegalStateException("boom")).subscribe(processor);

		ts.assertNoValues();
		ts.assertErrorMessage("boom");
	}

	@Test
	public void earlySubscriberThenErrorWithZeroRequest() {
		AssertSubscriber<String> ts = AssertSubscriber.create(0);

		LatestProcessor<String> processor = new LatestProcessor<>();
		processor.subscribe(ts);

		Flux.<String>error(new IllegalStateException("boom")).subscribe(processor);

		ts.assertNoValues();
		ts.assertErrorMessage("boom");
	}

	@Test
	public void lateSubscriberValue() {
		AssertSubscriber<String> ts = AssertSubscriber.create();

		LatestProcessor<String> processor = new LatestProcessor<>();
		Flux.just("A", "B").subscribe(processor);

		processor.subscribe(ts);

		ts.assertValues("B");
		ts.assertComplete();
	}

	@Test
	public void lateSubscriberValueBounded() {
		AssertSubscriber<String> ts = AssertSubscriber.create(0);

		LatestProcessor<String> processor = new LatestProcessor<>();

		Flux.just("A", "B").subscribe(processor);

		processor.subscribe(ts);

		ts.assertNoValues();
		ts.assertNotComplete();

		ts.request(2);

		ts.assertValues("B");
		ts.assertComplete();
	}

	@Test
	public void progressiveSubscribersFluxSource() {
		TestPublisher<String> testPublisher = TestPublisher.create();
		AssertSubscriber<String> sub1 = AssertSubscriber.create();
		AssertSubscriber<String> sub2 = AssertSubscriber.create();
		AssertSubscriber<String> sub3 = AssertSubscriber.create();
		AssertSubscriber<String> sub4 = AssertSubscriber.create();
		AssertSubscriber<String> sub5 = AssertSubscriber.create();

		LatestProcessor<String> processor = new LatestProcessor<>();
		processor.subscribe(sub1);
		testPublisher.subscribe(processor);

		sub1.assertNoValues().assertNotTerminated();
		sub2.assertNoValues().assertNotTerminated();
		sub3.assertNoValues().assertNotTerminated();
		sub4.assertNoValues().assertNotTerminated();
		sub5.assertNoValues().assertNotTerminated();

		testPublisher.next("A");
		processor.subscribe(sub2);
		//sub1 already subscribed
		sub1.assertValues("A").assertComplete();
		sub2.assertValues("A").assertComplete();

		testPublisher.next("B");
		processor.subscribe(sub3);
		sub3.assertValues("B").assertComplete();

		testPublisher.next("C");
		processor.subscribe(sub4);
		sub4.assertValues("C").assertComplete();

		testPublisher.complete();
		processor.subscribe(sub5);
		sub5.assertValues("C").assertComplete();
	}

	@Test
	public void progressiveSubscribersFluxSourceBounded() {
		TestPublisher<String> testPublisher = TestPublisher.create();
		AssertSubscriber<String> sub1 = AssertSubscriber.create(0);
		AssertSubscriber<String> sub2 = AssertSubscriber.create(0);
		AssertSubscriber<String> sub3 = AssertSubscriber.create(0);
		AssertSubscriber<String> sub4 = AssertSubscriber.create(0);
		AssertSubscriber<String> sub5 = AssertSubscriber.create(0);

		LatestProcessor<String> processor = new LatestProcessor<>();
		processor.subscribe(sub1);
		testPublisher.subscribe(processor);

		sub1.assertNoValues().assertNotTerminated();
		sub2.assertNoValues().assertNotTerminated();
		sub3.assertNoValues().assertNotTerminated();
		sub4.assertNoValues().assertNotTerminated();
		sub5.assertNoValues().assertNotTerminated();

		testPublisher.next("A");
		processor.subscribe(sub2);
		//sub1 already subscribed
		testPublisher.next("B");
		processor.subscribe(sub3);
		testPublisher.next("C");
		processor.subscribe(sub4);
		testPublisher.complete();
		processor.subscribe(sub5);

		sub1.assertNoValues().assertNotTerminated();
		sub2.assertNoValues().assertNotTerminated();
		sub3.assertNoValues().assertNotTerminated();
		sub4.assertNoValues().assertNotTerminated();
		sub5.assertNoValues().assertNotTerminated();

		sub1.request(1);
		sub2.request(2);
		sub3.request(3);
		sub4.request(4);
		sub5.request(5);

		sub1.assertValues("A").assertComplete();
		sub2.assertValues("A").assertComplete();
		sub3.assertValues("B").assertComplete();
		sub4.assertValues("C").assertComplete();
		sub5.assertValues("C").assertComplete();
	}

	@Test
	public void progressiveSubscribersFluxSourceEndError() {
		TestPublisher<String> testPublisher = TestPublisher.create();
		AssertSubscriber<String> sub1 = AssertSubscriber.create();
		AssertSubscriber<String> sub2 = AssertSubscriber.create();
		AssertSubscriber<String> sub3 = AssertSubscriber.create();
		AssertSubscriber<String> sub4 = AssertSubscriber.create();
		AssertSubscriber<String> sub5 = AssertSubscriber.create();

		LatestProcessor<String> processor = new LatestProcessor<>();
		processor.subscribe(sub1);
		testPublisher.subscribe(processor);

		sub1.assertNoValues().assertNotTerminated();
		sub2.assertNoValues().assertNotTerminated();
		sub3.assertNoValues().assertNotTerminated();
		sub4.assertNoValues().assertNotTerminated();
		sub5.assertNoValues().assertNotTerminated();

		testPublisher.next("A");
		processor.subscribe(sub2);
		//sub1 already subscribed
		sub1.assertValues("A").assertComplete();
		sub2.assertValues("A").assertComplete();

		testPublisher.next("B");
		processor.subscribe(sub3);
		sub3.assertValues("B").assertComplete();

		testPublisher.next("C");
		processor.subscribe(sub4);
		sub4.assertValues("C").assertComplete();

		testPublisher.error(new IllegalStateException("boom"));
		processor.subscribe(sub5);
		sub5.assertNoValues().assertErrorMessage("boom");
	}

	@Test
	public void progressiveSubscribersFluxSourceEndErrorBounded() {
		TestPublisher<String> testPublisher = TestPublisher.create();
		AssertSubscriber<String> sub1 = AssertSubscriber.create(0);
		AssertSubscriber<String> sub2 = AssertSubscriber.create(0);
		AssertSubscriber<String> sub3 = AssertSubscriber.create(0);
		AssertSubscriber<String> sub4 = AssertSubscriber.create(0);
		AssertSubscriber<String> sub5 = AssertSubscriber.create(0);

		LatestProcessor<String> processor = new LatestProcessor<>();
		processor.subscribe(sub1);
		testPublisher.subscribe(processor);

		sub1.assertNoValues().assertNotTerminated();
		sub2.assertNoValues().assertNotTerminated();
		sub3.assertNoValues().assertNotTerminated();
		sub4.assertNoValues().assertNotTerminated();
		sub5.assertNoValues().assertNotTerminated();

		testPublisher.next("A");
		processor.subscribe(sub2);
		//sub1 already subscribed
		testPublisher.next("B");
		processor.subscribe(sub3);
		testPublisher.next("C");
		processor.subscribe(sub4);
		testPublisher.error(new IllegalStateException("boom"));
		processor.subscribe(sub5);

		sub1.assertNoValues().assertNotTerminated();
		sub2.assertNoValues().assertNotTerminated();
		sub3.assertNoValues().assertNotTerminated();
		sub4.assertNoValues().assertNotTerminated();
		sub5.assertNoValues().assertErrorMessage("boom");

		sub1.request(1);
		sub2.request(2);
		sub3.request(3);
		sub4.request(4);
		sub5.request(1);

		sub1.assertValues("A").assertComplete();
		sub2.assertValues("A").assertComplete();
		sub3.assertValues("B").assertComplete();
		sub4.assertValues("C").assertComplete();
	}

	//TODO add more race condition tests

}
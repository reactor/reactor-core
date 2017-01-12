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

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.QueueSupplier;

public class FluxConcatMapTest {

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .concatMap(v -> Flux.range(v, 2))
		    .subscribe(ts);

		ts.assertValues(1, 2, 2, 3)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normal2() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .hide()
		    .concatMap(v -> Flux.range(v, 2))
		    .subscribe(ts);

		ts.assertValues(1, 2, 2, 3)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBoundary() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .concatMapDelayError(v -> Flux.range(v, 2))
		    .subscribe(ts);

		ts.assertValues(1, 2, 2, 3)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBoundary2() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .hide()
		    .concatMapDelayError(v -> Flux.range(v, 2))
		    .subscribe(ts);

		ts.assertValues(1, 2, 2, 3)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalLongRun() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1000)
		    .concatMap(v -> Flux.range(v, 1000))
		    .subscribe(ts);

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalLongRunJust() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1000_000)
		    .concatMap(v -> Flux.just(v))
		    .subscribe(ts);

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalLongRun2() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1000)
		    .hide()
		    .concatMap(v -> Flux.range(v, 1000))
		    .subscribe(ts);

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalLongRunBoundary() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1000)
		    .concatMapDelayError(v -> Flux.range(v, 1000))
		    .subscribe(ts);

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalLongRunJustBoundary() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1000_000)
		    .concatMapDelayError(v -> Flux.just(v))
		    .subscribe(ts);

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalLongRunBoundary2() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 1000)
		    .hide()
		    .concatMapDelayError(v -> Flux.range(v, 1000))
		    .subscribe(ts);

		ts.assertValueCount(1_000_000)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void singleSubscriberOnly() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		DirectProcessor<Integer> source = DirectProcessor.create();

		DirectProcessor<Integer> source1 = DirectProcessor.create();
		DirectProcessor<Integer> source2 = DirectProcessor.create();

		source.concatMap(v -> v == 1 ? source1 : source2)
		      .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		source.onNext(1);
		source.onNext(2);

		Assert.assertTrue("source1 no subscribers?", source1.hasDownstreams());
		Assert.assertFalse("source2 has subscribers?", source2.hasDownstreams());

		source1.onNext(1);
		source2.onNext(10);

		source1.onComplete();
		source.onComplete();

		source2.onNext(2);
		source2.onComplete();

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void singleSubscriberOnlyBoundary() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		DirectProcessor<Integer> source = DirectProcessor.create();

		DirectProcessor<Integer> source1 = DirectProcessor.create();
		DirectProcessor<Integer> source2 = DirectProcessor.create();

		source.concatMapDelayError(v -> v == 1 ? source1 : source2)
		      .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		source.onNext(1);

		Assert.assertTrue("source1 no subscribers?", source1.hasDownstreams());
		Assert.assertFalse("source2 has subscribers?", source2.hasDownstreams());

		source1.onNext(1);
		source2.onNext(10);

		source1.onComplete();
		source.onNext(2);
		source.onComplete();

		source2.onNext(2);
		source2.onComplete();

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertComplete();

		Assert.assertFalse("source1 has subscribers?", source1.hasDownstreams());
		Assert.assertFalse("source2 has subscribers?", source2.hasDownstreams());
	}

	@Test
	public void mainErrorsImmediate() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		DirectProcessor<Integer> source = DirectProcessor.create();

		DirectProcessor<Integer> source1 = DirectProcessor.create();
		DirectProcessor<Integer> source2 = DirectProcessor.create();

		source.concatMap(v -> v == 1 ? source1 : source2)
		      .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		source.onNext(1);

		Assert.assertTrue("source1 no subscribers?", source1.hasDownstreams());
		Assert.assertFalse("source2 has subscribers?", source2.hasDownstreams());

		source1.onNext(1);

		source.onError(new RuntimeException("forced failure"));

		ts.assertValues(1)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		Assert.assertFalse("source1 has subscribers?", source1.hasDownstreams());
		Assert.assertFalse("source2 has subscribers?", source2.hasDownstreams());
	}

	@Test
	public void mainErrorsBoundary() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		DirectProcessor<Integer> source = DirectProcessor.create();

		DirectProcessor<Integer> source1 = DirectProcessor.create();
		DirectProcessor<Integer> source2 = DirectProcessor.create();

		source.concatMapDelayError(v -> v == 1 ? source1 : source2)
		      .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		source.onNext(1);

		Assert.assertTrue("source1 no subscribers?", source1.hasDownstreams());
		Assert.assertFalse("source2 has subscribers?", source2.hasDownstreams());

		source1.onNext(1);

		source.onError(new RuntimeException("forced failure"));

		ts.assertValues(1)
		  .assertNoError()
		  .assertNotComplete();

		source1.onNext(2);
		source1.onComplete();

		ts.assertValues(1, 2)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		Assert.assertFalse("source1 has subscribers?", source1.hasDownstreams());
		Assert.assertFalse("source2 has subscribers?", source2.hasDownstreams());
	}

	@Test
	public void innerErrorsImmediate() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		DirectProcessor<Integer> source = DirectProcessor.create();

		DirectProcessor<Integer> source1 = DirectProcessor.create();
		DirectProcessor<Integer> source2 = DirectProcessor.create();

		source.concatMap(v -> v == 1 ? source1 : source2)
		      .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		source.onNext(1);

		Assert.assertTrue("source1 no subscribers?", source1.hasDownstreams());
		Assert.assertFalse("source2 has subscribers?", source2.hasDownstreams());

		source1.onNext(1);

		source1.onError(new RuntimeException("forced failure"));

		ts.assertValues(1)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		Assert.assertFalse("source1 has subscribers?", source1.hasDownstreams());
		Assert.assertFalse("source2 has subscribers?", source2.hasDownstreams());
	}

	@Test
	public void innerErrorsBoundary() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		DirectProcessor<Integer> source = DirectProcessor.create();

		DirectProcessor<Integer> source1 = DirectProcessor.create();
		DirectProcessor<Integer> source2 = DirectProcessor.create();

		source.concatMapDelayError(v -> v == 1 ? source1 : source2)
		      .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		source.onNext(1);

		Assert.assertTrue("source1 no subscribers?", source1.hasDownstreams());
		Assert.assertFalse("source2 has subscribers?", source2.hasDownstreams());

		source1.onNext(1);

		source1.onError(new RuntimeException("forced failure"));

		ts.assertValues(1)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		Assert.assertFalse("source1 has subscribers?", source1.hasDownstreams());
		Assert.assertFalse("source2 has subscribers?", source2.hasDownstreams());
	}

	@Test
	public void innerErrorsEnd() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		DirectProcessor<Integer> source = DirectProcessor.create();

		DirectProcessor<Integer> source1 = DirectProcessor.create();
		DirectProcessor<Integer> source2 = DirectProcessor.create();

		source.concatMapDelayError(v -> v == 1 ? source1 : source2, true, 32)
		      .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		source.onNext(1);

		Assert.assertTrue("source1 no subscribers?", source1.hasDownstreams());
		Assert.assertFalse("source2 has subscribers?", source2.hasDownstreams());

		source1.onNext(1);

		source1.onError(new RuntimeException("forced failure"));

		source.onNext(2);

		Assert.assertTrue("source2 no subscribers?", source2.hasDownstreams());

		source2.onNext(2);
		source2.onComplete();

		source.onComplete();

		ts.assertValues(1, 2)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		Assert.assertFalse("source1 has subscribers?", source1.hasDownstreams());
		Assert.assertFalse("source2 has subscribers?", source2.hasDownstreams());
	}

	@Test
	public void syncFusionMapToNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .map(v -> v == 2 ? null : v)
		    .concatMap(Flux::just)
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void syncFusionMapToNullFilter() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .map(v -> v == 2 ? null : v)
		    .filter(v -> true)
		    .concatMap(Flux::just)
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void asyncFusionMapToNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		UnicastProcessor<Integer> up = UnicastProcessor.create(QueueSupplier.<Integer>get(2).get());
		up.onNext(1);
		up.onNext(2);
		up.onComplete();

		up.map(v -> v == 2 ? null : v)
		  .concatMap(Flux::just)
		  .subscribe(ts);

		ts.assertValues(1)
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void asyncFusionMapToNullFilter() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		UnicastProcessor<Integer> up =
				UnicastProcessor.create(QueueSupplier.<Integer>get(2).get());
		up.onNext(1);
		up.onNext(2);
		up.onComplete();

		up.map(v -> v == 2 ? null : v)
		  .filter(v -> true)
		  .concatMap(Flux::just)
		  .subscribe(ts);

		ts.assertValues(1)
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void scalarAndRangeBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		@SuppressWarnings("unchecked") Publisher<Integer>[] sources =
				new Publisher[]{Flux.just(1), Flux.range(2, 3)};

		Flux.range(0, 2)
		    .concatMap(v -> sources[v])
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void allEmptyBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(0, 10)
		    .hide()
		    .concatMap(v -> Flux.<Integer>empty(), 2)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void publisherOfPublisher() {
		StepVerifier.create(Flux.concat(Flux.just(Flux.just(1, 2), Flux.just(3, 4))))
		            .expectNext(1, 2, 3, 4)
		            .verifyComplete();
	}

	@Test
	public void publisherOfPublisherDelay() {
		StepVerifier.create(Flux.concatDelayError(Flux.just(Flux.just(1, 2),
				Flux.just(3, 4))))
		            .expectNext(1, 2, 3, 4)
		            .verifyComplete();
	}

	@Test
	public void publisherOfPublisherDelayError() {
		StepVerifier.create(Flux.concatDelayError(Flux.just(Flux.just(1, 2).concatWith(Flux.error(new Exception("test"))),
				Flux.just(3, 4))))
		            .expectNext(1, 2, 3, 4)
		            .verifyErrorMessage("test");
	}

	@Test
	public void publisherOfPublisherDelayError2() {
		StepVerifier.create(Flux.just(Flux.just(1, 2)
		                                  .concatWith(Flux.error(new Exception("test"))),
				Flux.just(3, 4))
		                        .concatMap(f -> f))
		            .expectNext(1, 2)
		            .verifyErrorMessage("test");
	}

	@Test
	public void publisherOfPublisherDelayEnd3() {
		StepVerifier.create(Flux.just(Flux.just(1, 2)
		                                  .concatWith(Flux.error(new Exception("test"))),
				Flux.just(3, 4))
		                        .concatMapDelayError(f -> f, true, 128))
		            .expectNext(1, 2, 3, 4)
		            .verifyErrorMessage("test");
	}

	@Test
	public void publisherOfPublisherDelayEndNot3() {
		StepVerifier.create(Flux.just(Flux.just(1, 2)
		                                  .concatWith(Flux.error(new Exception("test"))),
				Flux.just(3, 4))
		                        .concatMapDelayError(f -> f, false, 128))
		            .expectNext(1, 2)
		            .verifyErrorMessage("test");
	}

	@Test
	public void publisherOfPublisherDelayEnd() {
		StepVerifier.create(Flux.concatDelayError(Flux.just(Flux.just(1, 2),
				Flux.just(3, 4)), false, 128))
		            .expectNext(1, 2, 3, 4)
		            .verifyComplete();
	}

	@Test
	public void publisherOfPublisherDelayEndError() {
		StepVerifier.create(Flux.concatDelayError(Flux.just(Flux.just(1, 2)
		                                                        .concatWith(Flux.error(new Exception(
				                                                        "test"))),
				Flux.just(3, 4)), false, 128))
		            .expectNext(1, 2)
		            .verifyErrorMessage("test");
	}

	@Test
	public void publisherOfPublisherDelayEnd2() {
		StepVerifier.create(Flux.concatDelayError(Flux.just(Flux.just(1, 2),
				Flux.just(3, 4)), true, 128))
		            .expectNext(1, 2, 3, 4)
		            .verifyComplete();
	}

	@Test
	public void publisherOfPublisherDelayEndError2() {
		StepVerifier.create(Flux.concatDelayError(Flux.just(Flux.just(1, 2)
		                                                        .concatWith(Flux.error(new Exception(
				                                                        "test"))),
				Flux.just(3, 4)), true, 128))
		            .expectNext(1, 2, 3, 4)
		            .verifyErrorMessage("test");
	}

}

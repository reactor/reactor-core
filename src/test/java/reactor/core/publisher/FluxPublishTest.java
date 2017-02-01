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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.QueueSupplier;

public class FluxPublishTest extends AbstractFluxOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.prefetch(QueueSupplier.SMALL_BUFFER_SIZE);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_threeNextAndComplete() {
		return Arrays.asList(
				scenario(f -> f.publish().autoConnect()),

				scenario(f -> f.publish().refCount())
		);
	}

	@Test(expected = IllegalArgumentException.class)
	public void failPrefetch(){
		Flux.never()
		    .publish( -1);
	}

	/*@Test
	public void constructors() {
		ConstructorTestBuilder ctb = new ConstructorTestBuilder(StreamPublish.class);
		
		ctb.addRef("source", Flux.never());
		ctb.addInt("prefetch", 1, Integer.MAX_VALUE);
		ctb.addRef("queueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
		
		ctb.test();
	}*/
	
	@Test
	public void normal() {
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();

		ConnectableFlux<Integer> p = Flux.range(1, 5).publish();
		
		p.subscribe(ts1);
		p.subscribe(ts2);
		
		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();
		
		p.connect();
		
		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

		ts2.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();
	}
	
	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create(0);
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create(0);

		ConnectableFlux<Integer> p = Flux.range(1, 5).publish();
		
		p.subscribe(ts1);
		p.subscribe(ts2);
		
		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();
		
		p.connect();

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts1.request(3);
		ts2.request(2);
		
		ts1.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();

		ts2.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();
		
		ts1.request(2);
		ts2.request(3);

		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

		ts2.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void normalAsyncFused() {
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();
		
		UnicastProcessor<Integer> up = UnicastProcessor.create(QueueSupplier.<Integer>get(8).get());
		up.onNext(1);
		up.onNext(2);
		up.onNext(3);
		up.onNext(4);
		up.onNext(5);
		up.onComplete();

		ConnectableFlux<Integer> p = up.publish();
		
		p.subscribe(ts1);
		p.subscribe(ts2);
		
		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();
		
		p.connect();
		
		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

		ts2.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();
	}
	
	@Test
	public void normalBackpressuredAsyncFused() {
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create(0);
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create(0);

		UnicastProcessor<Integer> up = UnicastProcessor.create(QueueSupplier.<Integer>get(8).get());
		up.onNext(1);
		up.onNext(2);
		up.onNext(3);
		up.onNext(4);
		up.onNext(5);
		up.onComplete();

		ConnectableFlux<Integer> p = up.publish();
		
		p.subscribe(ts1);
		p.subscribe(ts2);
		
		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();
		
		p.connect();

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts1.request(3);
		ts2.request(2);
		
		ts1.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();

		ts2.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();
		
		ts1.request(2);
		ts2.request(3);

		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

		ts2.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void normalHidden() {
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create();
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create();

		ConnectableFlux<Integer> p = Flux.range(1, 5).publish(5);
		
		p.subscribe(ts1);
		p.subscribe(ts2);
		
		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();
		
		p.connect();
		
		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

		ts2.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();
	}
	
	@Test
	public void normalHiddenBackpressured() {
		AssertSubscriber<Integer> ts1 = AssertSubscriber.create(0);
		AssertSubscriber<Integer> ts2 = AssertSubscriber.create(0);

		ConnectableFlux<Integer> p = Flux.range(1, 5).publish(5);
		
		p.subscribe(ts1);
		p.subscribe(ts2);
		
		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();
		
		p.connect();

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts2
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts1.request(3);
		ts2.request(2);
		
		ts1.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();

		ts2.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();
		
		ts1.request(2);
		ts2.request(3);

		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

		ts2.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void disconnect() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		EmitterProcessor<Integer> e = EmitterProcessor.create();
		e.connect();

		ConnectableFlux<Integer> p = e.publish();
		
		p.subscribe(ts);

		Disposable r = p.connect();
				
		e.onNext(1);
		e.onNext(2);
		
		r.dispose();
		
		ts.assertValues(1, 2)
		.assertError(CancellationException.class)
		.assertNotComplete();
		
		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);
	}

	@Test
	public void disconnectBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		EmitterProcessor<Integer> e = EmitterProcessor.create();
		e.connect();

		ConnectableFlux<Integer> p = e.publish();
		
		p.subscribe(ts);

		Disposable r = p.connect();
				
		r.dispose();
		
		ts.assertNoValues()
		.assertError(CancellationException.class)
		.assertNotComplete();

		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);
	}

	@Test
	public void error() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		EmitterProcessor<Integer> e = EmitterProcessor.create();
		e.connect();

		ConnectableFlux<Integer> p = e.publish();
		
		p.subscribe(ts);
		
		p.connect();
				
		e.onNext(1);
		e.onNext(2);
		e.onError(new RuntimeException("forced failure"));
		
		ts.assertValues(1, 2)
		.assertError(RuntimeException.class)
		  .assertErrorWith( x -> Assert.assertTrue(x.getMessage().contains("forced failure")))
		.assertNotComplete();
	}

	@Test
	public void fusedMapInvalid() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		ConnectableFlux<Integer> p = Flux.range(1, 5).map(v -> (Integer)null).publish();
		
		p.subscribe(ts);
		
		p.connect();
				
		ts.assertNoValues()
		.assertError(NullPointerException.class)
		.assertNotComplete();
	}

}

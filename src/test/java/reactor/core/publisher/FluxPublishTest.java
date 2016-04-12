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
import reactor.core.flow.Cancellation;
import reactor.core.queue.QueueSupplier;
import reactor.core.test.TestSubscriber;
import reactor.core.util.Exceptions;

public class FluxPublishTest {

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
		TestSubscriber<Integer> ts1 = new TestSubscriber<>();
		TestSubscriber<Integer> ts2 = new TestSubscriber<>();

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
		TestSubscriber<Integer> ts1 = new TestSubscriber<>(0);
		TestSubscriber<Integer> ts2 = new TestSubscriber<>(0);

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
		TestSubscriber<Integer> ts1 = new TestSubscriber<>();
		TestSubscriber<Integer> ts2 = new TestSubscriber<>();
		
		UnicastProcessor<Integer> up = new UnicastProcessor<>(QueueSupplier.<Integer>get(8).get());
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
		TestSubscriber<Integer> ts1 = new TestSubscriber<>(0);
		TestSubscriber<Integer> ts2 = new TestSubscriber<>(0);

		UnicastProcessor<Integer> up = new UnicastProcessor<>(QueueSupplier.<Integer>get(8).get());
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
		TestSubscriber<Integer> ts1 = new TestSubscriber<>();
		TestSubscriber<Integer> ts2 = new TestSubscriber<>();

		ConnectableFlux<Integer> p = Flux.range(1, 5).useCapacity(5).publish();
		
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
		TestSubscriber<Integer> ts1 = new TestSubscriber<>(0);
		TestSubscriber<Integer> ts2 = new TestSubscriber<>(0);

		ConnectableFlux<Integer> p = Flux.range(1, 5).useCapacity(5).publish();
		
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
		TestSubscriber<Integer> ts = new TestSubscriber<>();

		EmitterProcessor<Integer> e = EmitterProcessor.create();
		e.connect();

		ConnectableFlux<Integer> p = e.publish();
		
		p.subscribe(ts);

		Cancellation r = p.connect();
				
		e.onNext(1);
		e.onNext(2);
		
		r.dispose();
		
		ts.assertValues(1, 2)
		.assertError(Exceptions.CancelException.class)
		.assertNotComplete();
		
		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);
	}

	@Test
	public void disconnectBackpressured() {
		TestSubscriber<Integer> ts = new TestSubscriber<>(0);

		EmitterProcessor<Integer> e = EmitterProcessor.create();
		e.connect();

		ConnectableFlux<Integer> p = e.publish();
		
		p.subscribe(ts);

		Cancellation r = p.connect();
				
		r.dispose();
		
		ts.assertNoValues()
		.assertError(Exceptions.CancelException.class)
		.assertNotComplete();

		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);
	}

	@Test
	public void error() {
		TestSubscriber<Integer> ts = new TestSubscriber<>();

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
		TestSubscriber<Integer> ts = new TestSubscriber<>();

		ConnectableFlux<Integer> p = Flux.range(1, 5).map(v -> (Integer)null).publish();
		
		p.subscribe(ts);
		
		p.connect();
				
		ts.assertNoValues()
		.assertError(NullPointerException.class)
		.assertNotComplete();
	}

}

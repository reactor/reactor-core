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

import java.util.concurrent.CancellationException;

import org.junit.Test;
import reactor.core.queue.QueueSupplier;
import reactor.core.test.TestSubscriber;

public class FluxMulticastTest {

	@Test
	public void normal() {
		TestSubscriber<Integer> ts1 = new TestSubscriber<>();
		TestSubscriber<Integer> ts2 = new TestSubscriber<>();
		
		ConnectableFlux<Integer> p = Flux.range(1, 5).multicast(EmitterProcessor.create(), Flux::log);
		
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
		
		ConnectableFlux<Integer> p = Flux.range(1, 5).multicast(EmitterProcessor.create());
		
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
		
		ts1.assertValues(1, 2, 3)
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
	public void normalProcessorBackpressured() {
		TestSubscriber<Integer> ts1 = new TestSubscriber<>(0);

		ConnectableFlux<Integer> p = Flux.range(1, 5).multicast(EmitterProcessor.<Integer>create());

		p.subscribe(ts1);

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		p.connect();

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts1.request(3);

		ts1.assertValues(1, 2, 3)
		.assertNoError()
		.assertNotComplete();

		ts1.request(2);

		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

	}


	@Test
	public void normalAsyncFused() {
		TestSubscriber<Integer> ts1 = new TestSubscriber<>();

		UnicastProcessor<Integer> up = new UnicastProcessor<>(QueueSupplier.<Integer>get(8).get());
		up.onNext(1);
		up.onNext(2);
		up.onNext(3);
		up.onNext(4);
		up.onNext(5);
		up.onComplete();
		
		ConnectableFlux<Integer> p = up.multicast(EmitterProcessor.<Integer>create());
		
		p.subscribe(ts1);

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();


		p.connect();
		
		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

	}
	
	@Test
	public void normalBackpressuredAsyncFused() {
		TestSubscriber<Integer> ts1 = new TestSubscriber<>(0);

		UnicastProcessor<Integer> up = new UnicastProcessor<>(QueueSupplier.<Integer>get(8).get());
		up.onNext(1);
		up.onNext(2);
		up.onNext(3);
		up.onNext(4);
		up.onNext(5);
		up.onComplete();

		ConnectableFlux<Integer> p = up.multicast(EmitterProcessor.create());
		
		p.subscribe(ts1);

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();


		p.connect();

		ts1
		.assertNoValues()
		.assertNoError()
		.assertNotComplete();

		ts1.request(3);

		ts1.assertValues(1, 2, 3)
		.assertNoError()
		.assertNotComplete();

		ts1.request(2);

		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();
	}

	@Test
	public void normalHidden() {
		TestSubscriber<Integer> ts1 = new TestSubscriber<>();
		TestSubscriber<Integer> ts2 = new TestSubscriber<>();
		
		ConnectableFlux<Integer> p = Flux.range(1, 5).hide().multicast(EmitterProcessor.create(), Flux::log);
		
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
		
		ConnectableFlux<Integer> p = Flux.range(1, 5).hide().multicast(EmitterProcessor.create());
		
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
		
		ts1.assertValues(1, 2, 3)
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

		EmitterProcessor<Integer> sp = EmitterProcessor.replay();
		sp.start();
		ConnectableFlux<Integer> p = sp.multicast(EmitterProcessor.<Integer>create());
		
		p.subscribe(ts);
		
		Runnable r = p.connect();
				
		sp.onNext(1);
		sp.onNext(2);
		
		r.run();
		
		ts.assertValues(1, 2)
		.assertError(CancellationException.class)
		.assertNotComplete();
	}


	@Test
	public void cancel() {
		TestSubscriber<Integer> ts1 = new TestSubscriber<>();
		TestSubscriber<Integer> ts2 = new TestSubscriber<>();

		FluxProcessor<Integer, Integer> sp = EmitterProcessor.create();

		sp.start();

		ConnectableFlux<Integer> p = sp.multicast(EmitterProcessor.<Integer>create());

		p.subscribe(ts1);
		p.subscribe(ts2);

		Runnable r = p.connect();

		sp.onNext(1);
		sp.onNext(2);

		ts1.cancel();

		r.run();

		ts1.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts2.assertValues(1, 2)
		   .assertError(CancellationException.class)
		  .assertNotComplete();

	}


	@Test
	public void disconnectBackpressured() {
		TestSubscriber<Integer> ts = new TestSubscriber<>(0);
		
		FluxProcessor<Integer, Integer> sp = EmitterProcessor.<Integer>create();
		sp.start();
		ConnectableFlux<Integer> p = sp.multicast(EmitterProcessor.<Integer>create());
		
		p.subscribe(ts);
		
		Runnable r = p.connect();
				
		r.run();
		
		ts.assertNoValues()
		.assertError(CancellationException.class)
		.assertNotComplete();

	}

	@Test
	public void error() {
		TestSubscriber<Integer> ts = new TestSubscriber<>();

		EmitterProcessor<Integer> sp = EmitterProcessor.create();
		sp.start();
		ConnectableFlux<Integer> p = sp.multicast(EmitterProcessor.<Integer>create());
		
		p.subscribe(ts);
		
		p.connect();
				
		sp.onNext(1);
		sp.onNext(2);
		sp.onError(new RuntimeException("forced failure"));
		
		ts.assertValues(1, 2)
		.assertError(RuntimeException.class)
		.assertNotComplete();
	}

	@Test
	public void fusedMapInvalid() {
		TestSubscriber<Integer> ts = new TestSubscriber<>();
		
		ConnectableFlux<Integer> p = Flux.range(1, 5).map(v -> (Integer)null).multicast(EmitterProcessor.<Integer>create());
		
		p.subscribe(ts);
		
		p.connect();
				
		ts.assertNoValues()
		.assertError(NullPointerException.class)
		.assertNotComplete();
	}

}

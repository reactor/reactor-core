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
import reactor.core.test.TestSubscriber;

public class FluxRefCountTest {
	/*@Test
	public void constructors() {
		ConstructorTestBuilder ctb = new ConstructorTestBuilder(StreamRefCount.class);
		
		ctb.addRef("source", Flux.never().publish());
		ctb.addInt("n", 1, Integer.MAX_VALUE);
		
		ctb.test();
	}*/
	
	@Test
	public void normal() {
		EmitterProcessor<Integer> e = EmitterProcessor.create();
		e.start();

		Flux<Integer> p = e.publish().refCount();
		
		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);
		
		TestSubscriber<Integer> ts1 = new TestSubscriber<>();
		p.subscribe(ts1);

		Assert.assertTrue("sp has no subscribers?", e.downstreamCount() != 0);

		TestSubscriber<Integer> ts2 = new TestSubscriber<>();
		p.subscribe(ts2);

		Assert.assertTrue("sp has no subscribers?", e.downstreamCount() != 0);
		
		e.onNext(1);
		e.onNext(2);
		
		ts1.cancel();
		
		Assert.assertTrue("sp has no subscribers?", e.downstreamCount() != 0);
		
		e.onNext(3);
		
		ts2.cancel();
		
		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);
		
		ts1.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();

		ts2.assertValues(1, 2, 3)
		.assertNoError()
		.assertNotComplete();
	}

	@Test
	public void normalTwoSubscribers() {
		EmitterProcessor<Integer> e = EmitterProcessor.create();
		e.start();

		Flux<Integer> p = e.publish().refCount(2);
		
		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);
		
		TestSubscriber<Integer> ts1 = new TestSubscriber<>();
		p.subscribe(ts1);

		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);

		TestSubscriber<Integer> ts2 = new TestSubscriber<>();
		p.subscribe(ts2);

		Assert.assertTrue("sp has no subscribers?", e.downstreamCount() != 0);
		
		e.onNext(1);
		e.onNext(2);
		
		ts1.cancel();
		
		Assert.assertTrue("sp has no subscribers?", e.downstreamCount() != 0);
		
		e.onNext(3);
		
		ts2.cancel();
		
		Assert.assertFalse("sp has subscribers?", e.downstreamCount() != 0);
		
		ts1.assertValues(1, 2)
		.assertNoError()
		.assertNotComplete();

		ts2.assertValues(1, 2, 3)
		.assertNoError()
		.assertNotComplete();
	}

	@Test
	public void upstreamCompletes() {
		
		Flux<Integer> p = Flux.range(1, 5).publish().refCount();

		TestSubscriber<Integer> ts1 = new TestSubscriber<>();
		p.subscribe(ts1);

		ts1.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

		TestSubscriber<Integer> ts2 = new TestSubscriber<>();
		p.subscribe(ts2);

		ts2.assertValues(1, 2, 3, 4, 5)
		.assertNoError()
		.assertComplete();

	}

	@Test
	public void upstreamCompletesTwoSubscribers() {
		
		Flux<Integer> p = Flux.range(1, 5).publish().refCount(2);

		TestSubscriber<Integer> ts1 = new TestSubscriber<>();
		p.subscribe(ts1);

		ts1.assertValueCount(0);

		TestSubscriber<Integer> ts2 = new TestSubscriber<>();
		p.subscribe(ts2);

		ts1.assertValues(1, 2, 3, 4, 5);
		ts2.assertValues(1, 2, 3, 4, 5);
	}

	@Test
	public void subscribersComeAndGoBelowThreshold() {
		Flux<Integer> p = Flux.range(1, 5).publish().refCount(2);

		Runnable r = p.subscribe();
		r.run();
		p.subscribe().run();
		p.subscribe().run();
		p.subscribe().run();
		p.subscribe().run();
		
		TestSubscriber<Integer> ts1 = new TestSubscriber<>();
		p.subscribe(ts1);

		ts1.assertValueCount(0);

		TestSubscriber<Integer> ts2 = new TestSubscriber<>();
		p.subscribe(ts2);

		ts1.assertValues(1, 2, 3, 4, 5);
		ts2.assertValues(1, 2, 3, 4, 5);
	}
}

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

import java.io.IOException;
import java.time.Duration;
import java.util.logging.Level;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import reactor.core.Fuseable;
import reactor.test.TestSubscriber;

public class ReplayProcessorTest {

    @Test
    public void unbounded() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, true);

	    TestSubscriber<Integer> ts = TestSubscriber.create(0L);

	    rp.subscribe(ts);
        
        rp.onNext(1);
        rp.onNext(2);
        rp.onNext(3);
        rp.onComplete();

        Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

        ts.assertNoValues();
        
        ts.request(1);
        
        ts.assertValues(1);
        
        ts.request(2);
        
        ts.assertValues(1, 2, 3)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void bounded() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, false);

	    TestSubscriber<Integer> ts = TestSubscriber.create(0L);

	    rp.subscribe(ts);
        
        rp.onNext(1);
        rp.onNext(2);
        rp.onNext(3);
        rp.onComplete();

        Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

        ts.assertNoValues();
        
        ts.request(1);
        
        ts.assertValues(1);
        
        ts.request(2);
        
        ts.assertValues(1, 2, 3)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void cancel() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, false);

	    TestSubscriber<Integer> ts = TestSubscriber.create();

	    rp.subscribe(ts);
        
        ts.cancel();
        
        Assert.assertFalse("Has subscribers?", rp.hasDownstreams());
    }

    @Test
    public void unboundedAfter() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, true);

	    TestSubscriber<Integer> ts = TestSubscriber.create(0L);

	    rp.onNext(1);
        rp.onNext(2);
        rp.onNext(3);
        rp.onComplete();

        rp.subscribe(ts);

        Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

        ts.assertNoValues();
        
        ts.request(1);
        
        ts.assertValues(1);
        
        ts.request(2);
        
        ts.assertValues(1, 2, 3)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void boundedAfter() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, false);

	    TestSubscriber<Integer> ts = TestSubscriber.create(0L);

	    rp.onNext(1);
        rp.onNext(2);
        rp.onNext(3);
        rp.onComplete();

        rp.subscribe(ts);

        Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

        ts.assertNoValues();
        
        ts.request(1);
        
        ts.assertValues(1);
        
        ts.request(2);
        
        ts.assertValues(1, 2, 3)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void unboundedLong() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, true);

	    TestSubscriber<Integer> ts = TestSubscriber.create(0L);

	    for (int i = 0; i < 256; i++) {
            rp.onNext(i);
        }
        rp.onComplete();

        rp.subscribe(ts);

        Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

        ts.assertNoValues();
        
        ts.request(Long.MAX_VALUE);
        
        ts.assertValueCount(256)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void boundedLong() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, false);

	    TestSubscriber<Integer> ts = TestSubscriber.create(0L);

	    for (int i = 0; i < 256; i++) {
            rp.onNext(i);
        }
        rp.onComplete();

        rp.subscribe(ts);

        Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

        ts.assertNoValues();
        
        ts.request(Long.MAX_VALUE);
        
        ts.assertValueCount(16)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void fusedUnboundedAfterLong() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, true);

	    TestSubscriber<Integer> ts = TestSubscriber.create();
	    ts.requestedFusionMode(Fuseable.ASYNC);
        
        for (int i = 0; i < 256; i++) {
            rp.onNext(i);
        }
        rp.onComplete();

        rp.subscribe(ts);

        Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

        ts
        .assertFuseableSource()
        .assertFusionMode(Fuseable.ASYNC)
        .assertValueCount(256)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void fusedUnboundedLong() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, true);

	    TestSubscriber<Integer> ts = TestSubscriber.create();
	    ts.requestedFusionMode(Fuseable.ASYNC);

        rp.subscribe(ts);

        for (int i = 0; i < 256; i++) {
            rp.onNext(i);
        }
        rp.onComplete();


        Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

        ts
        .assertFuseableSource()
        .assertFusionMode(Fuseable.ASYNC)
        .assertValueCount(256)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void fusedBoundedAfterLong() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, false);

	    TestSubscriber<Integer> ts = TestSubscriber.create();
	    ts.requestedFusionMode(Fuseable.ASYNC);
        
        for (int i = 0; i < 256; i++) {
            rp.onNext(i);
        }
        rp.onComplete();

        rp.subscribe(ts);

        Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

        ts
        .assertFuseableSource()
        .assertFusionMode(Fuseable.ASYNC)
        .assertValueCount(16)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void fusedBoundedLong() {
	    ReplayProcessor<Integer> rp = ReplayProcessor.create(16, false);

	    TestSubscriber<Integer> ts = TestSubscriber.create();
	    ts.requestedFusionMode(Fuseable.ASYNC);

	    rp.subscribe(ts);

	    for (int i = 0; i < 256; i++) {
		    rp.onNext(i);
	    }
	    rp.onComplete();

	    Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

	    ts.assertFuseableSource()
	      .assertFusionMode(Fuseable.ASYNC)
	      .assertValueCount(256)
	      .assertNoError()
	      .assertComplete();
    }

	@Test
	public void timedFused() throws Exception {
		ReplayProcessor<Integer> rp =
				ReplayProcessor.createTimeout(Duration.ofSeconds(1));

		TestSubscriber<Integer> ts = TestSubscriber.create();
		ts.requestedFusionMode(Fuseable.ASYNC);

		for (int i = 0; i < 5; i++) {
			rp.onNext(i);
		}
		Thread.sleep(2000);
		for (int i = 5; i < 10; i++) {
			rp.onNext(i);
		}
		rp.onComplete();

		rp.log().subscribe(ts);

		Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

		ts.assertFuseableSource()
		  .assertFusionMode(Fuseable.ASYNC)
		  .assertValueCount(5)
		  .assertValues(5,6,7,8,9)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void timed() throws Exception {
		ReplayProcessor<Integer> rp =
				ReplayProcessor.createTimeout(Duration.ofSeconds(1));

		TestSubscriber<Integer> ts = TestSubscriber.create();

		for (int i = 0; i < 5; i++) {
			rp.onNext(i);
		}
		Thread.sleep(2000);
		for (int i = 5; i < 10; i++) {
			rp.onNext(i);
		}
		rp.onComplete();

		rp.subscribe(ts);

		Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

		ts.assertValueCount(5)
		  .assertNoError()
		  .assertValues(5,6,7,8,9)
		  .assertComplete();
	}

	@Test
	public void timedAndBound() throws Exception {
		ReplayProcessor<Integer> rp =
				ReplayProcessor.createSizeAndTimeout(5, Duration.ofSeconds(1));

		TestSubscriber<Integer> ts = TestSubscriber.create();

		for (int i = 0; i < 10; i++) {
			rp.onNext(i);
		}
		System.out.println("----");
		Thread.sleep(2000);
		for (int i = 10; i < 20; i++) {
			rp.onNext(i);
		}
		rp.onComplete();

		rp.log()
		  .subscribe(ts);


        Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

        ts.assertValueCount(5)
          .assertValues(15,16,17,18,19)
          .assertNoError()
          .assertComplete();
    }

	@Test
	public void timedAndBoundFused() throws Exception {
		ReplayProcessor<Integer> rp =
				ReplayProcessor.createSizeAndTimeout(5, Duration.ofSeconds(1));

		TestSubscriber<Integer> ts = TestSubscriber.create();
		ts.requestedFusionMode(Fuseable.ASYNC);

		for (int i = 0; i < 10; i++) {
			rp.onNext(i);
		}
		System.out.println("----");
		Thread.sleep(2000);
		for (int i = 10; i < 20; i++) {
			rp.onNext(i);
		}
		rp.onComplete();

		rp.log()
		  .subscribe(ts);

		Assert.assertFalse("Has subscribers?", rp.hasDownstreams());

		ts.assertFuseableSource()
		  .assertFusionMode(Fuseable.ASYNC)
		  .assertValueCount(5)
		  .assertValues(15,16,17,18,19)
		  .assertNoError()
		  .assertComplete();
	}

}

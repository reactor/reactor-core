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
import org.reactivestreams.Subscriber;
import reactor.core.test.TestSubscriber;

public class DirectProcessorTest {

    @Test(expected = NullPointerException.class)
    public void onNextNull() {
        new DirectProcessor<Integer>().onNext(null);
    }

    @Test(expected = NullPointerException.class)
    public void onErrorNull() {
        new DirectProcessor<Integer>().onError(null);
    }

    @Test(expected = NullPointerException.class)
    public void onSubscribeNull() {
        new DirectProcessor<Integer>().onSubscribe(null);
    }

    @Test(expected = NullPointerException.class)
    public void subscribeNull() {
        new DirectProcessor<Integer>().subscribe((Subscriber<Object>)null);
    }

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = TestSubscriber.create();

        DirectProcessor<Integer> tp = new DirectProcessor<>();

        tp.subscribe(ts);

        Assert.assertTrue("No subscribers?", tp.hasDownstreams());
        Assert.assertFalse("Completed?", tp.hasCompleted());
        Assert.assertNull("Has error?", tp.getError());
        Assert.assertFalse("Has error?", tp.hasError());

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        tp.onNext(1);
        tp.onNext(2);

        ts.assertValues(1, 2)
          .assertNotComplete()
          .assertNoError();

        tp.onNext(3);
        tp.onComplete();

        Assert.assertFalse("Subscribers present?", tp.hasDownstreams());
        Assert.assertTrue("Not completed?", tp.hasCompleted());
        Assert.assertNull("Has error?", tp.getError());
        Assert.assertFalse("Has error?", tp.hasError());


        ts.assertValues(1, 2, 3)
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = TestSubscriber.create(0);

        DirectProcessor<Integer> tp = new DirectProcessor<>();

        tp.subscribe(ts);

        Assert.assertTrue("No subscribers?", tp.hasDownstreams());
        Assert.assertFalse("Completed?", tp.hasCompleted());
        Assert.assertNull("Has error?", tp.getError());
        Assert.assertFalse("Has error?", tp.hasError());

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(10);

        tp.onNext(1);
        tp.onNext(2);
        tp.onComplete();

        Assert.assertFalse("Subscribers present?", tp.hasDownstreams());
        Assert.assertTrue("Not completed?", tp.hasCompleted());
        Assert.assertNull("Has error?", tp.getError());
        Assert.assertFalse("Has error?", tp.hasError());

        ts.assertValues(1, 2)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void notEnoughRequests() {

        TestSubscriber<Integer> ts = TestSubscriber.create(0);

        DirectProcessor<Integer> tp = new DirectProcessor<>();

        tp.subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(1);

        tp.onNext(1);
        tp.onNext(2);
        tp.onComplete();

        Assert.assertFalse("Subscribers present?", tp.hasDownstreams());
        Assert.assertTrue("Not completed?", tp.hasCompleted());
        Assert.assertNull("Has error?", tp.getError());
        Assert.assertFalse("Has error?", tp.hasError());

        ts.assertValues(1)
          .assertError(IllegalStateException.class)
          .assertNotComplete();
    }

    @Test
    public void error() {
        TestSubscriber<Integer> ts = TestSubscriber.create();

        DirectProcessor<Integer> tp = new DirectProcessor<>();

        tp.subscribe(ts);

        Assert.assertTrue("No subscribers?", tp.hasDownstreams());
        Assert.assertFalse("Completed?", tp.hasCompleted());
        Assert.assertNull("Has error?", tp.getError());
        Assert.assertFalse("Has error?", tp.hasError());

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        tp.onNext(1);
        tp.onNext(2);

        ts.assertValues(1, 2)
          .assertNotComplete()
          .assertNoError();

        tp.onNext(3);
        tp.onError(new RuntimeException("forced failure"));

        Assert.assertFalse("Subscribers present?", tp.hasDownstreams());
        Assert.assertFalse("Completed?", tp.hasCompleted());
        Assert.assertNotNull("Has error?", tp.getError());
        Assert.assertTrue("No error?", tp.hasError());

        Throwable e = tp.getError();
        Assert.assertTrue("Wrong exception? " + e, RuntimeException.class.isInstance(e));
        Assert.assertEquals("forced failure", e.getMessage());

        ts.assertValues(1, 2, 3)
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");
    }

    @Test
    public void terminatedWithError() {
        TestSubscriber<Integer> ts = TestSubscriber.create();

        DirectProcessor<Integer> tp = new DirectProcessor<>();
        tp.onError(new RuntimeException("forced failure"));

        tp.subscribe(ts);

        Assert.assertFalse("Subscribers present?", tp.hasDownstreams());
        Assert.assertFalse("Completed?", tp.hasCompleted());
        Assert.assertNotNull("No error?", tp.getError());
        Assert.assertTrue("No error?", tp.hasError());

        Throwable e = tp.getError();
        Assert.assertTrue("Wrong exception? " + e, RuntimeException.class.isInstance(e));
        Assert.assertEquals("forced failure", e.getMessage());

        ts.assertNoValues()
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");
    }

    @Test
    public void terminatedNormally() {
        TestSubscriber<Integer> ts = TestSubscriber.create();

        DirectProcessor<Integer> tp = new DirectProcessor<>();
        tp.onComplete();

        tp.subscribe(ts);

        Assert.assertFalse("Subscribers present?", tp.hasDownstreams());
        Assert.assertTrue("Not completed?", tp.hasCompleted());
        Assert.assertNull("Has error?", tp.getError());
        Assert.assertFalse("Has error?", tp.hasError());

        ts.assertNoValues()
          .assertComplete()
          .assertNoError();
    }

    @Test
    public void subscriberAlreadyCancelled() {
        TestSubscriber<Integer> ts = TestSubscriber.create();
        ts.cancel();

        DirectProcessor<Integer> tp = new DirectProcessor<>();

        tp.subscribe(ts);

        Assert.assertFalse("Subscribers present?", tp.hasDownstreams());

        tp.onNext(1);


        ts.assertNoValues()
          .assertNotComplete()
          .assertNoError();
    }

    @Test
    public void subscriberCancels() {
        TestSubscriber<Integer> ts = TestSubscriber.create();

        DirectProcessor<Integer> tp = new DirectProcessor<>();

        tp.subscribe(ts);

        Assert.assertTrue("No Subscribers present?", tp.hasDownstreams());

        tp.onNext(1);

        ts.assertValues(1)
          .assertNoError()
          .assertNotComplete();

        ts.cancel();

        Assert.assertFalse("Subscribers present?", tp.hasDownstreams());

        tp.onNext(2);

        ts.assertValues(1)
          .assertNotComplete()
          .assertNoError();
    }

}

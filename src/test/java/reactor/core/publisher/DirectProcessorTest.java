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
import org.mockito.Mockito;
import org.reactivestreams.Subscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class DirectProcessorTest {

    @Test(expected = NullPointerException.class)
    public void onNextNull() {
	    DirectProcessor.create().onNext(null);
    }

    @Test(expected = NullPointerException.class)
    public void onErrorNull() {
	    DirectProcessor.create().onError(null);
    }

    @Test(expected = NullPointerException.class)
    public void onSubscribeNull() {
        DirectProcessor.create().onSubscribe(null);
    }

    @Test(expected = NullPointerException.class)
    public void subscribeNull() {
	    DirectProcessor.create().subscribe((Subscriber<Object>)null);
    }

    @Test
    public void normal() {
        DirectProcessor<Integer> tp = DirectProcessor.create();

	    StepVerifier.create(tp)
	                .then(() -> {
		                Assert.assertTrue("No subscribers?", tp.hasDownstreams());
		                Assert.assertFalse("Completed?", tp.hasCompleted());
		                Assert.assertNull("Has error?", tp.getError());
		                Assert.assertFalse("Has error?", tp.hasError());
	                })
	                .then(() -> {
		                tp.onNext(1);
		                tp.onNext(2);
	                })
	                .expectNext(1, 2)
	                .then(() -> {
		                tp.onNext(3);
		                tp.onComplete();
	                })
	                .expectNext(3)
	                .expectComplete()
	                .verify();

	    Assert.assertFalse("Subscribers present?", tp.hasDownstreams());
	    Assert.assertTrue("Not completed?", tp.hasCompleted());
	    Assert.assertNull("Has error?", tp.getError());
	    Assert.assertFalse("Has error?", tp.hasError());
    }

    @Test
    public void normalBackpressured() {
        DirectProcessor<Integer> tp = DirectProcessor.create();

	    StepVerifier.create(tp, 0L)
	                .then(() -> {
		                Assert.assertTrue("No subscribers?", tp.hasDownstreams());
		                Assert.assertFalse("Completed?", tp.hasCompleted());
		                Assert.assertNull("Has error?", tp.getError());
		                Assert.assertFalse("Has error?", tp.hasError());
	                })
	                .thenRequest(10L)
	                .then(() -> {
		                tp.onNext(1);
		                tp.onNext(2);
		                tp.onComplete();
	                })
	                .expectNext(1, 2)
	                .expectComplete()
	                .verify();

	    Assert.assertFalse("Subscribers present?", tp.hasDownstreams());
	    Assert.assertTrue("Not completed?", tp.hasCompleted());
	    Assert.assertNull("Has error?", tp.getError());
	    Assert.assertFalse("Has error?", tp.hasError());
    }

    @Test
    public void notEnoughRequests() {
        DirectProcessor<Integer> tp = DirectProcessor.create();

	    StepVerifier.create(tp, 1L)
	                .then(() -> {
		                tp.onNext(1);
		                tp.onNext(2);
		                tp.onComplete();
	                })
	                .expectNext(1)
	                .expectError(IllegalStateException.class)
	                .verify();
    }

    @Test
    public void error() {
        AssertSubscriber<Integer> ts = AssertSubscriber.create();

        DirectProcessor<Integer> tp = DirectProcessor.create();

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
        AssertSubscriber<Integer> ts = AssertSubscriber.create();

        DirectProcessor<Integer> tp = DirectProcessor.create();
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
        AssertSubscriber<Integer> ts = AssertSubscriber.create();

        DirectProcessor<Integer> tp = DirectProcessor.create();
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
        AssertSubscriber<Integer> ts = AssertSubscriber.create();
        ts.cancel();

        DirectProcessor<Integer> tp = DirectProcessor.create();

        tp.subscribe(ts);

        Assert.assertFalse("Subscribers present?", tp.hasDownstreams());

        tp.onNext(1);


        ts.assertNoValues()
          .assertNotComplete()
          .assertNoError();
    }

    @Test
    public void subscriberCancels() {
        AssertSubscriber<Integer> ts = AssertSubscriber.create();

        DirectProcessor<Integer> tp = DirectProcessor.create();

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

    @Test
    @SuppressWarnings("unchecked")
    public void scanInner() {
        Subscriber<? super String> actual = mock(Subscriber.class);
        DirectProcessor<String> parent = new DirectProcessor<>();

        DirectProcessor.DirectInner<String> test =
                new DirectProcessor.DirectInner<>(actual, parent);

        assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();

        test.cancel();
        assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }

}

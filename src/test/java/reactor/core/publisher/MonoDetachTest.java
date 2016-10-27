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

import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import reactor.test.subscriber.AssertSubscriber;

public class MonoDetachTest {

	Object o;

	@Test
	public void just() throws Exception {
		o = new Object();

		WeakReference<Object> wr = new WeakReference<>(o);

		AssertSubscriber<Integer> ts = new AssertSubscriber<>();

		Mono.just(o)
		    .map(x -> 1)
		    .onTerminateDetach()
		    .subscribe(ts);

		ts.assertValues(1);
		ts.assertComplete();
		ts.assertNoError();

		o = null;

		System.gc();
		Thread.sleep(200);

		Assert.assertNull("Object retained!", wr.get());

	}

	@Test
	public void error() {
		AssertSubscriber<Object> ts = new AssertSubscriber<>();

		Mono.error(new RuntimeException("forced failure"))
		    .onTerminateDetach()
		    .subscribe(ts);

		ts.assertNoValues();
		ts.assertErrorMessage("forced failure");
		ts.assertNotComplete();
	}

	@Test
	public void empty() {
		AssertSubscriber<Object> ts = new AssertSubscriber<>();

		Mono.empty()
		    .onTerminateDetach()
		    .subscribe(ts);

		ts.assertNoValues();
		ts.assertNoError();
		ts.assertComplete();
	}

	@Test
	public void backpressured() throws Exception {
		o = new Object();

		WeakReference<Object> wr = new WeakReference<>(o);

		AssertSubscriber<Integer> ts = new AssertSubscriber<>(0L);

		Mono.just(o)
		    .map(x -> 1)
		    .onTerminateDetach()
		    .subscribe(ts);

		ts.assertNoValues();

		ts.request(1);

		ts.assertValues(1);
		ts.assertComplete();
		ts.assertNoError();

		o = null;

		System.gc();
		Thread.sleep(200);

		Assert.assertNull("Object retained!", wr.get());
	}

	@Test
	public void justUnsubscribed() throws Exception {
		o = new Object();

		WeakReference<Object> wr = new WeakReference<>(o);

		AssertSubscriber<Object> ts = new AssertSubscriber<>(0);

		Mono.just(o)
		    .onTerminateDetach()
		    .subscribe(ts);

		ts.cancel();
		o = null;

		System.gc();
		Thread.sleep(200);

		Assert.assertNull("Object retained!", wr.get());

	}

	@Test
	public void deferredUpstreamProducer() {
		final AtomicReference<Subscriber<? super Object>> subscriber =
				new AtomicReference<>();

		AssertSubscriber<Object> ts = new AssertSubscriber<>(0);

		Mono.<Object>from(subscriber::set).onTerminateDetach()
		                                  .subscribe(ts);

		Flux.just(1)
		    .subscribe(subscriber.get());

		ts.request(1);

		ts.assertValues(1);
		ts.assertComplete();
		ts.assertNoError();
	}
}
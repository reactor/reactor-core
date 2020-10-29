/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
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

import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxDetachTest {

	Object o;

	@Test
	public void just() throws Exception {
		o = new Object();

		WeakReference<Object> wr = new WeakReference<>(o);

		AssertSubscriber<Object> ts = new AssertSubscriber<>();

		Flux.just(o)
		    .count()
		    .flux()
		    .onTerminateDetach()
		    .subscribe(ts);

		ts.assertValues(1L);
		ts.assertComplete();
		ts.assertNoError();

		o = null;

		System.gc();
		Thread.sleep(200);

		assertThat(wr.get()).as("Object retained!").isNull();

	}

	@Test
	public void error() {
		AssertSubscriber<Object> ts = new AssertSubscriber<>();

		Flux.error(new RuntimeException("forced failure"))
		    .onTerminateDetach()
		    .subscribe(ts);

		ts.assertNoValues();
		ts.assertErrorMessage("forced failure");
		ts.assertNotComplete();
	}

	@Test
	public void empty() {
		AssertSubscriber<Object> ts = new AssertSubscriber<>();

		Flux.empty()
		    .onTerminateDetach()
		    .subscribe(ts);

		ts.assertNoValues();
		ts.assertNoError();
		ts.assertComplete();
	}

	@Test
	public void range() {
		AssertSubscriber<Object> ts = new AssertSubscriber<>();

		Flux.range(1, 1000)
		    .onTerminateDetach()
		    .subscribe(ts);

		ts.assertValueCount(1000);
		ts.assertNoError();
		ts.assertComplete();
	}

	@Test
	public void backpressured() throws Exception {
		o = new Object();

		WeakReference<Object> wr = new WeakReference<>(o);

		AssertSubscriber<Object> ts = new AssertSubscriber<>(0L);

		Flux.just(o)
		    .count()
		    .flux()
		    .onTerminateDetach()
		    .subscribe(ts);

		ts.assertNoValues();

		ts.request(1);

		ts.assertValues(1L);
		ts.assertComplete();
		ts.assertNoError();

		o = null;

		System.gc();
		Thread.sleep(200);

		assertThat(wr.get()).as("Object retained!").isNull();
	}

	@Test
	public void justUnsubscribed() throws Exception {
		o = new Object();

		WeakReference<Object> wr = new WeakReference<>(o);

		AssertSubscriber<Object> ts = new AssertSubscriber<>(0);

		Flux.just(o)
		    .count()
		    .flux()
		    .onTerminateDetach()
		    .subscribe(ts);

		ts.cancel();
		o = null;

		Awaitility.with().pollDelay(Duration.ZERO).pollInterval(Duration.ONE_MILLISECOND)
			.await()
			.atMost(Duration.FIVE_SECONDS)
			.untilAsserted(() -> {
				System.gc();
				Object garbage = new Object();
				assertThat(wr.get()).as("Object retained!").isNull();
				garbage.toString();
			});
	}

	@Test
	public void deferredUpstreamProducer() {
		final AtomicReference<Subscriber<? super Object>> subscriber =
				new AtomicReference<>();

		AssertSubscriber<Object> ts = new AssertSubscriber<>(0);

		Flux.<Object>from(subscriber::set).onTerminateDetach()
		                                  .subscribe(ts);

		ts.request(2);

		Flux.range(1, 3)
		    .subscribe(subscriber.get());

		ts.assertValues(1, 2);

		ts.request(1);

		ts.assertValues(1, 2, 3);
		ts.assertComplete();
		ts.assertNoError();
	}
	
	@Test
	public void scanOperator(){
	    FluxDetach<Integer> test = new FluxDetach<>(Flux.just(1));
	    
	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscriber() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {
		}, null, null);
		FluxDetach.DetachSubscriber<String> test = new FluxDetach.DetachSubscriber<>(actual);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isNull();
		assertThat(test.scan(Scannable.Attr.PARENT)).isNull();
	}

	@Test
	public void scanSubscriberCancelled() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxDetach.DetachSubscriber<String> test = new FluxDetach.DetachSubscriber<>(actual);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.cancel();
		assertThat(test.scan(Scannable.Attr.PARENT)).isNull();
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isNull();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
	}
}

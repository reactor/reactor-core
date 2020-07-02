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

import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoElementAtTest {

	@Test(expected = NullPointerException.class)
	public void source1Null() {
		new MonoElementAt<>(null, 1);
	}

	@Test(expected = NullPointerException.class)
	public void source2Null() {
		new MonoElementAt<>(null, 1, 1);
	}

	@Test(expected = NullPointerException.class)
	public void defaultSupplierNull() {
		Flux.never().elementAt(1, null);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void indexNegative1() {
		Flux.never().elementAt(-1);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void indexNegative2() {
		Flux.never().elementAt(-1, 1);
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10).elementAt(0).subscribe(ts);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10).elementAt(0).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(1)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normal2() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10).elementAt(4).subscribe(ts);

		ts.assertValues(5)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normal5Backpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10).elementAt(4).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(5)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normal3() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10).elementAt(9).subscribe(ts);

		ts.assertValues(10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void normal3Backpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10).elementAt(9).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(10)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void empty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>empty().elementAt(0).subscribe(ts);

		ts.assertNoValues()
		  .assertError(IndexOutOfBoundsException.class)
		  .assertNotComplete();
	}

	@Test
	public void emptyDefault() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>empty().elementAt(0, 20).subscribe(ts);

		ts.assertValues(20)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void emptyDefaultBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.<Integer>empty().elementAt(0, 20).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(20)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void nonEmptyDefault() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10).elementAt(20, 20).subscribe(ts);

		ts.assertValues(20)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void nonEmptyDefaultBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10).elementAt(20, 20).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(20)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void cancel() {
		TestPublisher<String> cancelTester = TestPublisher.create();

		MonoProcessor<String> processor = cancelTester.flux()
		                                              .elementAt(1000)
		                                              .toProcessor();
		processor.subscribe();
		processor.cancel();

		cancelTester.assertCancelled();
	}

	@Test
	public void scanOperator(){
	    MonoElementAt<String> test = new MonoElementAt(Flux.just(1, 2, 3), 1);

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscriber() {
		CoreSubscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoElementAt.ElementAtSubscriber<String> test = new MonoElementAt.ElementAtSubscriber<>(actual, 1, "foo");
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}
}

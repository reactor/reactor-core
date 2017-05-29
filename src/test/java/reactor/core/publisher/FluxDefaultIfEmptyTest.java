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

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxDefaultIfEmptyTest {

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxDefaultIfEmpty<>(null, 1);
	}

	@Test(expected = NullPointerException.class)
	public void valueNull() {
		Flux.never().defaultIfEmpty(null);
	}

	@Test
	public void nonEmpty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5).defaultIfEmpty(10).subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertComplete()
		  .assertNoError();

	}

	@Test
	public void nonEmptyBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 5).defaultIfEmpty(10).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertComplete()
		  .assertNoError();

	}

	@Test
	public void empty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>empty().defaultIfEmpty(10).subscribe(ts);

		ts.assertValues(10)
		  .assertComplete()
		  .assertNoError();

	}

	@Test
	public void emptyBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.<Integer>empty().defaultIfEmpty(10).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(10)
		  .assertComplete()
		  .assertNoError();

	}

	@Test
	public void nonEmptyHide() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 5).hide().defaultIfEmpty(10).subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertComplete()
		  .assertNoError();

	}

	@Test
	public void nonEmptyBackpressuredHide() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 5).hide().defaultIfEmpty(10).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertComplete()
		  .assertNoError();

	}

	@Test
	public void emptyHide() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>empty().hide().defaultIfEmpty(10).subscribe(ts);

		ts.assertValues(10)
		  .assertComplete()
		  .assertNoError();

	}

	@Test
	public void emptyBackpressuredHide() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.<Integer>empty().hide().defaultIfEmpty(10).subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(10)
		  .assertComplete()
		  .assertNoError();

	}

	@Test
	public void scanSubscriber() {
		Subscriber<String> actual = new LambdaSubscriber<>(null, e -> { }, null, null);
		FluxDefaultIfEmpty.DefaultIfEmptySubscriber<String> test =
				new FluxDefaultIfEmpty.DefaultIfEmptySubscriber<>(actual, "bar");
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		test.onComplete();
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
	}

	@Test
	public void scanSubscriberCancelled() {
		Subscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxDefaultIfEmpty.DefaultIfEmptySubscriber<String> test =
				new FluxDefaultIfEmpty.DefaultIfEmptySubscriber<>(actual, "bar");
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}

}

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
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.reactivestreams.Subscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxArrayTest {

	@Test(expected = NullPointerException.class)
	public void arrayNull() {
		Flux.fromArray((Integer[]) null);
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).subscribe(ts);

		ts.assertNoError()
		  .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).subscribe(ts);

		ts.assertNoError()
		  .assertNoValues()
		  .assertNotComplete();

		ts.request(5);

		ts.assertNoError()
		  .assertValues(1, 2, 3, 4, 5)
		  .assertNotComplete();

		ts.request(10);

		ts.assertNoError()
		  .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete();
	}

	@Test
	public void normalBackpressuredExact() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(10);

		Flux.just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).subscribe(ts);

		ts.assertNoError()
		  .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete();

		ts.request(10);

		ts.assertNoError()
		  .assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete();
	}

	@Test
	public void arrayContainsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.just(1, 2, 3, 4, 5, null, 7, 8, 9, 10).subscribe(ts);

		ts.assertError(NullPointerException.class)
		  .assertValues(1, 2, 3, 4, 5)
		  .assertNotComplete();
	}


	@Test
	public void scanConditionalSubscription() {
		Fuseable.ConditionalSubscriber<? super Object> subscriber = Mockito.mock(Fuseable.ConditionalSubscriber.class);

		FluxArray.ArrayConditionalSubscription<Object> test =
				new FluxArray.ArrayConditionalSubscription<>(subscriber,
						new Object[]{"foo", "bar", "baz"});

		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();

		assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(3);
		assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isZero();

		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(subscriber);

		test.poll();
		assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(2);
		test.poll();

		test.request(2);
		assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(2L);

		test.poll();
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
	}

	@Test
	public void scanConditionalSubscriptionRequested() {
		Fuseable.ConditionalSubscriber<? super Object> subscriber = Mockito.mock(Fuseable.ConditionalSubscriber.class);
		//the mock will not drain the request, so it can be tested
		Mockito.when(subscriber.tryOnNext(Mockito.any()))
		       .thenReturn(false);

		FluxArray.ArrayConditionalSubscription<Object> test =
				new FluxArray.ArrayConditionalSubscription<>(subscriber,
						new Object[]{"foo", "bar", "baz"});

		test.request(2);
		assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(2L);
		assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(3);
	}

	@Test
	public void scanConditionalSubscriptionCancelled() {
		Fuseable.ConditionalSubscriber<? super Object> subscriber = Mockito.mock(Fuseable.ConditionalSubscriber.class);

		FluxArray.ArrayConditionalSubscription<Object> test =
				new FluxArray.ArrayConditionalSubscription<>(subscriber, new Object[] {"foo", "bar", "baz"});

		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}

	@Test
	public void scanSubscription() {
		Subscriber<String> subscriber = Mockito.mock(Subscriber.class);
		FluxArray.ArraySubscription<String> test =
				new FluxArray.ArraySubscription<>(subscriber, new String[] {"foo", "bar", "baz"});

		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(3);
		assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(0L);
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(subscriber);

		test.poll();
		assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(2);

		test.request(2);
		assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(2L);

		test.poll();
		test.poll();
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
	}

	@Test
	public void scanSubscriptionCancelled() {
		Subscriber<String> subscriber = Mockito.mock(Subscriber.class);
		FluxArray.ArraySubscription<String> test =
				new FluxArray.ArraySubscription<>(subscriber, new String[] {"foo", "bar", "baz"});

		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();

		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}
}

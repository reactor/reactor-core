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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;
import org.mockito.Mockito;
import org.reactivestreams.Subscriber;

import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import static org.assertj.core.api.Assertions.assertThat;

public class FluxIterableTest {

	final Iterable<Integer> source = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

	@Test
	public void emptyIterable() {
		StepVerifier.create(Flux.never().zipWithIterable(new ArrayList<>()))
	                .verifyComplete();
	}

	@Test(expected = NullPointerException.class)
	public void nullIterable() {
		Flux.never().zipWithIterable(null);
	}

	@Test
	public void nullIterator() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.<Integer>fromIterable(() -> null).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.fromIterable(source)
		    .subscribe(ts);

		ts.assertValueSequence(source)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.fromIterable(source)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertNotComplete()
		  .assertNoError();

		ts.request(10);

		ts.assertValueSequence(source)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressuredExact() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(10);

		Flux.fromIterable(source)
		    .subscribe(ts);

		ts.assertValueSequence(source)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void iteratorReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.fromIterable(Arrays.asList(1, 2, 3, 4, 5, null, 7, 8, 9, 10))
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5)
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void emptyMapped() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.fromIterable(Collections.<Integer>emptyList())
		    .map(v -> v + 1)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void scanSubscription() {
		Subscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, sub -> sub.request(100));
		FluxIterable.IterableSubscription<String> test =
				new FluxIterable.IterableSubscription<>(actual, Collections.singleton("test").iterator());

		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
		test.request(123);
		assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(123);

		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		test.clear();
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}

	@Test
	public void scanConditionalSubscription() {
		Fuseable.ConditionalSubscriber<? super String> actual = Mockito.mock(Fuseable.ConditionalSubscriber.class);
        FluxIterable.IterableSubscriptionConditional<String> test =
				new FluxIterable.IterableSubscriptionConditional<>(actual, Collections.singleton("test").iterator());

		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
		test.request(123);
		assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(123);

		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		test.clear();
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}
}

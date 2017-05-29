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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.mockito.Mockito;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxDistinctUntilChangedTest extends FluxOperatorTest<String, String> {

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.distinctUntilChanged(d -> {
					throw exception();
				})),

				scenario(f -> f.distinctUntilChanged(d -> null))

		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.distinctUntilChanged())
		);
	}

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxDistinctUntilChanged<>(null, v -> v);
	}

	@Test(expected = NullPointerException.class)
	public void keyExtractorNull() {
		Flux.never().distinctUntilChanged(null);
	}

	@Test
	public void allDistinct() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .distinctUntilChanged(v -> v)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void allDistinctBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .distinctUntilChanged(v -> v)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(5);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void someRepetiton() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.just(1, 1, 2, 2, 1, 1, 2, 2, 1, 2, 3, 3)
		    .distinctUntilChanged(v -> v)
		    .subscribe(ts);

		ts.assertValues(1, 2, 1, 2, 1, 2, 3)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void someRepetitionBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.just(1, 1, 2, 2, 1, 1, 2, 2, 1, 2, 3, 3)
		    .distinctUntilChanged(v -> v)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(4);

		ts.assertValues(1, 2, 1, 2, 1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(10);

		ts.assertValues(1, 2, 1, 2, 1, 2, 3)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void withKeySelector() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .distinctUntilChanged(v -> v / 3)
		    .subscribe(ts);

		ts.assertValues(1, 3, 6, 9)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void keySelectorThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .distinctUntilChanged(v -> {
			    throw new RuntimeException("forced failure");
		    })
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void allDistinctConditional() {
		DirectProcessor<Integer> dp = new DirectProcessor<>();

		AssertSubscriber<Integer> ts = dp.distinctUntilChanged()
		                                 .filter(v -> true)
		                                 .subscribeWith(AssertSubscriber.create());

		dp.onNext(1);
		dp.onNext(2);
		dp.onNext(3);
		dp.onComplete();

		ts.assertValues(1, 2, 3).assertComplete();
	}

	@Test
	public void scanSubscriber() {
		Subscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxDistinctUntilChanged.DistinctUntilChangedSubscriber<String, Integer> test = new FluxDistinctUntilChanged.DistinctUntilChangedSubscriber<>(
			actual, String::hashCode);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
	}

	@Test
	public void scanConditionalSubscriber() {
		Fuseable.ConditionalSubscriber<String> actual = Mockito.mock(Fuseable.ConditionalSubscriber.class);
		FluxDistinctUntilChanged.DistinctUntilChangedConditionalSubscriber<String, Integer> test = new FluxDistinctUntilChanged.DistinctUntilChangedConditionalSubscriber<>(
				actual, String::hashCode);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
	}

}

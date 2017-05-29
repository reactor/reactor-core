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
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Scannable;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;

public class FluxScanSeedTest extends FluxOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.receive(4, i -> item(0));
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.scan(item(0), (a, b) -> {
					throw exception();
				})).receiveValues(item(0)),

				scenario(f -> f.scan(item(0), (a, b) -> null))
						.receiveValues(item(0)),

				scenario(f -> f.scanWith(() -> null, (a, b) -> b)),

				scenario(f -> f.scanWith(() -> {
							throw exception();
						},
						(a, b) -> b))

		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.scan(item(0), (a, b) -> a))
		);
	}

	@Test(expected = NullPointerException.class)
	public void sourceNull() {
		new FluxScanSeed<>(null, () -> 1, (a, b) -> a);
	}

	@Test(expected = NullPointerException.class)
	public void initialValueNull() {
		Flux.never()
		    .scan(null, (a, b) -> a);
	}

	@Test(expected = NullPointerException.class)
	public void accumulatorNull() {
		Flux.never()
		    .scan(1, null);
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .scan(0, (a, b) -> b)
		    .subscribe(ts);

		ts.assertValues(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .scan(0, (a, b) -> b)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(0, 1)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(8);

		ts.assertValues(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(1);

		ts.assertValues(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void accumulatorThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .scan(0, (a, b) -> {
			    throw new RuntimeException("forced failure");
		    })
		    .subscribe(ts);

		ts.assertValues(0)
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void accumulatorReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .scan(0, (a, b) -> null)
		    .subscribe(ts);

		ts.assertValues(0)
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
    public void scanSubscriber() {
        Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxScanSeed.ScanSeedSubscriber<Integer, Integer> test = new FluxScanSeed.ScanSeedSubscriber<>(actual,
        		(i, j) -> i + j, 0);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
        test.requested = 35;
        Assertions.assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);
        test.value = 5;
        Assertions.assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(1);

        Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        test.onComplete();
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
    }
}

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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.RaceTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class FluxScanTest extends FluxOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.receive(3, i -> item(0));
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.scan((a, b) -> a))
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.scan((a, b) -> {
					throw exception();
				})).receiveValues(item(0)),

				scenario(f -> f.scan((a, b) -> null))
						.receiveValues(item(0))
		);
	}

	@Test
	public void sourceNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new FluxScan<>(null, (a, b) -> a);
		});
	}

	@Test
	public void accumulatorNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.never().scan(null);
		});
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .scan((a, b) -> b)
		    .subscribe(ts);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .scan((a, b) -> b)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(1, 2)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(8);

		ts.assertValues(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void accumulatorThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .scan((a, b) -> {
			    throw new RuntimeException("forced failure");
		    })
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertNotComplete()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure");
	}

	@Test
	public void accumulatorReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .scan((a, b) -> null)
		    .subscribe(ts);

		ts.assertValues(1)
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void onNextAndCancelRaceDontPassNullToAccumulator() {
		AtomicBoolean accumulatorCheck = new AtomicBoolean(true);
		final AssertSubscriber<Integer> testSubscriber = AssertSubscriber.create();

		FluxScan.ScanSubscriber<Integer> sub =
				new FluxScan.ScanSubscriber<>(testSubscriber, (accumulated, next) -> {
					if (accumulated == null || next == null) {
						accumulatorCheck.set(false);
					}
					return next;
				});

		sub.onSubscribe(Operators.emptySubscription());

		for (int i = 0; i < 1000; i++) {
			RaceTestUtils.race(sub::cancel, () -> sub.onNext(1));

			testSubscriber.assertNoError();
			assertThat(accumulatorCheck).as("no NPE due to onNext/cancel race in round %d", i).isTrue();
		}
	}

	@Test
	public void noRetainValueOnComplete() {
		final AssertSubscriber<Object> testSubscriber = AssertSubscriber.create();

		FluxScan.ScanSubscriber<Integer> sub =
				new FluxScan.ScanSubscriber<>(testSubscriber,
						(current, next) -> current + next);

		sub.onSubscribe(Operators.emptySubscription());

		sub.onNext(1);
		sub.onNext(2);
		Assertions.assertThat(sub.value).isEqualTo(3);

		sub.onComplete();
		Assertions.assertThat(sub.value).isNull();

		testSubscriber.assertNoError();
	}

	@Test
	public void noRetainValueOnError() {
		final AssertSubscriber<Object> testSubscriber = AssertSubscriber.create();

		FluxScan.ScanSubscriber<Integer> sub =
				new FluxScan.ScanSubscriber<>(testSubscriber,
						(current, next) -> current + next);

		sub.onSubscribe(Operators.emptySubscription());

		sub.onNext(1);
		sub.onNext(2);
		Assertions.assertThat(sub.value).isEqualTo(3);

		sub.onError(new RuntimeException("boom"));
		Assertions.assertThat(sub.value).isNull();

		testSubscriber.assertErrorMessage("boom");
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxScan<Integer> test = new FluxScan<>(parent, (v1, v2) -> v1);

		Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
    public void scanSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxScan.ScanSubscriber<Integer> test = new FluxScan.ScanSubscriber<>(actual, (i, j) -> i + j);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
        test.value = 5;
        Assertions.assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onComplete();
        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }
}

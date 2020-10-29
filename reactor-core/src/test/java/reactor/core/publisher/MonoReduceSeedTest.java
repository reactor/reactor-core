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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.publisher.ReduceOperatorTest;
import reactor.test.subscriber.AssertSubscriber;
import reactor.test.util.RaceTestUtils;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MonoReduceSeedTest extends ReduceOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.shouldHitDropNextHookAfterTerminate(false);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.reduce(item(0), (a, b) -> a))
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.reduce(item(0), (a, b) -> null)),

				scenario(f -> f.reduceWith(() -> null, (a, b) -> a + b))
		);
	}

	@Test
	public void sourceNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			new MonoReduceSeed<>(null, () -> 1, (a, b) -> (Integer) b);
		});
	}

	@Test
	public void supplierNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.never()
					.reduceWith(null, (a, b) -> b);
		});
	}

	@Test
	public void accumulatorNull() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.never()
					.reduceWith(() -> 1, null);
		});
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .reduceWith(() -> 0, (a, b) -> b)
		    .subscribe(ts);

		ts.assertValues(10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void normalBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Flux.range(1, 10)
		    .reduceWith(() -> 0, (a, b) -> b)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(10)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void supplierThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10).<Integer>reduceWith(() -> {
			throw new RuntimeException("forced failure");
		}, (a, b) -> b).subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(RuntimeException.class)
				.assertErrorWith(e -> assertThat(e).hasMessageContaining("forced failure"));
	}

	@Test
	public void accumulatorThrows() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10).reduceWith(() -> 0, (a, b) -> {
			throw new RuntimeException("forced failure");
		}).subscribe(ts);

		ts.assertNoValues()
				.assertNotComplete()
				.assertError(RuntimeException.class)
				.assertErrorWith(e -> assertThat(e).hasMessageContaining("forced failure"));
	}

	@Test
	public void supplierReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10).<Integer>reduceWith(() -> null, (a, b) -> b).subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void accumulatorReturnsNull() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 10)
		    .reduceWith(() -> 0, (a, b) -> null)
		    .subscribe(ts);

		ts.assertNoValues()
		  .assertNotComplete()
		  .assertError(NullPointerException.class);
	}

	@Test
	public void onNextAndCancelRace() {
		List<Integer> list = new ArrayList<>();
		final AssertSubscriber<Object> testSubscriber = AssertSubscriber.create();

		MonoReduceSeed.ReduceSeedSubscriber<Integer, List<Integer>> sub =
				new MonoReduceSeed.ReduceSeedSubscriber<>(testSubscriber,
						(l, next) -> {
							l.add(next);
							return l;
						}, list);

		sub.onSubscribe(Operators.emptySubscription());

		//the race alone _could_ previously produce a NPE
		RaceTestUtils.race(() -> sub.onNext(1), sub::cancel);
		testSubscriber.assertNoError();

		//to be even more sure, we try an onNext AFTER the cancel
		sub.onNext(2);
		testSubscriber.assertNoError();
	}

	@Test
	public void discardAccumulatedOnCancel() {
		final List<Object> discarded = new ArrayList<>();
		final AssertSubscriber<Object> testSubscriber = new AssertSubscriber<>(
				Operators.enableOnDiscard(Context.empty(), discarded::add));

		MonoReduceSeed.ReduceSeedSubscriber<Integer, Integer> sub =
				new MonoReduceSeed.ReduceSeedSubscriber<>(testSubscriber,
						(current, next) -> current + next, 0);

		sub.onSubscribe(Operators. emptySubscription());

		sub.onNext(1);
		assertThat(sub.value).isEqualTo(1);

		sub.cancel();
		testSubscriber.assertNoError();
		assertThat(discarded).containsExactly(1);
	}

	@Test
	public void discardOnNextAfterCancel() {
		final List<Object> discarded = new ArrayList<>();
		final AssertSubscriber<Object> testSubscriber = new AssertSubscriber<>(
				Operators.enableOnDiscard(Context.empty(), discarded::add));

		MonoReduceSeed.ReduceSeedSubscriber<Integer, Integer> sub =
				new MonoReduceSeed.ReduceSeedSubscriber<>(testSubscriber,
						(current, next) -> current + next, 0);

		sub.onSubscribe(Operators. emptySubscription());

		sub.cancel(); //discards seed

		assertThat(sub.value).isNull();

		sub.onNext(1); //discards passed value since cancelled

		testSubscriber.assertNoError();
		assertThat(discarded).containsExactly(0, 1);
		assertThat(sub.value).isNull();
	}

	@Test
	public void discardOnError() {
		final List<Object> discarded = new ArrayList<>();
		final AssertSubscriber<Object> testSubscriber = new AssertSubscriber<>(
				Operators.enableOnDiscard(Context.empty(), discarded::add));

		MonoReduceSeed.ReduceSeedSubscriber<Integer, Integer> sub =
				new MonoReduceSeed.ReduceSeedSubscriber<>(testSubscriber,
						(current, next) -> current + next, 0);

		sub.onSubscribe(Operators. emptySubscription());

		sub.onNext(1);
		assertThat(sub.value).isEqualTo(1);

		sub.onError(new RuntimeException("boom"));
		testSubscriber.assertErrorMessage("boom");
		assertThat(discarded).containsExactly(1);
	}

	@Test
	public void noRetainValueOnCancel() {
		final AssertSubscriber<Object> testSubscriber = AssertSubscriber.create();

		MonoReduceSeed.ReduceSeedSubscriber<Integer, Integer> sub =
				new MonoReduceSeed.ReduceSeedSubscriber<>(testSubscriber,
						(current, next) -> current + next, 0);

		sub.onSubscribe(Operators.emptySubscription());

		sub.onNext(1);
		sub.onNext(2);
		assertThat(sub.value).isEqualTo(3);

		sub.cancel();
		assertThat(sub.value).isNull();

		testSubscriber.assertNoError();
	}

	@Test
	public void noRetainValueOnError() {
		final AssertSubscriber<Object> testSubscriber = AssertSubscriber.create();

		MonoReduceSeed.ReduceSeedSubscriber<Integer, Integer> sub =
				new MonoReduceSeed.ReduceSeedSubscriber<>(testSubscriber,
						(current, next) -> current + next, 0);

		sub.onSubscribe(Operators.emptySubscription());

		sub.onNext(1);
		sub.onNext(2);
		assertThat(sub.value).isEqualTo(3);

		sub.onError(new RuntimeException("boom"));
		assertThat(sub.value).isNull();

		testSubscriber.assertErrorMessage("boom");
	}

	@Test
	public void scanOperator(){
	    MonoReduceSeed<Integer, Integer> test = new MonoReduceSeed<>(Flux.just(1, 2, 3), () -> 0, (a, b) -> b);

	    assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscriber() {
		CoreSubscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoReduceSeed.ReduceSeedSubscriber<Integer, String> test = new MonoReduceSeed.ReduceSeedSubscriber<>(
				actual, (s, i) -> s + i, "foo");
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

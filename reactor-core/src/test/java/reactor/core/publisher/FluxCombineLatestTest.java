/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.QueueSupplier;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxCombineLatestTest extends FluxOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.fusionMode(Fuseable.ASYNC)
		                     .prefetch(QueueSupplier.XS_BUFFER_SIZE);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(scenario(f -> Flux.combineLatest(o -> null,
				f,
				Flux.just(1))),

				scenario(f -> Flux.combineLatest(o -> {
					throw exception();
				}, f, Flux.just(1))),

				scenario(f -> Flux.combineLatest(() -> {
					throw exception();
				}, o -> (String) o[0])).fusionMode(Fuseable.NONE),

				scenario(f -> Flux.combineLatest(() -> null,
						o -> (String) o[0])).fusionMode(Fuseable.NONE),

				scenario(f -> Flux.combineLatest(() -> new Iterator<Publisher<?>>() {
					@Override
					public boolean hasNext() {
						throw exception();
					}

					@Override
					public Publisher<?> next() {
						return null;
					}
				}, o -> (String) o[0])).fusionMode(Fuseable.NONE),

				scenario(f -> Flux.combineLatest(() -> new Iterator<Publisher<?>>() {
					@Override
					public boolean hasNext() {
						return true;
					}

					@Override
					public Publisher<?> next() {
						throw exception();
					}
				}, o -> (String) o[0])).fusionMode(Fuseable.NONE),

				scenario(f -> Flux.combineLatest(() -> new Iterator<Publisher<?>>() {
					@Override
					public boolean hasNext() {
						return true;
					}

					@Override
					public Publisher<?> next() {
						return null;
					}
				}, o -> (String) o[0])).fusionMode(Fuseable.NONE));
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(scenario(f -> Flux.combineLatest(o -> (String) o[0],
				f)).prefetch(-1),

				scenario(f -> Flux.combineLatest(o -> (String) o[0],
						f,
						Flux.never())).shouldHitDropNextHookAfterTerminate(false));
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(scenario(f -> Flux.combineLatest(o -> (String) o[0],
				f)).prefetch(-1),

				scenario(f -> Flux.combineLatest(o -> (String) o[1],
						f,
						Flux.just(item(0), item(1), item(2)),
						Flux.just(item(0), item(1), item(2))))
						.receiveValues(item(2), item(2), item(2)),

				scenario(f -> Flux.combineLatest(o -> (String) o[2],
						1,
						f,
						Flux.just(item(0), item(0), item(0)),
						Flux.just(item(0), item(0), item(0))))
						.prefetch(1)
						.receiveValues(item(0), item(0), item(0))
		);
	}

	@Test
	public void singleSourceIsMapped() {

		AssertSubscriber<String> ts = AssertSubscriber.create();

		Flux.combineLatest(a -> a[0].toString(), Flux.just(1))
		    .subscribe(ts);

		ts.assertValues("1")
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void iterableSingleSourceIsMapped() {

		AssertSubscriber<String> ts = AssertSubscriber.create();

		Flux.combineLatest(Collections.singleton(Flux.just(1)), a -> a[0].toString())
		    .subscribe(ts);

		ts.assertValues("1")
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void fused() {
		DirectProcessor<Integer> dp1 = DirectProcessor.create();
		DirectProcessor<Integer> dp2 = DirectProcessor.create();

		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		ts.requestedFusionMode(Fuseable.ANY);

		Flux.combineLatest(dp1, dp2, (a, b) -> a + b)
		    .subscribe(ts);

		dp1.onNext(1);
		dp1.onNext(2);

		dp2.onNext(10);
		dp2.onNext(20);
		dp2.onNext(30);

		dp1.onNext(3);

		dp1.onComplete();
		dp2.onComplete();

		ts.assertFuseableSource()
		  .assertFusionMode(Fuseable.ASYNC)
		  .assertValues(12, 22, 32, 33);
	}

	@Test
	public void combineLatest() {
		StepVerifier.create(Flux.combineLatest(obj -> (int) obj[0], Flux.just(1)))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void combineLatestEmpty() {
		StepVerifier.create(Flux.combineLatest(obj -> (int) obj[0]))
		            .verifyComplete();
	}

	@Test
	public void combineLatestHide() {
		StepVerifier.create(Flux.combineLatest(obj -> (int) obj[0],
				Flux.just(1)
				    .hide()))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void combineLatest2() {
		StepVerifier.create(Flux.combineLatest(Flux.just(1), Flux.just(2), (a, b) -> a))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void combineLatest3() {
		StepVerifier.create(Flux.combineLatest(Flux.just(1),
				Flux.just(2),
				Flux.just(3),
				obj -> (int) obj[0]))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void combineLatest4() {
		StepVerifier.create(Flux.combineLatest(Flux.just(1),
				Flux.just(2),
				Flux.just(3),
				Flux.just(4),
				obj -> (int) obj[0]))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void combineLatest5() {
		StepVerifier.create(Flux.combineLatest(Flux.just(1),
				Flux.just(2),
				Flux.just(3),
				Flux.just(4),
				Flux.just(5),
				obj -> (int) obj[0]))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void combineLatest6() {
		StepVerifier.create(Flux.combineLatest(Flux.just(1),
				Flux.just(2),
				Flux.just(3),
				Flux.just(4),
				Flux.just(5),
				Flux.just(6),
				obj -> (int) obj[0]))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void scanMain() {
		Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);

		FluxCombineLatest.CombineLatestCoordinator<String, Integer> test = new FluxCombineLatest.CombineLatestCoordinator<>(
				actual, arr -> { throw new IllegalStateException("boomArray");}, 123, QueueSupplier.<FluxCombineLatest.SourceAndArray>one().get(), 456);
		test.request(2L);
		test.error = new IllegalStateException("boom"); //most straightforward way to set it as otherwise it is drained

		assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(2L);
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();

		assertThat(test.scan(Scannable.ThrowableAttr.ERROR)).isSameAs(test.error);

		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		test.innerComplete(1);
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
	}

	@Test
	public void scanInner() {
		Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxCombineLatest.CombineLatestCoordinator<String, Integer> main = new FluxCombineLatest.CombineLatestCoordinator<>(
				actual, arr -> arr.length, 123, QueueSupplier.<FluxCombineLatest.SourceAndArray>one().get(), 456);

		FluxCombineLatest.CombineLatestInner<String> test = new FluxCombineLatest.CombineLatestInner<>(main, 1, 789);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(789);
		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(main);

		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}
}

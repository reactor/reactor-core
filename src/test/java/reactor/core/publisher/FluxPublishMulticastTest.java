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
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.QueueSupplier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.publisher.Flux.range;
import static reactor.core.publisher.Flux.zip;

public class FluxPublishMulticastTest extends FluxOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.prefetch(QueueSupplier.SMALL_BUFFER_SIZE)
		                     .fusionModeThreadBarrier(Fuseable.ANY);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorError() {
		return Arrays.asList(
				scenario(f -> f.publish(p -> {
					throw exception();
				})),

				scenario(f -> f.publish(p -> null))
					,

				scenario(f -> f.publish(p -> Flux.error(exception()))),

				scenario(f -> f.publish(p -> Flux.just(item(0), null)))
						.fusionMode(Fuseable.SYNC)
						.receiveValues(item(0))
		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_operatorSuccess() {
		return Arrays.asList(
				scenario(f -> f.publish(p -> p)),

				scenario(f -> f.publish(p -> Flux.just("test", "test1", "test2")))
						.fusionMode(Fuseable.SYNC),

				scenario(f -> f.publish(p -> p.subscribeWith(UnicastProcessor.create()), 256))
						.fusionMode(Fuseable.ASYNC),

				scenario(f -> f.publish(p -> Flux.empty()))
						.receiverEmpty(),

				scenario(f -> f.publish(p -> p, 1))
						.prefetch(1),

				scenario(f -> f.publish(p -> p, Integer.MAX_VALUE))
						.prefetch(Integer.MAX_VALUE)

		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(scenario(f -> f.publish(p -> p)),

				scenario(f -> f.publish(p ->
						p.subscribeWith(UnicastProcessor.create())))
						.fusionMode(Fuseable.ASYNC),

				scenario(f -> f.publish(p -> p))
						.shouldHitDropNextHookAfterTerminate(false),

				scenario(f -> Flux.never().publish(p -> {
					Disposable d = p.subscribe();
					Disposable d2 = p.subscribe();
					d.dispose();
					d2.dispose();

					return f;
				})).shouldHitDropNextHookAfterTerminate(false)

		);
	}

	@Test(expected = IllegalArgumentException.class)
	public void failPrefetch(){
		Flux.never()
	        .publish(f -> f, -1);
	}

	@Test
	public void subsequentSum() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		range(1, 5).publish(o -> zip((Object[] a) -> (Integer) a[0] + (Integer) a[1], o, o.skip(1)))
		           .subscribe(ts);

		ts.assertValues(1 + 2, 2 + 3, 3 + 4, 4 + 5)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void subsequentSumHidden() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		range(1, 5).hide()
		           .publish(o -> zip((Object[] a) -> (Integer) a[0] + (Integer) a[1], o, o
				           .skip(1)))
		           .subscribe(ts);

		ts.assertValues(1 + 2, 2 + 3, 3 + 4, 4 + 5)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void subsequentSumAsync() {

		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		UnicastProcessor<Integer> up =
				UnicastProcessor.create(QueueSupplier.<Integer>get(16).get());

		up.publish(o -> zip((Object[] a) -> (Integer) a[0] + (Integer) a[1], o, o.skip(1)))
		  .subscribe(ts);

		up.onNext(1);
		up.onNext(2);
		up.onNext(3);
		up.onNext(4);
		up.onNext(5);
		up.onComplete();

		ts.assertValues(1 + 2, 2 + 3, 3 + 4, 4 + 5)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void cancelComposes() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		EmitterProcessor<Integer> sp = EmitterProcessor.create();

		sp.publish(o -> Flux.<Integer>never())
		  .subscribe(ts);

		Assert.assertTrue("Not subscribed?", sp.downstreamCount() != 0);

		ts.cancel();

		Assert.assertFalse("Still subscribed?", sp.downstreamCount() == 0);
	}

	@Test
	public void cancelComposes2() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		EmitterProcessor<Integer> sp = EmitterProcessor.create();

		sp.publish(o -> Flux.<Integer>empty())
		  .subscribe(ts);

		Assert.assertFalse("Still subscribed?", sp.downstreamCount() == 0);
	}

	@Test
	public void pairWise() {
		AssertSubscriber<Tuple2<Integer, Integer>> ts = AssertSubscriber.create();

		range(1, 9).transform(o -> zip(o, o.skip(1)))
		           .subscribe(ts);

		ts.assertValues(Tuples.of(1, 2),
				Tuples.of(2, 3),
				Tuples.of(3, 4),
				Tuples.of(4, 5),
				Tuples.of(5, 6),
				Tuples.of(6, 7),
				Tuples.of(7, 8),
				Tuples.of(8, 9))
		  .assertComplete();
	}

	@Test
	public void innerCanFuse() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		ts.requestedFusionMode(Fuseable.ANY);

		Flux.never()
		    .publish(o -> range(1, 5))
		    .subscribe(ts);

		ts.assertFuseableSource()
		  .assertFusionMode(Fuseable.SYNC)
		  .assertValues(1, 2, 3, 4, 5)
		  .assertComplete()
		  .assertNoError();
	}

	@Test
	public void suppressedSubscriber() {
		Subscriber<Integer> s = new Subscriber<Integer>() {
			@Override
			public void onSubscribe(
					Subscription s) {
				s.request(Long.MAX_VALUE);
			}

			@Override
			public void onNext(
					Integer integer) {

			}

			@Override
			public void onError(
					Throwable t) {

			}

			@Override
			public void onComplete() {

			}
		};

		FluxPublishMulticast.CancelMulticaster<Integer> sfs = new FluxPublishMulticast.CancelMulticaster<>(s, null);


		assertThat(sfs.size()).isEqualTo(0);
		assertThat(sfs.isEmpty()).isFalse();
		assertThat(sfs.poll()).isNull();
		assertThat(sfs.requestFusion(Fuseable.ANY)).isEqualTo(Fuseable.NONE);

		sfs.clear(); //NOOP
	}
}

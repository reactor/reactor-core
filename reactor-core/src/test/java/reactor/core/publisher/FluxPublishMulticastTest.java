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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.FluxOperatorTest;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Fail.fail;
import static reactor.core.publisher.Flux.range;
import static reactor.core.publisher.Flux.zip;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class FluxPublishMulticastTest extends FluxOperatorTest<String, String> {

	@Override
	protected Scenario<String, String> defaultScenarioOptions(Scenario<String, String> defaultOptions) {
		return defaultOptions.prefetch(Queues.SMALL_BUFFER_SIZE)
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

				scenario(f -> f.publish(p -> {
					//TODO double check this Sink usage
					Sinks.Many<String> sink = Sinks.unsafe().many().unicast().onBackpressureBuffer();
					p.subscribe(v -> sink.emitNext(v, FAIL_FAST),
							e -> sink.emitError(e, FAIL_FAST),
							() -> sink.emitComplete(FAIL_FAST));
					return sink.asFlux();
				}, 256))
						.fusionMode(Fuseable.ASYNC),

				scenario(f -> f.publish(p -> p, 1))
						.prefetch(1),

				scenario(f -> f.publish(p -> p, Integer.MAX_VALUE))
						.prefetch(Integer.MAX_VALUE)

		);
	}

	@Override
	protected List<Scenario<String, String>> scenarios_errorFromUpstreamFailure() {
		return Arrays.asList(scenario(f -> f.publish(p -> p)),

				scenario(f -> f.publish(p -> {
					//TODO double check this Sink usage
					Sinks.Many<String> sink = Sinks.unsafe().many().unicast().onBackpressureBuffer();
					p.subscribe(v -> sink.emitNext(v, FAIL_FAST),
							e -> sink.emitError(e, FAIL_FAST),
							() -> sink.emitComplete(FAIL_FAST));
					return sink.asFlux();
				}))
						.fusionMode(Fuseable.ASYNC),

				scenario(f -> f.publish(p -> p))
						.shouldHitDropNextHookAfterTerminate(false),

				scenario(f -> Flux.never().publish(p -> {
					Disposable d = p.subscribe();
					Disposable d2 = p.subscribe();
					d.dispose();
					d2.dispose();

					return f;
				})).shouldHitDropErrorHookAfterTerminate(false)
				   .shouldHitDropNextHookAfterTerminate(false)

		);
	}

	@Test
	public void failPrefetch() {
		assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> {
			Flux.never()
					.publish(f -> f, -1);
		});
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

		Sinks.Many<Integer> up =
				Sinks.unsafe().many().unicast().onBackpressureBuffer(Queues.<Integer>get(16).get());

		up.asFlux()
		  .publish(o -> zip((Object[] a) -> (Integer) a[0] + (Integer) a[1], o, o.skip(1)))
		  .subscribe(ts);

		up.emitNext(1, FAIL_FAST);
		up.emitNext(2, FAIL_FAST);
		up.emitNext(3, FAIL_FAST);
		up.emitNext(4, FAIL_FAST);
		up.emitNext(5, FAIL_FAST);
		up.emitComplete(FAIL_FAST);

		ts.assertValues(1 + 2, 2 + 3, 3 + 4, 4 + 5)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void cancelComposes() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp = Sinks.many().multicast().onBackpressureBuffer();

		sp.asFlux()
		  .publish(o -> Flux.<Integer>never())
		  .subscribe(ts);

		assertThat(sp.currentSubscriberCount()).as("subscribed").isPositive();

		ts.cancel();

		assertThat(sp.currentSubscriberCount()).as("still subscribed").isZero();
	}

	@Test
	public void cancelComposes2() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp = Sinks.many().multicast().onBackpressureBuffer();

		sp.asFlux()
		  .publish(o -> Flux.<Integer>empty())
		  .subscribe(ts);

		assertThat(sp.currentSubscriberCount()).as("still subscribed").isZero();
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
		CoreSubscriber<Integer> s = new CoreSubscriber<Integer>() {
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

	@Test
	public void syncCancelBeforeComplete() {
	    assertThat(Flux.just(Flux.just(1).publish(v -> v)).flatMap(v -> v).blockLast()).isEqualTo(1);
	}

    @Test
    public void normalCancelBeforeComplete() {
        assertThat(Flux.just(Flux.just(1).hide().publish(v -> v)).flatMap(v -> v).blockLast()).isEqualTo(1);
    }

    @Test
    public void scanOperator(){
    	Flux<Integer> parent = Flux.just(1);
		FluxPublishMulticast<Integer, Integer> test = new FluxPublishMulticast<>(parent, v -> v, 123, Queues.one());

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
    	assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }

	@Test
    public void scanMulticaster() {
        FluxPublishMulticast.FluxPublishMulticaster<Integer> test =
        		new FluxPublishMulticast.FluxPublishMulticaster<>(123, Queues.<Integer>unbounded(), Context.empty());
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(123);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
        assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(0);
        test.queue.add(1);
        assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
        test.error = new IllegalArgumentException("boom");
        assertThat(test.scan(Scannable.Attr.ERROR)).isSameAs(test.error);
        test.onComplete();
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.terminate();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

	@Test
    public void scanMulticastInner() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxPublishMulticast.FluxPublishMulticaster<Integer> parent =
        		new FluxPublishMulticast.FluxPublishMulticaster<>(123, Queues.<Integer>unbounded(), Context.empty());
        FluxPublishMulticast.PublishMulticastInner<Integer> test =
        		new FluxPublishMulticast.PublishMulticastInner<>(parent, actual);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        test.request(789);
        assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(789);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

	@Test
    public void scanCancelMulticaster() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxPublishMulticast.FluxPublishMulticaster<Integer> parent =
        		new FluxPublishMulticast.FluxPublishMulticaster<>(123, Queues.<Integer>unbounded(), Context.empty());
        FluxPublishMulticast.CancelMulticaster<Integer> test =
        		new FluxPublishMulticast.CancelMulticaster<>(actual, parent);
        Subscription sub = Operators.emptySubscription();
        test.onSubscribe(sub);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(sub);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }

	@Test
    public void scanCancelFuseableMulticaster() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxPublishMulticast.FluxPublishMulticaster<Integer> parent =
        		new FluxPublishMulticast.FluxPublishMulticaster<>(123, Queues.<Integer>unbounded(), Context.empty());
        FluxPublishMulticast.CancelFuseableMulticaster<Integer> test =
        		new FluxPublishMulticast.CancelFuseableMulticaster<>(actual, parent);
        Subscription sub = Operators.emptySubscription();
        test.onSubscribe(sub);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(sub);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
    }

    @Test
	public void gh870() throws Exception {
	    CountDownLatch cancelled = new CountDownLatch(1);
	    StepVerifier.create(Flux.<Integer>create(sink -> {
		    int i = 0;
		    sink.onCancel(cancelled::countDown);
		    try {
			    while (true) {
				    sink.next(i++);
				    Thread.sleep(1);
				    if (sink.isCancelled()) {
					    break;
				    }
			    }
		    } catch (InterruptedException e) {
			    sink.error(e);
		    }
	    })
//                    .doOnCancel(() -> System.out.println("cancel 2"))
			    .publish(Function.identity())
//                    .doOnCancel(() -> System.out.println("cancel 1"))
			    .take(5))
	                .expectNextCount(5)
	                .verifyComplete();

	    if (!cancelled.await(5, TimeUnit.SECONDS)) {
		    fail("Flux.create() did not receive cancellation signal");
	    }
    }
}

/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.AutoDisposingExtension;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.Logger;
import reactor.util.Loggers;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

class FluxSwitchMapNoPrefetchTest {

	private static final Logger LOGGER = Loggers.getLogger(FluxSwitchMapNoPrefetchTest.class);

	@RegisterExtension
	AutoDisposingExtension afterTest = new AutoDisposingExtension();

	@Test
	@Timeout(value = 2, unit = TimeUnit.MINUTES)
	@Tag("slow")
	// test for issue https://github.com/reactor/reactor-core/issues/2554
	public void test2596() {
		final Scheduler scheduler = afterTest.autoDispose(Schedulers.newSingle("test"));
		final Scheduler scheduler2 = afterTest.autoDispose(Schedulers.newParallel("test2"));
		Flux<Integer> integerFlux = Flux.defer(() ->  {
			return Flux.range(0, 20)
			    .hide()
			    .publishOn(Schedulers.single(), 1)
			    .switchMap(s ->  {
				    return Flux.range(s * 20, 20)
				               .hide()
				               .publishOn(scheduler, 1);
			    })
			    .publishOn(scheduler2, 1)
			    .doOnNext(new Consumer<Integer>() {
				    int last = -1;

				    @Override
				    public void accept(Integer next) {
					    final int last = this.last;
					    this.last = next;

					    if (last >= next && LOGGER.isDebugEnabled()) {
							LOGGER.debug("last: " + last + "; next: " + next);
					    }
				    }
			    })
	           .onErrorResume(e -> e instanceof RejectedExecutionException ? Mono.empty() : Mono.error(e));
		});

		for (int i = 0; i < 100000; i++) {
			StepVerifier.create(integerFlux)
			            .expectNextMatches(new Predicate<Integer>() {
			            	int last = -1;
				            @Override
				            public boolean test(Integer next) {
					            final boolean result = last < next;
					            last = next;
					            return result;
				            }
			            })
			            .thenConsumeWhile(a -> true)
			            .verifyComplete();
		}
	}

	@Test
	public void noswitch() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp2 = Sinks.unsafe().many().multicast().directBestEffort();

		sp1.asFlux()
		   .switchMap(v -> sp2.asFlux())
		   .subscribe(ts);

		sp1.emitNext(1, FAIL_FAST);

		sp2.emitNext(10, FAIL_FAST);
		sp2.emitNext(20, FAIL_FAST);
		sp2.emitNext(30, FAIL_FAST);
		sp2.emitNext(40, FAIL_FAST);
		sp2.emitComplete(FAIL_FAST);

		ts.assertValues(10, 20, 30, 40)
		  .assertNoError()
		  .assertNotComplete();

		sp1.emitComplete(FAIL_FAST);

		ts.assertValues(10, 20, 30, 40)
		  .assertNoError()
		  .assertComplete();

	}

	@Test
	public void noswitchBackpressured() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create(0);

		Sinks.Many<Integer> sp1 = Sinks.unsafe().many().replay().all();
		Sinks.Many<Integer> sp2 = Sinks.unsafe().many().replay().all();

		sp1.asFlux()
		   .switchMap(v -> sp2.asFlux())
		   .subscribe(ts);

		sp1.emitNext(1, FAIL_FAST);

		sp2.emitNext(10, FAIL_FAST);
		sp2.emitNext(20, FAIL_FAST);
		sp2.emitNext(30, FAIL_FAST);
		sp2.emitNext(40, FAIL_FAST);
		sp2.emitComplete(FAIL_FAST);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(10, 20)
		  .assertNoError()
		  .assertNotComplete();

		sp1.emitComplete(FAIL_FAST);

		ts.assertValues(10, 20)
		  .assertNoError()
		  .assertNotComplete();

		ts.request(2);

		ts.assertValues(10, 20, 30, 40)
		  .assertNoError()
		  .assertComplete();

	}

	@Test
	public void doswitch() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp2 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp3 = Sinks.unsafe().many().multicast().directBestEffort();

		sp1.asFlux()
		   .switchMap(v -> v == 1 ? sp2.asFlux() : sp3.asFlux())
		   .subscribe(ts);

		sp1.emitNext(1, FAIL_FAST);

		sp2.emitNext(10, FAIL_FAST);
		sp2.emitNext(20, FAIL_FAST);

		sp1.emitNext(2, FAIL_FAST);

		assertThat(sp2.currentSubscriberCount()).as("sp2 has subscriber").isZero();

		sp2.emitNext(30, FAIL_FAST);
		sp3.emitNext(300, FAIL_FAST);
		sp2.emitNext(40, FAIL_FAST);
		sp3.emitNext(400, FAIL_FAST);
		sp2.emitComplete(FAIL_FAST);
		sp3.emitComplete(FAIL_FAST);

		ts.assertValues(10, 20, 300, 400)
		  .assertNoError()
		  .assertNotComplete();

		sp1.emitComplete(FAIL_FAST);

		ts.assertValues(10, 20, 300, 400)
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void mainCompletesBefore() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp2 = Sinks.unsafe().many().multicast().directBestEffort();

		sp1.asFlux().switchMap(v -> sp2.asFlux())
		   .subscribe(ts);

		sp1.emitNext(1, FAIL_FAST);
		sp1.emitComplete(FAIL_FAST);

		ts.assertNoValues()
		  .assertNoError()
		  .assertNotComplete();

		sp2.emitNext(10, FAIL_FAST);
		sp2.emitNext(20, FAIL_FAST);
		sp2.emitNext(30, FAIL_FAST);
		sp2.emitNext(40, FAIL_FAST);
		sp2.emitComplete(FAIL_FAST);

		ts.assertValues(10, 20, 30, 40)
		  .assertNoError()
		  .assertComplete();

	}

	@Test
	public void mainError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp2 = Sinks.unsafe().many().multicast().directBestEffort();

		sp1.asFlux().switchMap(v -> sp2.asFlux())
		   .subscribe(ts);

		sp1.emitNext(1, FAIL_FAST);
		sp1.emitError(new RuntimeException("forced failure"), FAIL_FAST);

		sp2.emitNext(10, FAIL_FAST);
		sp2.emitNext(20, FAIL_FAST);
		sp2.emitNext(30, FAIL_FAST);
		sp2.emitNext(40, FAIL_FAST);
		sp2.emitComplete(FAIL_FAST);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void innerError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.unsafe().many().multicast().directBestEffort();
		Sinks.Many<Integer> sp2 = Sinks.unsafe().many().multicast().directBestEffort();

		sp1.asFlux().switchMap(v -> sp2.asFlux())
		   .subscribe(ts);

		sp1.emitNext(1, FAIL_FAST);

		sp2.emitNext(10, FAIL_FAST);
		sp2.emitNext(20, FAIL_FAST);
		sp2.emitNext(30, FAIL_FAST);
		sp2.emitNext(40, FAIL_FAST);
		sp2.emitError(new RuntimeException("forced failure"), FAIL_FAST);

		ts.assertValues(10, 20, 30, 40)
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();

		assertThat(sp1.currentSubscriberCount()).as("sp1 has subscriber").isZero();
		assertThat(sp2.currentSubscriberCount()).as("sp2 has subscriber").isZero();
	}

	@Test
	public void mapperThrows() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.unsafe().many().multicast().directBestEffort();

		sp1.asFlux()
		   .switchMap(v -> {
			   throw new RuntimeException("forced failure");
		   })
		   .subscribe(ts);

		sp1.emitNext(1, FAIL_FAST);

		ts.assertNoValues()
		  .assertError(RuntimeException.class)
		  .assertErrorMessage("forced failure")
		  .assertNotComplete();
	}

	@Test
	public void mapperReturnsNull() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Sinks.Many<Integer> sp1 = Sinks.unsafe().many().multicast().directBestEffort();

		sp1.asFlux()
		   .switchMap(v -> null)
		   .subscribe(ts);

		sp1.emitNext(1, FAIL_FAST);

		ts.assertNoValues()
		  .assertError(NullPointerException.class)
		  .assertNotComplete();
	}

	@Test
	public void switchOnNextDynamically() {
		StepVerifier.create(Flux.just(1, 2, 3)
		                        .switchMap(s -> Flux.range(s, 3)))
		            .expectNext(1, 2, 3, 2, 3, 4, 3, 4, 5)
		            .verifyComplete();
	}

	@Test
	public void switchOnNextDynamicallyOnNext() {
		Sinks.Many<Flux<Integer>> up = Sinks.many().unicast().onBackpressureBuffer();
		up.emitNext(Flux.range(1, 3), FAIL_FAST);
		up.emitNext(Flux.range(2, 3).concatWith(Mono.never()), FAIL_FAST);
		up.emitNext(Flux.range(4, 3), FAIL_FAST);
		up.emitComplete(FAIL_FAST);
		StepVerifier.create(Flux.switchOnNext(up.asFlux()))
		            .expectNext(1, 2, 3, 2, 3, 4, 4, 5, 6)
		            .verifyComplete();
	}


	@Test
	public void scanOperator(){
		Flux<String> parent = Flux.just("a", "bb", "ccc");
		FluxSwitchMapNoPrefetch<String, Integer> test = new FluxSwitchMapNoPrefetch<>(
			parent, s -> Flux.range(1, s.length()));

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	void scanMain() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxSwitchMapNoPrefetch.SwitchMapMain<Integer, Integer> test =
			new FluxSwitchMapNoPrefetch.SwitchMapMain<>(actual, i -> Mono.just(i));
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(0);
		test.requested = 35;
		assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35L);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		//TODO no scanning for error field
//		test.error = new IllegalStateException("boom");
//		assertThat(test.scan(Scannable.Attr.ERROR)).hasMessage("boom");
		test.onComplete();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
		//CANCELLED needs to be tested separately as it is polluted by TERMINATED
	}

	@Test
	void scanMainCancelled() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxSwitchMapNoPrefetch.SwitchMapMain<Integer, Integer> test =
			new FluxSwitchMapNoPrefetch.SwitchMapMain<>(actual, i -> Mono.just(i));
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	void scanInner() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxSwitchMapNoPrefetch.SwitchMapMain<Integer, Integer> main =
			new FluxSwitchMapNoPrefetch.SwitchMapMain<>(actual, i -> Mono.just(i));
		FluxSwitchMapNoPrefetch.SwitchMapInner<Integer, Integer> test = new FluxSwitchMapNoPrefetch.SwitchMapInner<Integer, Integer>(main, actual, 0);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(main);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(0);
	}

	@Test
	void scanInnerDetectsCancelledByParent() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxSwitchMapNoPrefetch.SwitchMapMain<Integer, Integer> main =
			new FluxSwitchMapNoPrefetch.SwitchMapMain<>(actual, i -> Mono.just(i));
		main.onNext(1); //this creates first inner, under test

		FluxSwitchMapNoPrefetch.SwitchMapInner<Integer, Integer> test = main.inner;
		assertThat(test).as("extracted inner").isNotNull();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		main.onNext(2); //this creates second inner and cancels first inner
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}
}

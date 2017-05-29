/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.test.StepVerifier;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static reactor.core.Fuseable.*;

public class FluxDoFinallyTest implements Consumer<SignalType> {

	volatile SignalType signalType;
	volatile int calls;

	@Before
	public void before() {
		signalType = null;
		calls = 0;
	}

	@Override
	public void accept(SignalType signalType) {
		this.signalType = signalType;
		this.calls++;
	}

	@Test
	public void normalJust() {
		StepVerifier.create(Flux.just(1).hide().doFinally(this))
		            .expectNoFusionSupport()
		            .expectNext(1)
		            .expectComplete()
		            .verify();

		assertEquals(1, calls);
		assertEquals(SignalType.ON_COMPLETE, signalType);
	}

	@Test
	public void normalEmpty() {
		StepVerifier.create(Flux.empty().doFinally(this))
		            .expectNoFusionSupport()
		            .expectComplete()
		            .verify();

		assertEquals(1, calls);
		assertEquals(SignalType.ON_COMPLETE, signalType);
	}

	@Test
	public void normalError() {
		StepVerifier.create(Flux.error(new IllegalArgumentException()).doFinally(this))
		            .expectNoFusionSupport()
		            .expectError(IllegalArgumentException.class)
		            .verify();

		assertEquals(1, calls);
		assertEquals(SignalType.ON_ERROR, signalType);
	}

	@Test
	public void normalCancel() {
		StepVerifier.create(Flux.range(1, 10).hide().doFinally(this).take(5))
		            .expectNoFusionSupport()
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify();

		assertEquals(1, calls);
		assertEquals(SignalType.CANCEL, signalType);
	}

	@Test
	public void normalTake() {
		StepVerifier.create(Flux.range(1, 5)
		                        .hide()
		                        .doFinally(this))
		            .expectNoFusionSupport()
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify();

		assertEquals(1, calls);
		assertEquals(SignalType.ON_COMPLETE, signalType);
	}

	@Test
	public void syncFused() {
		StepVerifier.create(Flux.range(1, 5).doFinally(this))
		            .expectFusion(SYNC)
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify();

		assertEquals(1, calls); assertEquals(SignalType.ON_COMPLETE, signalType);
	}

	@Test
	public void syncFusedThreadBarrier() {
		StepVerifier.create(Flux.range(1, 5).doFinally(this))
		            .expectFusion(SYNC | THREAD_BARRIER , NONE)
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify();

		assertEquals(1, calls);
		assertEquals(SignalType.ON_COMPLETE, signalType);
	}

	@Test
	public void asyncFused() {
		UnicastProcessor<Integer> up = UnicastProcessor.create();
		up.onNext(1);
		up.onNext(2);
		up.onNext(3);
		up.onNext(4);
		up.onNext(5);
		up.onComplete();

		StepVerifier.create(up.doFinally(this))
		            .expectFusion(ASYNC)
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify();

		assertEquals(1, calls);
		assertEquals(SignalType.ON_COMPLETE, signalType);
	}

	@Test
	public void asyncFusedThreadBarrier() {
		UnicastProcessor<Integer> up = UnicastProcessor.create();
		up.onNext(1);
		up.onNext(2);
		up.onNext(3);
		up.onNext(4);
		up.onNext(5);
		up.onComplete();

		StepVerifier.create(up.doFinally(this))
		            .expectFusion(ASYNC | THREAD_BARRIER, NONE)
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify();

		assertEquals(1, calls);
		assertEquals(SignalType.ON_COMPLETE, signalType);
	}

	@Test
	public void normalJustConditional() {
		StepVerifier.create(Flux.just(1)
		                        .hide()
		                        .doFinally(this)
		                        .filter(i -> true))
		            .expectNoFusionSupport()
		            .expectNext(1)
		            .expectComplete()
		            .verify();

		assertEquals(1, calls);
		assertEquals(SignalType.ON_COMPLETE, signalType);
	}

	@Test
	public void normalEmptyConditional() {
		StepVerifier.create(Flux.empty()
		                        .hide()
		                        .doFinally(this)
		                        .filter(i -> true))
		            .expectNoFusionSupport()
		            .expectComplete()
		            .verify();

		assertEquals(1, calls);
		assertEquals(SignalType.ON_COMPLETE, signalType);
	}

	@Test
	public void normalErrorConditional() {
		StepVerifier.create(Flux.error(new IllegalArgumentException())
		                        .hide()
		                        .doFinally(this)
		                        .filter(i -> true))
		            .expectNoFusionSupport()
		            .expectError(IllegalArgumentException.class)
		            .verify();

		assertEquals(1, calls);
		assertEquals(SignalType.ON_ERROR, signalType);
	}

	@Test
	public void normalCancelConditional() {
		StepVerifier.create(Flux.range(1, 10)
		                        .hide()
		                        .doFinally(this)
		                        .filter(i -> true)
		                        .take(5))
		            .expectNoFusionSupport()
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify();

		assertEquals(1, calls);
		assertEquals(SignalType.CANCEL, signalType);
	}

	@Test
	public void normalTakeConditional() {
		StepVerifier.create(Flux.range(1, 5)
		                        .hide()
		                        .doFinally(this)
		                        .filter(i -> true))
		            .expectNoFusionSupport()
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify();

		assertEquals(1, calls);
		assertEquals(SignalType.ON_COMPLETE, signalType);
	}

	@Test
	public void syncFusedConditional() {
		StepVerifier.create(Flux.range(1, 5)
		                        .doFinally(this)
		                        .filter(i -> true))
		            .expectFusion(SYNC)
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify();

		assertEquals(1, calls);
		assertEquals(SignalType.ON_COMPLETE, signalType);
	}

	@Test
	public void syncFusedThreadBarrierConditional() {
		StepVerifier.create(Flux.range(1, 5)
		                        .doFinally(this)
		                        .filter(i -> true))
		            .expectFusion(SYNC | THREAD_BARRIER, NONE)
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify();

		assertEquals(1, calls);
		assertEquals(SignalType.ON_COMPLETE, signalType);
	}

	@Test
	public void asyncFusedConditional() {
		UnicastProcessor<Integer> up = UnicastProcessor.create();
		up.onNext(1);
		up.onNext(2);
		up.onNext(3);
		up.onNext(4);
		up.onNext(5);
		up.onComplete();

		StepVerifier.create(up.doFinally(this)
		                      .filter(i -> true))
		            .expectFusion(ASYNC)
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify();

		assertEquals(1, calls);
		assertEquals(SignalType.ON_COMPLETE, signalType);
	}

	@Test
	public void asyncFusedThreadBarrierConditional() {
		UnicastProcessor<Integer> up = UnicastProcessor.create();
		up.onNext(1);
		up.onNext(2);
		up.onNext(3);
		up.onNext(4);
		up.onNext(5);
		up.onComplete();

		StepVerifier.create(up.doFinally(this)
		                      .filter(i -> true))
		            .expectFusion(ASYNC | THREAD_BARRIER, NONE)
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify();

		assertEquals(1, calls);
		assertEquals(SignalType.ON_COMPLETE, signalType);
	}

	@Test(expected = NullPointerException.class)
	public void nullCallback() {
		Flux.just(1).doFinally(null);
	}

	@Test
	public void callbackThrows() {
		try {
			StepVerifier.create(Flux.just(1)
			                        .doFinally(signal -> {
				                        throw new IllegalStateException();
			                        }))
			            .expectNext(1)
			            .expectComplete()
			            .verify();
		}
		catch (Throwable e) {
			Throwable _e = Exceptions.unwrap(e);
			assertNotSame(e, _e);
			assertThat(_e, is(instanceOf(IllegalStateException.class)));
		}
	}

	@Test
	public void callbackThrowsConditional() {
		try {
			StepVerifier.create(Flux.just(1)
			                        .doFinally(signal -> {
				                        throw new IllegalStateException();
			                        })
			                        .filter(i -> true))
			            .expectNext(1)
			            .expectComplete()
			            .verify();
		}
		catch (Throwable e) {
			Throwable _e = Exceptions.unwrap(e);
			assertNotSame(e, _e);
			assertThat(_e, is(instanceOf(IllegalStateException.class)));
		}
	}

	@Test
	public void severalInARowExecutedInReverseOrder() {
		Queue<String> finallyOrder = new ConcurrentLinkedDeque<>();

		Flux.just("b")
		    .hide()
		    .doFinally(s -> finallyOrder.offer("FIRST"))
		    .doFinally(s -> finallyOrder.offer("SECOND"))
		    .blockLast();

		Assertions.assertThat(finallyOrder)
		          .containsExactly("SECOND", "FIRST");
	}

	@Test
	public void scanSubscriber() {
		Subscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxDoFinally.DoFinallySubscriber<String> test = new FluxDoFinally.DoFinallySubscriber<>(actual, st -> {});
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
		Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

		Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
		Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}

	//TODO test multiple subscriptions?
}
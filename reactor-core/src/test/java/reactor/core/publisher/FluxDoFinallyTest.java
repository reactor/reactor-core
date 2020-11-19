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
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Consumer;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.test.util.LoggerUtils;
import reactor.test.StepVerifier;
import reactor.test.util.TestLogger;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.Fuseable.*;
import static reactor.core.publisher.Sinks.EmitFailureHandler.FAIL_FAST;

public class FluxDoFinallyTest implements Consumer<SignalType> {

	volatile SignalType signalType;
	volatile int calls;

	@BeforeEach
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

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.ON_COMPLETE);
	}

	@Test
	public void normalEmpty() {
		StepVerifier.create(Flux.empty().doFinally(this))
		            .expectNoFusionSupport()
		            .expectComplete()
		            .verify();

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.ON_COMPLETE);
	}

	@Test
	public void normalError() {
		StepVerifier.create(Flux.error(new IllegalArgumentException()).doFinally(this))
		            .expectNoFusionSupport()
		            .expectError(IllegalArgumentException.class)
		            .verify();

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.ON_ERROR);
	}

	@Test
	public void normalCancel() {
		StepVerifier.create(Flux.range(1, 10).hide().doFinally(this).take(5))
		            .expectNoFusionSupport()
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify();

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.CANCEL);
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

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.ON_COMPLETE);
	}

	@Test
	public void syncFused() {
		StepVerifier.create(Flux.range(1, 5).doFinally(this))
		            .expectFusion(SYNC)
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify();

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.ON_COMPLETE);
	}

	@Test
	public void syncFusedThreadBarrier() {
		StepVerifier.create(Flux.range(1, 5).doFinally(this))
		            .expectFusion(SYNC | THREAD_BARRIER , NONE)
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify();

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.ON_COMPLETE);
	}

	@Test
	public void asyncFused() {
		Sinks.Many<Integer> up = Sinks.many().unicast().onBackpressureBuffer();
		up.emitNext(1, FAIL_FAST);
		up.emitNext(2, FAIL_FAST);
		up.emitNext(3, FAIL_FAST);
		up.emitNext(4, FAIL_FAST);
		up.emitNext(5, FAIL_FAST);
		up.emitComplete(FAIL_FAST);

		StepVerifier.create(up.asFlux().doFinally(this))
		            .expectFusion(ASYNC)
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify();

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.ON_COMPLETE);
	}

	@Test
	public void asyncFusedThreadBarrier() {
		Sinks.Many<Object> up = Sinks.many().unicast().onBackpressureBuffer();
		up.emitNext(1, FAIL_FAST);
		up.emitNext(2, FAIL_FAST);
		up.emitNext(3, FAIL_FAST);
		up.emitNext(4, FAIL_FAST);
		up.emitNext(5, FAIL_FAST);
		up.emitComplete(FAIL_FAST);

		StepVerifier.create(up.asFlux().doFinally(this))
		            .expectFusion(ASYNC | THREAD_BARRIER, NONE)
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify();

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.ON_COMPLETE);
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

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.ON_COMPLETE);
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

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.ON_COMPLETE);
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

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.ON_ERROR);
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

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.CANCEL);
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

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.ON_COMPLETE);
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

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.ON_COMPLETE);
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

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.ON_COMPLETE);
	}

	@Test
	public void asyncFusedConditional() {
		Sinks.Many<Object> up = Sinks.many().unicast().onBackpressureBuffer();
		up.emitNext(1, FAIL_FAST);
		up.emitNext(2, FAIL_FAST);
		up.emitNext(3, FAIL_FAST);
		up.emitNext(4, FAIL_FAST);
		up.emitNext(5, FAIL_FAST);
		up.emitComplete(FAIL_FAST);

		StepVerifier.create(up.asFlux().doFinally(this)
		                      .filter(i -> true))
		            .expectFusion(ASYNC)
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify();

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.ON_COMPLETE);
	}

	@Test
	public void asyncFusedThreadBarrierConditional() {
		Sinks.Many<Object> up = Sinks.many().unicast().onBackpressureBuffer();
		up.emitNext(1, FAIL_FAST);
		up.emitNext(2, FAIL_FAST);
		up.emitNext(3, FAIL_FAST);
		up.emitNext(4, FAIL_FAST);
		up.emitNext(5, FAIL_FAST);
		up.emitComplete(FAIL_FAST);

		StepVerifier.create(up.asFlux().doFinally(this)
		                      .filter(i -> true))
		            .expectFusion(ASYNC | THREAD_BARRIER, NONE)
		            .expectNext(1, 2, 3, 4, 5)
		            .expectComplete()
		            .verify();

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.ON_COMPLETE);
	}

	@Test
	public void nullCallback() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Flux.just(1).doFinally(null);
		});
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
			assertThat(_e).isNotSameAs(e);
			assertThat(_e).isInstanceOf(IllegalStateException.class);
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
			assertThat(_e).isNotSameAs(e);
			assertThat(_e).isInstanceOf(IllegalStateException.class);
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
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxDoFinally test = new FluxDoFinally<>(parent, v -> {});

		Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanFuseableOperator(){
		Flux<Integer> parent = Flux.just(1);
		FluxDoFinallyFuseable<Integer> test = new FluxDoFinallyFuseable<>(parent, s -> {});

		Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscriber() {
		CoreSubscriber<String> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxDoFinally.DoFinallySubscriber<String> test = new FluxDoFinally.DoFinallySubscriber<>(actual, st -> {});
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
		Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	//TODO test multiple subscriptions?

	@Test
	//see https://github.com/reactor/reactor-core/issues/951
	public void gh951_withoutConsumerInSubscribe() {
		List<String> events = new ArrayList<>();
		Mono.just(true)
		    .map(this::throwError)
		    .doOnError(e -> events.add("doOnError"))
		    .doFinally(any -> events.add("doFinally " + any.toString()))
		    .subscribe();

		Assertions.assertThat(events)
		          .as("subscribe without consumer: map_doOnError_doFinally")
		          .containsExactly("doOnError", "doFinally onError");

		events.clear();
		Mono.just(true)
		    .doFinally(any -> events.add("doFinally " + any.toString()))
		    .map(this::throwError)
		    .doOnError(e -> events.add("doOnError"))
		    .subscribe();

		Assertions.assertThat(events)
		          .as("subscribe without consumer: doFinally_map_doOnError")
		          .containsExactly("doFinally cancel", "doOnError");

		events.clear();
		Mono.just(true)
		    .map(this::throwError)
		    .doFinally(any -> events.add("doFinally " + any.toString()))
		    .doOnError(e -> events.add("doOnError"))
		    .subscribe();

		Assertions.assertThat(events)
		          .as("subscribe without consumer:  map_doFinally_doOnError")
		          .containsExactly("doOnError", "doFinally onError");
	}

	@Test
	//see https://github.com/reactor/reactor-core/issues/951
	public void gh951_withConsumerInSubscribe() {
		List<String> events = new ArrayList<>();

		Mono.just(true)
		    .map(this::throwError)
		    .doOnError(e -> events.add("doOnError"))
		    .doFinally(any -> events.add("doFinally " + any.toString()))
		    .subscribe(v -> { }, e -> { });

		Assertions.assertThat(events)
		          .as("subscribe with consumer: map_doOnError_doFinally")
		          .containsExactly("doOnError", "doFinally onError");

		events.clear();
		Mono.just(true)
		    .doFinally(any -> events.add("doFinally " + any.toString()))
		    .map(this::throwError)
		    .doOnError(e -> events.add("doOnError"))
		    .subscribe(v -> { }, e -> { });

		Assertions.assertThat(events)
		          .as("subscribe with consumer: doFinally_map_doOnError")
		          .containsExactly("doFinally cancel", "doOnError");

		events.clear();
		Mono.just(true)
		    .map(this::throwError)
		    .doFinally(any -> events.add("doFinally " + any.toString()))
		    .doOnError(e -> events.add("doOnError"))
		    .subscribe(v -> { }, e -> { });

		Assertions.assertThat(events)
		          .as("subscribe with consumer: map_doFinally_doOnError")
		          .containsExactly("doOnError", "doFinally onError");
	}

	@Test
	//see https://github.com/reactor/reactor-core/issues/951
	public void gh951_withoutDoOnError() {
		TestLogger testLogger = new TestLogger();
		LoggerUtils.enableCaptureWith(testLogger);
		try {
			List<String> events = new ArrayList<>();

			Mono.just(true)
			    .map(this::throwError)
			    .doFinally(any -> events.add("doFinally " + any.toString()))
			    .subscribe();

			Assertions.assertThat(events)
			          .as("withoutDoOnError")
			          .containsExactly("doFinally onError");
			Assertions.assertThat(testLogger.getErrContent())
			          .contains("Operator called default onErrorDropped")
			          .contains("reactor.core.Exceptions$ErrorCallbackNotImplemented: java.lang.IllegalStateException: boom");
		}
		finally {
			LoggerUtils.disableCapture();
		}
	}

	private Boolean throwError(Boolean x) {
		throw new IllegalStateException("boom");
	}
}

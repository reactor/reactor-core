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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.Fuseable.SYNC;

/**
 * Note: as {@link MonoDoFinally} and {@link MonoDoFinallyFuseable} delegate to
 * subscribers in {@link FluxDoFinally}, these tests are kind of duplicates of
 * {@link FluxDoFinallyTest} and thus are less numerous.
 */
public class MonoDoFinallyTest implements Consumer<SignalType> {

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
		StepVerifier.create(Mono.just(1).hide().doFinally(this))
		            .expectNoFusionSupport()
		            .expectNext(1)
		            .expectComplete()
		            .verify();

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.ON_COMPLETE);
	}

	@Test
	public void normalEmpty() {
		StepVerifier.create(Mono.empty().doFinally(this))
		            .expectNoFusionSupport()
		            .expectComplete()
		            .verify();

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.ON_COMPLETE);
	}

	@Test
	public void normalError() {
		StepVerifier.create(Mono.error(new IllegalArgumentException()).doFinally(this))
		            .expectNoFusionSupport()
		            .expectError(IllegalArgumentException.class)
		            .verify();

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.ON_ERROR);
	}


	@Test
	public void normalCancel() {
		AtomicBoolean cancelCheck = new AtomicBoolean(false);

		StepVerifier.create(Mono.just(1).hide()
		                        .doOnCancel(() -> cancelCheck.set(true))
		                        .doFinally(this))
		            .expectNoFusionSupport()
		            .expectNext(1)
		            .thenCancel()
		            .verify();
		
		assertThat(calls).as("expected doFinally to be invoked exactly once").isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.CANCEL);
		assertThat(cancelCheck.get()).as("expected tested mono to be cancelled").isTrue();
	}

	@Test
	public void normalJustConditional() {
		StepVerifier.create(Mono.just(1)
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
	public void syncFused() {
		StepVerifier.create(Mono.just(1).doFinally(this))
		            .expectFusion(SYNC)
		            .expectNext(1)
		            .expectComplete()
		            .verify();

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.ON_COMPLETE);
	}

	@Test
	public void syncFusedConditional() {
		StepVerifier.create(Mono.just(1).doFinally(this).filter(i -> true))
		            .expectFusion(SYNC)
		            .expectNext(1)
		            .expectComplete()
		            .verify();

		assertThat(calls).isEqualTo(1);
		assertThat(signalType).isEqualTo(SignalType.ON_COMPLETE);
	}


	@Test
	public void nullCallback() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
			Mono.just(1).doFinally(null);
		});
	}

	@Test
	public void callbackThrows() {
		try {
			StepVerifier.create(Mono.just(1)
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
			StepVerifier.create(Mono.just(1)
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
		MonoDoFinally<String> test = new MonoDoFinally<>(Mono.just("foo"), this);

		Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanFuseableOperator(){
		MonoDoFinallyFuseable<String> test = new MonoDoFinallyFuseable<>(Mono.just("foo"), this);

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}

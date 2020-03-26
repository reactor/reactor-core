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
import org.junit.Before;
import org.junit.Test;

import reactor.core.Exceptions;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import static reactor.core.Fuseable.SYNC;

/**
 * Note: as {@link MonoDoFinally} and {@link MonoDoFinallyFuseable} delegate to
 * subscribers in {@link FluxDoFinally}, these tests are kind of duplicates of
 * {@link FluxDoFinallyTest} and thus are less numerous.
 */
public class MonoDoFinallyTest implements Consumer<SignalType> {

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
		StepVerifier.create(Mono.just(1).hide().doFinally(this))
		            .expectNoFusionSupport()
		            .expectNext(1)
		            .expectComplete()
		            .verify();

		assertEquals(1, calls);
		assertEquals(SignalType.ON_COMPLETE, signalType);
	}

	@Test
	public void normalEmpty() {
		StepVerifier.create(Mono.empty().doFinally(this))
		            .expectNoFusionSupport()
		            .expectComplete()
		            .verify();

		assertEquals(1, calls);
		assertEquals(SignalType.ON_COMPLETE, signalType);
	}

	@Test
	public void normalError() {
		StepVerifier.create(Mono.error(new IllegalArgumentException()).doFinally(this))
		            .expectNoFusionSupport()
		            .expectError(IllegalArgumentException.class)
		            .verify();

		assertEquals(1, calls);
		assertEquals(SignalType.ON_ERROR, signalType);
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
		
		assertEquals("expected doFinally to be invoked exactly once", 1, calls);
		assertEquals(SignalType.CANCEL, signalType);
		assertTrue("expected tested mono to be cancelled", cancelCheck.get());
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

		assertEquals(1, calls);
		assertEquals(SignalType.ON_COMPLETE, signalType);
	}

	@Test
	public void syncFused() {
		StepVerifier.create(Mono.just(1).doFinally(this))
		            .expectFusion(SYNC)
		            .expectNext(1)
		            .expectComplete()
		            .verify();

		assertEquals(1, calls); assertEquals(SignalType.ON_COMPLETE, signalType);
	}

	@Test
	public void syncFusedConditional() {
		StepVerifier.create(Mono.just(1).doFinally(this).filter(i -> true))
		            .expectFusion(SYNC)
		            .expectNext(1)
		            .expectComplete()
		            .verify();

		assertEquals(1, calls); assertEquals(SignalType.ON_COMPLETE, signalType);
	}


	@Test(expected = NullPointerException.class)
	public void nullCallback() {
		Mono.just(1).doFinally(null);
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
			assertNotSame(e, _e);
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
			assertNotSame(e, _e);
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

}
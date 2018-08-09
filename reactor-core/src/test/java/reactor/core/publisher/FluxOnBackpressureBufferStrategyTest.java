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

import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import static reactor.core.publisher.BufferOverflowStrategy.*;

public class FluxOnBackpressureBufferStrategyTest implements Consumer<String>,
                                                             BiFunction<Throwable, Object, Throwable> {

	private String droppedValue;
	private Object hookCapturedValue;
	private Throwable hookCapturedError;

	@Override
	public void accept(String s) {
		this.droppedValue = s;
	}

	@Override
	public Throwable apply(Throwable throwable, Object o) {
		this.hookCapturedValue = o;
		this.hookCapturedError = throwable;
		return throwable;
	}

	@Before
	public void before() {
		this.droppedValue = null;
		this.hookCapturedError = null;
		this.hookCapturedValue = null;
		Hooks.onOperatorError(this);
	}

	@After
	public void after() {
		Hooks.resetOnOperatorError();
	}


	@Test
	public void drop() {
		FluxProcessorSink<String> processor = Processors.directSink();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor.asFlux(), 2, this, DROP_LATEST);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.next("normal");
			            processor.next("over1");
			            processor.next("over2");
			            processor.next("over3");
			            processor.complete();
		            })
		            .expectNext("normal")
		            .thenAwait()
		            .thenRequest(3)
		            .expectNext("over1", "over2")
		            .expectComplete()
		            .verify();

		assertEquals("over3", droppedValue);
		assertNull("unexpected hookCapturedValue", hookCapturedValue);
		assertNull("unexpected hookCapturedError",hookCapturedError);
	}

	@Test
	public void dropOldest() {
		FluxProcessorSink<String> processor = Processors.directSink();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor.asFlux(), 2, this, DROP_OLDEST);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.next("normal");
			            processor.next("over1");
			            processor.next("over2");
			            processor.next("over3");
			            processor.complete();
		            })
		            .expectNext("normal")
		            .thenAwait()
		            .thenRequest(3)
		            .expectNext("over2", "over3")
		            .expectComplete()
		            .verify();

		assertEquals("over1", droppedValue);
		assertNull("unexpected hookCapturedValue",hookCapturedValue);
		assertNull("unexpected hookCapturedError",hookCapturedError);
	}

	@Test
	public void error() {
		FluxProcessorSink<String> processor = Processors.directSink();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor.asFlux(), 2, this, ERROR);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.next("normal");
			            processor.next("over1");
			            processor.next("over2");
			            processor.next("over3");
			            processor.complete();
		            })
		            .expectNext("normal")
		            .thenAwait()
		            .thenRequest(3)
		            .expectNext("over1", "over2")
		            .expectErrorMessage("The receiver is overrun by more signals than expected (bounded queue...)")
		            .verify();

		assertEquals("over3", droppedValue);
		assertEquals("over3", hookCapturedValue);
		assertTrue("unexpected hookCapturedError: " + hookCapturedError, hookCapturedError instanceof IllegalStateException);
	}

	@Test
	public void dropCallbackError() {
		FluxProcessorSink<String> processor = Processors.directSink();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor.asFlux(), 2, v -> { throw new IllegalArgumentException("boom"); },
				DROP_LATEST);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.next("normal");
			            processor.next("over1");
			            processor.next("over2");
			            processor.next("over3");
			            processor.complete();
		            })
		            .expectNext("normal")
		            .thenAwait()
		            .thenRequest(3)
		            .expectNext("over1", "over2")
		            .expectErrorMessage("boom")
		            .verify();

		assertNull("unexpected droppedValue", droppedValue);
		assertEquals("over3", hookCapturedValue);
		assertTrue("unexpected hookCapturedError: " + hookCapturedError, hookCapturedError instanceof IllegalArgumentException);
		assertEquals("boom", hookCapturedError.getMessage());
	}

	@Test
	public void dropOldestCallbackError() {
		FluxProcessorSink<String> processor = Processors.directSink();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor.asFlux(), 2, v -> { throw new IllegalArgumentException("boom"); },
				DROP_OLDEST);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.next("normal");
			            processor.next("over1");
			            processor.next("over2");
			            processor.next("over3");
			            processor.complete();
		            })
		            .expectNext("normal")
		            .thenAwait()
		            .thenRequest(3)
		            .expectNext("over2", "over3")
		            .expectErrorMessage("boom")
		            .verify();

		assertNull("unexpected droppedValue", droppedValue);
		assertEquals("over1", hookCapturedValue);
		assertTrue("unexpected hookCapturedError: " + hookCapturedError, hookCapturedError instanceof IllegalArgumentException);
		assertEquals("boom", hookCapturedError.getMessage());
	}

	@Test
	public void errorCallbackError() {
		FluxProcessorSink<String> processor = Processors.directSink();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor.asFlux(), 2, v -> { throw new IllegalArgumentException("boom"); },
				ERROR);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.next("normal");
			            processor.next("over1");
			            processor.next("over2");
			            processor.next("over3");
			            processor.complete();
		            })
		            .expectNext("normal")
		            .thenAwait()
		            .thenRequest(3)
		            .expectNext("over1", "over2")
		            .expectErrorMessage("boom")
		            .verify();

		assertNull("unexpected droppedValue", droppedValue);
		assertEquals("over3", hookCapturedValue);
		assertTrue("unexpected hookCapturedError: " + hookCapturedError, hookCapturedError instanceof IllegalArgumentException);
		assertEquals("boom", hookCapturedError.getMessage());
	}

	@Test
	public void noCallbackWithErrorStrategyOnErrorImmediately() {
		FluxProcessorSink<String> processor = Processors.directSink();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor.asFlux(), 2, null, ERROR);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.next("normal");
			            processor.next("over1");
			            processor.next("over2");
			            processor.next("over3");
			            processor.complete();
		            })
		            .expectNext("normal")
		            .thenAwait()
		            .thenRequest(1)
		            .expectErrorMessage("The receiver is overrun by more signals than expected (bounded queue...)")
		            .verify();

		assertNull("unexpected droppedValue", droppedValue);
		assertEquals("over3", hookCapturedValue);
		assertTrue("unexpected hookCapturedError: " + hookCapturedError, hookCapturedError instanceof IllegalStateException);
		assertEquals("The receiver is overrun by more signals than expected (bounded queue...)", hookCapturedError.getMessage());
	}

	@Test
	public void noCallbackWithDropStrategyNoError() {
		FluxProcessorSink<String> processor = Processors.directSink();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor.asFlux(), 2, null, DROP_LATEST);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.next("normal");
			            processor.next("over1");
			            processor.next("over2");
			            processor.next("over3");
			            processor.complete();
		            })
		            .expectNext("normal")
		            .thenAwait()
		            .thenRequest(3)
		            .expectNext("over1", "over2")
		            .expectComplete()
		            .verify();

		assertNull("unexpected droppedValue", droppedValue);
		assertNull("unexpected hookCapturedValue", hookCapturedValue);
		assertNull("unexpected hookCapturedError",hookCapturedError);
	}

	@Test
	public void noCallbackWithDropOldestStrategyNoError() {
		FluxProcessorSink<String> processor = Processors.directSink();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor.asFlux(), 2, null, DROP_OLDEST);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.next("normal");
			            processor.next("over1");
			            processor.next("over2");
			            processor.next("over3");
			            processor.complete();
		            })
		            .expectNext("normal")
		            .thenAwait()
		            .thenRequest(3)
		            .expectNext("over2", "over3")
		            .expectComplete()
		            .verify();

		assertNull("unexpected droppedValue", droppedValue);
		assertNull("unexpected hookCapturedValue",hookCapturedValue);
		assertNull("unexpected hookCapturedError",hookCapturedError);
	}

	@Test
	public void fluxOnBackpressureBufferStrategyNoCallback() {
		FluxProcessorSink<String> processor = Processors.directSink();

		StepVerifier.create(processor.asFlux().onBackpressureBuffer(2, DROP_OLDEST), 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.next("normal");
			            processor.next("over1");
			            processor.next("over2");
			            processor.next("over3");
			            processor.complete();
		            })
		            .expectNext("normal")
		            .thenAwait()
		            .thenRequest(3)
		            .expectNext("over2", "over3")
		            .expectComplete()
		            .verify();

		assertNull("unexpected droppedValue", droppedValue);
		assertNull("unexpected hookCapturedValue",hookCapturedValue);
		assertNull("unexpected hookCapturedError",hookCapturedError);
	}

	@Test
	public void fluxOnBackpressureBufferStrategyRequiresCallback() {
		try {
			Flux.just("foo").onBackpressureBuffer(1,
					null,
					ERROR);
			fail("expected NullPointerException");
		}
		catch (NullPointerException e) {
			assertEquals("onBufferOverflow", e.getMessage());
		}
	}

	@Test
	public void fluxOnBackpressureBufferStrategyRequiresStrategy() {
		try {
			Flux.just("foo").onBackpressureBuffer(1,
					v -> { },
					null);
			fail("expected NullPointerException");
		}
		catch (NullPointerException e) {
			assertEquals("bufferOverflowStrategy", e.getMessage());
		}
	}

	@Test
    public void scanSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxOnBackpressureBufferStrategy.BackpressureBufferDropOldestSubscriber<Integer> test =
        		new FluxOnBackpressureBufferStrategy.BackpressureBufferDropOldestSubscriber<>(actual,
        				123, true, t -> {}, BufferOverflowStrategy.DROP_OLDEST);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.DELAY_ERROR)).isTrue();
        test.requested = 35;
        assertThat(test.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(35);
        assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
        test.offer(9);
        assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

        assertThat(test.scan(Scannable.Attr.ERROR)).isNull();
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.Attr.ERROR)).isSameAs(test.error);
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

        assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }
}
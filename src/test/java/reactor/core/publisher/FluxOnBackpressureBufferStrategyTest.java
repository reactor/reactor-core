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

import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.test.StepVerifier;

import static org.junit.Assert.*;
import static reactor.core.publisher.FluxOnBackpressureBufferStrategy.OverflowStrategy.*;

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
		DirectProcessor<String> processor = DirectProcessor.create();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor, 2, this, DROP_ELEMENT);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.onNext("normal");
			            processor.onNext("over1");
			            processor.onNext("over2");
			            processor.onNext("over3");
			            processor.onComplete();
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
		DirectProcessor<String> processor = DirectProcessor.create();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor, 2, this, DROP_OLDEST);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.onNext("normal");
			            processor.onNext("over1");
			            processor.onNext("over2");
			            processor.onNext("over3");
			            processor.onComplete();
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
		DirectProcessor<String> processor = DirectProcessor.create();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor, 2, this, ERROR);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.onNext("normal");
			            processor.onNext("over1");
			            processor.onNext("over2");
			            processor.onNext("over3");
			            processor.onComplete();
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
		DirectProcessor<String> processor = DirectProcessor.create();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor, 2, v -> { throw new IllegalArgumentException("boom"); },
				DROP_ELEMENT);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.onNext("normal");
			            processor.onNext("over1");
			            processor.onNext("over2");
			            processor.onNext("over3");
			            processor.onComplete();
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
		DirectProcessor<String> processor = DirectProcessor.create();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor, 2, v -> { throw new IllegalArgumentException("boom"); },
				DROP_OLDEST);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.onNext("normal");
			            processor.onNext("over1");
			            processor.onNext("over2");
			            processor.onNext("over3");
			            processor.onComplete();
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
		DirectProcessor<String> processor = DirectProcessor.create();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor, 2, v -> { throw new IllegalArgumentException("boom"); },
				ERROR);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.onNext("normal");
			            processor.onNext("over1");
			            processor.onNext("over2");
			            processor.onNext("over3");
			            processor.onComplete();
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
		DirectProcessor<String> processor = DirectProcessor.create();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor, 2, null, ERROR);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.onNext("normal");
			            processor.onNext("over1");
			            processor.onNext("over2");
			            processor.onNext("over3");
			            processor.onComplete();
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
		DirectProcessor<String> processor = DirectProcessor.create();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor, 2, null, DROP_ELEMENT);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.onNext("normal");
			            processor.onNext("over1");
			            processor.onNext("over2");
			            processor.onNext("over3");
			            processor.onComplete();
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
		DirectProcessor<String> processor = DirectProcessor.create();

		FluxOnBackpressureBufferStrategy<String> flux = new FluxOnBackpressureBufferStrategy<>(
				processor, 2, null, DROP_OLDEST);

		StepVerifier.create(flux, 0)
		            .thenRequest(1)
		            .then(() -> {
			            processor.onNext("normal");
			            processor.onNext("over1");
			            processor.onNext("over2");
			            processor.onNext("over3");
			            processor.onComplete();
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

}
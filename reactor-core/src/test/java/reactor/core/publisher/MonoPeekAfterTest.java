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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.junit.Test;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.Logger;
import reactor.util.Loggers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class MonoPeekAfterTest {

	private static final Logger LOG = Loggers.getLogger(MonoPeekAfterTest.class);

	@Test
	public void onSuccessNormal() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean hasNull = new AtomicBoolean();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.hide()
				.doOnSuccess(v -> {
					if (v == null) hasNull.set(true);
					invoked.increment();
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.ANY, Fuseable.NONE)
	                .expectNext(55)
	                .expectComplete()
	                .verify();

		assertFalse("unexpected call to onSuccess with null", hasNull.get());
		assertEquals(1, invoked.intValue());
	}

	@Test
	public void onSuccessNormalConditional() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean hasNull = new AtomicBoolean();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.hide()
				.filter(v -> true)
				.doOnSuccess(v -> {
					if (v == null) hasNull.set(true);
					invoked.increment();
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.ANY, Fuseable.NONE)
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertFalse("unexpected call to onSuccess with null", hasNull.get());
		assertEquals(1, invoked.intValue());

	}

	@Test
	public void onSuccessFusion() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean hasNull = new AtomicBoolean();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.doOnSuccess(v -> {
					if (v == null) hasNull.set(true);
					invoked.increment();
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.ASYNC)
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertFalse("unexpected call to onSuccess with null", hasNull.get());
		assertEquals(1, invoked.intValue());
	}

	@Test
	public void onSuccessFusionConditional() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean hasNull = new AtomicBoolean();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.filter(v -> true)
				.doOnSuccess(v -> {
					if (v == null) hasNull.set(true);
					invoked.increment();
				});

		StepVerifier.create(mono)
		            .expectFusion()
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertFalse("unexpected call to onSuccess with null", hasNull.get());
		assertEquals(1, invoked.intValue());
	}

	@Test
	public void onSuccessOrErrorNormal() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.hide()
				.doOnSuccessOrError((v, t) -> {
					if (v == null && t == null) completedEmpty.set(true);
					if (t != null) error.set(t);
					invoked.increment();
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.ANY, Fuseable.NONE)
	                .expectNext(55)
	                .expectComplete()
	                .verify();

		assertFalse("unexpected empty completion", completedEmpty.get());
		assertEquals(1, invoked.intValue());
		assertEquals("unexpected error", null, error.get());
	}

	@Test
	public void onSuccessOrErrorNormalConditional() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.hide()
				.filter(v -> true)
				.doOnSuccessOrError((v, t) -> {
					if (v == null && t == null) completedEmpty.set(true);
					if (t != null) error.set(t);
					invoked.increment();
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.ANY, Fuseable.NONE)
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertFalse("unexpected empty completion", completedEmpty.get());
		assertEquals(1, invoked.intValue());
		assertEquals("unexpected error", null, error.get());
	}

	@Test
	public void onSuccessOrErrorFusion() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.doOnSuccessOrError((v, t) -> {
					if (v == null && t == null) completedEmpty.set(true);
					if (t != null) error.set(t);
					invoked.increment();
				});

		StepVerifier.create(mono)
		            .expectFusion()
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertFalse("unexpected empty completion", completedEmpty.get());
		assertEquals(1, invoked.intValue());
		assertEquals("unexpected error", null, error.get());
	}

	@Test
	public void onSuccessOrErrorFusionConditional() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.filter(v -> true)
				.doOnSuccessOrError((v, t) -> {
					if (v == null && t == null) completedEmpty.set(true);
					if (t != null) error.set(t);
					invoked.increment();
				});

		StepVerifier.create(mono)
		            .expectFusion()
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertFalse("unexpected empty completion", completedEmpty.get());
		assertEquals(1, invoked.intValue());
		assertEquals("unexpected error", null, error.get());
	}

	@Test
	public void onAfterSuccessOrErrorNormal() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.hide()
				.doAfterSuccessOrError((v, t) -> {
					if (v == null && t == null) completedEmpty.set(true);
					if (t != null) error.set(t);
					invoked.increment();
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.ANY, Fuseable.NONE)
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertFalse("unexpected empty completion", completedEmpty.get());
		assertEquals(1, invoked.intValue());
		assertEquals("unexpected error", null, error.get());
	}

	@Test
	public void onAfterSuccessOrErrorNormalConditional() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.hide()
				.filter(v -> true)
				.doAfterSuccessOrError((v, t) -> {
					if (v == null && t == null) completedEmpty.set(true);
					if (t != null) error.set(t);
					invoked.increment();
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.ANY, Fuseable.NONE)
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertFalse("unexpected empty completion", completedEmpty.get());
		assertEquals(1, invoked.intValue());
		assertEquals("unexpected error", null, error.get());
	}

	@Test
	public void onAfterSuccessOrErrorFusion() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.doAfterSuccessOrError((v, t) -> {
					if (v == null && t == null) completedEmpty.set(true);
					if (t != null) error.set(t);
					invoked.increment();
				});

		StepVerifier.create(mono.log())
		            .expectFusion()
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertFalse("unexpected empty completion", completedEmpty.get());
		assertEquals(1, invoked.intValue());
		assertEquals("unexpected error", null, error.get());
	}

	@Test
	public void onAfterSuccessOrErrorFusionConditional() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.filter(v -> true)
				.doAfterSuccessOrError((v, t) -> {
					if (v == null && t == null) completedEmpty.set(true);
					if (t != null) error.set(t);
					invoked.increment();
				});

		StepVerifier.create(mono)
		            .expectFusion()
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertFalse("unexpected empty completion", completedEmpty.get());
		assertEquals(1, invoked.intValue());
		assertEquals("unexpected error", null, error.get());
	}

	@Test
	public void onAfterTerminateNormalConditional() {
		LongAdder invoked = new LongAdder();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.hide()
				.filter(v -> true)
				.doAfterTerminate(invoked::increment);

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.ANY, Fuseable.NONE)
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertEquals(1, invoked.intValue());
	}

	@Test
	public void onAfterTerminateFusion() {
		LongAdder invoked = new LongAdder();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.doAfterTerminate(invoked::increment);

		StepVerifier.create(mono.log())
		            .expectFusion()
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertEquals(1, invoked.intValue());
	}

	@Test
	public void onAfterTerminateFusionConditional() {
		LongAdder invoked = new LongAdder();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.filter(v -> true)
				.doAfterTerminate(invoked::increment);

		StepVerifier.create(mono)
		            .expectFusion()
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertEquals(1, invoked.intValue());
	}

	@Test
	public void onSuccessCallbackFailureInterruptsOnNext() {
		LongAdder invoked = new LongAdder();
		StepVerifier.create(Mono.just("foo")
		                        .doOnSuccess(s -> {
		                        	invoked.increment();
		                        	throw new IllegalArgumentException(s);
		                        }))
	                .expectErrorMessage("foo")
	                .verify();
		assertEquals(1, invoked.intValue());
	}

	@Test
	public void onSuccessOrErrorCallbackFailureInterruptsOnNext() {
		LongAdder invoked = new LongAdder();
		StepVerifier.create(Mono.just("foo")
		                        .doOnSuccessOrError((v, t) -> {
			                        invoked.increment();
			                        throw new IllegalArgumentException(v);
		                        }))
		            .expectErrorMessage("foo")
		            .verify();

		assertEquals(1, invoked.intValue());
	}

	@Test
	public void afterSuccessOrErrorCallbackFailureInterruptsOnNextAndThrows() {
		LongAdder invoked = new LongAdder();
		try {
			StepVerifier.create(Mono.just("foo")
			                        .doAfterSuccessOrError((v, t) -> {
				                        invoked.increment();
				                        throw new IllegalArgumentException(v);
			                        }))
			            .expectNext("bar") //irrelevant
			            .expectErrorMessage("baz") //irrelevant
			            .verify();
		}
		catch (Throwable t) {
			Throwable e = Exceptions.unwrap(t);
			assertEquals(IllegalArgumentException.class, e.getClass());
			assertEquals("foo", e.getMessage());
		}

		assertEquals(1, invoked.intValue());
	}

	@Test
	public void afterTerminateCallbackFailureInterruptsOnNextAndThrows() {
		LongAdder invoked = new LongAdder();
		try {
			StepVerifier.create(Mono.just("foo")
			                        .doAfterTerminate(() -> {
				                        invoked.increment();
				                        throw new IllegalArgumentException("boom");
			                        }))
			            .expectNext("bar") //irrelevant
			            .expectErrorMessage("baz") //irrelevant
			            .verify();
		}
		catch (Throwable t) {
			Throwable e = Exceptions.unwrap(t);
			assertEquals(IllegalArgumentException.class, e.getClass());
			assertEquals("boom", e.getMessage());
		}

		assertEquals(1, invoked.intValue());
	}

	@Test
	public void onSuccessNotCalledOnError() {
		LongAdder invoked = new LongAdder();
		IllegalArgumentException err = new IllegalArgumentException("boom");

		StepVerifier.create(Mono.error(err)
				.doOnSuccess(v -> invoked.increment()))
	                .expectErrorMessage("boom")
	                .verify();

		assertEquals(0, invoked.intValue());
	}

	@Test
	public void onSuccessOrErrorForOnError() {
		LongAdder invoked = new LongAdder();
		AtomicReference<String> value = new AtomicReference<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		IllegalArgumentException err = new IllegalArgumentException("boom");

		StepVerifier.create(Mono.<String>error(err)
		                        .doOnSuccessOrError((v, t) -> {
			                        invoked.increment();
			                        value.set(v);
			                        error.set(t);
		                        }))
		            .expectErrorMessage("boom")
		            .verify();

		assertEquals(1, invoked.intValue());
		assertEquals(null, value.get());
		assertEquals(err, error.get());
	}

	@Test
	public void afterSuccessOrErrorForOnError() {
		LongAdder invoked = new LongAdder();
		AtomicReference<String> value = new AtomicReference<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		IllegalArgumentException err = new IllegalArgumentException("boom");

		StepVerifier.create(Mono.<String>error(err)
				.doAfterSuccessOrError((v, t) -> {
					invoked.increment();
					value.set(v);
					error.set(t);
				}))
		            .expectErrorMessage("boom")
		            .verify();

		assertEquals(1, invoked.intValue());
		assertEquals(null, value.get());
		assertEquals(err, error.get());
	}

	@Test
	public void afterTerminateForOnError() {
		LongAdder invoked = new LongAdder();

		IllegalArgumentException err = new IllegalArgumentException("boom");

		StepVerifier.create(Mono.<String>error(err)
				.doAfterTerminate(invoked::increment))
		            .expectErrorMessage("boom")
		            .verify();

		assertEquals(1, invoked.intValue());
	}

	@Test
	public void onSuccessForEmpty() {
		LongAdder invoked = new LongAdder();
		AtomicReference<String> value = new AtomicReference<>();

		StepVerifier.create(Mono.<String>empty()
				.doOnSuccess(v -> {
					invoked.increment();
					value.set(v);
				}))
		            .expectComplete()
		            .verify();

		assertEquals(1, invoked.intValue());
		assertEquals(null, value.get());
	}

	@Test
	public void onSuccessOrErrorForEmpty() {
		LongAdder invoked = new LongAdder();
		AtomicReference<String> value = new AtomicReference<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		StepVerifier.create(Mono.<String>empty()
				.doOnSuccessOrError((v, t) -> {
					invoked.increment();
					value.set(v);
					error.set(t);
				}))
		            .expectComplete()
		            .verify();

		assertEquals(1, invoked.intValue());
		assertEquals(null, value.get());
		assertEquals(null, error.get());
	}

	@Test
	public void afterSuccessOrErrorForEmpty() {
		LongAdder invoked = new LongAdder();
		AtomicReference<String> value = new AtomicReference<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		StepVerifier.create(Mono.<String>empty()
				.doAfterSuccessOrError((v, t) -> {
					invoked.increment();
					value.set(v);
					error.set(t);
				}))
		            .expectComplete()
		            .verify();

		assertEquals(1, invoked.intValue());
		assertEquals(null, value.get());
		assertEquals(null, error.get());
	}

	@Test
	public void afterTerminateForEmpty() {
		LongAdder invoked = new LongAdder();

		StepVerifier.create(Mono.<String>empty()
				.doAfterTerminate(() -> {
					invoked.increment();
				}))
		            .expectComplete()
		            .verify();

		assertEquals(1, invoked.intValue());
	}

	@Test
	public void testCallbacksNoFusion() {
		AtomicReference<Integer> successInvocation = new AtomicReference<>();
		AtomicReference<Throwable> errorInvocation = new AtomicReference<>();
		AtomicReference<Integer> afterTerminateInvocation = new AtomicReference<>();

		Mono<Integer> source = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.hide();

		Mono<Integer> mono = new MonoPeekTerminal<>(source,
				successInvocation::set,
				errorInvocation::set,
				(v, t) -> {
					afterTerminateInvocation.set(v);
					errorInvocation.set(t);
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.NONE)
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertEquals(55, (Object) successInvocation.get());
		assertEquals(55, (Object) afterTerminateInvocation.get());
		assertEquals(null, errorInvocation.get());
	}

	@Test
	public void testCallbacksFusionSync() {
		AtomicReference<Integer> successInvocation = new AtomicReference<>();
		AtomicReference<Integer> afterTerminateInvocation = new AtomicReference<>();
		AtomicReference<Throwable> errorInvocation = new AtomicReference<>();

		Mono<Integer> source = Mono.fromDirect(Flux.range(55, 1));

		Mono<Integer> mono = new MonoPeekTerminal<>(source,
				successInvocation::set,
				errorInvocation::set,
				(v, t) -> {
					afterTerminateInvocation.set(v);
					errorInvocation.set(t);
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.SYNC, Fuseable.SYNC) //TODO in 3.0.3 this doesn't work
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertEquals(55, (Object) successInvocation.get());
		assertEquals(55, (Object) afterTerminateInvocation.get());
		assertEquals(null, errorInvocation.get());
	}

	@Test
	public void testCallbacksFusionAsync() {
		AtomicReference<Integer> successInvocation = new AtomicReference<>();
		AtomicReference<Throwable> errorInvocation = new AtomicReference<>();
		AtomicReference<Integer> afterTerminateInvocation = new AtomicReference<>();

		Mono<Integer> source = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b);

		Mono<Integer> mono = new MonoPeekTerminal<>(source,
				successInvocation::set,
				errorInvocation::set,
				(v, t) -> {
					afterTerminateInvocation.set(v);
					errorInvocation.set(t);
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.ASYNC)
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertEquals(55, (Object) successInvocation.get());
		assertEquals(null, errorInvocation.get());
		assertEquals(55, (Object) afterTerminateInvocation.get());
	}

	@Test
	public void should_reduce_to_10_events() {
		for (int i = 0; i < 20; i++) {
			AtomicInteger count = new AtomicInteger();
			Flux.range(0, 10)
			    .flatMap(x -> Flux.range(0, 2)
			                      .map(y -> FluxPeekFuseableTest.blockingOp(x, y))
			                      .subscribeOn(Schedulers.parallel())
			                      .reduce((l, r) -> l + "_" + r)
			                      .doOnSuccess(s -> {
				                      LOG.debug("success " + x + ": " + s);
				                      count.incrementAndGet();
			                      }))
			    .blockLast();

			assertEquals(10, count.get());
		}
	}

}
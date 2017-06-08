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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.junit.Test;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class MonoPeekAfterTest {

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
	public void onTerminateNormal() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.hide()
				.doOnTerminate((v, t) -> {
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
	public void onTerminateNormalConditional() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.hide()
				.filter(v -> true)
				.doOnTerminate((v, t) -> {
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
	public void onTerminateFusion() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.doOnTerminate((v, t) -> {
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
	public void onTerminateFusionConditional() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.filter(v -> true)
				.doOnTerminate((v, t) -> {
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
	public void onAfterTerminateNormal() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.hide()
				.doAfterTerminate((v, t) -> {
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
	public void onAfterTerminateNormalConditional() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.hide()
				.filter(v -> true)
				.doAfterTerminate((v, t) -> {
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
	public void onAfterTerminateFusion() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.doAfterTerminate((v, t) -> {
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
	public void onAfterTerminateFusionConditional() {
		LongAdder invoked = new LongAdder();
		AtomicBoolean completedEmpty = new AtomicBoolean();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Mono<Integer> mono = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.filter(v -> true)
				.doAfterTerminate((v, t) -> {
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
	public void onTerminateCallbackFailureInterruptsOnNext() {
		LongAdder invoked = new LongAdder();
		StepVerifier.create(Mono.just("foo")
		                        .doOnTerminate((v, t) -> {
			                        invoked.increment();
			                        throw new IllegalArgumentException(v);
		                        }))
		            .expectErrorMessage("foo")
		            .verify();

		assertEquals(1, invoked.intValue());
	}

	@Test
	public void afterTerminateCallbackFailureInterruptsOnNextAndThrows() {
		LongAdder invoked = new LongAdder();
		try {
			StepVerifier.create(Mono.just("foo")
			                        .doAfterTerminate((v, t) -> {
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
	public void onTerminateForOnError() {
		LongAdder invoked = new LongAdder();
		AtomicReference<String> value = new AtomicReference<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		IllegalArgumentException err = new IllegalArgumentException("boom");

		StepVerifier.create(Mono.<String>error(err)
		                        .doOnTerminate((v, t) -> {
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
		AtomicReference<String> value = new AtomicReference<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		IllegalArgumentException err = new IllegalArgumentException("boom");

		StepVerifier.create(Mono.<String>error(err)
				.doAfterTerminate((v, t) -> {
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
	public void onTerminateForEmpty() {
		LongAdder invoked = new LongAdder();
		AtomicReference<String> value = new AtomicReference<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		StepVerifier.create(Mono.<String>empty()
				.doOnTerminate((v, t) -> {
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
		AtomicReference<String> value = new AtomicReference<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		StepVerifier.create(Mono.<String>empty()
				.doAfterTerminate((v, t) -> {
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
	public void testCallbacksNoFusion() {
		AtomicReference<Integer> successInvocation = new AtomicReference<>();
		AtomicReference<Integer> onTerminateInvocation = new AtomicReference<>();
		AtomicReference<Integer> afterTerminateInvocation = new AtomicReference<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Mono<Integer> source = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b)
				.hide();

		Mono<Integer> mono = new MonoPeekTerminal<>(source,
				successInvocation::set,
				(v, t) -> {
					onTerminateInvocation.set(v);
					error.set(t);
				},
				(v, t) -> {
					afterTerminateInvocation.set(v);
					error.set(t);
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.NONE)
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertEquals(55, (Object) successInvocation.get());
		assertEquals(55, (Object) onTerminateInvocation.get());
		assertEquals(55, (Object) afterTerminateInvocation.get());
		assertEquals(null, error.get());
	}

	@Test
	public void testCallbacksFusionSync() {
		AtomicReference<Integer> successInvocation = new AtomicReference<>();
		AtomicReference<Integer> onTerminateInvocation = new AtomicReference<>();
		AtomicReference<Integer> afterTerminateInvocation = new AtomicReference<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Mono<Integer> source = Mono.fromDirect(Flux.range(55, 1));

		Mono<Integer> mono = new MonoPeekTerminal<>(source,
				successInvocation::set,
				(v, t) -> {
					onTerminateInvocation.set(v);
					error.set(t);
				},
				(v, t) -> {
					afterTerminateInvocation.set(v);
					error.set(t);
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.SYNC, Fuseable.SYNC) //TODO in 3.0.3 this doesn't work
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertEquals(55, (Object) successInvocation.get());
		assertEquals(55, (Object) onTerminateInvocation.get());
		assertEquals(55, (Object) afterTerminateInvocation.get());
		assertEquals(null, error.get());
	}

	@Test
	public void testCallbacksFusionAsync() {
		AtomicReference<Integer> successInvocation = new AtomicReference<>();
		AtomicReference<Integer> onTerminateInvocation = new AtomicReference<>();
		AtomicReference<Integer> afterTerminateInvocation = new AtomicReference<>();
		AtomicReference<Throwable> error = new AtomicReference<>();

		Mono<Integer> source = Flux
				.range(1, 10)
				.reduce((a, b) -> a + b);

		Mono<Integer> mono = new MonoPeekTerminal<>(source,
				successInvocation::set,
				(v, t) -> {
					onTerminateInvocation.set(v);
					error.set(t);
				},
				(v, t) -> {
					afterTerminateInvocation.set(v);
					error.set(t);
				});

		StepVerifier.create(mono)
		            .expectFusion(Fuseable.ASYNC)
		            .expectNext(55)
		            .expectComplete()
		            .verify();

		assertEquals(55, (Object) successInvocation.get());
		assertEquals(55, (Object) onTerminateInvocation.get());
		assertEquals(55, (Object) afterTerminateInvocation.get());
		assertEquals(null, error.get());
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
				                      System.out.println("success " + x + ": " + s);
				                      count.incrementAndGet();
			                      }))
			    .blockLast();

			assertEquals(10, count.get());
		}
	}

}
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
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import javax.annotation.Nullable;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.publisher.FluxPeekFuseable.PeekConditionalSubscriber;
import reactor.core.publisher.FluxPeekFuseable.PeekFuseableConditionalSubscriber;
import reactor.core.publisher.FluxPeekFuseable.PeekFuseableSubscriber;
import reactor.core.scheduler.Schedulers;
import reactor.test.MockUtils;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static reactor.core.scheduler.Schedulers.parallel;

public class FluxPeekFuseableTest {

	@Test(expected = NullPointerException.class)
	public void nullSource() {
		new FluxPeekFuseable<>(null, null, null, null, null, null, null, null);
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		AtomicLong onRequest = new AtomicLong();
		AtomicBoolean onAfterComplete = new AtomicBoolean();
		AtomicBoolean onCancel = new AtomicBoolean();

		new FluxPeekFuseable<>(Flux.just(1),
				onSubscribe::set,
				onNext::set,
				onError::set,
				() -> onComplete.set(true),
				() -> onAfterComplete.set(true),
				onRequest::set,
				() -> onCancel.set(true)).subscribe(ts);

		Assert.assertNotNull(onSubscribe.get());
		Assert.assertEquals((Integer) 1, onNext.get());
		Assert.assertNull(onError.get());
		Assert.assertTrue(onComplete.get());
		Assert.assertTrue(onAfterComplete.get());
		Assert.assertEquals(Long.MAX_VALUE, onRequest.get());
		Assert.assertFalse(onCancel.get());
	}

	@Test
	public void error() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		AtomicLong onRequest = new AtomicLong();
		AtomicBoolean onAfterComplete = new AtomicBoolean();
		AtomicBoolean onCancel = new AtomicBoolean();

		new FluxPeekFuseable<>(Flux.error(new RuntimeException("forced failure")),
				onSubscribe::set,
				onNext::set,
				onError::set,
				() -> onComplete.set(true),
				() -> onAfterComplete.set(true),
				onRequest::set,
				() -> onCancel.set(true)).subscribe(ts);

		Assert.assertNotNull(onSubscribe.get());
		Assert.assertNull(onNext.get());
		Assert.assertTrue(onError.get() instanceof RuntimeException);
		Assert.assertFalse(onComplete.get());
		Assert.assertTrue(onAfterComplete.get());
		Assert.assertEquals(Long.MAX_VALUE, onRequest.get());
		Assert.assertFalse(onCancel.get());
	}

	@Test
	public void empty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		AtomicLong onRequest = new AtomicLong();
		AtomicBoolean onAfterComplete = new AtomicBoolean();
		AtomicBoolean onCancel = new AtomicBoolean();

		new FluxPeekFuseable<>(Flux.empty(),
				onSubscribe::set,
				onNext::set,
				onError::set,
				() -> onComplete.set(true),
				() -> onAfterComplete.set(true),
				onRequest::set,
				() -> onCancel.set(true)).subscribe(ts);

		Assert.assertNotNull(onSubscribe.get());
		Assert.assertNull(onNext.get());
		Assert.assertNull(onError.get());
		Assert.assertTrue(onComplete.get());
		Assert.assertTrue(onAfterComplete.get());
		Assert.assertEquals(Long.MAX_VALUE, onRequest.get());
		Assert.assertFalse(onCancel.get());
	}

	@Test
	public void never() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		AtomicLong onRequest = new AtomicLong();
		AtomicBoolean onAfterComplete = new AtomicBoolean();
		AtomicBoolean onCancel = new AtomicBoolean();

		new FluxPeekFuseable<>(Flux.never(),
				onSubscribe::set,
				onNext::set,
				onError::set,
				() -> onComplete.set(true),
				() -> onAfterComplete.set(true),
				onRequest::set,
				() -> onCancel.set(true)).subscribe(ts);

		Assert.assertNotNull(onSubscribe.get());
		Assert.assertNull(onNext.get());
		Assert.assertNull(onError.get());
		Assert.assertFalse(onComplete.get());
		Assert.assertFalse(onAfterComplete.get());
		Assert.assertEquals(Long.MAX_VALUE, onRequest.get());
		Assert.assertFalse(onCancel.get());
	}

	@Test
	public void neverCancel() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Subscription> onSubscribe = new AtomicReference<>();
		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		AtomicLong onRequest = new AtomicLong();
		AtomicBoolean onAfterComplete = new AtomicBoolean();
		AtomicBoolean onCancel = new AtomicBoolean();

		new FluxPeekFuseable<>(Flux.never(),
				onSubscribe::set,
				onNext::set,
				onError::set,
				() -> onComplete.set(true),
				() -> onAfterComplete.set(true),
				onRequest::set,
				() -> onCancel.set(true)).subscribe(ts);

		Assert.assertNotNull(onSubscribe.get());
		Assert.assertNull(onNext.get());
		Assert.assertNull(onError.get());
		Assert.assertFalse(onComplete.get());
		Assert.assertFalse(onAfterComplete.get());
		Assert.assertEquals(Long.MAX_VALUE, onRequest.get());
		Assert.assertFalse(onCancel.get());

		ts.cancel();

		Assert.assertTrue(onCancel.get());
	}

	@Test
	public void callbackError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Throwable err = new Exception("test");

		Flux.just(1)
		    .doOnNext(d -> {
			    throw Exceptions.propagate(err);
		    })
		    .subscribe(ts);

		//nominal error path (DownstreamException)
		ts.assertErrorMessage("test");

		ts = AssertSubscriber.create();

		try {
			Flux.just(1)
			    .doOnNext(d -> {
				    throw Exceptions.bubble(err);
			    })
			    .subscribe(ts);

			Assert.fail();
		}
		catch (Exception e) {
			Assert.assertTrue(Exceptions.unwrap(e) == err);
		}
	}

	@Test
	public void completeCallbackError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Throwable err = new Exception("test");

		Flux.just(1)
		    .doOnComplete(() -> {
			    throw Exceptions.propagate(err);
		    })
		    .subscribe(ts);

		//nominal error path (DownstreamException)
		ts.assertErrorMessage("test");

		ts = AssertSubscriber.create();

		try {
			Flux.just(1)
			    .doOnComplete(() -> {
				    throw Exceptions.bubble(err);
			    })
			    .subscribe(ts);

			Assert.fail();
		}
		catch (Exception e) {
			Assert.assertTrue(Exceptions.unwrap(e) == err);
		}
	}

	@Test
	public void errorCallbackError() {
		IllegalStateException err = new IllegalStateException("test");

		FluxPeekFuseable<String> flux = new FluxPeekFuseable<>(
				Flux.error(new IllegalArgumentException("bar")), null, null,
				e -> { throw err; },
				null, null, null, null);

		AssertSubscriber<String> ts = AssertSubscriber.create();
		flux.subscribe(ts);

		ts.assertNoValues();
		ts.assertError(IllegalStateException.class);
		ts.assertErrorWith(e -> e.getSuppressed()[0].getMessage().equals("bar"));
	}

	//See https://github.com/reactor/reactor-core/issues/272
	@Test
	public void errorCallbackError2() {
		//test with alternate / wrapped error types

		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Throwable err = new Exception("test");

		Flux.just(1)
		    .doOnNext(d -> {
			    throw new RuntimeException();
		    })
		    .doOnError(e -> {
			    throw Exceptions.propagate(err);
		    })
		    .subscribe(ts);

		//nominal error path (DownstreamException)
		ts.assertErrorMessage("test");

		ts = AssertSubscriber.create();
		try {
			Flux.just(1)
			    .doOnNext(d -> {
				    throw new RuntimeException();
			    })
			    .doOnError(d -> {
				    throw Exceptions.bubble(err);
			    })
			    .subscribe(ts);

			Assert.fail();
		}
		catch (Exception e) {
			Assert.assertTrue(Exceptions.unwrap(e) == err);
		}
	}

	//See https://github.com/reactor/reactor-core/issues/253
	@Test
	public void errorCallbackErrorWithParallel() {
		AssertSubscriber<Integer> assertSubscriber = new AssertSubscriber<>();

		Mono.just(1)
		    .publishOn(parallel())
		    .doOnNext(i -> {
			    throw new IllegalArgumentException();
		    })
		    .doOnError(e -> {
			    throw new IllegalStateException(e);
		    })
		    .subscribe(assertSubscriber);

		assertSubscriber
				.await()
				.assertError(IllegalStateException.class)
				.assertNotComplete();
	}

	@Test
	public void afterTerminateCallbackErrorDoesNotInvokeOnError() {
		IllegalStateException err = new IllegalStateException("test");
		AtomicReference<Throwable> errorCallbackCapture = new AtomicReference<>();

		FluxPeekFuseable<String> flux = new FluxPeekFuseable<>(
				Flux.empty(), null, null, errorCallbackCapture::set, null,
				() -> { throw err; }, null, null);

		AssertSubscriber<String> ts = AssertSubscriber.create();

		try {
			flux.subscribe(ts);
			fail("expected thrown exception");
		}
		catch (Exception e) {
			assertThat(e).hasCause(err);
		}
		ts.assertNoValues();
		ts.assertComplete();

		//the onError wasn't invoked:
		assertThat(errorCallbackCapture.get()).isNull();
	}

	@Test
	public void afterTerminateCallbackFatalIsThrownDirectly() {
		AtomicReference<Throwable> errorCallbackCapture = new AtomicReference<>();
		Error fatal = new LinkageError();
		FluxPeekFuseable<String> flux = new FluxPeekFuseable<>(
				Flux.empty(), null, null, errorCallbackCapture::set, null,
				() -> { throw fatal; }, null, null);

		AssertSubscriber<String> ts = AssertSubscriber.create();

		try {
			flux.subscribe(ts);
			fail("expected thrown exception");
		}
		catch (Throwable e) {
			assertSame(fatal, e);
		}
		ts.assertNoValues();
		ts.assertComplete();

		Assert.assertThat(errorCallbackCapture.get(), is(nullValue()));


		//same with after error
		errorCallbackCapture.set(null);
		flux = new FluxPeekFuseable<>(
				Flux.error(new NullPointerException()), null, null, errorCallbackCapture::set, null,
				() -> { throw fatal; }, null, null);

		ts = AssertSubscriber.create();

		try {
			flux.subscribe(ts);
			fail("expected thrown exception");
		}
		catch (Throwable e) {
			assertSame(fatal, e);
		}
		ts.assertNoValues();
		ts.assertError(NullPointerException.class);

		Assert.assertThat(errorCallbackCapture.get(), is(instanceOf(NullPointerException.class)));
	}

	@Test
	public void afterTerminateCallbackErrorAndErrorCallbackError() {
		IllegalStateException err = new IllegalStateException("expected afterTerminate");
		IllegalArgumentException err2 = new IllegalArgumentException("error");

		FluxPeekFuseable<String> flux = new FluxPeekFuseable<>(
				Flux.empty(), null, null, e -> { throw err2; },
				null,
				() -> { throw err; }, null, null);

		AssertSubscriber<String> ts = AssertSubscriber.create();

		try {
			flux.subscribe(ts);
			fail("expected thrown exception");
		}
		catch (Exception e) {
			e.printStackTrace();
			assertSame(e.toString(), err, e.getCause());
			assertEquals(0, err2.getSuppressed().length);
			//err2 is never thrown
		}
		ts.assertNoValues();
		ts.assertComplete();
	}

	@Test
	public void afterTerminateCallbackErrorAndErrorCallbackError2() {
		IllegalStateException afterTerminate = new IllegalStateException("afterTerminate");
		IllegalArgumentException error = new IllegalArgumentException("error");
		NullPointerException err = new NullPointerException();

		FluxPeekFuseable<String> flux = new FluxPeekFuseable<>(
				Flux.error(err),
				null, null,
				e -> { throw error; }, null, () -> { throw afterTerminate; },
				null, null);

		AssertSubscriber<String> ts = AssertSubscriber.create();

		try {
			flux.subscribe(ts);
			fail("expected thrown exception");
		}
		catch (Exception e) {
			assertSame(afterTerminate, e.getCause());
			//afterTerminate suppressed error which itself suppressed original err
			assertEquals(1, afterTerminate.getSuppressed().length);
			assertEquals(error, afterTerminate.getSuppressed()[0]);

			assertEquals(1, error.getSuppressed().length);
			assertEquals(err, error.getSuppressed()[0]);
		}
		ts.assertNoValues();
		//the subscriber still sees the 'error' message since actual.onError is called before the afterTerminate callback
		ts.assertErrorMessage("error");
	}


	@Test
	public void syncFusionAvailable() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .doOnNext(v -> {
		    })
		    .subscribe(ts);

		Subscription s = ts.upstream();
		Assert.assertTrue("Non-fuseable upstream: " + s,
				s instanceof Fuseable.QueueSubscription);
	}

	@Test
	public void asyncFusionAvailable() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		UnicastProcessor.create(Queues.<Integer>get(2).get())
		                .doOnNext(v -> {
		                })
		                .subscribe(ts);

		Subscription s = ts.upstream();
		Assert.assertTrue("Non-fuseable upstream" + s,
				s instanceof Fuseable.QueueSubscription);
	}

	@Test
	public void conditionalFusionAvailable() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.from(u -> {
			if (!(u instanceof Fuseable.ConditionalSubscriber)) {
				Operators.error(u,
						new IllegalArgumentException("The subscriber is not conditional: " + u));
			}
			else {
				Operators.complete(u);
			}
		})
		          .doOnNext(v -> {
		          })
		          .filter(v -> true)
		          .subscribe(ts);

		ts.assertNoError()
		  .assertNoValues()
		  .assertComplete();
	}

	@Test
	public void conditionalFusionAvailableWithFuseable() {
		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.wrap(u -> {
			if (!(u instanceof Fuseable.ConditionalSubscriber)) {
				Operators.error(u,
						new IllegalArgumentException("The subscriber is not conditional: " + u));
			}
			else {
				Operators.complete(u);
			}
		})
		    .doOnNext(v -> {
		          })
		    .filter(v -> true)
		    .subscribe(ts);

		ts.assertNoError()
		  .assertNoValues()
		  .assertComplete();
	}

	//TODO was these 2 tests supposed to trigger sync fusion and go through poll() ?
	@Test
	public void noFusionCompleteCalled() {
		AtomicBoolean onComplete = new AtomicBoolean();

		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .doOnComplete(() -> onComplete.set(true))
		    .subscribe(ts);

		ts.assertNoError()
		  .assertValues(1, 2)
		  .assertComplete();

		Assert.assertTrue("onComplete not called back", onComplete.get());
	}

	@Test
	public void noFusionAfterTerminateCalled() {
		AtomicBoolean onTerminate = new AtomicBoolean();

		AssertSubscriber<Object> ts = AssertSubscriber.create();

		Flux.range(1, 2)
		    .doAfterTerminate(() -> onTerminate.set(true))
		    .subscribe(ts);

		ts.assertNoError()
		  .assertValues(1, 2)
		  .assertComplete();

		Assert.assertTrue("onComplete not called back", onTerminate.get());
	}

	@Test
	public void syncPollCompleteCalled() {
		AtomicBoolean onComplete = new AtomicBoolean();
		Flux<Integer> f = Flux.just(1)
		                      .doOnComplete(() -> onComplete.set(true));
		StepVerifier.create(f)
		            .expectFusion()
		            .expectNext(1)
		            .verifyComplete();

		assertThat(onComplete.get()).withFailMessage("onComplete not called back").isTrue();
	}

	@Test
	public void syncPollConditionalCompleteCalled() {
		AtomicBoolean onComplete = new AtomicBoolean();
		Flux<Integer> f = Flux.just(1)
		                      .doOnComplete(() -> onComplete.set(true))
		                      .filter(v -> true);
		StepVerifier.create(f)
		            .expectFusion()
		            .expectNext(1)
		            .verifyComplete();

		assertThat(onComplete.get()).withFailMessage("onComplete not called back").isTrue();
	}

	@Test
	public void syncPollAfterTerminateCalledWhenComplete() {
		AtomicBoolean onAfterTerminate = new AtomicBoolean();
		Flux<Integer> f = Flux.just(1)
		                      .doAfterTerminate(() -> onAfterTerminate.set(true));
		StepVerifier.create(f)
		            .expectFusion()
                    .expectNext(1)
		            .verifyComplete();

		assertThat(onAfterTerminate.get()).withFailMessage("onAfterTerminate not called back").isTrue();
	}

	@Test
	public void syncPollConditionalAfterTerminateCalledWhenComplete() {
		AtomicBoolean onAfterTerminate = new AtomicBoolean();
		Flux<Integer> f = Flux.just(1)
		                      .doAfterTerminate(() -> onAfterTerminate.set(true))
		                      .filter(v -> true);
		StepVerifier.create(f)
		            .expectFusion()
		            .expectNext(1)
		            .verifyComplete();

		assertThat(onAfterTerminate.get()).withFailMessage("onAfterTerminate not called back").isTrue();
	}

	@Test
	public void syncPollAfterTerminateCalledWhenError() {
		AtomicBoolean onAfterTerminate = new AtomicBoolean();
		Flux<Integer> f = Flux.just(1, 0, 3)
		                      .map(i -> 100 / i)
		                      .doAfterTerminate(() -> onAfterTerminate.set(true));
		StepVerifier.create(f)
		            .expectFusion()
		            .expectNext(100)
		            .verifyError(ArithmeticException.class);

		assertThat(onAfterTerminate.get()).withFailMessage("onAfterTerminate not called back").isTrue();
	}

	@Test
	public void syncPollConditionalAfterTerminateCalledWhenError() {
		AtomicBoolean onAfterTerminate = new AtomicBoolean();
		Flux<Integer> f = Flux.just(1, 0, 3)
		                      .map(i -> 100 / i)
		                      .doAfterTerminate(() -> onAfterTerminate.set(true))
		                      .filter(v -> true);
		StepVerifier.create(f)
		            .expectFusion()
		            .expectNext(100)
		            .verifyError(ArithmeticException.class);

		assertThat(onAfterTerminate.get()).withFailMessage("onAfterTerminate not called back").isTrue();
	}

	@Test
	public void syncPollAfterTerminateFailureWhenError() {
		Flux<Integer> f = Flux.just(1, 0, 3)
		                      .map(i -> 100 / i)
		                      .doAfterTerminate(() -> { throw new IllegalStateException("doAfterTerminate boom"); });
		StepVerifier.create(f)
		            .expectFusion()
		            .expectNext(100)
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessage("doAfterTerminate boom")
				            .hasSuppressedException(new ArithmeticException("/ by zero"))
		            );
	}

	@Test
	public void syncPollConditionalAfterTerminateFailureWhenError() {
		Flux<Integer> f = Flux.just(1, 0, 3)
		                      .map(i -> 100 / i)
		                      .doAfterTerminate(() -> { throw new IllegalStateException("doAfterTerminate boom"); })
		                      .filter(v -> true);
		StepVerifier.create(f)
		            .expectFusion()
		            .expectNext(100)
		            .verifyErrorSatisfies(e -> assertThat(e)
				            .isInstanceOf(IllegalStateException.class)
				            .hasMessage("doAfterTerminate boom")
				            .hasSuppressedException(new ArithmeticException("/ by zero"))
		            );
	}

	@Test
	public void fusedDoOnNextOnErrorBothFailing() {
		ConnectableFlux<Integer> f = Flux.just(1)
		                                 .doOnNext(i -> {
			                                 throw new IllegalArgumentException("fromOnNext");
		                                 })
		                                 .doOnError(e -> {
			                                 throw new IllegalStateException("fromOnError", e);
		                                 })
		                                 .publish();

		StepVerifier.create(f)
		            .then(f::connect)
		            .verifyErrorMatches(e -> e instanceof IllegalStateException
				            && "fromOnError".equals(e.getMessage())
				            && e.getCause() instanceof IllegalArgumentException
				            && "fromOnNext".equals(e.getCause().getMessage()));
	}

	@Test
	public void fusedDoOnNextOnErrorDoOnErrorAllFailing() {
		ConnectableFlux<Integer> f = Flux.just(1)
		                                 .doOnNext(i -> {
			                                 throw new IllegalArgumentException("fromOnNext");
		                                 })
		                                 .doOnError(e -> {
			                                 throw new IllegalStateException("fromOnError", e);
		                                 })
		                                 .doOnError(e -> {
			                                 throw new IllegalStateException("fromOnError2", e);
		                                 })
		                                 .publish();

		StepVerifier.create(f)
		            .then(f::connect)
		            .verifyErrorSatisfies(e -> {
					            assertThat(e)
					                      .isInstanceOf(IllegalStateException.class)
					                      .hasMessage("fromOnError2")
					                      .hasCauseInstanceOf(IllegalStateException.class);
					            assertThat(e.getCause())
					                      .hasMessage("fromOnError")
					                      .hasCauseInstanceOf(IllegalArgumentException.class);
					            assertThat(e.getCause().getCause())
					                      .hasMessage("fromOnNext");
				            });
	}

	@Test
	public void fusedDoOnNextCallsOnErrorWhenFailing() {
		AtomicBoolean passedOnError = new AtomicBoolean();

		ConnectableFlux<Integer> f = Flux.just(1)
		                                 .doOnNext(i -> {
			                                 throw new IllegalArgumentException("fromOnNext");
		                                 })
		                                 .doOnError(e -> passedOnError.set(true))
		                                 .publish();

		StepVerifier.create(f)
		            .then(f::connect)
		            .verifyErrorMatches(e -> e instanceof IllegalArgumentException
				            && "fromOnNext".equals(e.getMessage()));

		assertThat(passedOnError.get()).isTrue();
	}

	@Test
	public void conditionalFusedDoOnNextOnErrorBothFailing() {
		ConnectableFlux<Integer> f = Flux.just(1)
		                                 .doOnNext(i -> {
			                                 throw new IllegalArgumentException("fromOnNext");
		                                 })
		                                 .doOnError(e -> {
			                                 throw new IllegalStateException("fromOnError", e);
		                                 })
		                                 .filter(v -> true)
		                                 .publish();

		StepVerifier.create(f)
		            .then(f::connect)
		            .verifyErrorMatches(e -> e instanceof IllegalStateException
				            && "fromOnError".equals(e.getMessage())
				            && e.getCause() instanceof IllegalArgumentException
				            && "fromOnNext".equals(e.getCause().getMessage()));
	}

	@Test
	public void conditionalFusedDoOnNextOnErrorDoOnErrorAllFailing() {
		ConnectableFlux<Integer> f = Flux.just(1)
		                                 .doOnNext(i -> {
			                                 throw new IllegalArgumentException("fromOnNext");
		                                 })
		                                 .doOnError(e -> {
			                                 throw new IllegalStateException("fromOnError", e);
		                                 })
		                                 .doOnError(e -> {
			                                 throw new IllegalStateException("fromOnError2", e);
		                                 })
		                                 .filter(v -> true)
		                                 .publish();

		StepVerifier.create(f)
		            .then(f::connect)
		            .verifyErrorSatisfies(e -> {
					            assertThat(e)
					                      .isInstanceOf(IllegalStateException.class)
					                      .hasMessage("fromOnError2")
					                      .hasCauseInstanceOf(IllegalStateException.class);
					            assertThat(e.getCause())
					                      .hasMessage("fromOnError")
					                      .hasCauseInstanceOf(IllegalArgumentException.class);
					            assertThat(e.getCause().getCause())
					                      .hasMessage("fromOnNext");
				            });
	}

	@Test
	public void conditionalFusedDoOnNextCallsOnErrorWhenFailing() {
		AtomicBoolean passedOnError = new AtomicBoolean();

		ConnectableFlux<Integer> f = Flux.just(1)
		                                 .doOnNext(i -> {
			                                 throw new IllegalArgumentException("fromOnNext");
		                                 })
		                                 .doOnError(e -> passedOnError.set(true))
		                                 .filter(v -> true)
		                                 .publish();

		StepVerifier.create(f)
		            .then(f::connect)
		            .verifyErrorMatches(e -> e instanceof IllegalArgumentException
				            && "fromOnNext".equals(e.getMessage()));

		assertThat(passedOnError.get()).isTrue();
	}

	@Test
	public void should_reduce_to_10_events() {
		for (int i = 0; i < 20; i++) {
			int n = i;
			List<Integer> rs = Collections.synchronizedList(new ArrayList<>());
			AtomicInteger count = new AtomicInteger();
			Flux.range(0, 10)
			    .flatMap(x -> Flux.range(0, 2)
			                      .doOnNext(rs::add)
			                      .map(y -> blockingOp(x, y))
			                      .subscribeOn(Schedulers.parallel())
			                      .reduce((l, r) -> l + "_" + r +" ("+x+", it:"+n+")")
			    )
			    .doOnNext(s -> {
				    count.incrementAndGet();
			    })
			    .blockLast();

			assertEquals(10, count.get());
		}
	}

	@Test
	public void should_reduce_to_10_events_conditional() {
		for (int i = 0; i < 20; i++) {
			AtomicInteger count = new AtomicInteger();
			Flux.range(0, 10)
			    .flatMap(x -> Flux.range(0, 2)
			                      .map(y -> blockingOp(x, y))
			                      .subscribeOn(Schedulers.parallel())
			                      .reduce((l, r) -> l + "_" + r)
			                      .doOnSuccess(s -> {
				                      count.incrementAndGet();
			                      })
			                      .filter(v -> true))
			    .blockLast();

			assertEquals(10, count.get());
		}
	}

	static String blockingOp(Integer x, Integer y) {
		try {
			sleep(10);
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
		return "x" + x + "y" + y;
	}


	@Test
    public void scanFuseableSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxPeek<Integer> peek = new FluxPeek<>(Flux.just(1), s -> {}, s -> {},
        		e -> {}, () -> {}, () -> {}, r -> {}, () -> {});
        PeekFuseableSubscriber<Integer> test = new PeekFuseableSubscriber<>(actual, peek);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }

    @Test
    public void scanFuseableConditionalSubscriber() {
	    @SuppressWarnings("unchecked")
	    Fuseable.ConditionalSubscriber<Integer> actual = Mockito.mock(MockUtils.TestScannableConditionalSubscriber.class);
        FluxPeek<Integer> peek = new FluxPeek<>(Flux.just(1), s -> {}, s -> {},
        		e -> {}, () -> {}, () -> {}, r -> {}, () -> {});
        PeekFuseableConditionalSubscriber<Integer> test =
        		new PeekFuseableConditionalSubscriber<>(actual, peek);
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }

	static final class SignalPeekThrowNext<T> implements SignalPeek<T> {

		private final RuntimeException exception;

		SignalPeekThrowNext(RuntimeException exception) {
			this.exception = exception;
		}

		@Override
		public Consumer<? super T> onNextCall() {
			return t -> { throw exception; };
		}

		@Nullable
		@Override
		public Consumer<? super Subscription> onSubscribeCall() {
			return null;
		}

		@Nullable
		@Override
		public Consumer<? super Throwable> onErrorCall() {
			return null;
		}

		@Nullable
		@Override
		public Runnable onCompleteCall() {
			return null;
		}

		@Nullable
		@Override
		public Runnable onAfterTerminateCall() {
			return null;
		}

		@Nullable
		@Override
		public LongConsumer onRequestCall() {
			return null;
		}

		@Nullable
		@Override
		public Runnable onCancelCall() {
			return null;
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			return null;
		}
	}

	static class ConditionalAssertSubscriber<T> implements Fuseable.ConditionalSubscriber<T> {

		List<T> next = new ArrayList<>();
		boolean subscribed;
		Throwable error;
		boolean completed;

		private final Context context;

		ConditionalAssertSubscriber(Context context) {
			this.context = context;
		}

		ConditionalAssertSubscriber() {
			this(Context.empty());
		}

		@Override
		public boolean tryOnNext(T v) {
			next.add(v);
			return true;
		}

		@Override
		public void onSubscribe(Subscription s) {
			subscribed = true;
		}

		@Override
		public void onNext(T v) {
			next.add(v);
		}

		@Override
		public void onError(Throwable throwable) {
			error = throwable;
		}

		@Override
		public void onComplete() {
			completed = true;
		}

		@Override
		public Context currentContext() {
			return context;
		}
	}

	static class AssertQueueSubscription<T> implements Fuseable.QueueSubscription<T> {
		boolean isCancelled;
		int requested;
		boolean completeWithError;

		private Queue<T> q = Queues.<T>small().get();

		public void setCompleteWithError(boolean completeWithError) {
			this.completeWithError = completeWithError;
		}

		@Override
		public int requestFusion(int requestedMode) {
			return requestedMode;
		}

		@Override
		public boolean add(T t) {
			return q.add(t);
		}

		@Override
		public boolean offer(T t) {
			return q.offer(t);
		}

		@Override
		public T remove() {
			return q.remove();
		}

		@Override
		public T poll() {
			T value = q.poll();
			if (value == null && completeWithError) {
				throw new IllegalStateException("AssertQueueSubscriber poll error");
			}
			return value;
		}

		@Override
		public T element() {
			return q.element();
		}

		@Nullable
		@Override
		public T peek() {
			return q.peek();
		}

		@Override
		public int size() {
			return q.size();
		}

		@Override
		public boolean isEmpty() {
			return q.isEmpty();
		}

		@Override
		public void clear() {
			q.clear();
		}

		@Override
		public void request(long l) {
			requested++;
		}

		@Override
		public void cancel() {
			isCancelled = true;
		}
	}

	@Test
	public void resumeConditional() {
		RuntimeException nextError = new IllegalStateException("next");
		List<Throwable> resumedErrors = new ArrayList<>();
		List<Object> resumedValues = new ArrayList<>();
		Context context = Context.of(OnNextFailureStrategy.KEY_ON_NEXT_ERROR_STRATEGY,
				OnNextFailureStrategy.resume((t, s) -> {
					resumedErrors.add(t);
					resumedValues.add(s);
				}));

		ConditionalAssertSubscriber<Integer> actual = new ConditionalAssertSubscriber<>(context);
		SignalPeekThrowNext<Integer> peekParent = new SignalPeekThrowNext<>(nextError);
		AssertQueueSubscription<Integer> qs = new AssertQueueSubscription<>();

		PeekConditionalSubscriber<Integer> test = new PeekConditionalSubscriber<>(actual, peekParent);
		test.onSubscribe(qs);

		test.onNext(1);
		assertThat(actual.next).as("onNext skips").isEmpty();
		assertThat(qs.requested).as("onNext requested more").isEqualTo(1);

		boolean tryOnNext = test.tryOnNext(2);
		assertThat(tryOnNext).as("tryOnNext skips").isFalse();

		test.onComplete();

		assertThat(actual.error).isNull();
		assertThat(actual.completed).isTrue();

		assertThat(resumedErrors).containsExactly(nextError, nextError);
		assertThat(resumedValues).containsExactly(1, 2);
	}

	@Test
	public void resumeFuseable() {
		RuntimeException nextError = new IllegalStateException("next");
		List<Throwable> resumedErrors = new ArrayList<>();
		List<Object> resumedValues = new ArrayList<>();
		Context context = Context.of(OnNextFailureStrategy.KEY_ON_NEXT_ERROR_STRATEGY,
									 OnNextFailureStrategy.resume((t, s) -> {
										 resumedErrors.add(t);
										 resumedValues.add(s);
									 }));

		AssertSubscriber<Integer> actual = new AssertSubscriber<>(context, 0);
		SignalPeekThrowNext<Integer> peekParent = new SignalPeekThrowNext<>(nextError);
		AssertQueueSubscription<Integer> qs = new AssertQueueSubscription<>();

		PeekFuseableSubscriber<Integer> test = new PeekFuseableSubscriber<>(actual, peekParent);
		test.onSubscribe(qs);

		test.onNext(1);
		actual.assertNoValues();
		assertThat(qs.requested).as("onNext requested more").isEqualTo(1);

		qs.offer(3);
		Integer polled = test.poll();
		assertThat(polled).as("poll skips").isNull();

		test.onComplete();

		actual.assertNoValues();
		actual.assertNoError();
		actual.assertComplete();

		assertThat(resumedErrors).containsExactly(nextError, nextError);
		assertThat(resumedValues).containsExactly(1, 3);
	}

	@Test
	public void resumeFuseableConditional() {
		RuntimeException nextError = new IllegalStateException("next");
		List<Throwable> resumedErrors = new ArrayList<>();
		List<Object> resumedValues = new ArrayList<>();
		Context context = Context.of(OnNextFailureStrategy.KEY_ON_NEXT_ERROR_STRATEGY,
									 OnNextFailureStrategy.resume((t, s) -> {
										 resumedErrors.add(t);
										 resumedValues.add(s);
									 }));

		ConditionalAssertSubscriber<Integer> actual = new ConditionalAssertSubscriber<>(context);
		SignalPeekThrowNext<Integer> peekParent = new SignalPeekThrowNext<>(nextError);
		AssertQueueSubscription<Integer> qs = new AssertQueueSubscription<>();

		PeekFuseableConditionalSubscriber<Integer> test = new PeekFuseableConditionalSubscriber<>(actual, peekParent);
		test.onSubscribe(qs);

		test.onNext(1);
		assertThat(actual.next).as("onNext skips").isEmpty();
		assertThat(qs.requested).as("onNext requested more").isEqualTo(1);

		boolean tryOnNext = test.tryOnNext(2);
		assertThat(tryOnNext).as("tryOnNext skips").isFalse();

		qs.offer(3);
		Integer polled = test.poll();
		assertThat(polled).as("poll skips").isNull();

		test.onComplete();

		assertThat(actual.error).isNull();
		assertThat(actual.completed).isTrue();

		assertThat(resumedErrors).containsExactly(nextError, nextError, nextError);
		assertThat(resumedValues).containsExactly(1, 2, 3);
	}
}

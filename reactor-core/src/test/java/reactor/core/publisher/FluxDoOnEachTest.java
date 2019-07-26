/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Fuseable.ConditionalSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.FluxDoOnEach.DoOnEachConditionalSubscriber;
import reactor.core.publisher.FluxDoOnEach.DoOnEachFuseableConditionalSubscriber;
import reactor.core.publisher.FluxPeekFuseableTest.AssertQueueSubscription;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

@RunWith(JUnitParamsRunner.class)
public class FluxDoOnEachTest {

	@Test(expected = NullPointerException.class)
	public void nullSource() {
		new FluxDoOnEach<>(null, null);
	}

	private static final String sourceErrorMessage = "boomSource";

	private Object[] sourcesError() {
		return new Object[] {
				new Object[] { Flux.<Integer>error(new IllegalStateException(sourceErrorMessage))
						.hide() },
				new Object[] { Flux.<Integer>error(new IllegalStateException(sourceErrorMessage))
				                   .hide().filter(i -> true) },
				new Object[] { Flux.<Integer>error(new IllegalStateException(sourceErrorMessage)) },
				new Object[] { Flux.<Integer>error(new IllegalStateException(sourceErrorMessage))
						.filter(i -> true) }
		};
	}

	private Object[] sources12Complete() {
		return new Object[] {
				new Object[] { Flux.just(1,2).hide() },
				new Object[] { Flux.just(1,2).hide().filter(i -> true) },
				new Object[] { Flux.just(1,2) },
				new Object[] { Flux.just(1,2).filter(i -> true) }
		};
	}

	private Object[] sourcesEmpty() {
		return new Object[] {
				new Object[] { Flux.<Integer>empty().hide() },
				new Object[] { Flux.<Integer>empty().hide().filter(i -> true) },
				new Object[] { Flux.<Integer>empty() },
				new Object[] { Flux.<Integer>empty().filter(i -> true) }
		};
	}

	private Object[] sourcesNever() {
		return new Object[] {
				new Object[] { Flux.<Integer>never().hide() },
				new Object[] { Flux.<Integer>never().hide().filter(i -> true) },
				new Object[] { Flux.<Integer>never() },
				new Object[] { Flux.<Integer>never().filter(i -> true) }
		};
	}

	@Test
	@Parameters(method = "sources12Complete")
	public void normal(Flux<Integer> source) {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		LongAdder state = new LongAdder();

		source.doOnEach(s -> {
			    if (s.isOnNext()) {
				    onNext.set(s.get());
				    state.increment();
			    }
			    else if (s.isOnError()) {
				    onError.set(s.getThrowable());
			    }
			    else if (s.isOnComplete()) {
				    onComplete.set(true);
			    }
		})
		      .filter(t -> true)
		    .subscribe(ts);

		assertThat(onNext).hasValue(2);
		assertThat(onError).hasValue(null);
		assertThat(onComplete).isTrue();

		assertThat(state.intValue()).isEqualTo(2);
	}

	//see https://github.com/reactor/reactor-core/issues/1056
	@Test
	public void fusion() {
		AtomicInteger invocationCount = new AtomicInteger();

		Flux<String> sourceWithFusionAsync = Flux.just("foo")
		                                         .publishOn(Schedulers.elastic())
		                                         .flatMap(v -> Flux.just("flatMap_" + v)
		                                                           .doOnEach(sig -> invocationCount.incrementAndGet())
		                                         );

		StepVerifier.create(sourceWithFusionAsync)
		            .expectNoFusionSupport()
		            .expectNext("flatMap_foo")
		            .verifyComplete();

		assertThat(invocationCount).as("doOnEach invoked").hasValue(2);
	}

	@Test
	public void fusedSync() {
		AtomicReference<String> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		LongAdder state = new LongAdder();

		StepVerifier.create(Flux.just("sync")
		                        .doOnEach(s -> {
			                        if (s.isOnNext()) {
				                        onNext.set(s.get());
				                        state.increment();
			                        }
			                        else if (s.isOnError()) {
				                        onError.set(s.getThrowable());
			                        }
			                        else if (s.isOnComplete()) {
				                        onComplete.set(true);
			                        }
		                        }))
		            .expectFusion(Fuseable.SYNC, Fuseable.SYNC)
		            .expectNext("sync")
		            .verifyComplete();

		assertThat(onNext).hasValue("sync");
		assertThat(onError).hasValue(null);
		assertThat(onComplete).isTrue();
	}

	@Test
	public void fusedSyncCallbackError() {
		AtomicReference<String> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		LongAdder state = new LongAdder();

		StepVerifier.create(Flux.just("sync")
		                        .doOnEach(s -> {
			                        if (s.isOnNext()) {
				                        onNext.set(s.get());
				                        state.increment();
			                        }
			                        else if (s.isOnError()) {
				                        onError.set(s.getThrowable());
			                        }
			                        else if (s.isOnComplete()) {
			                        	throw new IllegalStateException("boom");
			                        }
		                        }))
		            .expectFusion(Fuseable.SYNC, Fuseable.SYNC)
		            .expectNext("sync")
		            .verifyErrorMessage("boom");

		assertThat(onNext).hasValue("sync");
		assertThat(onError.get()).isNull();
		assertThat(onComplete).isFalse();
	}

	@Test
	public void fusedAsync() {
		AtomicReference<String> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		LongAdder state = new LongAdder();

		StepVerifier.create(Flux.just("foo")
		                        .publishOn(Schedulers.immediate())
		                        .map(s -> s + "_async")
		                        .doOnEach(s -> {
			                        if (s.isOnNext()) {
				                        onNext.set(s.get());
				                        state.increment();
			                        }
			                        else if (s.isOnError()) {
				                        onError.set(s.getThrowable());
			                        }
			                        else if (s.isOnComplete()) {
				                        onComplete.set(true);
			                        }
		                        }))
		            .expectFusion(Fuseable.ASYNC, Fuseable.ASYNC)
		            .expectNext("foo_async")
		            .verifyComplete();

		assertThat(onNext).hasValue("foo_async");
		assertThat(onError).hasValue(null);
		assertThat(onComplete).isTrue();
	}

	@Test
	public void fusedAsyncCallbackTransientError() {
		AtomicReference<String> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		LongAdder state = new LongAdder();

		StepVerifier.create(Flux.just("foo")
		                        .publishOn(Schedulers.immediate())
		                        .map(s -> s + "_async")
		                        .doOnEach(s -> {
			                        if (s.isOnNext()) {
				                        onNext.set(s.get());
				                        state.increment();
			                        }
			                        else if (s.isOnError()) {
				                        onError.set(s.getThrowable());
			                        }
			                        else if (s.isOnComplete()) {
			                        	throw new IllegalStateException("boom");
			                        }
		                        }))
		            .expectFusion(Fuseable.ASYNC, Fuseable.ASYNC)
		            .expectNext("foo_async")
		            .verifyErrorMessage("boom");

		assertThat(onNext).hasValue("foo_async");
		assertThat(onError.get()).isNotNull().hasMessage("boom");
		assertThat(onComplete).isFalse();
	}

	@Test
	public void fusedAsyncCallbackErrorsOnTerminal() {
		AtomicReference<String> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		LongAdder state = new LongAdder();

		StepVerifier.create(Flux.just("foo")
		                        .publishOn(Schedulers.immediate())
		                        .map(s -> s + "_async")
		                        .doOnEach(s -> {
		                        	if (s.isOnNext()) {
		                        		onNext.set(s.get());
			                        }
			                        else {
				                        throw new IllegalStateException("boom");
			                        }
		                        }))
		            .expectFusion(Fuseable.ASYNC, Fuseable.ASYNC)
		            .expectNext("foo_async")
		            .verifyErrorMessage("boom");

		assertThat(onNext).hasValue("foo_async");
		assertThat(onError.get()).isNull();
		assertThat(onComplete).isFalse();
	}

	@Test
	@Parameters(method = "sourcesError")
	public void error(Flux<Integer> source) {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		LongAdder state = new LongAdder();

		source
		    .doOnEach(s -> {
			    if (s.isOnNext()) {
				    onNext.set(s.get());
				    state.increment();
			    }
			    else if (s.isOnError()) {
				    onError.set(s.getThrowable());
			    }
			    else if (s.isOnComplete()) {
				    onComplete.set(true);
			    }
		    })
		    .filter(t -> true)
		    .subscribe(ts);

		assertThat(onNext).hasValue(null);
		assertThat(onError.get()).isInstanceOf(IllegalStateException.class)
		                         .hasMessage(sourceErrorMessage);
		assertThat(onComplete).isFalse();
		assertThat(state.intValue()).isZero();
	}

	@Test
	@Parameters(method = "sourcesEmpty")
	public void empty(Flux<Integer> source) {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		LongAdder state = new LongAdder();

		source
		    .doOnEach(s -> {
			    if (s.isOnNext()) {
				    onNext.set(s.get());
				    state.increment();
			    }
			    else if (s.isOnError()) {
				    onError.set(s.getThrowable());
			    }
			    else if (s.isOnComplete()) {
				    onComplete.set(true);
			    }
		    })
		    .filter(t -> true)
		    .subscribe(ts);

		assertThat(onNext).hasValue(null);
		assertThat(onError).hasValue(null);
		assertThat(onComplete).isTrue();
		assertThat(state.intValue()).isEqualTo(0);
	}

	@Test
	@Parameters(method = "sourcesNever")
	public void never(Flux<Integer> source) {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		LongAdder state = new LongAdder();

		source
		    .doOnEach(s -> {
			    if (s.isOnNext()) {
				    onNext.set(s.get());
				    state.increment();
			    }
			    else if (s.isOnError()) {
				    onError.set(s.getThrowable());
			    }
			    else if (s.isOnComplete()) {
				    onComplete.set(true);
			    }
		    })
		    .filter(t -> true)
		    .subscribe(ts);

		assertThat(onNext).hasValue(null);
		assertThat(onError).hasValue(null);
		assertThat(onComplete).isFalse();
		assertThat(state.intValue()).isEqualTo(0);
	}

	@Test
	@Parameters(method = "sources12Complete")
	public void nextCallbackError(Flux<Integer> source) {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		LongAdder state = new LongAdder();

		Throwable err = new Exception("test");

		source
		    .doOnEach(s -> {
			    if (s.isOnNext()) {
				    state.increment();
				    throw Exceptions.propagate(err);
			    }
		    })
		    .filter(t -> true)
		    .subscribe(ts);

		//nominal error path (DownstreamException)
		ts.assertErrorMessage("test");
		Assert.assertEquals(1, state.intValue());
	}

	@Test
	@Parameters(method = "sources12Complete")
	public void nextCallbackBubbleError(Flux<Integer> source) {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		LongAdder state = new LongAdder();

		Throwable err = new Exception("test");

		try {
			source
			    .doOnEach(s -> {
				    if (s.isOnNext()) {
					    state.increment();
					    throw Exceptions.bubble(err);
				    }
			    })
			    .filter(t -> true)
			    .subscribe(ts);

			fail();
		}
		catch (Exception e) {
			Assert.assertTrue(Exceptions.unwrap(e) == err);
			Assert.assertEquals(1, state.intValue());
		}
	}

	@Test
	@Parameters(method = "sources12Complete")
	public void completeCallbackError(Flux<Integer> source) {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		AtomicBoolean completeHandled = new AtomicBoolean();
		AtomicBoolean errorHandled = new AtomicBoolean();

		Throwable err = new Exception("test");

		source
		    .doOnEach(s -> {
			    if (s.isOnComplete()) {
				    completeHandled.set(true);
				    throw Exceptions.propagate(err);
			    }
			    if (s.isOnError()) {
			    	errorHandled.set(true);
			    }
		    })
		    .filter(t -> true)
		    .subscribe(ts);

		ts.assertErrorMessage("test");
		assertThat(completeHandled).as("complete() handler triggered")
		                        .isTrue();
		assertThat(errorHandled).as("complete() failure passed to error handler triggered")
		                        .isTrue();
	}

	@Test
	@Parameters(method = "sourcesError")
	public void errorCallbackError(Flux<Integer> source) {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		LongAdder state = new LongAdder();

		IllegalStateException err = new IllegalStateException("test");

		source
		    .doOnEach(s -> {
			    if (s.isOnError()) {
				    state.increment();
				    throw Exceptions.propagate(err);
			    }
		    })
		    .filter(t -> true)
		    .subscribe(ts);

		ts.assertNoValues();
		ts.assertError(IllegalStateException.class);
		ts.assertErrorWith(e -> e.getSuppressed()[0].getMessage().equals(sourceErrorMessage));
		ts.assertErrorWith(e -> e.getMessage().equals("test"));

		Assert.assertEquals(1, state.intValue());
	}

	@Test
	public void conditionalTryOnNext() {
		ArrayList<Signal<Boolean>> signals = new ArrayList<>();
		ConditionalSubscriber<Boolean> actual = new FluxPeekFuseableTest.ConditionalAssertSubscriber<Boolean>() {
			@Override
			public boolean tryOnNext(Boolean v) {
				super.tryOnNext(v);
				return v;
			}
		};
		DoOnEachConditionalSubscriber<Boolean> test = new DoOnEachConditionalSubscriber<>(actual, signals::add, false);
		AssertQueueSubscription<Boolean> qs = new AssertQueueSubscription<>();

		test.onSubscribe(qs);

		assertThat(test.tryOnNext(true)).isTrue();
		assertThat(test.tryOnNext(false)).isFalse();
		test.onComplete();

		assertThat(signals).hasSize(2);
		assertThat(signals.get(0)).matches(Signal::isOnNext)
		                          .matches(s -> s.get() == Boolean.TRUE);
		assertThat(signals.get(1)).matches(Signal::isOnComplete);

		List<Boolean> actualTryNext = ((FluxPeekFuseableTest.ConditionalAssertSubscriber<Boolean>) actual).next;
		assertThat(actualTryNext).hasSize(2);
		assertThat(actualTryNext.get(0)).isTrue();
		assertThat(actualTryNext.get(1)).isFalse();
	}

	@Test
	public void conditionalFuseableTryOnNext() {
		ArrayList<Signal<Boolean>> signals = new ArrayList<>();
		FluxPeekFuseableTest.ConditionalAssertSubscriber<Boolean> actual = new FluxPeekFuseableTest.ConditionalAssertSubscriber<Boolean>() {
			@Override
			public boolean tryOnNext(Boolean v) {
				super.tryOnNext(v);
				return v;
			}
		};
		DoOnEachFuseableConditionalSubscriber<Boolean> test = new DoOnEachFuseableConditionalSubscriber<>(actual, signals::add, false);
		AssertQueueSubscription<Boolean> qs = new AssertQueueSubscription<>();

		test.onSubscribe(qs);

		assertThat(test.tryOnNext(true)).isTrue();
		assertThat(test.tryOnNext(false)).isFalse();
		test.onComplete();

		assertThat(signals).hasSize(2);
		assertThat(signals.get(0)).matches(Signal::isOnNext)
		                          .matches(s -> s.get() == Boolean.TRUE);
		assertThat(signals.get(1)).matches(Signal::isOnComplete);

		List<Boolean> actualTryNext = actual.next;
		assertThat(actualTryNext).hasSize(2);
		assertThat(actualTryNext.get(0)).isTrue();
		assertThat(actualTryNext.get(1)).isFalse();
	}

	@Test
	public void nextCompleteAndErrorHaveContext() {
		Context context = Context.of("foo", "bar");
		List<Signal> signals = new ArrayList<>();

		StepVerifier.create(Flux.just("hello")
		                        .doOnEach(signals::add),
				StepVerifierOptions.create().withInitialContext(context))
		            .expectNext("hello")
		            .verifyComplete();

		assertThat(signals)
		          .allSatisfy(signal -> assertThat(signal.getContext().hasKey("foo"))
				          .as("has Context value")
				          .isTrue());
	}

	@Test
    public void scanSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxDoOnEach<Integer> peek =
				new FluxDoOnEach<>(Flux.just(1), s -> { });
		FluxDoOnEach.DoOnEachSubscriber<Integer> test =
				new FluxDoOnEach.DoOnEachSubscriber<>(actual, peek.onSignal, false);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

        assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

        assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }

    //https://github.com/reactor/reactor-core/issues/1067
	@Test
	public void shallExecuteSideEffectsCallback() {

		Flux<Integer> result = Mono.just(Arrays.asList(1, 2))
		                           //.doOnEach(sig -> System.out.println("SIGNAL beforeMap " + sig))// <- if enabled than everything is fine
		                           .map(x -> x)
		                           .doOnEach(sig -> {throw new RuntimeException("expected");})
		                           .flatMapIterable(Function.identity());


		StepVerifier.create(result).expectError().verify();
	}

}
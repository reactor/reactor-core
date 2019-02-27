/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class FluxDoOnEachTest {

	@Test(expected = NullPointerException.class)
	public void nullSource() {
		new FluxDoOnEach<>(null, null);
	}

	@Test
	public void normal() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		LongAdder state = new LongAdder();

		Flux.just(1, 2)
		    .hide()
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
		    .subscribe(ts);

		Assert.assertEquals((Integer) 2, onNext.get());
		Assert.assertNull(onError.get());
		Assert.assertTrue(onComplete.get());

		Assert.assertEquals(2, state.intValue());
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
	public void error() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		LongAdder state = new LongAdder();

		Flux.error(new RuntimeException("forced " + "failure"))
		    .cast(Integer.class)
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
		    .subscribe(ts);

		Assert.assertNull(onNext.get());
		Assert.assertTrue(onError.get() instanceof RuntimeException);
		Assert.assertFalse(onComplete.get());
	}

	@Test
	public void empty() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		LongAdder state = new LongAdder();

		Flux.empty()
		    .cast(Integer.class)
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
		    .subscribe(ts);

		Assert.assertNull(onNext.get());
		Assert.assertNull(onError.get());
		Assert.assertTrue(onComplete.get());
	}

	@Test
	public void never() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();

		AtomicReference<Integer> onNext = new AtomicReference<>();
		AtomicReference<Throwable> onError = new AtomicReference<>();
		AtomicBoolean onComplete = new AtomicBoolean();
		LongAdder state = new LongAdder();

		Flux.never()
		    .cast(Integer.class)
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
		    .subscribe(ts);

		Assert.assertNull(onNext.get());
		Assert.assertNull(onError.get());
		Assert.assertFalse(onComplete.get());
	}

	@Test
	public void nextCallbackError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		LongAdder state = new LongAdder();

		Throwable err = new Exception("test");

		Flux.just(1)
		    .doOnEach(s -> {
			    if (s.isOnNext()) {
				    state.increment();
				    throw Exceptions.propagate(err);
			    }
		    })
		    .subscribe(ts);

		//nominal error path (DownstreamException)
		ts.assertErrorMessage("test");
		Assert.assertEquals(1, state.intValue());
	}

	@Test
	public void nextCallbackBubbleError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		LongAdder state = new LongAdder();

		Throwable err = new Exception("test");

		try {
			Flux.just(1)
			    .doOnEach(s -> {
				    if (s.isOnNext()) {
					    state.increment();
					    throw Exceptions.bubble(err);
				    }
			    })
			    .subscribe(ts);

			fail();
		}
		catch (Exception e) {
			Assert.assertTrue(Exceptions.unwrap(e) == err);
			Assert.assertEquals(1, state.intValue());
		}
	}

	@Test
	public void completeCallbackError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		LongAdder state = new LongAdder();

		Throwable err = new Exception("test");

		Flux.just(1)
		    .doOnEach(s -> {
			    if (s.isOnComplete()) {
				    state.increment();
				    throw Exceptions.propagate(err);
			    }
		    })
		    .subscribe(ts);

		ts.assertErrorMessage("test");
		Assert.assertEquals(1, state.intValue());
	}

	@Test
	public void errorCallbackError() {
		AssertSubscriber<String> ts = AssertSubscriber.create();
		LongAdder state = new LongAdder();

		IllegalStateException err = new IllegalStateException("test");

		Flux.error(new IllegalStateException("bar"))
		    .cast(String.class)
		    .doOnEach(s -> {
			    if (s.isOnError()) {
				    state.increment();
				    throw Exceptions.propagate(err);
			    }
		    })
		    .subscribe(ts);

		ts.assertNoValues();
		ts.assertError(IllegalStateException.class);
		ts.assertErrorWith(e -> e.getSuppressed()[0].getMessage().equals("bar"));

		Assert.assertEquals(1, state.intValue());
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
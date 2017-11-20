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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.context.Context;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static org.junit.Assert.fail;

public class FluxDoOnEachTest {

	@Test
	public void nullSourceWithConsumer() {
		Assertions.assertThatNullPointerException()
		          .isThrownBy(() -> new FluxDoOnEach<>(null, s -> {}))
		          .withMessage(null);
	}

	@Test
	public void nullConsumer() {
		Assertions.assertThatNullPointerException()
		          .isThrownBy(() -> new FluxDoOnEach<>(Flux.just("foo"), (Consumer) null))
		          .withMessage("onSignal");
	}

	@Test
	public void nullSourceWithBiConsumer() {
		Assertions.assertThatNullPointerException()
		          .isThrownBy(() -> new FluxDoOnEach<>(null, (s,c) -> {}))
		          .withMessage(null);
	}
	@Test
	public void nullBiConsumer() {
		Assertions.assertThatNullPointerException()
		          .isThrownBy(() -> new FluxDoOnEach<>(Flux.just("foo"), (BiConsumer) null))
		          .withMessage("onSignalAndContext");
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
    public void scanSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxDoOnEach<Integer> peek =
				new FluxDoOnEach<>(Flux.just(1), s -> { });
		FluxDoOnEach.DoOnEachSubscriber<Integer> test =
				new FluxDoOnEach.DoOnEachSubscriber<>(actual, peek.onSignalAndContext);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
    }

    @Test
	public void nextCompleteWithContext() {
	    List<Tuple2<Signal, Context>> signalsAndContext = new ArrayList<>();
		Flux.just(1, 2)
		    .doWithContext((s, c) -> signalsAndContext.add(Tuples.of(s, c)))
		    .subscriberContext(Context.of("foo", "bar"))
		    .subscribe();

	    Assertions.assertThat(signalsAndContext)
	              .allSatisfy(t2 -> {
		              Assertions.assertThat(t2.getT1())
		                        .isNotNull();
		              Assertions.assertThat(t2.getT2().getOrDefault("foo", "baz"))
		                        .isEqualTo("bar");
	              });

	    Assertions.assertThat(signalsAndContext.stream().map(t2 -> t2.getT1().getType()))
	              .containsExactly(SignalType.ON_NEXT, SignalType.ON_NEXT, SignalType.ON_COMPLETE);
    }

    @Test
	public void nextErrorWithContext() {
	    List<Tuple2<Signal, Context>> signalsAndContext = new ArrayList<>();
	    Flux.just(1, 0)
	        .map(i -> 10 / i)
	        .doWithContext((s,c) -> signalsAndContext.add(Tuples.of(s,c)))
	        .subscriberContext(Context.of("foo", "bar"))
	        .subscribe();

	    Assertions.assertThat(signalsAndContext)
	              .allSatisfy(t2 -> {
		              Assertions.assertThat(t2.getT1())
		                        .isNotNull();
		              Assertions.assertThat(t2.getT2().getOrDefault("foo", "baz"))
		                        .isEqualTo("bar");
	              });

	    Assertions.assertThat(signalsAndContext.stream()
	                                           .map(t2 -> t2.getT1().getType()))
	              .containsExactly(SignalType.ON_NEXT, SignalType.ON_ERROR);
    }
}

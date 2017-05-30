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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;

import org.assertj.core.api.Assertions;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class FluxPeekStatefulTest {

	@Test(expected = NullPointerException.class)
	public void nullSource() {
		new FluxPeekStateful<>(null, null,null, null, null, null, null, null, null);
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
		LongAdder seedCount = new LongAdder();
		LongAdder state = new LongAdder();

		new FluxPeekStateful<>(Flux.just(1, 2).hide(),
				() -> {
					seedCount.increment();
					return state;
				},
				(sub, st) -> onSubscribe.set(sub),
				(v, st) -> {
					onNext.set(v);
					st.increment();
				},
				(e, st) -> onError.set(e),
				(st) -> onComplete.set(true),
				(st) -> onAfterComplete.set(true),
				(r, st) -> onRequest.set(r),
				(st) -> onCancel.set(true))
				.subscribe(ts);

		Assert.assertNotNull(onSubscribe.get());
		Assert.assertEquals((Integer) 2, onNext.get());
		Assert.assertNull(onError.get());
		Assert.assertTrue(onComplete.get());
		Assert.assertTrue(onAfterComplete.get());
		Assert.assertEquals(Long.MAX_VALUE, onRequest.get());
		Assert.assertFalse(onCancel.get());

		Assert.assertEquals(1, seedCount.intValue());
		Assert.assertEquals(2, state.intValue());
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
		LongAdder seedCount = new LongAdder();
		LongAdder state = new LongAdder();

		new FluxPeekStateful<>(new FluxError<Integer>(new RuntimeException("forced " + "failure"),	false),
				() -> {
					seedCount.increment();
					return state;
				},
				(sub, st) -> onSubscribe.set(sub),
				(v, st) -> {
					onNext.set(v);
					st.increment();
				},
				(e, st) -> onError.set(e),
				(st) -> onComplete.set(true),
				(st) -> onAfterComplete.set(true),
				(r, st) -> onRequest.set(r),
				(st) -> onCancel.set(true))
				.subscribe(ts);

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
		LongAdder seedCount = new LongAdder();
		LongAdder state = new LongAdder();

		new FluxPeekStateful<>(Flux.<Integer>empty(),
				() -> {
					seedCount.increment();
					return state;
				},
				(sub, st) -> onSubscribe.set(sub),
				(v, st) -> {
					onNext.set(v);
					st.increment();
				},
				(e, st) -> onError.set(e),
				(st) -> onComplete.set(true),
				(st) -> onAfterComplete.set(true),
				(r, st) -> onRequest.set(r),
				(st) -> onCancel.set(true))
				.subscribe(ts);

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
		LongAdder seedCount = new LongAdder();
		LongAdder state = new LongAdder();

		new FluxPeekStateful<>(Flux.<Integer>never(),
				() -> {
					seedCount.increment();
					return state;
				},
				(sub, st) -> onSubscribe.set(sub),
				(v, st) -> {
					onNext.set(v);
					st.increment();
				},
				(e, st) -> onError.set(e),
				(st) -> onComplete.set(true),
				(st) -> onAfterComplete.set(true),
				(r, st) -> onRequest.set(r),
				(st) -> onCancel.set(true))
				.subscribe(ts);

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
		LongAdder seedCount = new LongAdder();
		LongAdder state = new LongAdder();

		new FluxPeekStateful<>(Flux.<Integer>never(),
				() -> {
					seedCount.increment();
					return state;
				},
				(sub, st) -> onSubscribe.set(sub),
				(v, st) -> {
					onNext.set(v);
					st.increment();
				},
				(e, st) -> onError.set(e),
				(st) -> onComplete.set(true),
				(st) -> onAfterComplete.set(true),
				(r, st) -> onRequest.set(r),
				(st) -> onCancel.set(true))
				.subscribe(ts);

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
	public void nextCallbackError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		LongAdder seedCount = new LongAdder();
		LongAdder state = new LongAdder();

		Throwable err = new Exception("test");

		new FluxPeekStateful<>(Flux.just(1).hide(),
				() -> {
					seedCount.increment();
					return state;
				},
				null,
				(v, s) -> {
					s.increment();
					throw Exceptions.propagate(err);
				},
				null, null, null, null, null)
				.subscribe(ts);

		//nominal error path (DownstreamException)
		ts.assertErrorMessage("test");
		Assert.assertEquals(1, seedCount.intValue());
		Assert.assertEquals(1, state.intValue());
	}

	@Test
	public void nextCallbackBubbleError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		LongAdder seedCount = new LongAdder();
		LongAdder state = new LongAdder();

		Throwable err = new Exception("test");

		try {
			new FluxPeekStateful<>(Flux.just(1).hide(),
					() -> {
						seedCount.increment();
						return state;
					},
					null,
					(v, s) -> {
						s.increment();
						throw Exceptions.bubble(err);
					}, null, null, null, null, null)
					.subscribe(ts);

			fail();
		}
		catch (Exception e) {
			Assert.assertTrue(Exceptions.unwrap(e) == err);
			Assert.assertEquals(1, seedCount.intValue());
			Assert.assertEquals(1, state.intValue());
		}
	}

	@Test
	public void completeCallbackError() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		LongAdder seedCount = new LongAdder();
		LongAdder state = new LongAdder();

		Throwable err = new Exception("test");

		new FluxPeekStateful<>(Flux.just(1).hide(),
				() -> {
					seedCount.increment();
					return state;
				},
				null, null,
				null,
				(s) -> {
					s.increment();
					throw Exceptions.propagate(err);
				}, null, null, null)
				.subscribe(ts);

		ts.assertErrorMessage("test");
		Assert.assertEquals(1, seedCount.intValue());
		Assert.assertEquals(1, state.intValue());
	}

	@Test
	public void errorCallbackError() {
		AssertSubscriber<String> ts = AssertSubscriber.create();
		LongAdder seedCount = new LongAdder();
		LongAdder state = new LongAdder();

		IllegalStateException err = new IllegalStateException("test");

		FluxPeekStateful<String, LongAdder> flux = new FluxPeekStateful<>(
				Flux.error(new IllegalArgumentException("bar")),
				() -> {
					seedCount.increment();
					return state;
				},
				null, null,
				(e, s) -> {
					s.increment();
					throw err;
				},
				null, null, null, null);

		flux.subscribe(ts);

		ts.assertNoValues();
		ts.assertError(IllegalStateException.class);
		ts.assertErrorWith(e -> e.getSuppressed()[0].getMessage().equals("bar"));

		Assert.assertEquals(1, seedCount.intValue());
		Assert.assertEquals(1, state.intValue());
	}

	@Test
	public void afterTerminateCallbackErrorDoesNotCallErrorCallback() {
		AssertSubscriber<String> ts = AssertSubscriber.create();
		LongAdder seedCount = new LongAdder();
		LongAdder state = new LongAdder();
		AtomicReference<Throwable> errorCallbackCapture = new AtomicReference<>();

		IllegalStateException err = new IllegalStateException("test");

		FluxPeekStateful<String, LongAdder> flux = new FluxPeekStateful<>(
				Flux.empty(),
				() -> {
					seedCount.increment();
					return state;
				},
				null, null,
				(e, s) -> {
					errorCallbackCapture.set(e);
					s.increment();
				}, null,
				(s) -> {
					s.increment();
					throw err;
				}, null, null);

		try {
			flux.subscribe(ts);
			fail("expected thrown exception");
		}
		catch (Exception e) {
			e.getCause().getMessage().equals(err);
		}
		ts.assertNoValues();
		ts.assertComplete();

		assertThat(errorCallbackCapture.get(), is(nullValue()));
		Assert.assertEquals(1, seedCount.intValue());
		Assert.assertEquals(1, state.intValue());
	}

	@Test
	public void afterTerminateCallbackFatalIsThrownDirectly() {
		AssertSubscriber<String> ts = AssertSubscriber.create();
		LongAdder seedCount = new LongAdder();
		LongAdder state = new LongAdder();
		AtomicReference<Throwable> errorCallbackCapture = new AtomicReference<>();
		Error fatal = new LinkageError();

		FluxPeekStateful<String, LongAdder> flux = new FluxPeekStateful<>(Flux.empty(),
				() -> {
					seedCount.increment();
					return state;
				},
				null, null,
				(e, s) -> {
					s.add(-100);
					errorCallbackCapture.set(e);
				},
				null,
				(s) -> {
					s.increment();
					throw fatal;
				},
				null,
				null);

		try {
			flux.subscribe(ts);
			fail("expected thrown exception");
		}
		catch (Throwable e) {
			assertSame(fatal, e);
		}
		ts.assertNoValues();
		ts.assertComplete();

		assertThat(errorCallbackCapture.get(), is(nullValue()));
		Assert.assertEquals(1, seedCount.intValue());
		Assert.assertEquals(1, state.intValue());
	}

	@Test
	public void afterTerminateCallbackFatalIsThrownDirectlyWithOnError() {
		AssertSubscriber<String> ts = AssertSubscriber.create();
		LongAdder seedCount = new LongAdder();
		LongAdder state = new LongAdder();
		AtomicReference<Throwable> errorCallbackCapture = new AtomicReference<>();
		Error fatal = new LinkageError();

		errorCallbackCapture.set(null);
		FluxPeekStateful<String, LongAdder> flux = new FluxPeekStateful<>(
				Flux.error(new NullPointerException()),
				() -> {
					seedCount.increment();
					return state;
				},
				null, null,
				(e, s) -> {
					s.add(-100);
					errorCallbackCapture.set(e);
				},
				null,
				(s) -> {
					s.increment();
					throw fatal;
				}, null, null);

		try {
			flux.subscribe(ts);
			fail("expected thrown exception");
		}
		catch (Throwable e) {
			assertSame(fatal, e);
		}
		ts.assertNoValues();
		ts.assertError(NullPointerException.class);
		assertThat(errorCallbackCapture.get(), is(instanceOf(NullPointerException.class)));
		Assert.assertEquals(1, seedCount.intValue());
		Assert.assertEquals(-99, state.intValue()); //onError + afterTerminate
	}

	@Test
	public void afterTerminateCallbackErrorAndErrorCallbackError2() {
		AssertSubscriber<String> ts = AssertSubscriber.create();
		LongAdder seedCount = new LongAdder();
		LongAdder state = new LongAdder();
		IllegalStateException afterTerminate = new IllegalStateException("afterTerminate");
		IllegalArgumentException error = new IllegalArgumentException("error");
		NullPointerException err = new NullPointerException();

		FluxPeekStateful<String, LongAdder> flux = new FluxPeekStateful<>(
				Flux.error(err),
				() -> {
					seedCount.increment();
					return state;
				},
				null, null,
				(e, s) -> {
					s.add(-100);
					throw error;
				}, null,
				(s) -> {
					s.increment();
					throw afterTerminate;
				},
				null, null);

		try {
			flux.subscribe(ts);
			fail("expected thrown exception");
		}
		catch (Exception e) {
			assertSame(afterTerminate, e.getCause());
			assertEquals(1, afterTerminate.getSuppressed().length);
			assertEquals(error, afterTerminate.getSuppressed()[0]);
			assertEquals(1, error.getSuppressed().length);
			assertEquals(err, error.getSuppressed()[0]);
		}
		ts.assertNoValues();
		ts.assertErrorMessage("error");
		Assert.assertEquals(1, seedCount.intValue());
		Assert.assertEquals(-99, state.intValue()); //original onError + afterTerminate
	}

	@Test
    public void scanSubscriber() {
        Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxPeekStateful<Integer, String> peek = new FluxPeekStateful<>(Flux.just(1),
        		() -> "", (t, s) -> {}, (t, s) -> {},
        		(t, s) -> {}, s -> {}, s -> {}, (r, s) -> {}, s -> {});
        FluxPeekStateful.PeekStatefulSubscriber<Integer, String> test =
        		new FluxPeekStateful.PeekStatefulSubscriber<Integer, String>(actual, peek, "");
        Subscription parent = Operators.emptySubscription();
        test.onSubscribe(parent);

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
        Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

        Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
        test.onError(new IllegalStateException("boom"));
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
    }
}
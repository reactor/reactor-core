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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.reactivestreams.Subscription;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class BaseSubscriberTest {

	@Test
	public void partialRequestAndCancel() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);
		AtomicInteger lastValue = new AtomicInteger(0);

		Flux<Integer> intFlux = Flux.range(1, 1000);
		intFlux.subscribe(new BaseSubscriber<Integer>() {

			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				request(1);
			}

			@Override
			public void hookOnNext(Integer integer) {
				assertTrue("unexpected previous value for " + integer,
						lastValue.compareAndSet(integer - 1, integer));
				if (integer < 10)
					request(1);
				else
					cancel();
			}

			@Override
			protected void hookOnComplete() {
				fail("expected cancellation, not completion");
			}

			@Override
			protected void hookOnError(Throwable throwable) {
				fail("expected cancellation, not error " + throwable);
			}

			@Override
			protected void hookFinally(SignalType type) {
				latch.countDown();
				assertThat(type, is(SignalType.CANCEL));
			}
		});

		latch.await(500, TimeUnit.MILLISECONDS);
		assertThat(lastValue.get(), is(10));
	}

	@Test
	public void onErrorCallbackNotImplemented() {
		Flux<String> flux = Flux.error(new IllegalStateException());

		try {
			flux.subscribe(new BaseSubscriber<>());
			fail("expected UnsupportedOperationException");
		} catch (UnsupportedOperationException e) {
			assertThat(e.getClass().getSimpleName(), is("ErrorCallbackNotImplemented"));
			assertThat(e.getCause(), is(instanceOf(IllegalStateException.class)));
		}
	}

	@Test
	public void onSubscribeErrorPropagatedToOnError() {
		Flux<String> flux = Flux.just("foo");
		AtomicReference<Throwable> error = new AtomicReference<>();

		flux.subscribe(new BaseSubscriber<String>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				throw new IllegalStateException("boom");
			}

			@Override
			protected void hookOnError(Throwable throwable) {
				error.set(throwable);
			}
		});
		assertThat(error.get(), is(instanceOf(IllegalStateException.class)));
	}

	@Test(expected = OutOfMemoryError.class)
	public void onSubscribeFatalThrown() {
		Flux<String> flux = Flux.just("foo");
		AtomicReference<Throwable> error = new AtomicReference<>();

		flux.subscribe(new BaseSubscriber<String>() {
			@Override
			protected void hookOnSubscribe(Subscription subscription) {
				throw new OutOfMemoryError("boom");
			}

			@Override
			protected void hookOnError(Throwable throwable) {
				error.set(throwable);
			}
		});
		assertThat(error.get(), is(nullValue()));
	}

	@Test
	public void onNextErrorPropagatedToOnError() {
		AtomicReference<Throwable> error = new AtomicReference<>();
		Flux<String> flux = Flux.just("foo");

		flux.subscribe(new BaseSubscriber<String>() {
			@Override
			protected void hookOnNext(String value) {
				throw new IllegalArgumentException("boom");
			}

			@Override
			protected void hookOnError(Throwable throwable) {
				error.set(throwable);
			}
		});

		assertThat(error.get(), is(instanceOf(IllegalArgumentException.class)));
	}

	@Test
	public void onCompleteErrorPropagatedToOnError() {
		AtomicReference<Throwable> error = new AtomicReference<>();
		Flux<String> flux = Flux.just("foo");

		flux.subscribe(new BaseSubscriber<String>() {
			@Override
			protected void hookOnComplete() {
				throw new IllegalArgumentException("boom");
			}

			@Override
			protected void hookOnError(Throwable throwable) {
				error.set(throwable);
			}
		});

		assertThat(error.get(), is(instanceOf(IllegalArgumentException.class)));
	}
}
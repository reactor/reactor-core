/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.publisher.scenarios;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.core.publisher.Signal;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.*;

public class ContextPropagationTest {

	@Before
	public void setFatal() {
		System.setProperty(Context.CONTEXT_UNSUPPORTED_PROPERTY, "true");
		Context.reloadFeatureFlag();
	}

	@After
	public void resetFatal() {
		System.clearProperty(Context.CONTEXT_UNSUPPORTED_PROPERTY);
		Context.reloadFeatureFlag();
	}

	@Test
	public void operatorsHookResolutionIgnoresContextunsupported() {
		String message = "This simple test doesn't support Context";
		Context unsupported = Context.unsupported(message);

		assertThatCode(() -> {
			Operators.enableOnDiscard(unsupported, v -> {});

			Operators.onDiscard("foo", unsupported);
			Operators.onDiscardMultiple(Stream.of("foo"), unsupported);
			Operators.onDiscard(Collections.singleton("foo"), unsupported);
			Operators.onDiscardQueueWithClear(Queues.one().get(), unsupported, null);

			Operators.onNextDropped("foo", unsupported);

			Operators.onOperatorError(new RuntimeException(), unsupported);
			Operators.onOperatorError(null, new RuntimeException(), unsupported);
			Operators.onOperatorError(null, new RuntimeException(), null, unsupported);

			Operators.onNextError(null, new RuntimeException(), unsupported, Operators.emptySubscription());
			Operators.onNextErrorFunction(unsupported);
			Operators.onNextInnerError(new RuntimeException(), unsupported, Operators.emptySubscription());
			Operators.onNextPollError("foo", new RuntimeException(), unsupported);
		})
				.doesNotThrowAnyException();

		//onErrorDropped throws if no hook
		assertThatExceptionOfType(RuntimeException.class)
				.isThrownBy(() -> Operators.onErrorDropped(new RuntimeException("onErrorDropped, expected"), unsupported))
				.withMessage("java.lang.RuntimeException: onErrorDropped, expected")
				.matches(Exceptions::isBubbling);
	}

	@Test
	public void rsSubscriberDirectIsRejected() throws InterruptedException {
		Mono<String> source = Mono.subscriberContext().map(ctx -> ctx.getOrDefault("foo", "baz"));

		AtomicReference<String> nextRef = new AtomicReference<>();
		AtomicReference<Signal<String>> endRef = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(1);

		Subscriber<String> rsSubscriber = newRsSubscriber(nextRef, endRef, latch);

		source.subscribe(rsSubscriber);
		latch.await(1, TimeUnit.SECONDS);

		assertThat(nextRef).hasValue(null);
		assertThat(endRef.get()).matches(Signal::isOnError);
		assertThat(endRef.get().getThrowable()).satisfies(e -> assertThat(e)
		                        .hasCause(new RuntimeException("vanilla Reactive Streams Subscriber"))
		                        .hasMessage("Context#getOrDefault is not supported due to Context-incompatible element in the chain"));
	}

	@Test
	public void rsSubscriberWithContextWriteIsOk() throws InterruptedException {
		Mono<String> source = Mono.subscriberContext()
		                          .map(ctx -> ctx.getOrDefault("foo", "baz"))
		                          .subscriberContext(Context.of("notFoo", "bar"));

		AtomicReference<String> nextRef = new AtomicReference<>();
		AtomicReference<Signal<String>> endRef = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(1);

		Subscriber<String> rsSubscriber = newRsSubscriber(nextRef, endRef, latch);

		source.subscribe(rsSubscriber);
		latch.await(1, TimeUnit.SECONDS);

		assertThat(nextRef).hasValue("baz");
		assertThat(endRef.get()).matches(Signal::isOnComplete);
	}

	private static <T> Subscriber<T> newRsSubscriber(AtomicReference<T> nextRef,
			AtomicReference<Signal<T>> endRef, CountDownLatch latch) {
		return new Subscriber<T>() {
			@Override
			public void onSubscribe(Subscription subscription) {
				subscription.request(Long.MAX_VALUE);
			}

			@Override
			public void onNext(T s) {
				nextRef.set(s);
			}

			@Override
			public void onError(Throwable throwable) {
				endRef.set(Signal.error(throwable));
				latch.countDown();
			}

			@Override
			public void onComplete() {
				endRef.set(Signal.complete());
				latch.countDown();
			}
		};
	}

}

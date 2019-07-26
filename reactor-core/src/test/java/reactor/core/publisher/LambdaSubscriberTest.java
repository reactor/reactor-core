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

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import reactor.util.context.Context;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class LambdaSubscriberTest {

	@Test
	public void initialContextIsVisibleToUpstream() {
		AtomicReference<Context> contextRef = new AtomicReference<>();

		Flux.just("foo")
		    .flatMap(c -> Mono.subscriberContext())
		    .doOnNext(contextRef::set)
		    .subscribe(null, null, null, Context.of("subscriber", "context"));

		Assertions.assertThat(contextRef.get())
		          .isNotNull()
		          .matches(c -> c.hasKey("subscriber"));
	}

	@Test
	public void initialContextIsUsedForOnErrorDropped() {
		AtomicReference<Throwable> droppedRef = new AtomicReference<>();
		Context ctx = Context.of(Hooks.KEY_ON_ERROR_DROPPED, (Consumer<Throwable>) droppedRef::set);
		IllegalStateException expectDropped = new IllegalStateException("boom2");
		LambdaSubscriber<Object> sub = new LambdaSubscriber<>(null, e -> { }, null, null, ctx);

		sub.onError(new IllegalStateException("boom1"));
		//now trigger drop
		sub.onError(expectDropped);

		Assertions.assertThat(droppedRef).hasValue(expectDropped);
	}

	@Test
	public void consumeOnSubscriptionNotifiesError() {
		AtomicReference<Throwable> errorHolder = new AtomicReference<>(null);

		LambdaSubscriber<String> tested = new LambdaSubscriber<>(
				value -> {},
				errorHolder::set,
				() -> {},
				subscription -> { throw new IllegalArgumentException(); });

		TestSubscription testSubscription = new TestSubscription();

		//the error is expected to be propagated through onError
		tested.onSubscribe(testSubscription);

		assertThat("unexpected exception in onError",
				errorHolder.get(), is(instanceOf(IllegalArgumentException.class)));
		assertThat("subscription has not been cancelled",
				testSubscription.isCancelled, is(true));
		assertThat("unexpected request",
				testSubscription.requested, is(equalTo(-1L)));
	}

	@Test
	public void consumeOnSubscriptionThrowsFatal() {
		AtomicReference<Throwable> errorHolder = new AtomicReference<>(null);

		LambdaSubscriber<String> tested = new LambdaSubscriber<>(
				value -> {},
				errorHolder::set,
				() -> {},
				subscription -> { throw new OutOfMemoryError(); });

		TestSubscription testSubscription = new TestSubscription();

		//the error is expected to be thrown as it is fatal
		try {
			tested.onSubscribe(testSubscription);
			fail("Expected OutOfMemoryError to be thrown");
		}
		catch (OutOfMemoryError e) {
			//expected
		}

		assertThat("unexpected onError", errorHolder.get(), is(nullValue()));
		assertThat("subscription has been cancelled despite fatal exception",
				testSubscription.isCancelled, is(not(true)));
		assertThat("unexpected request",
				testSubscription.requested, is(equalTo(-1L)));
	}

	@Test
	public void consumeOnSubscriptionReceivesSubscriptionAndRequests32() {
		AtomicReference<Throwable> errorHolder = new AtomicReference<>(null);
		AtomicReference<Subscription> subscriptionHolder = new AtomicReference<>(null);
		LambdaSubscriber<String> tested = new LambdaSubscriber<>(
				value -> {},
				errorHolder::set,
				() -> { },
				s -> {
					subscriptionHolder.set(s);
					s.request(32);
				});
		TestSubscription testSubscription = new TestSubscription();

		tested.onSubscribe(testSubscription);

		assertThat("unexpected onError", errorHolder.get(), is(nullValue()));
		assertThat("subscription has been cancelled",
				testSubscription.isCancelled, is(not(true)));
		assertThat("didn't consume the subscription",
				subscriptionHolder.get(), is(equalTo(testSubscription)));
		assertThat("didn't request the subscription",
				testSubscription.requested, is(equalTo(32L)));
	}

	@Test
	public void noSubscriptionConsumerTriggersRequestOfMax() {
		AtomicReference<Throwable> errorHolder = new AtomicReference<>(null);
		LambdaSubscriber<String> tested = new LambdaSubscriber<>(
				value -> {},
				errorHolder::set,
				() -> {},
				null); //defaults to initial request of max
		TestSubscription testSubscription = new TestSubscription();

		tested.onSubscribe(testSubscription);

		assertThat("unexpected onError", errorHolder.get(), is(nullValue()));
		assertThat("subscription has been cancelled",
				testSubscription.isCancelled, is(not(true)));
		assertThat("didn't request the subscription",
				testSubscription.requested, is(not(equalTo(-1L))));
		assertThat("didn't request max",
				testSubscription.requested, is(equalTo(Long.MAX_VALUE)));
	}

	@Test
	public void onNextConsumerExceptionTriggersCancellation() {
		AtomicReference<Throwable> errorHolder = new AtomicReference<>(null);

		LambdaSubscriber<String> tested = new LambdaSubscriber<>(
				value -> { throw new IllegalArgumentException(); },
				errorHolder::set,
				() -> {},
				null);

		TestSubscription testSubscription = new TestSubscription();
		tested.onSubscribe(testSubscription);

		//the error is expected to be propagated through onError
		tested.onNext("foo");

		assertThat("unexpected exception in onError",
				errorHolder.get(), is(instanceOf(IllegalArgumentException.class)));
		assertThat("subscription has not been cancelled",
				testSubscription.isCancelled, is(true));
	}

	@Test
	public void onNextConsumerFatalDoesntTriggerCancellation() {
		AtomicReference<Throwable> errorHolder = new AtomicReference<>(null);

		LambdaSubscriber<String> tested = new LambdaSubscriber<>(
				value -> { throw new OutOfMemoryError(); },
				errorHolder::set,
				() -> {},
				null);

		TestSubscription testSubscription = new TestSubscription();
		tested.onSubscribe(testSubscription);

		//the error is expected to be thrown as it is fatal
		try {
			tested.onNext("foo");
			fail("Expected OutOfMemoryError to be thrown");
		}
		catch (OutOfMemoryError e) {
			//expected
		}

		assertThat("unexpected onError", errorHolder.get(), is(nullValue()));
		assertThat("subscription has been cancelled despite fatal exception",
				testSubscription.isCancelled, is(not(true)));
	}

	@Test
	public void scan() {
		LambdaSubscriber<String> test = new LambdaSubscriber<>(null, null, null, null);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		Assertions.assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		Assertions.assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

		Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();

		test.dispose();

		Assertions.assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();
		Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	private static class TestSubscription implements Subscription {

		volatile boolean isCancelled = false;
		volatile long    requested   = -1L;

		@Override
		public void request(long n) {
			this.requested = n;
		}

		@Override
		public void cancel() {
			this.isCancelled = true;
		}
	}

}
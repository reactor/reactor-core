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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.reactivestreams.Subscription;

import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class LambdaMonoSubscriberTest {


	@Test
	public void initialContextIsVisibleToUpstream() {
		AtomicReference<Context> contextRef = new AtomicReference<>();

		Mono.subscriberContext()
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
		LambdaMonoSubscriber<Object> sub = new LambdaMonoSubscriber<>(null, e -> { }, null, null, ctx);

		sub.onError(new IllegalStateException("boom1"));
		//now trigger drop
		sub.onError(expectDropped);

		Assertions.assertThat(droppedRef).hasValue(expectDropped);
	}

	@Test
	public void consumeOnSubscriptionNotifiesError() {
		AtomicReference<Throwable> errorHolder = new AtomicReference<>(null);

		LambdaMonoSubscriber<String> tested = new LambdaMonoSubscriber<>(
				value -> {},
				errorHolder::set,
				() -> {},
				subscription -> { throw new IllegalArgumentException(); });

		TestSubscription testSubscription = new TestSubscription();

		//the error is expected to be propagated through onError
		tested.onSubscribe(testSubscription);

		assertThat(errorHolder.get()).as("onError").isInstanceOf(IllegalArgumentException.class);
		assertThat(testSubscription.isCancelled).as("subscription should be cancelled").isTrue();
		assertThat(testSubscription.requested).as("request").isEqualTo(-1L);
	}

	@Test
	public void consumeOnSubscriptionThrowsFatal() {
		AtomicReference<Throwable> errorHolder = new AtomicReference<>(null);

		LambdaMonoSubscriber<String> tested = new LambdaMonoSubscriber<>(
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

		assertThat(errorHolder.get()).as("onError").isNull();
		assertThat(testSubscription.isCancelled)
				.as("subscription should not be cancelled due to fatal exception")
				.isFalse();
		assertThat(testSubscription.requested).as("unexpected request").isEqualTo(-1L);
	}

	@Test
	public void consumeOnSubscriptionReceivesSubscriptionAndRequests32() {
		AtomicReference<Throwable> errorHolder = new AtomicReference<>(null);
		AtomicReference<Subscription> subscriptionHolder = new AtomicReference<>(null);
		LambdaMonoSubscriber<String> tested = new LambdaMonoSubscriber<>(
				value -> {},
				errorHolder::set,
				() -> { },
				s -> {
					subscriptionHolder.set(s);
					s.request(32);
				});
		TestSubscription testSubscription = new TestSubscription();

		tested.onSubscribe(testSubscription);

		assertThat(errorHolder.get()).as("onError").isNull();
		assertThat(testSubscription.isCancelled).as("subscription should not be cancelled").isFalse();
		assertThat(subscriptionHolder.get()).as("subscription should be consumed").isEqualTo(testSubscription);
		assertThat(testSubscription.requested).as("subscription should be requested").isEqualTo(32L);
	}

	@Test
	public void noSubscriptionConsumerTriggersRequestOfMax() {
		AtomicReference<Throwable> errorHolder = new AtomicReference<>(null);
		LambdaMonoSubscriber<String> tested = new LambdaMonoSubscriber<>(
				value -> {},
				errorHolder::set,
				() -> {},
				null); //defaults to initial request of max
		TestSubscription testSubscription = new TestSubscription();

		tested.onSubscribe(testSubscription);

		assertThat(errorHolder.get()).as("onError").isNull();
		assertThat(testSubscription.isCancelled).as("subscription should not be cancelled").isFalse();
		assertThat(testSubscription.requested)
				.as("should request max")
				.isNotEqualTo(-1L)
				.isEqualTo(Long.MAX_VALUE);
	}

	@Test
	public void onNextConsumerExceptionBubblesUpDoesntTriggerCancellation() {
		AtomicReference<Throwable> errorHolder = new AtomicReference<>(null);

		LambdaMonoSubscriber<String> tested = new LambdaMonoSubscriber<>(
				value -> { throw new IllegalArgumentException(); },
				errorHolder::set,
				() -> {},
				null);

		TestSubscription testSubscription = new TestSubscription();
		tested.onSubscribe(testSubscription);

		//as Mono is single-value, it cancels early on onNext. this leads to an exception
		//during onNext to be bubbled up as a BubbledException, not propagated through onNext
		try {
			tested.onNext("foo");
			fail("Expected a bubbling Exception");
		} catch (RuntimeException e) {
			assertThat(e).matches(Exceptions::isBubbling, "Expected a bubbling Exception")
			             .hasCauseInstanceOf(IllegalArgumentException.class);
		}

		assertThat(errorHolder.get()).as("onError").isNull();
		assertThat(testSubscription.isCancelled).as("subscription isCancelled").isFalse();
	}

	@Test
	public void onNextConsumerFatalDoesntTriggerCancellation() {
		AtomicReference<Throwable> errorHolder = new AtomicReference<>(null);

		LambdaMonoSubscriber<String> tested = new LambdaMonoSubscriber<>(
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

		assertThat(errorHolder.get()).as("onError").isNull();
		assertThat(testSubscription.isCancelled).as("subscription isCancelled").isFalse();
	}

	@Test
	public void emptyMonoState(){
		assertTrue(Mono.fromDirect(s -> {
			assertTrue(s instanceof LambdaMonoSubscriber);
			LambdaMonoSubscriber<?> bfs = (LambdaMonoSubscriber<?>)s;
			assertTrue(bfs.scan(Scannable.Attr.PREFETCH) == Integer.MAX_VALUE);
			assertFalse(bfs.scan(Scannable.Attr.TERMINATED));
			bfs.onSubscribe(Operators.emptySubscription());
			bfs.onSubscribe(Operators.emptySubscription()); // noop
			s.onComplete();
			assertTrue(bfs.scan(Scannable.Attr.TERMINATED));
			bfs.dispose();
			bfs.dispose();
		}).subscribe(s -> {}, null, () -> {}).isDisposed());

		assertFalse(Mono.never().subscribe(null, null, () -> {}).isDisposed());
	}

	@Test
	public void errorMonoState(){
		Hooks.onErrorDropped(e -> assertTrue(e.getMessage().equals("test2")));
		Hooks.onNextDropped(d -> assertTrue(d.equals("test2")));
		Mono.fromDirect(s -> {
			assertTrue(s instanceof LambdaMonoSubscriber);
			LambdaMonoSubscriber<?> bfs = (LambdaMonoSubscriber<?>) s;
			Operators.error(s, new Exception("test"));
			s.onComplete();
			s.onError(new Exception("test2"));
			s.onNext("test2");
			assertTrue(bfs.scan(Scannable.Attr.TERMINATED));
			bfs.dispose();
		})
		          .subscribe(s -> {
		          }, e -> {
		          }, () -> {
		          });
	}

	@Test
	public void completeHookErrorDropped() {
		Hooks.onErrorDropped(e -> assertTrue(e.getMessage().equals("complete")));
		Mono.just("foo")
	        .subscribe(v -> {},
			        e -> {},
			        () -> { throw new IllegalStateException("complete");});
	}

	@Test
	public void noErrorHookThrowsCallbackNotImplemented() {
		RuntimeException boom = new IllegalArgumentException("boom");
		Assertions.assertThatExceptionOfType(RuntimeException.class)
		          .isThrownBy(() -> Mono.error(boom).subscribe(v -> {}))
	              .withCause(boom)
	              .hasToString("reactor.core.Exceptions$ErrorCallbackNotImplemented: java.lang.IllegalArgumentException: boom");
	}

	@Test
	public void testCancel() {
		AtomicLong cancelCount = new AtomicLong();
		Mono.delay(Duration.ofMillis(500))
		    .doOnCancel(cancelCount::incrementAndGet)
		    .subscribe(v -> {})
		    .dispose();
		Assertions.assertThat(cancelCount.get()).isEqualTo(1);
	}

	@Test
	public void scan() {
		LambdaMonoSubscriber<String> test = new LambdaMonoSubscriber<>(null, null, null, null);
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
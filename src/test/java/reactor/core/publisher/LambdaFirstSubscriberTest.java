package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.reactivestreams.Subscription;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class LambdaFirstSubscriberTest {

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
		assertThat("subscription has been cancelled",
				testSubscription.isCancelled, is(not(true)));
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
		assertThat("subscription has been cancelled",
				testSubscription.isCancelled, is(not(true)));
		assertThat("unexpected request",
				testSubscription.requested, is(equalTo(-1L)));
	}

	@Test
	public void consumeOnSubscriptionReceivesSubscription() {
		AtomicReference<Throwable> errorHolder = new AtomicReference<>(null);
		AtomicReference<Subscription> subscriptionHolder = new AtomicReference<>(null);
		LambdaSubscriber<String> tested = new LambdaSubscriber<>(
				value -> {},
				errorHolder::set,
				() -> { },
				subscriptionHolder::set);
		TestSubscription testSubscription = new TestSubscription();

		tested.onSubscribe(testSubscription);

		assertThat("unexpected onError", errorHolder.get(), is(nullValue()));
		assertThat("subscription has been cancelled",
				testSubscription.isCancelled, is(not(true)));
		assertThat("didn't consume the subscription",
				subscriptionHolder.get(), is(equalTo(testSubscription)));
		assertThat("didn't request the subscription",
				testSubscription.requested, is(equalTo(Long.MAX_VALUE)));
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
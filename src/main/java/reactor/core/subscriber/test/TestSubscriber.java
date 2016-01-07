/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.subscriber.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.reactivestreams.Subscription;
import reactor.core.subscriber.SubscriberWithDemand;
import reactor.core.support.Assert;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveStateUtils;
import reactor.fn.Supplier;

/**
 * @author Anatoly Kadyshev
 * @author Stephane Maldini
 * @since 2.5
 */
public class TestSubscriber<T> extends SubscriberWithDemand<T, T> {

	/**
	 *
	 * @param timeoutSecs
	 * @return
	 */
	public static <T> TestSubscriber<T> createWithTimeoutSecs(int timeoutSecs) {
		return new TestSubscriber<>(timeoutSecs);
	}

	/**
	 *
	 * @param timeoutSecs
	 * @param errorMessageSupplier
	 * @param conditionSupplier
	 * @throws InterruptedException
	 */
	public static void waitFor(long timeoutSecs,
			Supplier<String> errorMessageSupplier,
			Supplier<Boolean> conditionSupplier) throws InterruptedException {
		Assert.notNull(errorMessageSupplier);
		Assert.notNull(conditionSupplier);
		Assert.isTrue(timeoutSecs > 0);

		long timeoutNs = TimeUnit.SECONDS.toNanos(timeoutSecs);
		long startTime = System.nanoTime();
		do {
			if (conditionSupplier.get()) {
				return;
			}
			Thread.sleep(100);
		}
		while (System.nanoTime() - startTime < timeoutNs);
		throw new AssertionError(errorMessageSupplier.get());
	}

	/**
	 *
	 * @param timeoutSecs
	 * @param errorMessage
	 * @param resultSupplier
	 * @throws InterruptedException
	 */
	public static void waitFor(long timeoutSecs, final String errorMessage, Supplier<Boolean> resultSupplier)
			throws InterruptedException {
		waitFor(timeoutSecs, new Supplier<String>() {
			@Override
			public String get() {
				return errorMessage;
			}
		}, resultSupplier);
	}

	//
	// Instance
	//

	/**
	 * Total number of received Next signals
	 */
	protected volatile long numNextSignalsReceived = 0L;

	private final CountDownLatch completeLatch = new CountDownLatch(1);

	private final CountDownLatch errorLatch = new CountDownLatch(1);

	protected final int timeoutSecs;

	/**
	 * Last Error signal
	 */
	private volatile Throwable lastErrorSignal;

	protected TestSubscriber(int timeoutSecs) {
		super(null);
		this.timeoutSecs = timeoutSecs;
	}

	/**
	 *
	 * @throws InterruptedException
	 */
	public TestSubscriber<T> requestUnboundedWithTimeout() throws InterruptedException {
		requestWithTimeout(Long.MAX_VALUE);
		return this;
	}

	/**
	 *
	 * @param n
	 * @throws InterruptedException
	 */
	public TestSubscriber<T> requestWithTimeout(long n) throws InterruptedException {
		waitFor(timeoutSecs,
				String.format("onSubscribe wasn't called within %d secs", timeoutSecs),
				new Supplier<Boolean>() {
					@Override
					public Boolean get() {
						return subscription != null;
					}
				});

		requestMore(n);
		return this;
	}

	/**
	 *
	 * @throws InterruptedException
	 */
	public TestSubscriber<T> sendUnboundedRequest() throws InterruptedException {
		requestMore(Long.MAX_VALUE);
		return this;
	}

	/**
	 *
	 * @param n
	 */
	public TestSubscriber<T> sendRequest(long n) {
		requestMore(n);
		return this;
	}

	/**
	 *
	 * @param n
	 * @throws InterruptedException
	 */
	public TestSubscriber<T> assertNumNextSignalsReceived(final int n) throws InterruptedException {
		Supplier<String> errorSupplier = new Supplier<String>() {
			@Override
			public String get() {
				return String.format("%d out of %d Next signals received within %d secs",
						numNextSignalsReceived,
						n,
						timeoutSecs);
			}
		};

		waitFor(timeoutSecs, errorSupplier, new Supplier<Boolean>() {
			@Override
			public Boolean get() {
				return numNextSignalsReceived == n;
			}
		});
		return this;
	}

	/**
	 *
	 * @throws InterruptedException
	 */
	public TestSubscriber<T> assertCompleteReceived() throws InterruptedException {
		boolean result = completeLatch.await(timeoutSecs, TimeUnit.SECONDS);
		if (!result) {
			throw new AssertionError(String.format("Haven't received Complete signal within %d seconds", timeoutSecs));
		}
		return this;
	}

	/**
	 *
	 * @throws InterruptedException
	 */
	public TestSubscriber<T> assertNoCompleteReceived() throws InterruptedException {
		long startTime = System.nanoTime();
		do {
			if (completeLatch.getCount() == 0) {
				throw new AssertionError();
			}
			Thread.sleep(100);
		}
		while (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime) < 1);
		return this;
	}

	/**
	 *
	 * @throws InterruptedException
	 */
	public TestSubscriber<T> assertErrorReceived() throws InterruptedException {
		boolean result = errorLatch.await(timeoutSecs, TimeUnit.SECONDS);
		if (!result) {
			throw new AssertionError(String.format("Haven't received Error signal within %d seconds", timeoutSecs));
		}
		return this;
	}

	/**
	 * @return last Error signal
	 */
	public Throwable getLastErrorSignal() {
		return lastErrorSignal;
	}

	/**
	 *
	 */
	public TestSubscriber<T> cancelSubscription() {
		doCancel();
		return this;
	}

	@Override
	protected void doOnSubscribe(Subscription s) {
		subscription = s;
		long toRequest = requestedFromDownstream();
		if (toRequest > 0L) {
			s.request(toRequest);
		}
	}

	@Override
	protected void doNext(T buffer) {
		BackpressureUtils.getAndSub(REQUESTED, this, 1L);
		numNextSignalsReceived++;
	}

	@Override
	protected void doComplete() {
		completeLatch.countDown();
	}

	@Override
	protected void doError(Throwable t) {
		this.lastErrorSignal = t;
		errorLatch.countDown();
	}

	public long getNumNextSignalsReceived() {
		return numNextSignalsReceived;
	}


	public ReactiveStateUtils.Graph debug(){
		return ReactiveStateUtils.scan(this);
	}

}

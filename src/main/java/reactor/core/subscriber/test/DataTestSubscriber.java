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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import reactor.core.support.internal.PlatformDependent;
import reactor.fn.Consumer;

/**
 * Subscriber capturing Next signals for assertion
 *
 * @author Anatoly Kadyshev
 * @author Stephane Maldini
 * @author Brian Clozel
 */
public class DataTestSubscriber<T> extends TestSubscriber<T> {

	/**
	 * Received Next signals
	 */
	volatile List<T> nextSignals;

	static final AtomicReferenceFieldUpdater<DataTestSubscriber, List> NEXT_SIGNALS = PlatformDependent
			.newAtomicReferenceFieldUpdater(DataTestSubscriber.class, "nextSignals");

	/**
	 * Total number of asserted Next signals
	 */
	private volatile int numAssertedNextSignals = 0;

	/**
	 * Creates a new test subscriber
	 *
	 * @param timeoutSecs timeout interval in seconds
	 * @return a newly created test subscriber
	 */
	public static <T> DataTestSubscriber<T> createWithTimeoutSecs(int timeoutSecs) {
		return new DataTestSubscriber<>(timeoutSecs);
	}

	private DataTestSubscriber(int timeoutSecs) {
		super(timeoutSecs);
		NEXT_SIGNALS.lazySet(this, new ArrayList<>());
	}

	/**
	 * Asserts that since the last call of the method Next signals
	 * {@code expectedNextSignals} were received in order.
	 *
	 * @param expectedNextSignals expected Next signals in order
	 *
	 * @throws InterruptedException if a thread was interrupted during a waiting
	 */
	@SafeVarargs
	@SuppressWarnings("unchecked")
	public final DataTestSubscriber<T> assertNextSignalsEqual(T... expectedNextSignals) throws InterruptedException {

		final int expectedNum = expectedNextSignals.length;
		final List<Consumer<T>> expectations = new ArrayList<>();
		for (int i = 0; i < expectedNum; i++) {
			final T expectedSignal = expectedNextSignals[i];
			expectations.add(new Consumer<T>() {
				@Override
				public void accept(T actualSignal) {
					if (!actualSignal.equals(expectedSignal)) {
						throw new AssertionError(
								String.format("Expected Next signal: %s, but got: %s", expectedSignal, actualSignal));
					}
				}
			});
		}
		assertNextSignals(expectations.toArray((Consumer<T>[])new Consumer[0]));
		return this;
	}

	/**
	 * Asserts that since the last call of the method Next signals
	 * match the {@code expectations} given as arguments.
	 *
	 * @param expectations Consumers that will accept Next signals in order
	 *
	 * @throws InterruptedException if a thread was interrupted during a waiting
	 */
	@SafeVarargs
	public final DataTestSubscriber<T> assertNextSignals(Consumer<T>... expectations) throws InterruptedException {
		int expectedNum = expectations.length;
		assertNumNextSignalsReceived(numAssertedNextSignals + expectedNum);

		List<T> nextSignalsSnapshot;
		List<T> empty = new ArrayList<>();
		for(;;){
			nextSignalsSnapshot = nextSignals;
			if(NEXT_SIGNALS.compareAndSet(this, nextSignals, empty)){
				break;
			}
		}

		if (nextSignalsSnapshot.size() != expectedNum) {
			throw new AssertionError(String.format("Expected %d number of signals but received %d",
					expectedNum,
					nextSignalsSnapshot.size()));
		}

		for (int i = 0; i < expectedNum; i++) {
			Consumer<T> consumer = expectations[i];
			T actualSignal = nextSignalsSnapshot.get(i);
			consumer.accept(actualSignal);
		}

		numAssertedNextSignals += expectedNum;
		return this;
	}

	@Override
	public DataTestSubscriber<T> sendRequest(long n) {
		super.sendRequest(n);
		return this;
	}

	@Override
	public DataTestSubscriber<T> requestUnboundedWithTimeout() throws InterruptedException {
		super.requestUnboundedWithTimeout();
		return this;
	}

	@Override
	public DataTestSubscriber<T> sendUnboundedRequest() throws InterruptedException {
		super.sendUnboundedRequest();
		return this;
	}

	@Override
	public DataTestSubscriber<T> requestWithTimeout(long n) throws InterruptedException {
		super.requestWithTimeout(n);
		return this;
	}

	@Override
	public DataTestSubscriber<T> assertNumNextSignalsReceived(int n) throws InterruptedException {
		super.assertNumNextSignalsReceived(n);
		return this;
	}

	@Override
	public DataTestSubscriber<T> assertCompleteReceived() throws InterruptedException {
		super.assertCompleteReceived();
		return this;
	}

	@Override
	public DataTestSubscriber<T> assertNoCompleteReceived() throws InterruptedException {
		super.assertNoCompleteReceived();
		return this;
	}

	@Override
	public DataTestSubscriber<T> assertErrorReceived() throws InterruptedException {
		super.assertErrorReceived();
		return this;
	}

	@Override
	protected void doNext(T data) {
		List<T> nextSignalsSnapshot;
		for (; ; ) {
			nextSignalsSnapshot = nextSignals;
			nextSignalsSnapshot.add(data);
			if (NEXT_SIGNALS.compareAndSet(this, nextSignalsSnapshot, nextSignalsSnapshot)) {
				break;
			}
		}
		super.doNext(data);
	}

}

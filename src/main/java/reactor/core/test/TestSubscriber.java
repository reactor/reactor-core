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

package reactor.core.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.subscriber.EmptySubscriber;
import reactor.core.subscriber.SubscriberDeferredSubscription;
import reactor.core.util.Assert;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.PlatformDependent;
import reactor.core.util.ReactiveStateUtils;
import reactor.fn.Consumer;
import reactor.fn.Supplier;

/**
 * A Subscriber implementation that hosts assertion tests for its state and allows
 * asynchronous cancellation and requesting.
 * <p>
 * You can extend this class but only the onNext, onError and onComplete can be overridden.
 * <p>
 * You can call {@link #request(long)} and {@link #cancel()} from any thread or from within
 * the overridable methods but you should avoid calling the assertXXX methods asynchronously.
 *
 * <p>Usage:
 * <pre>
 * {@code
 * TestSubscriber<String> testSubscriber = new TestSubscriber<>();
 * Publisher<String> publisher = new FooPublisher<>();
 * publisher.subscribe(testSubscriber);
 * testSubscriber.assertValue("ABC", "DEF")
			  .assertComplete()
			  .assertNoError();
 * }
 * </pre>
 *
 * @param <T> the value type.
 *
 * @author Sebastien Deleuze
 * @author David Karnok
 * @author Anatoly Kadyshev
 * @author Stephane Maldini
 * @author Brian Clozel
 * @since 2.5
 */
public class TestSubscriber<T> extends SubscriberDeferredSubscription<T, T> {

	/**
	 * Default timeout in seconds for waiting next values to be received
	 */
	public static final long DEFAULT_VALUES_TIMEOUT = 3;


	private final CountDownLatch cdl = new CountDownLatch(1);

	private int subscriptionCount = 0;

	private int completionCount = 0;

	private volatile long valueCount = 0L;

	private volatile long nextValueAssertedCount = 0L;

	volatile List<T> values = new LinkedList<>();

	private static final AtomicReferenceFieldUpdater<TestSubscriber, List> NEXT_VALUES =
			PlatformDependent.newAtomicReferenceFieldUpdater(TestSubscriber.class, "values");

	final List<Throwable> errors = new LinkedList<>();

	private long valuesTimeout = DEFAULT_VALUES_TIMEOUT;

	private boolean valuesStorage = true;


//	 ==============================================================================================================
//	 Static methods
//	 ==============================================================================================================

	/**
	 * Blocking method that waits until {@code conditionSupplier} returns true, or if it does
	 * not before the specified timeout, throws an {@link AssertionError} with the specified
	 * error message.
	 * @throws AssertionError
	 */
	public static void await(long timeoutInSeconds, Supplier<String> errorMessageSupplier,
			Supplier<Boolean> conditionSupplier) {

		Assert.notNull(errorMessageSupplier);
		Assert.notNull(conditionSupplier);
		Assert.isTrue(timeoutInSeconds > 0);

		long timeoutNs = TimeUnit.SECONDS.toNanos(timeoutInSeconds);
		long startTime = System.nanoTime();
		do {
			if (conditionSupplier.get()) {
				return;
			}
			try {
				Thread.sleep(100);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new RuntimeException(e);
			}
		}
		while (System.nanoTime() - startTime < timeoutNs);
		throw new AssertionError(errorMessageSupplier.get());
	}

	/**
	 * Blocking method that waits until {@code conditionSupplier} returns true, or if it does
	 * not before the specified timeout, throw an {@link AssertionError} with the specified
	 * error message.
	 * @throws AssertionError
	 */
	public static void await(long timeoutInSeconds, final String errorMessage, Supplier<Boolean> resultSupplier) {
		await(timeoutInSeconds, new Supplier<String>() {
			@Override
			public String get() {
				return errorMessage;
			}
		}, resultSupplier);
	}


//	 ==============================================================================================================
//	 Constructors
//	 ==============================================================================================================

	/**
	 * Create a new {@link TestSubscriber} that requests an unbounded number of values
	 */
	public TestSubscriber() {
		 this(EmptySubscriber.instance(), Long.MAX_VALUE);
	}

	/**
	 * Create a new {@link TestSubscriber} that request {@code n} values
	 * @param n Number of values to request (can be 0 if you want to request values later
	 * with {@code request(long n)} method).
	 */
	public TestSubscriber(long n) {
		super(EmptySubscriber.instance(), n);
	}

	/**
	 * Create a new {@link TestSubscriber} that requests an unbounded number of values
	 * @param delegate The subscriber to delegate to
	 */
	public TestSubscriber(Subscriber<? super T> delegate) {
		this(delegate, Long.MAX_VALUE);
	}

	/**
	 * Create a new {@link TestSubscriber} that request {@code n} values
	 * @param delegate The subscriber to delegate to
	 * @param n Number of values to request (can be 0 if you want to request values later
	 * with {@code request(long n)} method).
	 */
	public TestSubscriber(Subscriber<? super T> delegate, long n) {
		super(delegate, n);
	}

//	 ==============================================================================================================
//	 Configuration
//	 ==============================================================================================================

	/**
	 * Configure the timeout in seconds for waiting next values to be received (3 seconds
	 * by default).
	 */
	public final TestSubscriber<T> configureValuesTimeout(long timeoutInSeconds) {
		this.valuesTimeout = timeoutInSeconds;
		return this;
	}

	/**
	 * Enable or disabled the values storage. It is enabled by default, and can be disable
	 * in order to be able to perform performance benchmarks or tests with a huge amount
	 * values.
	 */
	public final TestSubscriber<T> configureValuesStorage(boolean enabled) {
		this.valuesStorage = enabled;
		return this;
	}

//	 ==============================================================================================================
//	 Assertions
//	 ==============================================================================================================

	/**
	 * Assert subscription occurred (once).
	 */
	public final TestSubscriber<T> assertSubscribed() {
		int s = subscriptionCount;

		if (s == 0) {
			throw new AssertionError("OnSubscribe not called", null);
		}
		if (s > 1) {
			throw new AssertionError("OnSubscribe called multiple times: " + s, null);
		}

		return this;
	}

	/**
	 * Assert no subscription occurred.
	 */
	public final TestSubscriber<T> assertNotSubscribed() {
		int s = subscriptionCount;

		if (s == 1) {
			throw new AssertionError("OnSubscribe called once", null);
		}
		if (s > 1) {
			throw new AssertionError("OnSubscribe called multiple times: " + s, null);
		}

		return this;
	}

	/**
	 * Assert a complete successfully signal has been received.
	 */
	public final TestSubscriber<T> assertComplete() {
		int c = completionCount;
		if (c == 0) {
			throw new AssertionError("Not completed", null);
		}
		if (c > 1) {
			throw new AssertionError("Multiple completions: " + c, null);
		}
		return this;
	}

	/**
	 * Assert no complete successfully signal has been received.
	 */
	public final TestSubscriber<T> assertNotComplete() {
		int c = completionCount;
		if (c == 1) {
			throw new AssertionError("Completed", null);
		}
		if (c > 1) {
			throw new AssertionError("Multiple completions: " + c, null);
		}
		return this;
	}

	/**
	 * Assert an error signal has been received.
	 */
	public final TestSubscriber<T> assertError() {
		int s = errors.size();
		if (s == 0) {
			throw new AssertionError("No error", null);
		}
		if (s > 1) {
			throw new AssertionError("Multiple errors: " + s, null);
		}
		return this;
	}

	/**
	 * Assert an error signal has been received.
	 * @param clazz The class of the exception contained in the error signal
	 */
	public final TestSubscriber<T> assertError(Class<? extends Throwable> clazz) {
		 int s = errors.size();
		if (s == 0) {
			throw new AssertionError("No error", null);
		}
		if (s == 1) {
			Throwable e = errors.get(0);
			if (!clazz.isInstance(e)) {
				throw new AssertionError("Error class incompatible: expected = " +
						clazz.getSimpleName() + ", actual = " + clazz, null);
			}
		}
		if (s > 1) {
			throw new AssertionError("Multiple errors: " + s, null);
		}
		return this;
	}

	/**
	 * Assert an error signal has been received.
	 * @param expectation A method that can verify the exception contained in the error signal
	 * and throw an exception (like an {@link AssertionError}) if the exception is not valid.
	 */
	public final TestSubscriber<T> assertErrorWith(Consumer<? super Throwable> expectation) {
		int s = errors.size();
		if (s == 0) {
			throw new AssertionError("No error", null);
		}
		if (s == 1) {
			expectation.accept(errors.get(0));
		}
		if (s > 1) {
			throw new AssertionError("Multiple errors: " + s, null);
		}
		return this;
	}

	/**
	 * Assert no error signal has been received.
	 */
	public final TestSubscriber<T> assertNoError() {
		int s = errors.size();
		if (s == 1) {
			Throwable e = errors.get(0);
			String valueAndClass = e == null ? null : e + " (" + e.getClass().getSimpleName() + ")";
			throw new AssertionError("Error present: " + valueAndClass, null);
		}
		if (s > 1) {
			throw new AssertionError("Multiple errors: " + s, null);
		}
		return this;
	}

	/**
	 * Assert either complete successfully or error signal has been received.
	 */
	public final TestSubscriber<T> assertTerminated() {
		if (cdl.getCount() != 0) {
			throw new AssertionError("Not terminated", null);
		}
		return this;
	}

	/**
	 * Assert no complete successfully or error signal has been received.
	 */
	public final TestSubscriber<T> assertNotTerminated() {
		if (cdl.getCount() == 0) {
			throw new AssertionError("Terminated", null);
		}
		return this;
	}

	/**
	 * Assert {@code n} values has been received.
	 */
	public final TestSubscriber<T> assertValueCount(long n) {
		if (valueCount != n) {
			throw new AssertionError("Different value count: expected = " + n + ", actual = " + valueCount, null);
		}
		return this;
	}

	/**
	 * Assert no values have been received.
	 */
	public final TestSubscriber<T> assertNoValues() {
		if (valueCount != 0) {
			throw new AssertionError("No values expected but received: [length = " + values.size() + "] " + values, null);
		}
		return this;
	}

	/**
	 * Assert the specified values have been received. Values storage should be enabled to
	 * use this method.
	 * @param expectedValues the values to assert
	 * @see #configureValuesStorage(boolean)
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	public final TestSubscriber<T> assertValues(T... expectedValues) {
		return assertValueSequence(Arrays.asList(expectedValues));
	}

	/**
	 * Assert the specified values have been received. Values storage should be enabled to
	 * use this method.
	 * @param expectations One or more methods that can verify the values and throw a
	 * exception (like an {@link AssertionError}) if the value is not valid.
	 * @see #configureValuesStorage(boolean)
	 */
	@SafeVarargs
	public final TestSubscriber<T> assertValuesWith(Consumer<T>... expectations) {
		if (!valuesStorage) {
			throw new IllegalStateException("Using assertNoValues() requires enabling values storage");
		}
		final int expectedValueCount = expectations.length;
		if (expectedValueCount != values.size()) {
			throw new AssertionError("Different value count: expected = " + expectedValueCount + ", actual = " + valueCount, null);
		}
		for (int i = 0; i < expectedValueCount; i++) {
			Consumer<T> consumer = expectations[i];
			T actualValue = values.get(i);
			consumer.accept(actualValue);
		}
		return this;
	}

	/**
	 * Assert the specified values have been received. Values storage should be enabled to
	 * use this method.
	 * @param expectedSequence the values to assert
	 * @see #configureValuesStorage(boolean)
	 */
	public final TestSubscriber<T> assertValueSequence(Iterable<? extends T> expectedSequence) {
		if (!valuesStorage) {
			throw new IllegalStateException("Using assertNoValues() requires enabling values storage");
		}
		Iterator<T> actual = values.iterator();
		Iterator<? extends T> expected = expectedSequence.iterator();
		int i = 0;
		for (; ; ) {
			boolean n1 = actual.hasNext();
			boolean n2 = expected.hasNext();
			if (n1 && n2) {
				T t1 = actual.next();
				T t2 = expected.next();
				if (!Objects.equals(t1, t2)) {
					throw new AssertionError("The " + i + " th elements differ: expected = " + valueAndClass(t2) + ", actual ="
					  + valueAndClass(
					  t1), null);
				}
				i++;
			} else if (n1 && !n2) {
				throw new AssertionError("Actual contains more elements" + values, null);
			} else if (!n1 && n2) {
				throw new AssertionError("Actual contains fewer elements: " + values, null);
			} else {
				break;
			}
		}
		return this;
	}

//	 ==============================================================================================================
//	 Await methods
//	 ==============================================================================================================

	/**
	 * Blocking method that waits until a complete successfully or error signal is received.
	 */
	public final TestSubscriber<T> await() {
		if (cdl.getCount() == 0) {
			return this;
		}
		try {
			cdl.await();
		} catch (InterruptedException ex) {
			throw new AssertionError("Wait interrupted", ex);
		}
		return this;
	}

	/**
	 * Blocking method that waits until a complete successfully or error signal is received
	 * or until a timeout occurs.
	 * @param timeoutInSec The timeout value in seconds
	 */
	public final TestSubscriber<T> await(long timeoutInSec) {
		return await(timeoutInSec, TimeUnit.SECONDS);
	}

	/**
	 * Blocking method that waits until a complete successfully or error signal is received
	 * or until a timeout occurs.
	 * @param timeout The timeout value
	 * @param unit Time unit of the timeout
	 */
	public final TestSubscriber<T> await(long timeout, TimeUnit unit) {
		if (cdl.getCount() == 0) {
			return this;
		}
		try {
			if (!cdl.await(timeout, unit)) {
				throw new AssertionError("No complete or error signal before timeout");
			}
			return this;
		} catch (InterruptedException ex) {
			throw new AssertionError("Wait interrupted", ex);
		}
	}

	/**
	 * Blocking method that waits until {@code n} next values have been received
	 * (n is the number of expectations provided) to assert them.
	 * @param expectations One or more methods that can verify the values and throw a
	 * exception (like an {@link AssertionError}) if the value is not valid.
	 */
	@SafeVarargs
	public final TestSubscriber<T> awaitAndAssertValuesWith(Consumer<T>... expectations) {
		valuesStorage = true;
		final int expectedValueCount = expectations.length;
		await(valuesTimeout,
				String.format("%d out of %d next values received within %d secs", valueCount - nextValueAssertedCount, expectedValueCount, valuesTimeout),
				new Supplier<Boolean>() {
			@Override
			public Boolean get() {
				return valueCount == (nextValueAssertedCount + expectedValueCount);
			}
		});
		List<T> nextValuesSnapshot;
		List<T> empty = new ArrayList<>();
		for(;;){
			nextValuesSnapshot = values;
			if(NEXT_VALUES.compareAndSet(this, values, empty)){
				break;
			}
		}
		if (nextValuesSnapshot.size() != expectedValueCount) {
			throw new AssertionError(String.format("Expected %d number of signals but received %d",
					expectedValueCount,
					nextValuesSnapshot.size()));
		}
		for (int i = 0; i < expectedValueCount; i++) {
			Consumer<T> consumer = expectations[i];
			T actualValue = nextValuesSnapshot.get(i);
			consumer.accept(actualValue);
		}
		nextValueAssertedCount += expectedValueCount;
		return this;
	}

	/**
	 * Blocking method that waits until {@code n} next values have been received
	 * (n is the number of values provided) to assert them.
	 * @param expectedValues the values to assert
	 */
	@SafeVarargs
	@SuppressWarnings("unchecked")
	public final TestSubscriber<T> awaitAndAssertValues(T... expectedValues) {
		final int expectedNum = expectedValues.length;
		final List<Consumer<T>> expectations = new ArrayList<>();
		for (int i = 0; i < expectedNum; i++) {
			final T expectedValue = expectedValues[i];
			expectations.add(new Consumer<T>() {
				@Override
				public void accept(T actualValue) {
					if (!actualValue.equals(expectedValue)) {
						throw new AssertionError(
								String.format("Expected Next signal: %s, but got: %s", expectedValue, actualValue));
					}
				}
			});
		}
		awaitAndAssertValuesWith(expectations.toArray((Consumer<T>[])new Consumer[0]));
		return this;
	}

	/**
	 * Blocking method that waits until {@code n} next values have been received.
	 * @param n the value count to assert
	 */
	public final TestSubscriber<T> awaitAndAssertValueCount(final long n) {
		await(valuesTimeout,
				String.format("%d out of %d next values received within %d secs", valueCount - nextValueAssertedCount, n, valuesTimeout),
				new Supplier<Boolean>() {
			@Override
			public Boolean get() {
				return valueCount == (nextValueAssertedCount + n);
			}
		});
		nextValueAssertedCount += n;
		return this;
	}

//	 ==============================================================================================================
//	 Utility methods
//	 ==============================================================================================================

	/**
	 * Create a "Nodes" and "Links" complete representation of a given component if available.
	 */
	public ReactiveStateUtils.Graph debug(){
		return ReactiveStateUtils.scan(this);
	}

//	 ==============================================================================================================
//	 Subscriber overrides
//	 ==============================================================================================================

	@Override
	public void onSubscribe(Subscription s) {
		subscriptionCount++;
		if (!set(s)) {
			if (!isCancelled()) {
				errors.add(new IllegalStateException("subscription already set"));
			}
		}
	}

	@Override
	public void onNext(T t) {
		valueCount++;
		if (valuesStorage) {
			List<T> nextValuesSnapshot;
			for (; ; ) {
			   nextValuesSnapshot = values;
			   nextValuesSnapshot.add(t);
			   if (NEXT_VALUES.compareAndSet(this, nextValuesSnapshot, nextValuesSnapshot)) {
				   break;
			   }
			}
		}
		subscriber.onNext(t);
	}

	@Override
	public void onError(Throwable t) {
		errors.add(t);
		subscriber.onError(t);
		cdl.countDown();
	}

	@Override
	public void onComplete() {
		completionCount++;
		subscriber.onComplete();
		cdl.countDown();
	}

	@Override
	public void request(long n) {
		if (BackpressureUtils.validate(n)) {
			super.request(n);
		}
	}

//	 ==============================================================================================================
//	 Non public methods
//	 ==============================================================================================================

	private final String valueAndClass(Object o) {
		if (o == null) {
			return null;
		}
		return o + " (" + o.getClass()
		  .getSimpleName() + ")";
	}

}

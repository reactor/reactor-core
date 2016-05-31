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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Fuseable;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.DeferredSubscription;
import reactor.core.util.PlatformDependent;
import reactor.core.util.ReactiveStateUtils;

/**
 * A Subscriber implementation that hosts assertion tests for its state and allows
 * asynchronous cancellation and requesting.
 *
 * <p> Creating a new instance with {@code new TestSubscriber<>()} creates a subscriber
 * that automatically requests an unbounded number of values. If you want to control more
 * finely how data are requested, you can use for example {@code new TestSubscriber<>(0)}
 * and then call {@link #request(long)} when you need. Subscription can be performed using
 * either {@link #bindTo(Publisher)} or {@link Publisher#subscribe(Subscriber)}.
 * If you are testing asynchronous publishers, don't forget to use one of the
 * {@code await*()} methods to wait for the data to assert.
 *
 * <p> You can extend this class but only the onNext, onError and onComplete can be overridden.
 * You can call {@link #request(long)} and {@link #cancel()} from any thread or from within
 * the overridable methods but you should avoid calling the assertXXX methods asynchronously.
 *
 * <p>Usage:
 * <pre>
 * {@code
 * Publisher<String> publisher = ...
 * TestSubscriber<String> ts = new TestSubscriber<>();
 * ts.bindTo(publisher)
 *   .await()
 *   .assertValues("ABC", "DEF");
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
public class TestSubscriber<T> extends DeferredSubscription implements Subscriber<T> {

	/**
	 * Default timeout for waiting next values to be received
	 */
	public static final Duration DEFAULT_VALUES_TIMEOUT = Duration.ofSeconds(3);


	private final CountDownLatch cdl = new CountDownLatch(1);

	private int subscriptionCount = 0;

	private int completionCount = 0;

	private volatile long valueCount = 0L;

	private volatile long nextValueAssertedCount = 0L;

	volatile List<T> values = new LinkedList<>();

	@SuppressWarnings("rawtypes")
    private static final AtomicReferenceFieldUpdater<TestSubscriber, List> NEXT_VALUES =
			PlatformDependent.newAtomicReferenceFieldUpdater(TestSubscriber.class, "values");

	final List<Throwable> errors = new LinkedList<>();

	private Duration valuesTimeout = DEFAULT_VALUES_TIMEOUT;

	private boolean valuesStorage = true;

	/** The fusion mode to request. */
	int requestedFusionMode = -1;

	/** The established fusion mode. */
	volatile int establishedFusionMode = -1;

	/** The fuseable QueueSubscription in case a fusion mode was specified. */
	Fuseable.QueueSubscription<T> qs;



//	 ==============================================================================================================
//	 Static methods
//	 ==============================================================================================================

	/**
	 * Blocking method that waits until {@code conditionSupplier} returns true, or if it does
	 * not before the specified timeout, throws an {@link AssertionError} with the specified
	 * error message supplier.
	 * @throws AssertionError
	 */
	public static void await(Duration timeout, Supplier<String> errorMessageSupplier,
			Supplier<Boolean> conditionSupplier) {

		Objects.requireNonNull(errorMessageSupplier);
		Objects.requireNonNull(conditionSupplier);
		Objects.requireNonNull(timeout);

		long timeoutNs = timeout.toNanos();
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
	public static void await(Duration timeout, final String errorMessage, Supplier<Boolean> resultSupplier) {
		await(timeout, new Supplier<String>() {
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
		 this(Long.MAX_VALUE);
	}

	/**
	 * Create a new {@link TestSubscriber} that request {@code n} values
	 * @param n Number of values to request (can be 0 if you want to request values later
	 * with {@code request(long n)} method).
	 */
	public TestSubscriber(long n) {
		if (n < 0) {
			throw new IllegalArgumentException("initialRequest >= required but it was " + n);
		}
		setInitialRequest(n);
	}

//	 ==============================================================================================================
//	 Configuration
//	 ==============================================================================================================

	/**
	 * Configure the timeout in seconds for waiting next values to be received (3 seconds
	 * by default).
	 */
	public final TestSubscriber<T> configureValuesTimeout(Duration timeout) {
		this.valuesTimeout = timeout;
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

	public final TestSubscriber<T> assertErrorMessage(String message) {
		int s = errors.size();
		if (s == 0) {
			assertionError("No error", null);
		}
		if (s == 1) {
			if (!Objects.equals(message,
					errors.get(0)
					      .getMessage())) {
				assertionError("Error class incompatible: expected = \"" + message + "\", actual = \"" + errors.get(
						0)
				                                                                                               .getMessage() + "\"",
						null);
			}
		}
		if (s > 1) {
			assertionError("Multiple errors: " + s, null);
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
					throw new AssertionError("The element with index " + i + " does not match: expected = " + valueAndClass(t2) + ", actual = "
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
	 * @param timeout The timeout value
	 */
	public final TestSubscriber<T> await(Duration timeout) {
		if (cdl.getCount() == 0) {
			return this;
		}
		try {
			if (!cdl.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
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
	public final TestSubscriber<T> awaitAndAssertNextValuesWith(Consumer<T>... expectations) {
		valuesStorage = true;
		final int expectedValueCount = expectations.length;
		await(valuesTimeout, () -> {
			return String.format("%d out of %d next values received within %d ms",
					valueCount - nextValueAssertedCount,
					expectedValueCount,
					valuesTimeout.toMillis());
		}, () -> valueCount >= (nextValueAssertedCount + expectedValueCount));
		List<T> nextValuesSnapshot;
		List<T> empty = new ArrayList<>();
		for(;;){
			nextValuesSnapshot = values;
			if(NEXT_VALUES.compareAndSet(this, values, empty)){
				break;
			}
		}
		if (nextValuesSnapshot.size() < expectedValueCount) {
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
	 * @param values the values to assert
	 */
	@SafeVarargs
	@SuppressWarnings("unchecked")
	public final TestSubscriber<T> awaitAndAssertNextValues(T... values) {
		final int expectedNum = values.length;
		final List<Consumer<T>> expectations = new ArrayList<>();
		for (int i = 0; i < expectedNum; i++) {
			final T expectedValue = values[i];
			expectations.add(actualValue -> {
				if (!actualValue.equals(expectedValue)) {
					throw new AssertionError(String.format(
							"Expected Next signal: %s, but got: %s",
							expectedValue,
							actualValue));
				}
			});
		}
		awaitAndAssertNextValuesWith(expectations.toArray((Consumer<T>[])new Consumer[0]));
		return this;
	}

	/**
	 * Blocking method that waits until {@code n} next values have been received.
	 * @param n the value count to assert
	 */
	public final TestSubscriber<T> awaitAndAssertNextValueCount(final long n) {
		await(valuesTimeout,
				new Supplier<String>() {
					@Override
					public String get() {
						return String.format("%d out of %d next values received within %d",
						valueCount - nextValueAssertedCount, n, valuesTimeout.toMillis());
					}
				},
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
	 * Allow the specified {@code publisher} to subscribe to this {@link TestSubscriber} instance.
	 */
	public TestSubscriber<T> bindTo(Publisher<T> publisher) {
		publisher.subscribe(this);
		return this;
	}

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
	@SuppressWarnings("unchecked")
	public void onSubscribe(Subscription s) {
		subscriptionCount++;
		int requestMode = requestedFusionMode;
		if (requestMode >= 0) {
			if (!setWithoutRequesting(s)) {
				if (!isCancelled()) {
					errors.add(new IllegalStateException("Subscription already set: " +
							subscriptionCount));
				}
			} else {
				if (s instanceof Fuseable.QueueSubscription) {
					this.qs = (Fuseable.QueueSubscription<T>)s;

					int m = qs.requestFusion(requestMode);
					establishedFusionMode = m;

					if (m == Fuseable.SYNC) {
						for (;;) {
							T v = qs.poll();
							if (v == null) {
								onComplete();
								break;
							}

							onNext(v);
						}
					} else {
						requestDeferred();
					}
				} else {
					requestDeferred();
				}
			}
		} else {
			if (!set(s)) {
				if (!isCancelled()) {
					errors.add(new IllegalStateException("Subscription already set: " +
							subscriptionCount));
				}
			}
		}
	}

	@Override
	public void onNext(T t) {
		if (establishedFusionMode == Fuseable.ASYNC) {
			for (; ; ) {
				t = qs.poll();
				if (t == null) {
					break;
				}
				valueCount++;
				if (valuesStorage) {
					List<T> nextValuesSnapshot;
					for (; ; ) {
						nextValuesSnapshot = values;
						nextValuesSnapshot.add(t);
						if (NEXT_VALUES.compareAndSet(this,
								nextValuesSnapshot,
								nextValuesSnapshot)) {
							break;
						}
					}
				}
			}
			}
		else {
			valueCount++;
			if (valuesStorage) {
				List<T> nextValuesSnapshot;
				for (; ; ) {
					nextValuesSnapshot = values;
					nextValuesSnapshot.add(t);
					if (NEXT_VALUES.compareAndSet(this,
							nextValuesSnapshot,
							nextValuesSnapshot)) {
						break;
					}
				}
			}
		}
	}

	/**
	 * Setup what fusion mode should be requested from the incomining
	 * Subscription if it happens to be QueueSubscription
	 * @param requestMode the mode to request, see Fuseable constants
	 * @return this
	 */
	public final TestSubscriber<T> requestedFusionMode(int requestMode) {
		this.requestedFusionMode = requestMode;
		return this;
	}

	/**
	 * Returns the established fusion mode or -1 if it was not enabled
	 * @return the fusion mode, see Fuseable constants
	 */
	public final int establishedFusionMode() {
		return establishedFusionMode;
	}

	String fusionModeName(int mode) {
		switch (mode) {
			case -1: return "Disabled";
			case Fuseable.NONE: return "None";
			case Fuseable.SYNC: return "Sync";
			case Fuseable.ASYNC: return "Async";
			default: return "Unknown(" + mode + ")";
		}
	}

	public final TestSubscriber<T> assertFusionMode(int expectedMode) {
		if (establishedFusionMode != expectedMode) {
			throw new AssertionError("Wrong fusion mode: expected: "
					+ fusionModeName(expectedMode) + ", actual: "
					+ fusionModeName(establishedFusionMode));
		}
		return this;
	}

	/**
	 * Assert that the fusion mode was granted.
	 * @return this
	 */
	public final TestSubscriber<T> assertFusionEnabled() {
		if (establishedFusionMode != Fuseable.SYNC
				&& establishedFusionMode != Fuseable.ASYNC) {
			throw new AssertionError("Fusion was not enabled");
		}
		return this;
	}

	/**
	 * Assert that the fusion mode was granted.
	 * @return this
	 */
	public final TestSubscriber<T> assertFusionRejected() {
		if (establishedFusionMode != Fuseable.NONE) {
			throw new AssertionError("Fusion was granted");
		}
		return this;
	}

	/**
	 * Assert that the upstream was a Fuseable source.
	 * @return this
	 */
	public final TestSubscriber<T> assertFuseableSource() {
		if (qs == null) {
			throw new AssertionError("Upstream was not Fuseable");
		}
		return this;
	}

	/**
	 * Assert that the upstream was not a Fuseable source.
	 * @return this
	 */
	public final TestSubscriber<T> assertNonFuseableSource() {
		if (qs != null) {
			throw new AssertionError("Upstream was Fuseable");
		}
		return this;
	}

	@Override
	public void onError(Throwable t) {
		errors.add(t);
		cdl.countDown();
	}

	@Override
	public void onComplete() {
		completionCount++;
		cdl.countDown();
	}

	@Override
	public void request(long n) {
		if (BackpressureUtils.validate(n)) {
			if (establishedFusionMode != Fuseable.SYNC) {
				super.request(n);
			}
		}
	}

//	 ==============================================================================================================
//	 Non public methods
//	 ==============================================================================================================

	final String valueAndClass(Object o) {
		if (o == null) {
			return null;
		}
		return o + " (" + o.getClass()
		  .getSimpleName() + ")";
	}

	/**
	 * Prepares and throws an AssertionError exception based on the message, cause, the
	 * active state and the potential errors so far.
	 *
	 * @param message the message
	 * @param cause the optional Throwable cause
	 *
	 * @throws AssertionError as expected
	 */
	final void assertionError(String message, Throwable cause) {
		StringBuilder b = new StringBuilder();

		if (cdl.getCount() != 0) {
			b.append("(active) ");
		}
		b.append(message);

		List<Throwable> err = errors;
		if (!err.isEmpty()) {
			b.append(" (+ ")
			 .append(err.size())
			 .append(" errors)");
		}
		AssertionError e = new AssertionError(b.toString(), cause);

		for (Throwable t : err) {
			e.addSuppressed(t);
		}

		throw e;
	}

}

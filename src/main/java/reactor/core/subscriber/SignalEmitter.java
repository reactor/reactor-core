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

package reactor.core.subscriber;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.LockSupport;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Producer;
import reactor.core.state.Backpressurable;
import reactor.core.state.Cancellable;
import reactor.core.state.Failurable;
import reactor.core.state.Requestable;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.EmptySubscription;
import reactor.core.util.Exceptions;
import reactor.fn.Consumer;
import reactor.fn.Predicate;

/**
 *
 * A "hot" sequence source to provide to any {@link Subscriber} and {@link org.reactivestreams.Processor} in particular.
 *
 * The key property of {@link SignalEmitter} is the pending demand tracker. Any emission can be safely sent to the
 * delegate {@link Subscriber} by using {@link #submit} to block on backpressure (missing demand) or {@link #emit} to
 * never block and return instead an {@link Emission} status.
 *
 * The emitter is itself a {@link Subscriber} that will request an unbounded value if subscribed.
 *
 * @author Stephane Maldini
 * @since 2.5
 */
public class SignalEmitter<E>
		implements Producer, Subscriber<E>, Subscription, Backpressurable, Failurable, Cancellable, Requestable,
		           Consumer<E>,
		           Closeable {

	/**
	 * An acknowledgement signal returned by {@link #emit}.
	 * {@link Emission#isOk()} is the only successful signal, the other define the emission failure cause.
	 *
	 */
	public enum Emission {
		FAILED, BACKPRESSURED, OK, CANCELLED;

		public boolean isBackpressured(){
			return this == BACKPRESSURED;
		}
		public boolean isOk(){
			return this == OK;
		}
		public boolean isFailed(){
			return this == FAILED;
		}
		public boolean isCancelled(){
			return this == CANCELLED;
		}
	}

	private static final Predicate NEVER = new Predicate(){
		@Override
		public boolean test(Object o) {
			return false;
		}
	};

	private final Subscriber<? super E> actual;

	@SuppressWarnings("unused")
	private volatile long                                  requested = 0L;
	@SuppressWarnings("rawtypes")
	static final     AtomicLongFieldUpdater<SignalEmitter> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(SignalEmitter.class, "requested");

	private Throwable uncaughtException;

	private volatile boolean cancelled;

	/**
	 *
	 * Create a
	 * {@link SignalEmitter} to safely signal a target {@link Subscriber} or {@link org.reactivestreams.Processor}.
	 *
	 * The subscriber will be immediately  {@link #start started} via {@link Subscriber#onSubscribe(Subscription)} as the result of
	 * this call.
	 *
	 * @param subscriber the decorated {@link Subscriber}
	 * @param <E> the reified {@link Subscriber} type
	 *
	 * @return a new pre-subscribed {@link SignalEmitter}
	 */
	public static <E> SignalEmitter<E> create(Subscriber<? super E> subscriber) {
		return create(subscriber, true);
	}

	/**
	 *
	 * Create a
	 * {@link SignalEmitter} to safely signal a target {@link Subscriber} or {@link org.reactivestreams.Processor}.
	 *
	 * The subscriber will be immediately {@link #start started} only if the autostart property is true.  via
	 * {@link Subscriber#onSubscribe(Subscription)} as the
	 * result of
	 * this call.
	 *
	 * @param subscriber the decorated {@link Subscriber}
	 * @param autostart true if {@link Subscriber#onSubscribe(Subscription)} is invoked during this call
	 * @param <E> the reified {@link Subscriber} type
	 *
	 * @return a new {@link SignalEmitter}
	 */
	public static <E> SignalEmitter<E> create(Subscriber<? super E> subscriber, boolean autostart) {
		SignalEmitter<E> sub = new SignalEmitter<>(subscriber);
		if (autostart) {
			sub.start();
		}
		return sub;
	}

	protected SignalEmitter(Subscriber<? super E> actual) {
		this.actual = actual;
	}

	/**
	 * Subscribe the decorated subscriber
	 * {@link Subscriber#onSubscribe(Subscription)}. If called twice, the current {@link SignalEmitter} might be
	 * cancelled as per Reactive Streams Specification enforce.
	 */
	public void start() {
		try {
			actual.onSubscribe(this);
		}
		catch (Throwable t) {
			uncaughtException = t;
			EmptySubscription.error(actual, t);
		}
	}

	/**
	 *
	 *
	 * @param data
	 * @return
	 */
	public Emission emit(E data) {
		if (uncaughtException != null) {
			return Emission.FAILED;
		}
		if (cancelled) {
			return Emission.CANCELLED;
		}
		try {
			if (BackpressureUtils.getAndSub(REQUESTED, this, 1L) == 0L) {
				return Emission.BACKPRESSURED;
			}
			actual.onNext(data);
			return Emission.OK;
		}
		catch (Exceptions.CancelException ce) {
			return Emission.CANCELLED;
		}
		catch (Exceptions.InsufficientCapacityException ice) {
			return Emission.BACKPRESSURED;
		}
		catch (Throwable t) {
			Exceptions.throwIfFatal(t);
			uncaughtException = t;
			if (cancelled) {
				return Emission.FAILED;
			}
			actual.onError(t);
			return Emission.FAILED;
		}
	}

	/**
	 *
	 * Try calling {@link Subscriber#onError(Throwable)} on the delegate {@link Subscriber}. Might fail with an
	 * unchecked exception if an error has already been recorded by this {@link SignalEmitter}.
	 *
	 * @param error the exception to signal
	 */
	public void failWith(Throwable error) {
		if (uncaughtException == null) {
			uncaughtException = error;
			if(!cancelled) {
				cancelled = true;
				actual.onError(error);
			}
			else{
				IllegalStateException ise = new IllegalStateException("Session has been cancelled previously");
				Exceptions.addCause(ise, error);
				throw ise;
			}
		}
		else {
			IllegalStateException ise = new IllegalStateException("Session already failed");
			Exceptions.addCause(ise, error);
			throw ise;
		}
	}

	/**
	 *
	 * @return
	 */
	public Emission finish() {
		if (uncaughtException != null) {
			return Emission.FAILED;
		}
		if (cancelled) {
			return Emission.CANCELLED;
		}
		try {
			cancelled = true;
			actual.onComplete();
			return Emission.OK;
		}
		catch (Exceptions.CancelException ce) {
			return Emission.CANCELLED;
		}
		catch (Exceptions.InsufficientCapacityException ice) {
			return Emission.BACKPRESSURED;
		}
		catch (Throwable t) {
			Exceptions.throwIfFatal(t);
			uncaughtException = t;
			return Emission.FAILED;
		}
	}

	/**
	 *
	 * @param data
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public long submit(E data) {
		return submit(data, -1L, TimeUnit.MILLISECONDS, NEVER);
	}

	/**
	 *
	 * @param data
	 * @param timeout
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public long submit(E data, long timeout) {
		return submit(data, timeout, TimeUnit.MILLISECONDS, NEVER);
	}

	/**
	 *
	 * @param data
	 * @param timeout
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public long submit(E data, long timeout, Predicate<E> dropPredicate) {
		return submit(data, timeout, TimeUnit.MILLISECONDS, dropPredicate);
	}

	/**
	 *
	 * @param data
	 * @param timeout
	 * @param unit
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public long submit(E data, long timeout, TimeUnit unit) {
		return submit(data, timeout, unit, NEVER);
	}

	/**
	 *
	 * @param data
	 * @param timeout
	 * @param unit
	 * @param dropPredicate
	 * @return
	 */
	public long submit(E data, long timeout, TimeUnit unit, Predicate<E> dropPredicate) {
		final long start = System.currentTimeMillis();
		long timespan =
				timeout != -1L ? (start + TimeUnit.MILLISECONDS.convert(timeout, unit)) :
						Long.MAX_VALUE;

		Emission res;
		try {
			while ((res = emit(data)).isBackpressured()) {
				if (timeout != -1L && System.currentTimeMillis() > timespan) {
					if(dropPredicate.test(data)){
						timespan += TimeUnit.MILLISECONDS.convert(timeout, unit);
					}
					else{
						break;
					}
				}
				Thread.sleep(10);
			}
		}
		catch (InterruptedException ie) {
			return -1L;
		}

		return res == Emission.OK ? unit.convert(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS) : -1L;
	}

	/**
	 *
	 * @return
	 */
	public boolean hasRequested() {
		return !cancelled && requested != 0L;
	}

	@Override
	public long requestedFromDownstream() {
		return requested;
	}

	/**
	 *
	 * @return
	 */
	public boolean hasFailed() {
		return uncaughtException != null;
	}

	/**
	 *
	 * @return
	 */
	public boolean hasEnded() {
		return cancelled;
	}

	@Override
	public Throwable getError() {
		return uncaughtException;
	}

	@Override
	public void request(long n) {
		if (BackpressureUtils.checkRequest(n, actual)) {
			BackpressureUtils.getAndAdd(REQUESTED, this, n);
		}
	}

	@Override
	public void cancel() {
		cancelled = true;
	}

	@Override
	public void accept(E e) {
		while (emit(e) == Emission.BACKPRESSURED) {
			LockSupport.parkNanos(1L);
		}
	}

	@Override
	public long getCapacity() {
		return Backpressurable.class.isAssignableFrom(actual.getClass()) ? ((Backpressurable) actual).getCapacity() :
				Long.MAX_VALUE;
	}

	@Override
	public Subscriber<? super E> downstream() {
		return actual;
	}

	@Override
	public void close() throws IOException {
		finish();
	}

	@Override
	public void onSubscribe(Subscription s) {
		s.request(Long.MAX_VALUE);
	}

	@Override
	public void onNext(E e) {
		Emission emission = emit(e);
		if(emission.isCancelled()){
			Exceptions.onNextDropped(e);
		}
		if(emission.isOk()){
			return;
		}
		if(emission.isBackpressured()){
			Exceptions.failWithOverflow();
		}
		if(emission.isFailed()){
			if(uncaughtException != null) {
				actual.onError(uncaughtException);
				return;
			}
			throw new IllegalStateException("Cached error cannot be null");
		}
	}

	@Override
	public void onError(Throwable t) {
		actual.onError(t);
	}

	@Override
	public void onComplete() {
		actual.onComplete();
	}

	@Override
	public boolean isCancelled() {
		return cancelled;
	}

	@Override
	public long getPending() {
		return -1L;
	}

	@Override
	public String toString() {
		return "SignalEmitter{" +
				"requested=" + requested +
				", uncaughtException=" + uncaughtException +
				", cancelled=" + cancelled +
				'}';
	}
}

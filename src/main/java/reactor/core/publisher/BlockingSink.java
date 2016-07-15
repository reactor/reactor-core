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

package reactor.core.publisher;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Producer;
import reactor.core.Trackable;
import reactor.util.Exceptions;

/**
 *
 * A "hot" sequence source to decorate any {@link Subscriber} or {@link org.reactivestreams.Processor}.
 *
 * The {@link BlockingSink} keeps track of the decorated {@link Subscriber} demand. Therefore any emission can be
 * safely sent to
 * the delegate
 * {@link Subscriber} by using {@link #submit} to block on backpressure (missing demand) or {@link #emit} to
 * never block and return instead an {@link BlockingSink.Emission} status.
 *
 * The emitter is itself a {@link Subscriber} that will request an unbounded value if subscribed.
 *
 * @author Stephane Maldini
 * @param <E> the element type
 */
public final class BlockingSink<E>
		implements Producer, Subscription, Trackable, Consumer<E>, Closeable {
	/**
	 * An acknowledgement signal returned by {@link #emit}.
	 * {@link BlockingSink.Emission#isOk()} is the only successful signal, the other define the emission failure cause.
	 *
	 */
	public enum Emission {
		FAILED, BACKPRESSURED, OK, CANCELLED;

		public boolean isBackpressured(){
			return this == BACKPRESSURED;
		}

		public boolean isCancelled(){
			return this == CANCELLED;
		}

		public boolean isFailed(){
			return this == FAILED;
		}

		public boolean isOk(){
			return this == OK;
		}
	}

	/**
	 *
	 * Create a
	 * {@link BlockingSink} to safely signal a target {@link Subscriber} or {@link org.reactivestreams.Processor}.
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
	 * @return a new {@link BlockingSink}
	 */
	public static <E> BlockingSink<E> create(Subscriber<? super E> subscriber, boolean autostart) {
		BlockingSink<E> sub = new BlockingSink<>(subscriber);
		if (autostart) {
			sub.start();
		}
		return sub;
	}

	/**
	 *
	 * Create a
	 * {@link BlockingSink} to safely signal a target {@link Subscriber} or {@link org.reactivestreams.Processor}.
	 *
	 * The subscriber will be immediately  {@link #start started} via {@link Subscriber#onSubscribe(Subscription)} as the result of
	 * this call.
	 *
	 * @param subscriber the decorated {@link Subscriber}
	 * @param <E> the reified {@link Subscriber} type
	 *
	 * @return a new pre-subscribed {@link BlockingSink}
	 */
	public static <E> BlockingSink<E> create(Subscriber<? super E> subscriber) {
		return create(subscriber, true);
	}
	final Subscriber<? super E> actual;
	volatile     long requested;
	Throwable uncaughtException;

	volatile boolean cancelled;

	protected BlockingSink(Subscriber<? super E> actual) {
		this.actual = actual;
	}

	@Override
	public void accept(E e) {
		while (emit(e) == Emission.BACKPRESSURED) {
			LockSupport.parkNanos(1L);
		}
	}

	@Override
	public void cancel() {
		cancelled = true;
	}

	@Override
	public void close() throws IOException {
		finish();
	}

	@Override
	public Subscriber<? super E> downstream() {
		return actual;
	}

	/**
	 * Try emitting, might throw an unchecked exception.
	 * @see Subscriber#onNext(Object)
	 * @param t the value to emit, not null
	 * @throws RuntimeException
	 */
	public void next(E t) {
		Emission emission = emit(t);
		if(emission.isOk()) {
			return;
		}
		if(emission.isBackpressured()){
			Operators.reportMoreProduced();
			return;
		}
		if(emission.isCancelled()){
			Exceptions.onNextDropped(t);
			return;
		}
		if(getError() != null){
			throw Exceptions.bubble(getError());
		}
		throw new IllegalStateException("Emission has failed");
	}

	/**
	 * A non-blocking {@link Subscriber#onNext(Object)} that will return a status 
	 * {@link BlockingSink.Emission}. The status will
	 * indicate if the decorated
	 * subscriber is backpressuring this {@link BlockingSink} and if it has previously been terminated successfully or
	 * not.
	 *
	 * @param data the data to signal
	 * @return an {@link BlockingSink.Emission} status
	 */
	public Emission emit(E data) {
		if (uncaughtException != null) {
			return Emission.FAILED;
		}
		if (cancelled) {
			return Emission.CANCELLED;
		}
		try {
			if (Operators.getAndSub(REQUESTED, this, 1L) == 0L) {
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
	 * Try calling {@link Subscriber#onError(Throwable)} on the delegate {@link Subscriber}. {@link BlockingSink#fail(Throwable)}
	 * might fail itself with an
	 * unchecked exception if an error has already been recorded or it
	 * has previously been terminated via {@link #cancel()}, {@link #finish()} or {@link #complete()}.
	 *
	 * @param error the exception to signal
	 */
	public void fail(Throwable error) {
		if (uncaughtException == null) {
			uncaughtException = error;
			if(!cancelled) {
				cancelled = true;
				actual.onError(error);
			}
			else{
				IllegalStateException ise = new IllegalStateException("Session has been cancelled previously");
				ise.addSuppressed(error);
				throw ise;
			}
		}
		else {
			IllegalStateException ise = new IllegalStateException("Session already failed");
			ise.addSuppressed(error);
			throw ise;
		}
	}

	/**
	 * Try emitting {@link #complete()} to the decorated {@link Subscriber}. The completion might not return a
	 * successful {@link BlockingSink.Emission#isOk()} if this {@link BlockingSink} was previously terminated or the delegate
	 * failed consuming the signal.
	 *
	 * @return an {@link BlockingSink.Emission} status
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

	@Override
	public long getCapacity() {
		return Trackable.class.isAssignableFrom(actual.getClass()) ? ((Trackable)
				actual).getCapacity() :
				Long.MAX_VALUE;
	}

	@Override
	public Throwable getError() {
		return uncaughtException;
	}

	/**
	 * @return true if the {@link BlockingSink} has observed any error
	 */
	public boolean hasFailed() {
		return uncaughtException != null;
	}

	/**
	 * @return true if the decorated {@link Subscriber} is actively demanding
	 */
	public boolean hasRequested() {
		return !cancelled && requested != 0L;
	}

	@Override
	public boolean isCancelled() {
		return cancelled;
	}

	/**
	 * @see Subscriber#onComplete()
	 */
	public void complete() {
		finish();
	}

	@Override
	public void request(long n) {
		if (Operators.checkRequest(n, actual)) {
			Operators.getAndAddCap(REQUESTED, this, n);
		}
	}

	@Override
	public long requestedFromDownstream() {
		return requested;
	}

	/**
	 * Subscribe the decorated subscriber
	 * {@link Subscriber#onSubscribe(Subscription)}. If called twice, the current {@link BlockingSink} might be
	 * cancelled as per Reactive Streams Specification enforce.
	 */
	public void start() {
		try {
			actual.onSubscribe(this);
		}
		catch (Throwable t) {
			uncaughtException = t;
			Operators.error(actual, t);
		}
	}
	/**
	 * Marks the emitter as terminated without completing downstream
	 */
	public void stop() {
		cancelled = true;
	}

	/**
	 * Blocking {@link Subscriber#onNext(Object)} call with an infinite wait on backpressure.
	 *
	 * @param data the data to signal
	 *
	 * @return the emission latency in milliseconds or {@literal -1} if emission failed.
	 */
	@SuppressWarnings("unchecked")
	public long submit(E data) {
		return submit(data, -1L, TimeUnit.MILLISECONDS, NEVER);
	}

	/**
	 * Blocking {@link Subscriber#onNext(Object)} call with a timed wait on backpressure.
	 *
	 * @param data the data to signal
	 * @param timeout the maximum waiting time in milliseconds before giving up
	 *
	 * @return the emission latency in milliseconds or {@literal -1} if emission failed.
	 */
	@SuppressWarnings("unchecked")
	public long submit(E data, long timeout) {
		return submit(data, timeout, TimeUnit.MILLISECONDS, NEVER);
	}

	/**
	 * Blocking {@link Subscriber#onNext(Object)} call with a timed wait on backpressure. A retry predicate will
	 * evaluate when a timeout occurs, returning true will re-schedule an emission while false will drop the signal.
	 *
	 * @param data the data to signal
	 * @param timeout the maximum waiting time in milliseconds before giving up
	 * @param dropPredicate the dropped signal callback evaluating if retry should occur or not
	 *
	 * @return the emission latency in milliseconds or {@literal -1} if emission failed.
	 */
	public long submit(E data, long timeout, Predicate<E> dropPredicate) {
		return submit(data, timeout, TimeUnit.MILLISECONDS, dropPredicate);
	}

	/**
	 * Blocking {@link Subscriber#onNext(Object)} call with a timed wait on backpressure.
	 *
	 * @param data the data to signal
	 * @param timeout the maximum waiting time in given unit before giving up
	 * @param unit the waiting time unit
	 *
	 * @return the emission latency in milliseconds or {@literal -1} if emission failed.
	 */
	@SuppressWarnings("unchecked")
	public long submit(E data, long timeout, TimeUnit unit) {
		return submit(data, timeout, unit, NEVER);
	}

	/**
	 * Blocking {@link Subscriber#onNext(Object)} call with a timed wait on backpressure. A retry predicate will
	 * evaluate when a timeout occurs, returning true will re-schedule an emission while false will drop the signal.
	 *
	 * @param data the data to signal
	 * @param timeout the maximum waiting time in given unit before giving up
	 * @param unit the waiting time unit
	 * @param dropPredicate the dropped signal callback evaluating if retry should occur or not
	 * @return the emission latency in milliseconds or {@literal -1} if emission failed.
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

	@Override
	public String toString() {
		return "BlockingSink{" +
				"requested=" + requested +
				", uncaughtException=" + uncaughtException +
				", cancelled=" + cancelled +
				'}';
	}

//

	@SuppressWarnings("rawtypes")
    static final Predicate                            NEVER     = o -> false;
	@SuppressWarnings("rawtypes")
	static final AtomicLongFieldUpdater<BlockingSink> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(BlockingSink.class, "requested");
}

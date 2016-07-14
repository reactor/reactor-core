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

package reactor.util.concurrent;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.util.Exceptions;
import reactor.util.ReactorProperties;
import reactor.util.concurrent.Sequence;
import reactor.util.concurrent.Slot;

/**
 * Ring based store of reusable entries containing the data representing an event being exchanged between event producer
 * and ringbuffer consumers.
 * @param <E> implementation storing the data for sharing during exchange or parallel coordination of an event.
 */
public abstract class RingBuffer<E> implements LongSupplier {

	@SuppressWarnings("rawtypes")
	public static final Supplier EMITTED = Slot::new;
	/**
	 * Set to -1 as sequence starting point
	 */
	public static final long     INITIAL_CURSOR_VALUE = -1L;

	/**
	 *
	 * Create a
	 * {@link Runnable} event loop that will keep monitoring a {@link LongSupplier} and compare it to a {@link RingBuffer}
	 *
	 * @param upstream the {@link Subscription} to request/cancel on
	 * @param stopCondition {@link Runnable} evaluated in the spin loop that may throw
	 * @param postWaitCallback a {@link Consumer} notified with the latest sequence read
	 * @param readCount a {@link LongSupplier} a sequence cursor to wait on
	 * @param waitStrategy a {@link WaitStrategy} to trade off cpu cycle for latency
	 * @param errorSubscriber an error subscriber if request/cancel fails
	 * @param prefetch the target prefetch size
	 *
	 * @return a {@link Runnable} loop to execute to start the requesting loop
	 */
	public static Runnable createRequestTask(Subscription upstream,
			Runnable stopCondition,
			Consumer<Long> postWaitCallback,
			LongSupplier readCount,
			WaitStrategy waitStrategy,
			Subscriber<?> errorSubscriber,
			int prefetch) {
		return new RequestTask(upstream,
				stopCondition,
				postWaitCallback,
				readCount,
				waitStrategy,
				errorSubscriber,
				prefetch);
	}

	/**
	 * Create a new multiple producer RingBuffer using the default wait strategy   {@link WaitStrategy#busySpin()}.
     * <p>See {@code MultiProducer}.
	 *
	 * @param <E> the element type
	 * @param bufferSize number of elements to create within the ring buffer.
	 * 
	 * @return the new RingBuffer instance
	 * @throws IllegalArgumentException if <tt>bufferSize</tt> is less than 1 or not a power of 2
	 */
	@SuppressWarnings("unchecked")
	public static <E> RingBuffer<Slot<E>> createMultiProducer(int bufferSize) {
		return createMultiProducer(EMITTED, bufferSize, WaitStrategy.blocking());
	}

	/**
	 * Create a new multiple producer RingBuffer with the specified wait strategy.
     * <p>See {@code MultiProducer}.
	 * 
	 * @param <E> the element type
	 * @param factory used to create the events within the ring buffer.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @param waitStrategy used to determine how to wait for new elements to become available.
	 * @return the new RingBuffer instance
	 */
	public static <E> RingBuffer<E> createMultiProducer(Supplier<E> factory,
			int bufferSize,
			WaitStrategy waitStrategy) {
		return createMultiProducer(factory, bufferSize, waitStrategy, null);
	}

	/**
	 * Create a new multiple producer RingBuffer with the specified wait strategy.
     * <p>See {@code MultiProducer}.
	 * @param <E> the element type
	 * @param factory used to create the events within the ring buffer.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @param waitStrategy used to determine how to wait for new elements to become available.
	 * @param spinObserver the Runnable to call on a spin loop wait
	 * @return the new RingBuffer instance
	 */
	public static <E> RingBuffer<E> createMultiProducer(Supplier<E> factory,
			int bufferSize,
			WaitStrategy waitStrategy, Runnable spinObserver) {

		if (ReactorProperties.hasUnsafe() && QueueSupplier.isPowerOfTwo(bufferSize)) {
			MultiProducer sequencer = new MultiProducer(bufferSize, waitStrategy, spinObserver);

			return new UnsafeRingBuffer<>(factory, sequencer);
		}
		else {
			NotFunMultiProducer sequencer =
					new NotFunMultiProducer(bufferSize, waitStrategy, spinObserver);

			return new NotFunRingBuffer<>(factory, sequencer);
		}
	}

	/**
	 * Create a {@link Queue} view over the given {@link RingBuffer} of {@link Slot}.
	 * The read cursor will be added to the gating sequences and will be incremented on polling.
	 * Offer will return false if the {@link RingBuffer#tryNext()} does not succeed.
	 *
	 * @param buffer the {@link RingBuffer} repository
	 * @param startSequence the starting sequence to track in the {@link Queue}
	 * @param <T> the {@link Slot} data content type
	 *
	 * @return a non blocking {@link Queue} view of the given {@link RingBuffer}
	 */
	public static <T> Queue<T> nonBlockingBoundedQueue(RingBuffer<Slot<T>> buffer, long startSequence){
		return new NonBlockingSPSCQueue<>(buffer, startSequence);
	}
	/**
	 * Create a {@link Queue} view over the given {@link RingBuffer} of {@link Slot}.
	 * The read cursor will be added to the gating sequences and will be incremented on polling.
	 * Offer will spin on {@link RingBuffer#next()} if the ringbuffer is overrun.
	 *
	 * @param buffer the {@link RingBuffer} repository
	 * @param startSequence the starting sequence to track in the {@link Queue}
	 * @param <T> the {@link Slot} data content type
	 *
	 * @return a non blocking {@link Queue} view of the given {@link RingBuffer}
	 */
	public static <T> Queue<T> blockingBoundedQueue(RingBuffer<Slot<T>> buffer, long startSequence){
		return new BlockingSPSCQueue<>(buffer, startSequence);
	}

	/**
	 * Create a new single producer RingBuffer using the default wait strategy  {@link WaitStrategy#busySpin()}.
     * <p>See {@code MultiProducer}.
	 * @param <E> the element type
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @return the new RingBuffer instance
	 */
	@SuppressWarnings("unchecked")
	public static <E> RingBuffer<Slot<E>> createSingleProducer(int bufferSize) {
		return createSingleProducer(EMITTED, bufferSize, WaitStrategy.busySpin());
	}

	/**
	 * Create a new single producer RingBuffer using the default wait strategy   {@link WaitStrategy#busySpin()}.
     * <p>See {@code MultiProducer}.
	 * @param <E> the element type
	 * @param factory used to create the events within the ring buffer.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @return the new RingBuffer instance
	 */
	public static <E> RingBuffer<E> createSingleProducer(Supplier<E> factory, int bufferSize) {
		return createSingleProducer(factory, bufferSize, WaitStrategy.busySpin());
	}

	/**
	 * Create a new single producer RingBuffer with the specified wait strategy.
     * <p>See {@code MultiProducer}.
	 * @param <E> the element type
	 * @param factory used to create the events within the ring buffer.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @param waitStrategy used to determine how to wait for new elements to become available.
	 * @return the new RingBuffer instance
	 */
	public static <E> RingBuffer<E> createSingleProducer(Supplier<E> factory,
			int bufferSize,
			WaitStrategy waitStrategy) {
		return createSingleProducer(factory, bufferSize, waitStrategy, null);
	}

	/**
	 * Create a new single producer RingBuffer with the specified wait strategy.
     * <p>See {@code MultiProducer}.
     * @param <E> the element type
	 * @param factory used to create the events within the ring buffer.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @param waitStrategy used to determine how to wait for new elements to become available.
	 * @param spinObserver called each time the next claim is spinning and waiting for a slot
     * @return the new RingBuffer instance
	 */
	public static <E> RingBuffer<E> createSingleProducer(Supplier<E> factory,
			int bufferSize,
			WaitStrategy waitStrategy,
			Runnable spinObserver) {
		SingleProducerSequencer sequencer = new SingleProducerSequencer(bufferSize, waitStrategy, spinObserver);

		if (ReactorProperties.hasUnsafe() && QueueSupplier.isPowerOfTwo(bufferSize)) {
			return new UnsafeRingBuffer<>(factory, sequencer);
		}
		else {
			return new NotFunRingBuffer<>(factory, sequencer);
		}
	}

	/**
	 * Get the minimum sequence from an array of {@link Sequence}s.
	 *
	 * @param sequences to compare.
	 * @param minimum an initial default minimum.  If the array is empty this value will be returned.
	 *
	 * @return the minimum sequence found or Long.MAX_VALUE if the array is empty.
	 */
	public static long getMinimumSequence(final Sequence[] sequences, long minimum) {
		for (int i = 0, n = sequences.length; i < n; i++) {
			long value = sequences[i].getAsLong();
			minimum = Math.min(minimum, value);
		}

		return minimum;
	}

	/**
	 * Get the minimum sequence from an array of {@link Sequence}s.
	 *
	 * @param excludeSequence to exclude from search.
	 * @param sequences to compare.
	 * @param minimum an initial default minimum.  If the array is empty this value will be returned.
	 *
	 * @return the minimum sequence found or Long.MAX_VALUE if the array is empty.
	 */
	public static long getMinimumSequence(Sequence excludeSequence, final Sequence[] sequences, long minimum) {
		for (int i = 0, n = sequences.length; i < n; i++) {
			if (excludeSequence == null || sequences[i] != excludeSequence) {
				long value = sequences[i].getAsLong();
				minimum = Math.min(minimum, value);
			}
		}

		return minimum;
	}

	/**
	 * Calculate the log base 2 of the supplied integer, essentially reports the location of the highest bit.
	 *
	 * @param i Value to calculate log2 for.
	 *
	 * @return The log2 value
	 */
	public static int log2(int i) {
		int r = 0;
		while ((i >>= 1) != 0) {
			++r;
		}
		return r;
	}

	/**
	 * Test if exception is alert
	 * @param t exception checked
	 * @return true if this is an alert signal
	 */
	public static boolean isAlert(Throwable t){
		return t == AlertException.INSTANCE;
	}

	/**
	 * @param init the initial value
	 *
	 * @return a safe or unsafe sequence set to the passed init value
	 */
	public static Sequence newSequence(long init) {
		if (ReactorProperties.hasUnsafe()) {
			return new UnsafeSequence(init);
		}
		else {
			return new AtomicSequence(init);
		}
    }

	/**
	 * Signal a new {@link Slot} value to a {@link RingBuffer} typed with them.
	 *
	 * @param value the data to store
	 * @param ringBuffer the target {@link RingBuffer} of {@link Slot}
	 * @param <E> the {@link Slot} reified type
	 */
	public static <E> void onNext(E value, RingBuffer<Slot<E>> ringBuffer) {
		final long seqId = ringBuffer.next();
		final Slot<E> signal = ringBuffer.get(seqId);
		signal.value = value;
		ringBuffer.publish(seqId);
	}

	/**
	 * Throw a signal singleton exception that can be checked against
	 * {@link #isAlert(Throwable)}
	 */
	public static void throwAlert() {
		throw AlertException.INSTANCE;
	}

	/**
	 * Spin CPU until the request {@link LongSupplier} is populated at least once by a strict positive value.
	 * To relieve the spin loop, the read sequence itself will be used against so it will wake up only when a signal
	 * is emitted upstream or other stopping condition including terminal signals thrown by the
	 * {@link RingBufferReceiver} waiting barrier.
	 *
	 * @param pendingRequest the {@link LongSupplier} request to observe
	 * @param barrier {@link RingBufferReceiver} to wait on
	 * @param isRunning {@link AtomicBoolean} calling loop running state
	 * @param nextSequence {@link LongSupplier} ring buffer read cursor
	 * @param waiter an optional extra spin observer for the wait strategy in {@link RingBufferReceiver}
	 *
	 * @return true if a request has been received, false in any other case.
	 */
	public static boolean waitRequestOrTerminalEvent(LongSupplier pendingRequest,
			RingBufferReceiver barrier,
			AtomicBoolean isRunning,
			LongSupplier nextSequence,
			Runnable waiter) {
		try {
			long waitedSequence;
			while (pendingRequest.getAsLong() <= 0L) {
				//pause until first request
				waitedSequence = nextSequence.getAsLong() + 1;
				if (waiter != null) {
					waiter.run();
					barrier.waitFor(waitedSequence, waiter);
				}
				else {
					barrier.waitFor(waitedSequence);
				}
				if (!isRunning.get()) {
					throw Exceptions.CancelException.INSTANCE;
				}
				LockSupport.parkNanos(1L);
			}
		}
		catch (Exceptions.CancelException | AlertException ae) {
			return false;
		}
		catch (InterruptedException ie) {
			Thread.currentThread()
			      .interrupt();
		}

		return true;
	}

	/**
	 * Add the specified gating sequence to this instance of the Disruptor.  It will safely and atomically be added to
	 * the list of gating sequences and not RESET to the current ringbuffer cursor.
	 * @param gatingSequence The sequences to add.
	 */
	abstract public void addGatingSequence(Sequence gatingSequence);

	/**
	 * <p>Get the event for a given sequence in the RingBuffer.</p>
	 *
	 * <p>This call has 2 uses.  Firstly use this call when publishing to a ring buffer. After calling {@link
	 * RingBuffer#next()} use this call to get hold of the preallocated event to fill with data before calling {@link
	 * RingBuffer#publish(long)}.</p>
	 *
	 * <p>Secondly use this call when consuming data from the ring buffer.  After calling {@link
	 * RingBufferReceiver#waitFor(long)} call this method with any value greater than that your current consumer sequence
	 * and less than or equal to the value returned from the {@link RingBufferReceiver#waitFor(long)} method.</p>
	 * @param sequence for the event
	 * @return the event for the given sequence
	 */
	abstract public E get(long sequence);

	@Override
	public long getAsLong() {
		return getCursor();
	}

	/**
	 * Get the current cursor value for the ring buffer.  The actual value recieved will depend on the type of {@code
	 * RingBufferProducer} that is being used.
	 * <p>
     * See {@code MultiProducer}.
     * See {@code SingleProducerSequencer}
	 * @return the current cursor value
	 */
	abstract public long getCursor();

	/**
	 * Get the minimum sequence value from all of the gating sequences added to this ringBuffer.
	 * @return The minimum gating sequence or the cursor sequence if no sequences have been added.
	 */
	abstract public long getMinimumGatingSequence();

	/**
	 * Get the minimum sequence value from all of the gating sequences added to this ringBuffer.
	 * @param sequence the target sequence
	 * @return The minimum gating sequence or the cursor sequence if no sequences have been added.
	 */
	abstract public long getMinimumGatingSequence(Sequence sequence);

	/**
	 * Get the buffered count
	 * @return the buffered count
	 */
	abstract public int getPending();

	/**
	 * Get the current cursor value for the ring buffer.  The actual value recieved will depend on the type of {@code
	 * RingBufferProducer} that is being used.
     * <p>
     * See {@code MultiProducer}.
     * See {@code SingleProducerSequencer}.
	 * @return the current cursor sequence
	 */
	abstract public Sequence getSequence();

	/**
	 * @return the fixed buffer size
	 */
	abstract public int bufferSize();

	/**
	 *
	 * @return the current list of read cursors
	 */
	public Sequence[] getSequenceReceivers() {
		return getSequencer().getGatingSequences();
	}

	abstract RingBufferProducer getSequencer();/*

	*//*
	 * Mark the remaining capacity of this buffer to 0 to prevent later next.
	 *//*
	public final void markAsTerminated(){
		addGatingSequence(newSequence(getCursor()));
		try{
			tryNext((int)remainingCapacity());
		}
		catch (Exceptions.AlertException | Exceptions.InsufficientCapacityException ice){
			//ignore;
		}
	}*/

	/**
	 * Create a new {@link RingBufferReceiver} to be used by an EventProcessor to track which messages are available to be read
	 * from the ring buffer given a list of sequences to track.
	 * @return A sequence barrier that will track the ringbuffer.
	 * @see RingBufferReceiver
	 */
	abstract public RingBufferReceiver newBarrier();

	/**
	 * Increment and return the next sequence for the ring buffer.  Calls of this method should ensure that they always
	 * publish the sequence afterward.  E.g.
	 * <pre>
	 * long sequence = ringBuffer.next();
	 * try {
	 *     Event e = ringBuffer.get(sequence);
	 *     // Do some work with the event.
	 * } finally {
	 *     ringBuffer.publish(sequence);
	 * }
	 * </pre>
	 * @return The next sequence to publish to.
	 * @see RingBuffer#publish(long)
	 * @see RingBuffer#get(long)
	 */
	abstract public long next();

	/**
	 * The same functionality as {@link RingBuffer#next()}, but allows the caller to claim the next n sequences.
	 * <p>
     * See {@code RingBufferProducer.next(int)}
	 * @param n number of slots to claim
	 * @return sequence number of the highest slot claimed
	 */
	abstract public long next(int n);

	/**
	 * Publish the specified sequence.  This action marks this particular message as being available to be read.
	 * @param sequence the sequence to publish.
	 */
	abstract public void publish(long sequence);

	/**
	 * Publish the specified sequences.  This action marks these particular messages as being available to be read.
	 * <p>
     * See {@code RingBufferProducer.next(int)}
	 * @param lo the lowest sequence number to be published
	 * @param hi the highest sequence number to be published
	 */
	abstract public void publish(long lo, long hi);

	/**
	 * Get the remaining capacity for this ringBuffer.
	 * @return The number of slots remaining.
	 */
	abstract public long remainingCapacity();

	/**
	 * Remove the specified sequence from this ringBuffer.
	 * @param sequence to be removed.
	 * @return <tt>true</tt> if this sequence was found, <tt>false</tt> otherwise.
	 */
	abstract public boolean removeGatingSequence(Sequence sequence);

	/**
	 * Resets the cursor to a specific value.  This can be applied at any time, but it is worth noting that it can cause
	 * a data race and should only be used in controlled circumstances.  E.g. during initialisation.
	 * @param sequence The sequence to reset too.
	 * @throws IllegalStateException If any gating sequences have already been specified.
	 */
	abstract public void resetTo(long sequence);

	@Override
	public String toString() {
		return "RingBuffer{remaining size:" + remainingCapacity() + ", size:" + bufferSize() +
				", " +
				"cursor:" +
				getAsLong() + ", " +
				"min:" + getMinimumGatingSequence() + ", subscribers:" + getSequencer().gatingSequences.length + "}";
	}

	/**
	 * <p>Increment and return the next sequence for the ring buffer.  Calls of this method should ensure that they
	 * always publish the sequence afterward.  E.g.
	 * <pre>
	 * long sequence = ringBuffer.next();
	 * try {
	 *     Event e = ringBuffer.get(sequence);
	 *     // Do some work with the event.
	 * } finally {
	 *     ringBuffer.publish(sequence);
	 * }
	 * </pre>
	 * <p>This method will not block if there is not space available in the ring buffer, instead it will throw a {@link
	 * RuntimeException}.
	 *
	 * @return The next sequence to publish to.
	 *
	 * @throws Exceptions.InsufficientCapacityException if the necessary space in the ring buffer is not available
	 * @see RingBuffer#publish(long)
	 * @see RingBuffer#get(long)
	 */
	abstract public long tryNext() throws Exceptions.InsufficientCapacityException;

	/**
	 * The same functionality as {@link RingBuffer#tryNext()}, but allows the caller to attempt to claim the next n
	 * sequences.
	 * @param n number of slots to claim
	 * @return sequence number of the highest slot claimed
	 * @throws Exceptions.InsufficientCapacityException if the necessary space in the ring buffer is not available
	 */
	abstract public long tryNext(int n) throws Exceptions.InsufficientCapacityException;

	/**
	 * Used to alert consumers waiting with a {@link WaitStrategy} for status changes.
	 * <p>
	 * It does not fill in a stack trace for performance reasons.
	 */
	@SuppressWarnings("serial")
	static final class AlertException extends RuntimeException {
		/** Pre-allocated exception to avoid garbage generation */
		public static final AlertException INSTANCE = new AlertException();

		/**
		 * Private constructor so only a single instance any.
		 */
		private AlertException() {
		}

		/**
		 * Overridden so the stack trace is not filled in for this exception for performance reasons.
		 *
		 * @return this instance.
		 */
		@Override
		public Throwable fillInStackTrace() {
			return this;
		}

	}
}

abstract class SPSCQueue<T> implements Queue<T> {

	final Sequence pollCursor;
	final RingBuffer<Slot<T>> buffer;


	SPSCQueue(RingBuffer<Slot<T>> buffer, long startingSequence) {
		this.buffer = buffer;
		this.pollCursor = RingBuffer.newSequence(startingSequence);
		buffer.addGatingSequence(pollCursor);
		this.pollCursor.set(startingSequence);
	}

	@Override
	final public void clear() {
		pollCursor.set(buffer.getCursor());
	}

	@Override
	final public T element() {
		T e = peek();
		if (e == null) {
			throw new NoSuchElementException();
		}
		return e;
	}

	@Override
	final public boolean isEmpty() {
		return buffer.getCursor() == pollCursor.getAsLong();
	}

	@Override
	public Iterator<T> iterator() {
		return new QueueSupplier.QueueIterator<>(this);
	}

	@Override
	final public T peek() {
		long current = buffer.getCursor();
		long cachedSequence = pollCursor.getAsLong() + 1L;

		if (cachedSequence <= current) {
			return buffer.get(cachedSequence).value;
		}
		return null;
	}

	@Override
	final public T poll() {
		long current = buffer.getCursor();
		long cachedSequence = pollCursor.getAsLong() + 1L;

		if (cachedSequence <= current) {
			T v = buffer.get(cachedSequence).value;
			if (v != null) {
				pollCursor.set(cachedSequence);
			}
			return v;
		}
		return null;
	}

	@Override
	final public T remove() {
		T e = poll();
		if (e == null) {
			throw new NoSuchElementException();
		}
		return e;
	}

	@Override
	public final boolean add(T o) {
		long seq = buffer.next();

		buffer.get(seq).value = o;
		buffer.publish(seq);
		return true;
	}

	@Override
	public final boolean addAll(Collection<? extends T> c) {
		if (c.isEmpty()) {
			return false;
		}
		for (T t : c) {
			add(t);
		}
		return true;
	}

	@Override
	final public boolean contains(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	final public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	final public boolean remove(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	final public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	final public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public final int size() {
		return (int) (buffer.getCursor() - pollCursor.getAsLong());
	}
	@Override
	@SuppressWarnings("unchecked")
	final public T[] toArray() {
		return toArray((T[]) new Object[buffer.bufferSize()]);
	}

	@Override
	@SuppressWarnings("unchecked")
	final public <E> E[] toArray(E[] a) {

		final long cursor = buffer.getCursor();
		long s = pollCursor.getAsLong() + 1L;
		final E[] array;
		final int n = (int) (cursor - s);

		if (n == 0) {
			return a;
		}

		if (a.length < n) {
			array = (E[]) new Object[n];
		}
		else {
			array = a;
		}

		int i = 0;
		while (s < cursor) {
			array[i++] = (E) buffer.get(cursor).value;
			s++;
		}
		return array;
	}

	@Override
	public String toString() {
		return "SPSCQueue{" +
				"pollCursor=" + pollCursor +
				", parent=" + buffer.toString() +
				'}';
	}
}


/**
 * An async request client for ring buffer impls
 *
 * @author Stephane Maldini
 */
final class RequestTask implements Runnable {

	final WaitStrategy waitStrategy;

	final LongSupplier readCount;

	final Subscription upstream;

	final Runnable spinObserver;

	final Consumer<Long> postWaitCallback;

	final Subscriber<?> errorSubscriber;

	final int prefetch;

	public RequestTask(Subscription upstream,
			Runnable stopCondition,
			Consumer<Long> postWaitCallback,
			LongSupplier readCount,
			WaitStrategy waitStrategy,
			Subscriber<?> errorSubscriber,
			int prefetch) {
		this.waitStrategy = waitStrategy;
		this.readCount = readCount;
		this.postWaitCallback = postWaitCallback;
		this.errorSubscriber = errorSubscriber;
		this.upstream = upstream;
		this.spinObserver = stopCondition;
		this.prefetch = prefetch;
	}

	@Override
	public void run() {
		final long bufferSize = prefetch;
		final long limit = bufferSize - Math.max(bufferSize >> 2, 1);
		long cursor = -1;
		try {
			spinObserver.run();
			upstream.request(bufferSize - 1);

			for (; ; ) {
				cursor = waitStrategy.waitFor(cursor + limit, readCount, spinObserver);
				if (postWaitCallback != null) {
					postWaitCallback.accept(cursor);
				}
				//spinObserver.accept(null);
				upstream.request(limit);
			}
		}
		catch (RingBuffer.AlertException e) {
			//completed
		}
		catch (Exceptions.CancelException ce) {
			upstream.cancel();
		}
		catch (InterruptedException e) {
			Thread.currentThread()
			      .interrupt();
		}
		catch (Throwable t) {
			Exceptions.throwIfFatal(t);
			errorSubscriber.onError(t);
		}
	}
}

final class NonBlockingSPSCQueue<T> extends SPSCQueue<T>{
	NonBlockingSPSCQueue(RingBuffer<Slot<T>> buffer, long startingSequence) {
		super(buffer, startingSequence);
	}

	@Override
	public final boolean offer(T o) {
		try {
			long seq = buffer.tryNext();

			buffer.get(seq).value = o;
			buffer.publish(seq);
			return true;
		}
		catch (Exceptions.InsufficientCapacityException ice) {
			return false;
		}
	}
}
final class BlockingSPSCQueue<T> extends SPSCQueue<T>{
	BlockingSPSCQueue(RingBuffer<Slot<T>> buffer, long startingSequence) {
		super(buffer, startingSequence);
	}

	@Override
	public final boolean offer(T o) {
			long seq = buffer.next();
			buffer.get(seq).value = o;
			buffer.publish(seq);
			return true;
	}
}
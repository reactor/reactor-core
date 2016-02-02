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

package reactor.core.queue;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Producer;
import reactor.core.state.Backpressurable;
import reactor.core.state.Introspectable;
import reactor.core.util.Exceptions;
import reactor.core.util.PlatformDependent;
import reactor.core.util.Sequence;
import reactor.core.util.WaitStrategy;
import reactor.fn.Consumer;
import reactor.fn.LongSupplier;
import reactor.fn.Supplier;

/**
 * Ring based store of reusable entries containing the data representing an event being exchanged between event producer
 * and ringbuffer consumers.
 * @param <E> implementation storing the data for sharing during exchange or parallel coordination of an event.
 */
public abstract class RingBuffer<E> implements LongSupplier, Backpressurable {

	@SuppressWarnings("raw")
	public static final Supplier EMITTED = new Supplier() {
		@Override
		public Slot get() {
			return new Slot<>();
		}
	};
	/**
	 * Set to -1 as sequence starting point
	 */
	public static final long     INITIAL_CURSOR_VALUE = -1L;

	/**
	 * Calculate the next power of 2, greater than or equal to x.<p> From Hacker's Delight, Chapter 3, Harry S. Warren
	 * Jr.
	 *
	 * @param x Value to round up
	 *
	 * @return The next power of 2 from x inclusive
	 */
	public static int ceilingNextPowerOfTwo(final int x) {
		return 1 << (32 - Integer.numberOfLeadingZeros(x - 1));
	}

	/**
	 *
	 * Create a
	 * {@link Runnable} event loop that will keep monitoring a {@link LongSupplier} and compare it to a {@link RingBuffer}
	 *
	 * @param upstream
	 * @param stopCondition
	 * @param postWaitCallback
	 * @param readCount
	 * @param waitStrategy
	 * @param errorSubscriber
	 * @param ringbuffer
	 * @return
	 */
	public static Runnable createRequestTask(Subscription upstream,
			Runnable stopCondition,
			Consumer<Long> postWaitCallback,
			LongSupplier readCount,
			WaitStrategy waitStrategy,
			Subscriber<?> errorSubscriber,
			RingBuffer ringbuffer) {
		return new RequestTask(upstream,
				stopCondition,
				postWaitCallback,
				readCount,
				waitStrategy,
				errorSubscriber,
				ringbuffer);
	}

	/**
	 * Create a new multiple producer RingBuffer using the default wait strategy   {@link WaitStrategy#busySpin()}.
	 *
	 * @param bufferSize number of elements to create within the ring buffer.
	 *
	 * @throws IllegalArgumentException if <tt>bufferSize</tt> is less than 1 or not a power of 2
	 * @see MultiProducerSequencer
	 */
	@SuppressWarnings("unchecked")
	public static <E> RingBuffer<Slot<E>> createMultiProducer(int bufferSize) {
		return createMultiProducer(EMITTED, bufferSize, WaitStrategy.blocking());
	}

	/**
	 * Create a new multiple producer RingBuffer using the default wait strategy   {@link WaitStrategy#busySpin()}.
	 * @param factory used to create the events within the ring buffer.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @throws IllegalArgumentException if <tt>bufferSize</tt> is less than 1 or not a power of 2
	 * @see MultiProducerSequencer
	 */
	public static <E> RingBuffer<E> createMultiProducer(Supplier<E> factory, int bufferSize) {
		return createMultiProducer(factory, bufferSize, WaitStrategy.busySpin());
	}

	/**
	 * Create a new multiple producer RingBuffer with the specified wait strategy.
	 * @param factory used to create the events within the ring buffer.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @param waitStrategy used to determine how to wait for new elements to become available.
	 *
	 * @see MultiProducerSequencer
	 */
	public static <E> RingBuffer<E> createMultiProducer(Supplier<E> factory,
			int bufferSize,
			WaitStrategy waitStrategy) {
		return createMultiProducer(factory, bufferSize, waitStrategy, null);
	}

	/**
	 * Create a new multiple producer RingBuffer with the specified wait strategy.
	 * @param factory used to create the events within the ring buffer.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @param waitStrategy used to determine how to wait for new elements to become available.
	 *
	 * @see MultiProducerSequencer
	 */
	public static <E> RingBuffer<E> createMultiProducer(Supplier<E> factory,
			int bufferSize,
			WaitStrategy waitStrategy, Runnable spinObserver) {

		if (PlatformDependent.hasUnsafe() && isPowerOfTwo(bufferSize)) {
			MultiProducerSequencer sequencer = new MultiProducerSequencer(bufferSize, waitStrategy, spinObserver);

			return new UnsafeRingBuffer<E>(factory, sequencer);
		}
		else {
			NotFunMultiProducerSequencer sequencer =
					new NotFunMultiProducerSequencer(bufferSize, waitStrategy, spinObserver);

			return new NotFunRingBuffer<E>(factory, sequencer);
		}
	}

	/**
	 *
	 * @param buffer
	 * @param startSequence
	 * @param <T>
	 * @return
	 */
	public static <T> Queue<T> nonBlockingBoundedQueue(RingBuffer<Slot<T>> buffer, long startSequence){
		return new NonBlockingSPSCQueue<>(buffer, startSequence);
	}
	/**
	 *
	 * @param buffer
	 * @param startSequence
	 * @param <T>
	 * @return
	 */
	public static <T> Queue<T> blockingBoundedQueue(RingBuffer<Slot<T>> buffer, long startSequence){
		return new BlockingSPSCQueue<>(buffer, startSequence);
	}

	/**
	 * Create a new single producer RingBuffer using the default wait strategy  {@link WaitStrategy#busySpin()}.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @see MultiProducerSequencer
	 */
	@SuppressWarnings("unchecked")
	public static <E> RingBuffer<Slot<E>> createSingleProducer(int bufferSize) {
		return createSingleProducer(EMITTED, bufferSize, WaitStrategy.busySpin());
	}

	/**
	 * Create a new single producer RingBuffer using the default wait strategy  {@link WaitStrategy#busySpin()}.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @see MultiProducerSequencer
	 */
	@SuppressWarnings("unchecked")
	public static <E> RingBuffer<Slot<E>> createSingleProducer(int bufferSize, Runnable spinObserver) {
		return createSingleProducer(EMITTED, bufferSize, WaitStrategy.busySpin(), spinObserver);
	}

	/**
	 * Create a new single producer RingBuffer using the default wait strategy   {@link WaitStrategy#busySpin()}.
	 * @param factory used to create the events within the ring buffer.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @see MultiProducerSequencer
	 */
	public static <E> RingBuffer<E> createSingleProducer(Supplier<E> factory, int bufferSize) {
		return createSingleProducer(factory, bufferSize, WaitStrategy.busySpin());
	}

	/**
	 * Create a new single producer RingBuffer with the specified wait strategy.
	 * @param factory used to create the events within the ring buffer.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @param waitStrategy used to determine how to wait for new elements to become available.
	 *
	 * @see SingleProducerSequencer
	 */
	public static <E> RingBuffer<E> createSingleProducer(Supplier<E> factory,
			int bufferSize,
			WaitStrategy waitStrategy) {
		return createSingleProducer(factory, bufferSize, waitStrategy, null);
	}

	/**
	 * Create a new single producer RingBuffer with the specified wait strategy.
	 * @param factory used to create the events within the ring buffer.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @param waitStrategy used to determine how to wait for new elements to become available.
	 * @param spinObserver called each time the next claim is spinning and waiting for a slot
	 *
	 * @see SingleProducerSequencer
	 */
	public static <E> RingBuffer<E> createSingleProducer(Supplier<E> factory,
			int bufferSize,
			WaitStrategy waitStrategy,
			Runnable spinObserver) {
		SingleProducerSequencer sequencer = new SingleProducerSequencer(bufferSize, waitStrategy, spinObserver);

		if (PlatformDependent.hasUnsafe() && isPowerOfTwo(bufferSize)) {
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
			long value = sequences[i].get();
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
				long value = sequences[i].get();
				minimum = Math.min(minimum, value);
			}
		}

		return minimum;
	}

	/**
	 *
	 * @param x
	 * @return
	 */
	public static boolean isPowerOfTwo(final int x) {
		return Integer.bitCount(x) == 1;
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
	 * @param init
	 *
	 * @return
	 */
	public static Sequence newSequence(long init) {
		if (PlatformDependent.hasUnsafe()) {
			return new UnsafeSequence(init);
		}
		else {
			return new AtomicSequence(init);
		}
    }

	/**
	 *
	 * @param value
	 * @param ringBuffer
	 * @param <E>
	 */
	public static <E> void onNext(E value, RingBuffer<Slot<E>> ringBuffer) {
		final long seqId = ringBuffer.next();
		final Slot<E> signal = ringBuffer.get(seqId);
		signal.value = value;
		ringBuffer.publish(seqId);
	}

	/**
	 * @param pendingRequest
	 * @param barrier
	 * @param isRunning
	 * @param nextSequence
	 * @param waiter
	 *
	 * @return
	 */
	public static boolean waitRequestOrTerminalEvent(LongSupplier pendingRequest,
			RingBufferReceiver barrier,
			AtomicBoolean isRunning,
			LongSupplier nextSequence,
			Runnable waiter) {
		try {
			long waitedSequence;
			while (pendingRequest.get() <= 0L) {
				//pause until first request
				waitedSequence = nextSequence.get() + 1;
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
		catch (Exceptions.CancelException | Exceptions.AlertException ae) {
			return false;
		}
		catch (InterruptedException ie) {
			Thread.currentThread()
			      .interrupt();
		}

		return true;
	}

	/**
	 * Wrap a new sequence into a traceable {@link Producer} thus keeping reference and adding an extra stack level
	 * when
	 * peeking. Mostly invisible cost but the option is left open. Keeping reference of the arbitrary consumer allows
	 * expanded operational navigation (graph) by finding all target subscribers of a given ring buffer.
	 *
	 * @param init the initial sequence index
	 * @param delegate the target to proxy
	 *
	 * @return a wrapped {@link Sequence}
	 */
	public static Sequence wrap(long init, Object delegate) {
		if (PlatformDependent.TRACEABLE_RING_BUFFER_PROCESSOR) {
			return wrap(newSequence(init), delegate);
		}
		else {
			return newSequence(init);
		}
	}

	/**
	 * Wrap a sequence into a traceable {@link Producer} thus keeping reference and adding an extra stack level when
	 * peeking. Mostly invisible cost but the option is left open.
	 *
	 * @param init the sequence reference
	 * @param delegate the object to wrap
	 *
	 * @return a wrapped {@link Sequence}
	 */
	public static Sequence wrap(Sequence init, Object delegate){
		return new Wrapped<>(delegate, init);
    }

	/**
	 * Add the specified gating sequence to this instance of the Disruptor.  It will safely and atomically be added to
	 * the list of gating sequences and not RESET to the current ringbuffer cursor unlike addGatingSequences.
	 * @param gatingSequence The sequences to add.
	 */
	abstract public void addGatingSequence(Sequence gatingSequence);

	/**
	 * Add the specified gating sequences to this instance of the Disruptor.  They will safely and atomically added to
	 * the list of gating sequences.
	 * @param gatingSequences The sequences to add.
	 */
	abstract public void addGatingSequences(Sequence... gatingSequences);

	/**
	 * Get the cached remaining capacity for this ringBuffer.
	 * @return The number of slots remaining.
	 */
	abstract public long cachedRemainingCapacity();

	/**
	 * Sets the cursor to a specific sequence and returns the preallocated entry that is stored there.  This can cause a
	 * data race and should only be done in controlled circumstances, e.g. during initialisation.
	 * @param sequence The sequence to claim.
	 * @return The preallocated event.
	 */
	abstract public E claimAndGetPreallocated(long sequence);

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
	public long get() {
		return getCursor();
	}

	/**
	 * Get the current cursor value for the ring buffer.  The actual value recieved will depend on the type of {@link
	 * RingBufferProducer} that is being used.
	 * @see MultiProducerSequencer
	 * @see SingleProducerSequencer
	 */
	abstract public long getCursor();

	/**
	 * Get the minimum sequence value from all of the gating sequences added to this ringBuffer.
	 * @return The minimum gating sequence or the cursor sequence if no sequences have been added.
	 */
	abstract public long getMinimumGatingSequence();

	/**
	 * Get the minimum sequence value from all of the gating sequences added to this ringBuffer.
	 * @return The minimum gating sequence or the cursor sequence if no sequences have been added.
	 */
	abstract public long getMinimumGatingSequence(Sequence sequence);

	/**
	 * Get the current cursor value for the ring buffer.  The actual value recieved will depend on the type of {@link
	 * RingBufferProducer} that is being used.
	 * @see MultiProducerSequencer
	 * @see SingleProducerSequencer
	 */
	abstract public Sequence getSequence();

	/**
	 *
	 * @return
	 */
	public Sequence[] getSequenceReceivers() {
		return getSequencer().getGatingSequences();
	}

	abstract RingBufferProducer getSequencer();

	/**
	 * Given specified <tt>requiredCapacity</tt> determines if that amount of space is available.  Note, you can not
	 * assume that if this method returns <tt>true</tt> that a call to {@link RingBuffer#next()} will not block.
	 * Especially true if this ring buffer is set up to handle multiple producers.
	 * @param requiredCapacity The capacity to check for.
	 * @return <tt>true</tt> If the specified <tt>requiredCapacity</tt> is available <tt>false</tt> if now.
	 */
	abstract public boolean hasAvailableCapacity(int requiredCapacity);

	/**
	 * Determines if a particular entry has been published.
	 * @param sequence The sequence to identify the entry.
	 * @return If the value has been published or not.
	 */
	abstract public boolean isPublished(long sequence);

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
	 * @param n number of slots to claim
	 * @return sequence number of the highest slot claimed
	 * @see RingBufferProducer#next(int)
	 */
	abstract public long next(int n);

	/**
	 * Publish the specified sequence.  This action marks this particular message as being available to be read.
	 * @param sequence the sequence to publish.
	 */
	abstract public void publish(long sequence);

	/**
	 * Publish the specified sequences.  This action marks these particular messages as being available to be read.
	 * @param lo the lowest sequence number to be published
	 * @param hi the highest sequence number to be published
	 * @see RingBufferProducer#next(int)
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
		return "RingBuffer{pending:" + getPending() + ", size:" + getCapacity() + ", cursor:" + get() + ", " +
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
	 * <p>This method will not block if there is not space available in the ring buffer, instead it will throw an {@link
	 * Exceptions.InsufficientCapacityException}.
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
		return buffer.getCursor() == pollCursor.get();
	}

	@Override
	public Iterator<T> iterator() {
		return new QueueSupplier.QueueIterator<>(this);
	}

	@Override
	final public T peek() {
		long current = buffer.getCursor();
		long cachedSequence = pollCursor.get() + 1L;

		if (cachedSequence <= current) {
			return buffer.get(cachedSequence).value;
		}
		return null;
	}

	@Override
	final public T poll() {
		long current = buffer.getCursor();
		long cachedSequence = pollCursor.get() + 1L;

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

	final public int size() {
		return (int) (buffer.getCursor() - pollCursor.get());
	}
	@Override
	@SuppressWarnings("unchecked")
	final public T[] toArray() {
		return toArray((T[]) new Object[(int) buffer.getCapacity()]);
	}

	@Override
	@SuppressWarnings("unchecked")
	final public <E> E[] toArray(E[] a) {

		final long cursor = buffer.getCursor();
		long s = pollCursor.get() + 1L;
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

final class Wrapped<E> implements Sequence, Introspectable, Producer {

	public final E        delegate;
	public final Sequence sequence;

	public Wrapped(E delegate, Sequence sequence) {
		this.delegate = delegate;
		this.sequence = sequence;
	}

	@Override
	public long get() {
		return sequence.get();
	}

	@Override
	public Object downstream() {
		return delegate;
	}

	@Override
	public void set(long value) {
		sequence.set(value);
	}

	@Override
	public void setVolatile(long value) {
		sequence.setVolatile(value);
	}

	@Override
	public boolean compareAndSet(long expectedValue, long newValue) {
		return sequence.compareAndSet(expectedValue, newValue);
	}

	@Override
	public long incrementAndGet() {
		return sequence.incrementAndGet();
	}

	@Override
	public long addAndGet(long increment) {
		return sequence.addAndGet(increment);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		Wrapped<?> wrapped = (Wrapped<?>) o;

		return sequence.equals(wrapped.sequence);

	}

	@Override
	public int hashCode() {
		return sequence.hashCode();
	}

	@Override
	public int getMode() {
		return TRACE_ONLY | INNER;
	}

	@Override
	public String getName() {
		return Wrapped.class.getSimpleName();
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

	final RingBuffer<?> ringBuffer;

	public RequestTask(Subscription upstream,
			Runnable stopCondition,
			Consumer<Long> postWaitCallback,
			LongSupplier readCount,
			WaitStrategy waitStrategy,
			Subscriber<?> errorSubscriber,
			RingBuffer r) {
		this.waitStrategy = waitStrategy;
		this.readCount = readCount;
		this.postWaitCallback = postWaitCallback;
		this.errorSubscriber = errorSubscriber;
		this.upstream = upstream;
		this.spinObserver = stopCondition;
		this.ringBuffer = r;
	}

	@Override
	public void run() {
		final long bufferSize = ringBuffer.getCapacity();
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
		catch (Exceptions.AlertException e) {
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
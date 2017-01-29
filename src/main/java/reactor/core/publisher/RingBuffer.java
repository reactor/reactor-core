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

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import reactor.util.concurrent.QueueSupplier;
import reactor.util.concurrent.WaitStrategy;
import sun.misc.Unsafe;

import static java.util.Arrays.copyOf;

/**
 * Ring based store of reusable entries containing the data representing an event being exchanged between event producer
 * and ringbuffer consumers.
 * @param <E> implementation storing the data for sharing during exchange or parallel coordination of an event.
 *
 * This is an adaption of the original LMAX Disruptor RingBuffer code from
 * https://lmax-exchange.github.io/disruptor/.
 */
abstract class RingBuffer<E> implements LongSupplier {

	/**
	 * Set to -1 as sequence starting point
	 */
	static final long     INITIAL_CURSOR_VALUE = -1L;

	/**
	 * Create a new multiple producer RingBuffer with the specified wait strategy.
     * <p>See {@code MultiProducerRingBuffer}.
	 * @param <E> the element type
	 * @param factory used to create the events within the ring buffer.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @param waitStrategy used to determine how to wait for new elements to become available.
	 * @param spinObserver the Runnable to call on a spin loop wait
	 * @return the new RingBuffer instance
	 */
	static <E> RingBuffer<E> createMultiProducer(Supplier<E> factory,
			int bufferSize,
			WaitStrategy waitStrategy, Runnable spinObserver) {

		if (hasUnsafe()) {
			MultiProducerRingBuffer sequencer = new MultiProducerRingBuffer(bufferSize, waitStrategy, spinObserver);

			return new UnsafeRingBuffer<>(factory, sequencer);
		}
		else {
			throw new IllegalStateException("This JVM does not support sun.misc.Unsafe");
		}
	}

	/**
	 * Create a new single producer RingBuffer with the specified wait strategy.
     * <p>See {@code MultiProducerRingBuffer}.
	 * @param <E> the element type
	 * @param factory used to create the events within the ring buffer.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @param waitStrategy used to determine how to wait for new elements to become available.
	 * @return the new RingBuffer instance
	 */
	static <E> RingBuffer<E> createSingleProducer(Supplier<E> factory,
			int bufferSize,
			WaitStrategy waitStrategy) {
		return createSingleProducer(factory, bufferSize, waitStrategy, null);
	}

	/**
	 * Create a new single producer RingBuffer with the specified wait strategy.
     * <p>See {@code MultiProducerRingBuffer}.
     * @param <E> the element type
	 * @param factory used to create the events within the ring buffer.
	 * @param bufferSize number of elements to create within the ring buffer.
	 * @param waitStrategy used to determine how to wait for new elements to become available.
	 * @param spinObserver called each time the next claim is spinning and waiting for a slot
     * @return the new RingBuffer instance
	 */
	static <E> RingBuffer<E> createSingleProducer(Supplier<E> factory,
			int bufferSize,
			WaitStrategy waitStrategy,
			Runnable spinObserver) {
		SingleProducerSequencer sequencer = new SingleProducerSequencer(bufferSize, waitStrategy, spinObserver);

		if (hasUnsafe()) {
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
	static long getMinimumSequence(final Sequence[] sequences, long minimum) {
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
	static long getMinimumSequence(Sequence excludeSequence, final Sequence[] sequences, long minimum) {
		for (int i = 0, n = sequences.length; i < n; i++) {
			if (excludeSequence == null || sequences[i] != excludeSequence) {
				long value = sequences[i].getAsLong();
				minimum = Math.min(minimum, value);
			}
		}

		return minimum;
	}

	/**
	 * Return the {@code sun.misc.Unsafe} instance if found on the classpath and can be used for acclerated
	 * direct memory access.
	 *
	 * @param <T> the Unsafe type
	 * @return the Unsafe instance
	 */
	@SuppressWarnings("unchecked")
	static <T> T getUnsafe() {
		return (T) UnsafeSupport.getUnsafe();
	}

	/**
	 * Calculate the log base 2 of the supplied integer, essentially reports the location of the highest bit.
	 *
	 * @param i Value to calculate log2 for.
	 *
	 * @return The log2 value
	 */
	static int log2(int i) {
		int r = 0;
		while ((i >>= 1) != 0) {
			++r;
		}
		return r;
	}

	/**
	 * @param init the initial value
	 *
	 * @return a safe or unsafe sequence set to the passed init value
	 */
	static Sequence newSequence(long init) {
		if (hasUnsafe()) {
			return new UnsafeSequence(init);
		}
		else {
			return new AtomicSequence(init);
		}
    }

	/**
	 * Add the specified gating sequence to this instance of the Disruptor.  It will safely and atomically be added to
	 * the list of gating sequences and not RESET to the current ringbuffer cursor.
	 * @param gatingSequence The sequences to add.
	 */
	abstract public void addGatingSequence(Sequence gatingSequence);

	/**
	 * @return the fixed buffer size
	 */
	abstract public int bufferSize();

	/**
	 * <p>Get the event for a given sequence in the RingBuffer.</p>
	 *
	 * <p>This call has 2 uses.  Firstly use this call when publishing to a ring buffer. After calling {@link
	 * RingBuffer#next()} use this call to get hold of the preallocated event to fill with data before calling {@link
	 * RingBuffer#publish(long)}.</p>
	 *
	 * <p>Secondly use this call when consuming data from the ring buffer.  After calling {@link
	 * Reader#waitFor(long)} call this method with any value greater than that your current consumer sequence
	 * and less than or equal to the value returned from the {@link Reader#waitFor(long)} method.</p>
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
     * See {@code MultiProducerRingBuffer}.
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
     * See {@code MultiProducerRingBuffer}.
     * See {@code SingleProducerSequencer}.
	 * @return the current cursor sequence
	 */
	abstract public Sequence getSequence();

	/**
	 *
	 * @return the current list of read cursors
	 */
	public Sequence[] getSequenceReceivers() {
		return getSequencer().getGatingSequences();
	}

	/**
	 * Create a new {@link Reader} to track
	 * which
	 * messages are available to be read
	 * from the ring buffer given a list of sequences to track.
	 * @return A sequence barrier that will track the ringbuffer.
	 * @see Reader
	 */
	abstract public Reader newReader();

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

	abstract RingBufferProducer getSequencer();/*


	/**
	 * Return {@code true} if {@code sun.misc.Unsafe} was found on the classpath and can be used for acclerated
	 * direct memory access.
	 * @return true if unsafe is present
	 */
	static boolean hasUnsafe() {
		return HAS_UNSAFE;
	}

	static boolean hasUnsafe0() {

		try {
			return !isAndroid() && UnsafeSupport.hasUnsafe();
		} catch (Throwable t) {
			// Probably failed to initialize Reactor0.
			return false;
		}
	}

	static boolean isAndroid() {
		try {
			Class.forName("android.app.Application", false, UnsafeSupport.getSystemClassLoader());
			return true;
		} catch (Exception e) {
			// Failed to load the class uniquely available in Android.
			return false;
		}
	}

	private static final boolean HAS_UNSAFE = hasUnsafe0();

	/**
	 * <p>Concurrent sequence class used for tracking the progress of
	 * the ring buffer and event processors.  Support a number
	 * of concurrent operations including CAS and order writes.
	 *
	 * <p>Also attempts to be more efficient with regards to false
	 * sharing by adding padding around the volatile field.
	 */
	public interface Sequence extends LongSupplier
	{
	    long INITIAL_VALUE = -1L;

	    /**
	     * Perform a volatile read of this sequence's value.
	     *
	     * @return The current value of the sequence.
	     */
	    @Override
	    long getAsLong();

	    /**
	     * Perform an ordered write of this sequence.  The intent is
	     * a Store/Store barrier between this write and any previous
	     * store.
	     *
	     * @param value The new value for the sequence.
	     */
	    void set(long value);

	    /**
	     * Performs a volatile write of this sequence.  The intent is
	     * a Store/Store barrier between this write and any previous
	     * write and a Store/Load barrier between this write and any
	     * subsequent volatile read.
	     *
	     * @param value The new value for the sequence.
	     */
	    void setVolatile(long value);

	    /**
	     * Perform a compare and set operation on the sequence.
	     *
	     * @param expectedValue The expected current value.
	     * @param newValue The value to update to.
	     * @return true if the operation succeeds, false otherwise.
	     */
	    boolean compareAndSet(long expectedValue, long newValue);

	    /**
	     * Atomically increment the sequence by one.
	     *
	     * @return The value after the increment
	     */
	    long incrementAndGet();

	    /**
	     * Atomically add the supplied value.
	     *
	     * @param increment The value to add to the sequence.
	     * @return The value after the increment.
	     */
	    long addAndGet(final long increment);
	}

	/**
	 * Used for Gating ringbuffer consumers on a cursor sequence and optional dependent ringbuffer consumer(s),
	 * using the given WaitStrategy.
	 */
	static final class Reader implements Runnable, LongSupplier {
	    private final WaitStrategy waitStrategy;
	    private volatile boolean alerted = false;
	    private final Sequence cursorSequence;
	    private final RingBufferProducer  sequenceProducer;

	    Reader(final RingBufferProducer sequenceProducer,
	                           final WaitStrategy waitStrategy,
	                           final Sequence cursorSequence) {
	        this.sequenceProducer = sequenceProducer;
	        this.waitStrategy = waitStrategy;
	        this.cursorSequence = cursorSequence;
	    }

	    /**
	     * Wait for the given sequence to be available for consumption.
	     *
	     * @param sequence to wait for
	     * @return the sequence up to which is available
	     * @throws InterruptedException if the thread needs awaking on a condition variable.
	     */
	    public long waitFor(final long sequence)
			    throws InterruptedException {
	        checkAlert();

	        long availableSequence = waitStrategy.waitFor(sequence, cursorSequence, this);

	        if (availableSequence < sequence) {
	            return availableSequence;
	        }

	        return sequenceProducer.getHighestPublishedSequence(sequence, availableSequence);
	    }

	    /**
	     * Wait for the given sequence to be available for consumption.
	     *
	     * @param consumer a spin observer to invoke when nothing is available to read
	     * @param sequence to wait for
	     * @return the sequence up to which is available
	     * @throws InterruptedException if the thread needs awaking on a condition variable.
	     */
	    public long waitFor(final long sequence, Runnable consumer)
			    throws InterruptedException {
	        checkAlert();

	        long availableSequence = waitStrategy.waitFor(sequence, cursorSequence, consumer);

	        if (availableSequence < sequence) {
	            return availableSequence;
	        }

	        return sequenceProducer.getHighestPublishedSequence(sequence, availableSequence);
	    }
	    /**
	         * The current alert status for the barrier.
	         *
	         * @return true if in alert otherwise false.
	         */
	    public boolean isAlerted() {
	        return alerted;
	    }

	    /**
	         * Alert the ringbuffer consumers of a status change and stay in this status until cleared.
	         */
	    public void alert() {
	        alerted = true;
	        waitStrategy.signalAllWhenBlocking();
	    }

	    /**
	         * Signal the ringbuffer consumers.
	         */
	    public void signal() {
	        waitStrategy.signalAllWhenBlocking();
	    }

	    /**
	         * Clear the current alert status.
	         */
	    public void clearAlert() {
	        alerted = false;
	    }

	    /**
	     * Check if an alert has been raised and throw an if it has.
	     */
	    public void checkAlert() {
	        if (alerted)
	        {
	            WaitStrategy.alert();
	        }
	    }

	    @Override
	    public long getAsLong() {
	        return cursorSequence.getAsLong();
	    }

	    @Override
	    public void run() {
	        checkAlert();
	    }
	}
}

//
abstract class UnsafeSupport {
	static {
		ByteBuffer direct = ByteBuffer.allocateDirect(1);
		Field addressField;
		try {
			addressField = Buffer.class.getDeclaredField("address");
			addressField.setAccessible(true);
			if (addressField.getLong(ByteBuffer.allocate(1)) != 0) {
				// A heap buffer must have 0 address.
				addressField = null;
			} else {
				if (addressField.getLong(direct) == 0) {
					// A direct buffer must have non-zero address.
					addressField = null;
				}
			}
		} catch (Throwable t) {
			// Failed to access the address field.
			addressField = null;
		}
		Unsafe unsafe;
		if (addressField != null) {
			try {
				Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
				unsafeField.setAccessible(true);
				unsafe = (Unsafe) unsafeField.get(null);

				// Ensure the unsafe supports all necessary methods to work around the mistake in the latest OpenJDK.
				// https://github.com/netty/netty/issues/1061
				// http://www.mail-archive.com/jdk6-dev@openjdk.java.net/msg00698.html
				if (unsafe != null) {
					unsafe.getClass().getDeclaredMethod(
							"copyMemory", Object.class, long.class, Object.class, long.class, long.class);
				}
			} catch (Throwable cause) {
				// Unsafe.copyMemory(Object, long, Object, long, long) unavailable.
				unsafe = null;
			}
		} else {
			// If we cannot access the address of a direct buffer, there's no point of using unsafe.
			// Let's just pretend unsafe is unavailable for overall simplicity.
			unsafe = null;
		}

		UNSAFE = unsafe;
	}

	static ClassLoader getSystemClassLoader() {
		if (System.getSecurityManager() == null) {
			return ClassLoader.getSystemClassLoader();
		} else {
			return AccessController.doPrivileged((PrivilegedAction<ClassLoader>) ClassLoader::getSystemClassLoader);
		}
	}

	static Unsafe getUnsafe(){
		return UNSAFE;
	}

	static boolean hasUnsafe() {
		return UNSAFE != null;
	}


	UnsafeSupport() {
	}

	private static final Unsafe UNSAFE;
}
/**
 * Base class for the various sequencer types (single/multi).  Provides common functionality like the management of
 * gating sequences (add/remove) and ownership of the current cursor.
 */
abstract class RingBufferProducer {

	static final AtomicReferenceFieldUpdater<RingBufferProducer, RingBuffer.Sequence[]>
			SEQUENCE_UPDATER = AtomicReferenceFieldUpdater.newUpdater(RingBufferProducer.class, RingBuffer.Sequence[].class,
			"gatingSequences");

	final Runnable     spinObserver;
	final int          bufferSize;
	final WaitStrategy waitStrategy;
	final    RingBuffer.Sequence   cursor          = RingBuffer.newSequence(RingBuffer.INITIAL_CURSOR_VALUE);
	volatile RingBuffer.Sequence[] gatingSequences = new RingBuffer.Sequence[0];

	/**
	 * Create with the specified buffer size and wait strategy.
	 *
	 * @param bufferSize The total number of entries, must be a positive power of 2.
	 * @param waitStrategy The {@link WaitStrategy} to use.
	 * @param spinObserver an iteration observer
	 */
	public RingBufferProducer(int bufferSize, WaitStrategy waitStrategy, Runnable spinObserver) {
		if (bufferSize < 1) {
			throw new IllegalArgumentException("bufferSize must not be less than 1");
		}

		this.spinObserver = spinObserver;
		this.bufferSize = bufferSize;
		this.waitStrategy = waitStrategy;
	}

	/**
	 * Get the current cursor value.
	 *
	 * @return current cursor value
	 */
	public final long getCursor() {
		return cursor.getAsLong();
	}

	/**
	 * Get the current cursor value.
	 *
	 * @return current cursor value
	 */
	public final RingBuffer.Sequence getSequence() {
		return cursor;
	}

	/**
	 * The capacity of the data structure to hold entries.
	 *
	 * @return the size of the RingBuffer.
	 */
	public final int getBufferSize() {
		return bufferSize;
	}

	/**
	 * Add the specified gating sequences to this instance of the Disruptor.  They will
	 * safely and atomically added to the list of gating sequences.
	 *
	 * @param gatingSequence The sequences to add.
	 */
	public final void addGatingSequence(RingBuffer.Sequence gatingSequence) {
		SequenceGroups.addSequence(this, SEQUENCE_UPDATER, gatingSequence);
	}

	/**
	 * Remove the specified sequence from this sequencer.
	 *
	 * @param sequence to be removed.
	 * @return <tt>true</tt> if this sequence was found, <tt>false</tt> otherwise.
	 */
	public boolean removeGatingSequence(RingBuffer.Sequence sequence) {
		return SequenceGroups.removeSequence(this, SEQUENCE_UPDATER, sequence);
	}

	/**
	 * Get the minimum sequence value from all of the gating sequences
	 * added to this ringBuffer.
	 *
	 * @param excludeSequence to exclude from search
	 * @return The minimum gating sequence or the cursor sequence if
	 * no sequences have been added.
	 */
	public long getMinimumSequence(RingBuffer.Sequence excludeSequence) {
		return RingBuffer.getMinimumSequence(excludeSequence, gatingSequences, cursor.getAsLong());
	}

	/**
	 * Create a new {@link RingBuffer.Reader} to be used by an EventProcessor to track which messages
	 * are available to be read from the ring buffer
	 *
	 * @see RingBuffer.Reader
	 * @return A sequence barrier that will track the specified sequences.
	 */
	public RingBuffer.Reader newBarrier() {
		return new RingBuffer.Reader(this, waitStrategy, cursor);
	}

	/**
	 * Confirms if a sequence is published and the event is available for use; non-blocking.
	 *
	 * @param sequence of the buffer to check
	 * @return true if the sequence is available for use, false if not
	 */
	public abstract boolean isAvailable(long sequence);

	/**
	 * Get the highest sequence number that can be safely read from the ring buffer.  Depending
	 * on the implementation of the Sequencer this call may need to scan a number of values
	 * in the Sequencer.  The scan will range from nextSequence to availableSequence.  If
	 * there are no available values <code>&gt;= nextSequence</code> the return value will be
	 * <code>nextSequence - 1</code>.  To work correctly a consumer should pass a value that
	 * it 1 higher than the last sequence that was successfully processed.
	 *
	 * @param nextSequence The sequence to start scanning from.
	 * @param availableSequence The sequence to scan to.
	 * @return The highest value that can be safely read, will be at least <code>nextSequence - 1</code>.
	 */
	public abstract long getHighestPublishedSequence(long nextSequence, long availableSequence);

		/**
	 * Get the remaining capacity for this sequencer.
	 * @return The number of slots remaining.
	 */
	public abstract long remainingCapacity();

	/**
	 * Get the pending capacity for this sequencer.
	 * @return The number of slots pending consuming.
	 */
	public abstract long getPending();

	/**
	 * Claim the next event in sequence for publishing.
	 * @return the claimed sequence value
	 */
	public abstract long next();

	/**
	 * Claim the next n events in sequence for publishing.  This is for batch event producing.  Using batch producing
	 * requires a little care and some math.
	 * <pre>
	 * int n = 10;
	 * long hi = sequencer.next(n);
	 * long lo = hi - (n - 1);
	 * for (long sequence = lo; sequence &lt;= hi; sequence++) {
	 *     // Do work.
	 * }
	 * sequencer.publish(lo, hi);
	 * </pre>
	 *
	 * @param n the number of sequences to claim
	 * @return the highest claimed sequence value
	 */
	public abstract long next(int n);

	/**
	 * Publishes a sequence. Call when the event has been filled.
	 *
	 * @param sequence the sequence number to be published
	 */
	public abstract void publish(long sequence);

	/**
	 * Batch publish sequences.  Called when all of the events have been filled.
	 *
	 * @param lo first sequence number to publish
	 * @param hi last sequence number to publish
	 */
	public abstract void publish(long lo, long hi);

	/**
	 *
	 * @return the gating sequences array
	 */
	public RingBuffer.Sequence[] getGatingSequences() {
		return gatingSequences;
	}
}

/**
 * Provides static methods for managing a {@link RingBuffer.Sequence} object.
 */
final class SequenceGroups {

	static <T> void addSequence(final T holder,
			final AtomicReferenceFieldUpdater<T, RingBuffer.Sequence[]> updater,
			final RingBuffer.Sequence sequence) {

		RingBuffer.Sequence[] updatedSequences;
		RingBuffer.Sequence[] currentSequences;

		do {
			currentSequences = updater.get(holder);
			updatedSequences = copyOf(currentSequences, currentSequences.length + 1);

			updatedSequences[currentSequences.length] = sequence;
		}
		while (!updater.compareAndSet(holder, currentSequences, updatedSequences));
	}

	static <T> boolean removeSequence(final T holder,
			final AtomicReferenceFieldUpdater<T, RingBuffer.Sequence[]> sequenceUpdater,
			final RingBuffer.Sequence sequence) {
		int numToRemove;
		RingBuffer.Sequence[] oldSequences;
		RingBuffer.Sequence[] newSequences;

		do {
			oldSequences = sequenceUpdater.get(holder);

			numToRemove = countMatching(oldSequences, sequence);

			if (0 == numToRemove) {
				break;
			}

			final int oldSize = oldSequences.length;
			newSequences = new RingBuffer.Sequence[oldSize - numToRemove];

			for (int i = 0, pos = 0; i < oldSize; i++) {
				final RingBuffer.Sequence testSequence = oldSequences[i];
				if (sequence != testSequence) {
					newSequences[pos++] = testSequence;
				}
			}
		}
		while (!sequenceUpdater.compareAndSet(holder, oldSequences, newSequences));

		return numToRemove != 0;
	}

	private static <T> int countMatching(T[] values, final T toMatch) {
		int numToRemove = 0;
		for (T value : values) {
			if (value == toMatch) // Specifically uses identity
			{
				numToRemove++;
			}
		}
		return numToRemove;
	}
}

abstract class SingleProducerSequencerPad extends RingBufferProducer
{
	protected long p1, p2, p3, p4, p5, p6, p7;
	public SingleProducerSequencerPad(int bufferSize, WaitStrategy waitStrategy, Runnable spinObserver)
	{
		super(bufferSize, waitStrategy, spinObserver);
	}
}

abstract class SingleProducerSequencerFields extends SingleProducerSequencerPad
{
	public SingleProducerSequencerFields(int bufferSize, WaitStrategy waitStrategy, Runnable spinObserver)
	{
		super(bufferSize, waitStrategy, spinObserver);
	}

	/** Set to -1 as sequence starting point */
	protected long nextValue = RingBuffer.Sequence.INITIAL_VALUE;
	protected long cachedValue = RingBuffer.Sequence.INITIAL_VALUE;
}

/**
 * <p>Coordinator for claiming sequences for access to a data structure while tracking dependent {@link RingBuffer.Sequence}s.
 * Not safe for use from multiple threads as it does not implement any barriers.</p>
 *
 * <p>Note on {@code RingBufferProducer.getCursor()}:  With this sequencer the cursor value is updated after the call
 * to {@code RingBufferProducer.publish(long)} is made.
 */

final class SingleProducerSequencer extends SingleProducerSequencerFields {
	protected long p1, p2, p3, p4, p5, p6, p7;

	/**
	 * Construct a Sequencer with the selected wait strategy and buffer size.
	 *
	 * @param bufferSize the size of the buffer that this will sequence over.
	 * @param waitStrategy for those waiting on sequences.
	 * @param spinObserver the runnable to call on a spin-wait
	 */
	public SingleProducerSequencer(int bufferSize, final WaitStrategy waitStrategy, Runnable spinObserver) {
		super(bufferSize, waitStrategy, spinObserver);
	}

	/**
	 * See {@code RingBufferProducer.next()}.
	 */
	@Override
	public long next() {
		return next(1);
	}

	/**
	 * See {@code RingBufferProducer.next(int)}.
	 */
	@Override
	public long next(int n) {
		if (n < 1)
		{
			throw new IllegalArgumentException("n must be > 0");
		}

		long nextValue = this.nextValue;

		long nextSequence = nextValue + n;
		long wrapPoint = nextSequence - bufferSize;
		long cachedGatingSequence = this.cachedValue;

		if (wrapPoint > cachedGatingSequence || cachedGatingSequence > nextValue)
		{
			long minSequence;
			while (wrapPoint > (minSequence = RingBuffer.getMinimumSequence(gatingSequences, nextValue)))
			{
				if(spinObserver != null) {
					spinObserver.run();
				}
				LockSupport.parkNanos(1L); // TODO: Use waitStrategy to spin?
			}

			this.cachedValue = minSequence;
		}

		this.nextValue = nextSequence;

		return nextSequence;
	}

	/**
	 * See {@code RingBufferProducer.remainingCapacity()}.
	 */
	@Override
	public long remainingCapacity() {
		return getBufferSize() - getPending();
	}

	/**
	 * See {@code RingBufferProducer.getPending()}.
	 */
	@Override
	public long getPending() {
		long nextValue = this.nextValue;

		long consumed = RingBuffer.getMinimumSequence(gatingSequences, nextValue);
		long produced = nextValue;
		return produced - consumed;
	}

	/**
	 * See {@code RingBufferProducer.publish(long)}.
	 */
	@Override
	public void publish(long sequence) {
		cursor.set(sequence);
		waitStrategy.signalAllWhenBlocking();
	}

	/**
	 * See {@code RingBufferProducer.publish(long, long)}.
	 */
	@Override
	public void publish(long lo, long hi) {
		publish(hi);
	}

	/**
	 * See {@code RingBufferProducer.isAvailable(long)}.
	 */
	@Override
	public boolean isAvailable(long sequence) {
		return sequence <= cursor.getAsLong();
	}

	@Override
	public long getHighestPublishedSequence(long lowerBound, long availableSequence) {
		return availableSequence;
	}
}

abstract class NotFunRingBufferFields<E> extends RingBuffer<E>
{
	private final   long               indexMask;
	private final   Object[]           entries;
	protected final int                bufferSize;
	protected final RingBufferProducer sequenceProducer;

	NotFunRingBufferFields(Supplier<E> eventFactory,
			RingBufferProducer sequenceProducer)
	{
		this.sequenceProducer = sequenceProducer;
		this.bufferSize = sequenceProducer.getBufferSize();

		if (bufferSize < 1)
		{
			throw new IllegalArgumentException("bufferSize must not be less than 1");
		}

		this.indexMask = bufferSize - 1;
		this.entries   = new Object[sequenceProducer.getBufferSize()];
		fill(eventFactory);
	}

	private void fill(Supplier<E> eventFactory)
	{
		for (int i = 0; i < bufferSize; i++)
		{
			entries[i] = eventFactory.get();
		}
	}

	@SuppressWarnings("unchecked")
	protected final E elementAt(long sequence)
	{
		return (E) entries[(int) (sequence & indexMask)];
	}
}

/**
 * Ring based store of reusable entries containing the data representing
 * an event being exchanged between event producer and ringbuffer consumers.
 *
 * @param <E> implementation storing the data for sharing during exchange or parallel coordination of an event.
 */
final class NotFunRingBuffer<E> extends NotFunRingBufferFields<E>
{
	/**
	 * Construct a RingBuffer with the full option set.
	 *
	 * @param eventFactory to newInstance entries for filling the RingBuffer
	 * @param sequenceProducer sequencer to handle the ordering of events moving through the RingBuffer.
	 * @throws IllegalArgumentException if bufferSize is less than 1 or not a power of 2
	 */
	NotFunRingBuffer(Supplier<E> eventFactory,
			RingBufferProducer sequenceProducer)
	{
		super(eventFactory, sequenceProducer);
	}

	@Override
	public E get(long sequence)
	{
		return elementAt(sequence);
	}

	@Override
	public long next()
	{
		return sequenceProducer.next();
	}

	@Override
	public long next(int n)
	{
		return sequenceProducer.next(n);
	}

	@Override
	public void addGatingSequence(Sequence gatingSequence)
	{
		sequenceProducer.addGatingSequence(gatingSequence);
	}

	@Override
	public long getMinimumGatingSequence()
	{
		return getMinimumGatingSequence(null);
	}

	@Override
	public long getMinimumGatingSequence(Sequence sequence)
	{
		return sequenceProducer.getMinimumSequence(sequence);
	}

	@Override
	public boolean removeGatingSequence(Sequence sequence)
	{
		return sequenceProducer.removeGatingSequence(sequence);
	}

	@Override
	public Reader newReader()
	{
		return sequenceProducer.newBarrier();
	}

	@Override
	public long getCursor()
	{
		return sequenceProducer.getCursor();
	}

	@Override
	public Sequence getSequence()
	{
		return sequenceProducer.getSequence();
	}

	@Override
	public int bufferSize()
	{
		return bufferSize;
	}

	@Override
	public void publish(long sequence)
	{
		sequenceProducer.publish(sequence);
	}

	@Override
	public void publish(long lo, long hi)
	{
		sequenceProducer.publish(lo, hi);
	}

	@Override
	public long remainingCapacity()
	{
		return sequenceProducer.remainingCapacity();
	}

	@Override
	public int getPending() {
		return (int)sequenceProducer.getPending();
	}

	@Override
	public RingBufferProducer getSequencer() {
		return sequenceProducer;
	}
}
final class AtomicSequence extends RhsPadding implements LongSupplier, RingBuffer.Sequence
{

	private static final AtomicLongFieldUpdater<Value> UPDATER =
			AtomicLongFieldUpdater.newUpdater(Value.class, "value");

	/**
	 * Create a sequence with a specified initial value.
	 *
	 * @param initialValue The initial value for this sequence.
	 */
	AtomicSequence(final long initialValue)
	{
		UPDATER.lazySet(this, initialValue);
	}

	@Override
	public long getAsLong()
	{
		return value;
	}

	@Override
	public void set(final long value)
	{
		UPDATER.set(this, value);
	}

	@Override
	public void setVolatile(final long value)
	{
		UPDATER.lazySet(this, value);
	}

	@Override
	public boolean compareAndSet(final long expectedValue, final long newValue)
	{
		return UPDATER.compareAndSet(this, expectedValue, newValue);
	}

	@Override
	public long incrementAndGet()
	{
		return addAndGet(1L);
	}

	@Override
	public long addAndGet(final long increment)
	{
		long currentValue;
		long newValue;

		do
		{
			currentValue = getAsLong();
			newValue = currentValue + increment;
		}
		while (!compareAndSet(currentValue, newValue));

		return newValue;
	}

	@Override
	public String toString()
	{
		return Long.toString(getAsLong());
	}
}
abstract class RingBufferPad<E> extends RingBuffer<E>
{
	protected long p1, p2, p3, p4, p5, p6, p7;
}

abstract class RingBufferFields<E> extends RingBufferPad<E>
{
	private static final int  BUFFER_PAD;
	private static final long REF_ARRAY_BASE;
	private static final int  REF_ELEMENT_SHIFT;
	private static final Unsafe UNSAFE = RingBuffer.getUnsafe();

	static {
		final int scale = UNSAFE.arrayIndexScale(Object[].class);
		if (4 == scale) {
			REF_ELEMENT_SHIFT = 2;
		} else if (8 == scale) {
			REF_ELEMENT_SHIFT = 3;
		} else {
			throw new IllegalStateException("Unknown pointer size");
		}
		BUFFER_PAD = 128 / scale;
		// Including the buffer pad in the array base offset
		REF_ARRAY_BASE = UNSAFE.arrayBaseOffset(Object[].class) + (BUFFER_PAD << REF_ELEMENT_SHIFT);
	}

	private final   long               indexMask;
	private final   Object[]           entries;
	protected final int                bufferSize;
	protected final RingBufferProducer sequenceProducer;

	RingBufferFields(Supplier<E> eventFactory,
			RingBufferProducer sequenceProducer) {
		this.sequenceProducer = sequenceProducer;
		this.bufferSize = sequenceProducer.getBufferSize();

		if (bufferSize < 1) {
			throw new IllegalArgumentException("bufferSize must not be less than 1");
		}
		if (!QueueSupplier.isPowerOfTwo(bufferSize))
		{
			throw new IllegalArgumentException("bufferSize must be a power of 2");
		}

		this.indexMask = bufferSize - 1;
		this.entries   = new Object[sequenceProducer.getBufferSize() + 2 * BUFFER_PAD];
		fill(eventFactory);
	}

	private void fill(Supplier<E> eventFactory)
	{
		for (int i = 0; i < bufferSize; i++)
		{
			entries[BUFFER_PAD + i] = eventFactory.get();
		}
	}

	@SuppressWarnings("unchecked")
	protected final E elementAt(long sequence)
	{
		return (E) UNSAFE.getObject(entries, REF_ARRAY_BASE + ((sequence & indexMask) << REF_ELEMENT_SHIFT));
	}
}

/**
 * Ring based store of reusable entries containing the data representing
 * an event being exchanged between event producer and ringbuffer consumers.
 *
 * @param <E> implementation storing the data for sharing during exchange or parallel coordination of an event.
 */
final class UnsafeRingBuffer<E> extends RingBufferFields<E>
{
	protected long p1, p2, p3, p4, p5, p6, p7;

	/**
	 * Construct a RingBuffer with the full option set.
	 *
	 * @param eventFactory to newInstance entries for filling the RingBuffer
	 * @param sequenceProducer sequencer to handle the ordering of events moving through the RingBuffer.
	 * @throws IllegalArgumentException if bufferSize is less than 1 or not a power of 2
	 */
	UnsafeRingBuffer(Supplier<E> eventFactory,
			RingBufferProducer sequenceProducer)
	{
		super(eventFactory, sequenceProducer);
	}

	@Override
	public E get(long sequence)
	{
		return elementAt(sequence);
	}

	@Override
	public long next()
	{
		return sequenceProducer.next();
	}

	@Override
	public long next(int n)
	{
		return sequenceProducer.next(n);
	}

	@Override
	public void addGatingSequence(Sequence gatingSequence)
	{
		sequenceProducer.addGatingSequence(gatingSequence);
	}

	@Override
	public long getMinimumGatingSequence()
	{
		return getMinimumGatingSequence(null);
	}

	@Override
	public long getMinimumGatingSequence(Sequence sequence)
	{
		return sequenceProducer.getMinimumSequence(sequence);
	}

	@Override
	public boolean removeGatingSequence(Sequence sequence)
	{
		return sequenceProducer.removeGatingSequence(sequence);
	}

	@Override
	public Reader newReader()
	{
		return sequenceProducer.newBarrier();
	}

	@Override
	public long getCursor()
	{
		return sequenceProducer.getCursor();
	}

	@Override
	public Sequence getSequence()
	{
		return sequenceProducer.getSequence();
	}

	@Override
	public int bufferSize()
	{
		return bufferSize;
	}

	@Override
	public void publish(long sequence)
	{
		sequenceProducer.publish(sequence);
	}

	@Override
	public void publish(long lo, long hi)
	{
		sequenceProducer.publish(lo, hi);
	}

	@Override
	public int getPending() {
		return (int)sequenceProducer.getPending();
	}

	@Override
	public long remainingCapacity()
	{
		return sequenceProducer.remainingCapacity();
	}

	@Override
	public RingBufferProducer getSequencer() {
		return sequenceProducer;
	}


}

class LhsPadding
{
	protected long p1, p2, p3, p4, p5, p6, p7;
}

class Value extends LhsPadding
{
	protected volatile long value;
}

class RhsPadding extends Value
{
	protected long p9, p10, p11, p12, p13, p14, p15;
}

/**
 * <p>Concurrent sequence class used for tracking the progress of
 * the ring buffer and event processors.  Support a number
 * of concurrent operations including CAS and order writes.
 *
 * <p>Also attempts to be more efficient with regards to false
 * sharing by adding padding around the volatile field.
 */
final class UnsafeSequence extends RhsPadding implements RingBuffer.Sequence, LongSupplier
{
	private static final Unsafe UNSAFE;
	private static final long VALUE_OFFSET;

	static
	{
		UNSAFE = RingBuffer.getUnsafe();
		try
		{
			VALUE_OFFSET = UNSAFE.objectFieldOffset(Value.class.getDeclaredField("value"));
		}
		catch (final Exception e)
		{
			throw new RuntimeException(e);
		}
	}

	/**
	 * Create a sequence with a specified initial value.
	 *
	 * @param initialValue The initial value for this sequence.
	 */
	UnsafeSequence(final long initialValue)
	{
		UNSAFE.putOrderedLong(this, VALUE_OFFSET, initialValue);
	}

	@Override
	public long getAsLong()
	{
		return value;
	}

	@Override
	public void set(final long value)
	{
		UNSAFE.putOrderedLong(this, VALUE_OFFSET, value);
	}

	@Override
	public void setVolatile(final long value)
	{
		UNSAFE.putLongVolatile(this, VALUE_OFFSET, value);
	}

	@Override
	public boolean compareAndSet(final long expectedValue, final long newValue)
	{
		return UNSAFE.compareAndSwapLong(this, VALUE_OFFSET, expectedValue, newValue);
	}

	@Override
	public long incrementAndGet()
	{
		return addAndGet(1L);
	}

	@Override
	public long addAndGet(final long increment)
	{
		long currentValue;
		long newValue;

		do
		{
			currentValue = getAsLong();
			newValue = currentValue + increment;
		}
		while (!compareAndSet(currentValue, newValue));

		return newValue;
	}

	@Override
	public String toString()
	{
		return Long.toString(getAsLong());
	}

}
/**
 * <p>Coordinator for claiming sequences for access to a data structure while tracking dependent {@link RingBuffer.Sequence}s.
 * Suitable for use for sequencing across multiple publisher threads.</p>
 *
 * <p>
 * <p>Note on {@code RingBufferProducer.getCursor()}:  With this sequencer the cursor value is updated after the call
 * to {@code RingBufferProducer.next()}, to determine the highest available sequence that can be read, then
 * {@code RingBufferProducer.getHighestPublishedSequence(long, long)} should be used.
 */
final class MultiProducerRingBuffer extends RingBufferProducer
{
	private static final Unsafe UNSAFE = RingBuffer.getUnsafe();
	private static final long   BASE   = UNSAFE.arrayBaseOffset(int[].class);
	private static final long   SCALE  = UNSAFE.arrayIndexScale(int[].class);

	private final RingBuffer.Sequence gatingSequenceCache = new UnsafeSequence(RingBuffer.INITIAL_CURSOR_VALUE);

	// availableBuffer tracks the state of each ringbuffer slot
	// see below for more details on the approach
	private final int[] availableBuffer;
	private final int   indexMask;
	private final int   indexShift;

	/**
	 * Construct a Sequencer with the selected wait strategy and buffer size.
	 *
	 * @param bufferSize the size of the buffer that this will sequence over.
	 * @param waitStrategy for those waiting on sequences.
	 */
	MultiProducerRingBuffer(int bufferSize, final WaitStrategy waitStrategy, Runnable spinObserver) {
		super(bufferSize, waitStrategy, spinObserver);

		if (!QueueSupplier.isPowerOfTwo(bufferSize)) {
			throw new IllegalArgumentException("bufferSize must be a power of 2");
		}

		availableBuffer = new int[bufferSize];
		indexMask = bufferSize - 1;
		indexShift = RingBuffer.log2(bufferSize);
		initialiseAvailableBuffer();
	}

	/**
	 * See {@code RingBufferProducer.next()}.
	 */
	@Override
	public long next()
	{
		return next(1);
	}

	/**
	 * See {@code RingBufferProducer.next(int)}.
	 */
	@Override
	public long next(int n)
	{
		long current;
		long next;

		do
		{
			current = cursor.getAsLong();
			next = current + n;

			long wrapPoint = next - bufferSize;
			long cachedGatingSequence = gatingSequenceCache.getAsLong();

			if (wrapPoint > cachedGatingSequence || cachedGatingSequence > current)
			{
				long gatingSequence = RingBuffer.getMinimumSequence(gatingSequences, current);

				if (wrapPoint > gatingSequence)
				{
					if(spinObserver != null) {
						spinObserver.run();
					}
					LockSupport.parkNanos(1); // TODO, should we spin based on the wait strategy?
					continue;
				}

				gatingSequenceCache.set(gatingSequence);
			}
			else if (cursor.compareAndSet(current, next))
			{
				break;
			}
		}
		while (true);

		return next;
	}

	/**
	 * See {@code RingBufferProducer.remainingCapacity()}.
	 */
	@Override
	public long remainingCapacity()
	{
		return getBufferSize() - getPending();
	}
	/**
	 * See {@code RingBufferProducer.getPending()}.
	 */
	@Override
	public long getPending()
	{
		long consumed = RingBuffer.getMinimumSequence(gatingSequences, cursor.getAsLong());
		long produced = cursor.getAsLong();
		return produced - consumed;
	}

	private void initialiseAvailableBuffer()
	{
		for (int i = availableBuffer.length - 1; i != 0; i--)
		{
			setAvailableBufferValue(i, -1);
		}

		setAvailableBufferValue(0, -1);
	}

	/**
	 * See {@code RingBufferProducer.publish(long)}.
	 */
	@Override
	public void publish(final long sequence)
	{
		setAvailable(sequence);
		waitStrategy.signalAllWhenBlocking();
	}

	/**
	 * See {@code RingBufferProducer.publish(long, long)}.
	 */
	@Override
	public void publish(long lo, long hi)
	{
		for (long l = lo; l <= hi; l++)
		{
			setAvailable(l);
		}
		waitStrategy.signalAllWhenBlocking();
	}

	/**
	 * The below methods work on the availableBuffer flag.
	 *
	 * The prime reason is to avoid a shared sequence object between publisher threads.
	 * (Keeping single pointers tracking start and end would require coordination
	 * between the threads).
	 *
	 * --  Firstly we have the constraint that the delta between the cursor and minimum
	 * gating sequence will never be larger than the buffer size (the code in
	 * next/tryNext in the Sequence takes care of that).
	 * -- Given that; take the sequence value and mask off the lower portion of the
	 * sequence as the index into the buffer (indexMask). (aka modulo operator)
	 * -- The upper portion of the sequence becomes the value to check for availability.
	 * ie: it tells us how many times around the ring buffer we've been (aka division)
	 * -- Because we can't wrap without the gating sequences moving forward (i.e. the
	 * minimum gating sequence is effectively our last available position in the
	 * buffer), when we have new data and successfully claimed a slot we can simply
	 * write over the top.
	 */
	private void setAvailable(final long sequence)
	{
		setAvailableBufferValue(calculateIndex(sequence), calculateAvailabilityFlag(sequence));
	}

	private void setAvailableBufferValue(int index, int flag)
	{
		long bufferAddress = (index * SCALE) + BASE;
		UNSAFE.putOrderedInt(availableBuffer, bufferAddress, flag);
	}

	/**
	 * See {@code RingBufferProducer.isAvailable(long)}
	 */
	@Override
	public boolean isAvailable(long sequence)
	{
		int index = calculateIndex(sequence);
		int flag = calculateAvailabilityFlag(sequence);
		long bufferAddress = (index * SCALE) + BASE;
		return UNSAFE.getIntVolatile(availableBuffer, bufferAddress) == flag;
	}

	@Override
	public long getHighestPublishedSequence(long lowerBound, long availableSequence)
	{
		for (long sequence = lowerBound; sequence <= availableSequence; sequence++)
		{
			if (!isAvailable(sequence))
			{
				return sequence - 1;
			}
		}

		return availableSequence;
	}

	private int calculateAvailabilityFlag(final long sequence)
	{
		return (int) (sequence >>> indexShift);
	}

	private int calculateIndex(final long sequence)
	{
		return ((int) sequence) & indexMask;
	}
}
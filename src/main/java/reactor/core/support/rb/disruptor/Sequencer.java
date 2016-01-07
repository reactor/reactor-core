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
package reactor.core.support.rb.disruptor;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import reactor.core.error.InsufficientCapacityException;
import reactor.core.support.ReactiveState;
import reactor.core.support.WaitStrategy;
import reactor.core.support.internal.PlatformDependent;

import static java.util.Arrays.copyOf;

/**
 * Base class for the various sequencer types (single/multi).  Provides
 * common functionality like the management of gating sequences (add/remove) and
 * ownership of the current cursor.
 */
public abstract class Sequencer
{
    /** Set to -1 as sequence starting point */
    public static final  long                                               INITIAL_CURSOR_VALUE = -1L;
    private static final AtomicReferenceFieldUpdater<Sequencer, Sequence[]> SEQUENCE_UPDATER     =
      AtomicReferenceFieldUpdater.newUpdater(Sequencer.class, Sequence[].class, "gatingSequences");

    protected final Runnable spinObserver;
    protected final int            bufferSize;
    protected final WaitStrategy   waitStrategy;
    protected final    Sequence   cursor          = Sequencer.newSequence(Sequencer.INITIAL_CURSOR_VALUE);
    protected volatile Sequence[] gatingSequences = new Sequence[0];

	/**
     *
     * @param init
     * @return
     */
    public static Sequence newSequence(long init) {
        if (PlatformDependent.hasUnsafe()) {
            return new UnsafeSequence(init);
        } else {
            return new AtomicSequence(init);
        }
    }

	/**
	 *
     * @param init
     * @param delegate
     * @return
     */
    public static Sequence wrap(long init, Object delegate){
        if(ReactiveState.TRACEABLE_RING_BUFFER_PROCESSOR) {
            return wrap(newSequence(init), delegate);
        }
        else{
            return newSequence(init);
        }
    }

	/**
	 *
     * @param init
     * @param delegate
     * @param <E>
     * @return
     */
    public static <E> Wrapped<E> wrap(Sequence init, E delegate){
        return new Wrapped<>(delegate, init);
    }

	public static boolean isPowerOfTwo(final int x){
		return Integer.bitCount(x) == 1;
	}

	/**
	 * Calculate the next power of 2, greater than or equal to x.<p>
	 * From Hacker's Delight, Chapter 3, Harry S. Warren Jr.
	 *
	 * @param x Value to round up
	 * @return The next power of 2 from x inclusive
	 */
	public static int ceilingNextPowerOfTwo(final int x) {
		return 1 << (32 - Integer.numberOfLeadingZeros(x - 1));
	}

	/**
	 * Get the minimum sequence from an array of {@link Sequence}s.
	 *
	 * @param sequences to compare.
	 * @return the minimum sequence found or Long.MAX_VALUE if the array is empty.
	 */
	public static long getMinimumSequence(final Sequence[] sequences) {
		return getMinimumSequence(sequences, Long.MAX_VALUE);
	}

	/**
	 * Get the minimum sequence from an array of {@link Sequence}s.
	 *
	 * @param sequences to compare.
	 * @param minimum   an initial default minimum.  If the array is empty this value will be
	 *                  returned.
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
	 * @param minimum   an initial default minimum.  If the array is empty this value will be
	 *                  returned.
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
	 * Calculate the log base 2 of the supplied integer, essentially reports the location
	 * of the highest bit.
	 *
	 * @param i Value to calculate log2 for.
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
	 * Create with the specified buffer size and wait strategy.
	 *
	 * @param bufferSize The total number of entries, must be a positive power of 2.
	 * @param waitStrategy
	 * @param spinObserver
	 */
	public Sequencer(int bufferSize, WaitStrategy waitStrategy, Runnable spinObserver) {
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
        return cursor.get();
    }


    /**
     * Get the current cursor value.
     *
     * @return current cursor value
     */
    public final Sequence getSequence() {
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
         * @param gatingSequences The sequences to add.
         */
    public final void addGatingSequences(Sequence... gatingSequences)
    {
        SequenceGroups.addSequences(this, SEQUENCE_UPDATER, this, gatingSequences);
    }

    /**
     * Add the specified gating sequences to this instance of the Disruptor.  They will
     * safely and atomically added to the list of gating sequences.
     *
     * @param gatingSequence The sequences to add.
     */
    public final void addGatingSequence(Sequence gatingSequence)
    {
        SequenceGroups.addSequence(this, SEQUENCE_UPDATER, gatingSequence);
    }

    /**
         * Remove the specified sequence from this sequencer.
         *
         * @param sequence to be removed.
         * @return <tt>true</tt> if this sequence was found, <tt>false</tt> otherwise.
         */
    public boolean removeGatingSequence(Sequence sequence)
    {
        return SequenceGroups.removeSequence(this, SEQUENCE_UPDATER, sequence);
    }

    /**
         * Get the minimum sequence value from all of the gating sequences
         * added to this ringBuffer.
         *
         * @return The minimum gating sequence or the cursor sequence if
         * no sequences have been added.
         */
    public long getMinimumSequence(Sequence excludeSequence)
    {
        return getMinimumSequence(excludeSequence, gatingSequences, cursor.get());
    }

    /**
         * Create a new SequenceBarrier to be used by an EventProcessor to track which messages
         * are available to be read from the ring buffer
         *
         * @see SequenceBarrier
         * @return A sequence barrier that will track the specified sequences.
         */
    public SequenceBarrier newBarrier()
    {
        return new SequenceBarrier(this, waitStrategy, cursor);
    }

    /**
     * Claim a specific sequence.  Only used if initialising the ring buffer to
     * a specific value.
     *
     * @param sequence The sequence to initialise too.
     */
    public abstract void claim(long sequence);

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
     * @return Get the latest cached consumed value
     */
    public abstract long cachedRemainingCapacity();

    /**
     * Has the buffer got capacity to allocate another sequence.  This is a concurrent
     * method so the response should only be taken as an indication of available capacity.
     * @param requiredCapacity in the buffer
     * @return true if the buffer has the capacity to allocate the next sequence otherwise false.
     */
    public abstract boolean hasAvailableCapacity(final int requiredCapacity);

    /**
     * Get the remaining capacity for this sequencer.
     * @return The number of slots remaining.
     */
    public abstract long remainingCapacity();

    /**
     * Get the pending capacity for this sequencer.
     * @return The number of slots pending consuming.
     */
    public abstract long pending();

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
     * Attempt to claim the next event in sequence for publishing.  Will return the
     * number of the slot if there is at least <code>requiredCapacity</code> slots
     * available.
     * @return the claimed sequence value
     * @throws InsufficientCapacityException
     */
    public abstract long tryNext() throws InsufficientCapacityException;

    /**
     * Attempt to claim the next n events in sequence for publishing.  Will return the
     * highest numbered slot if there is at least <code>requiredCapacity</code> slots
     * available.  Have a look at {@link Sequencer#next()} for a description on how to
     * use this method.
     *
     * @param n the number of sequences to claim
     * @return the claimed sequence value
     * @throws InsufficientCapacityException
     */
    public abstract long tryNext(int n) throws InsufficientCapacityException;

    /**
     * Publishes a sequence. Call when the event has been filled.
     *
     * @param sequence
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
     * @return
     */
    public WaitStrategy getWaitStrategy() {
        return waitStrategy;
    }

	/**
     *
     * @return
     */
    public Sequence[] getGatingSequences() {
        return gatingSequences;
    }

    /**
	 *
     * @param <E>
     */
    public final static class Wrapped<E> implements Sequence, ReactiveState.Trace, ReactiveState.Downstream {
        public final E delegate;
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
    }

	/**
	 * Provides static methods for managing a {@link Sequence} object.
	 */
	static class SequenceGroups
	{
	    static <T> void addSequences(final T holder,
	                                 final AtomicReferenceFieldUpdater<T, Sequence[]> updater,
	                                 final Sequencer cursor,
	                                 final Sequence... sequencesToAdd)
	    {
	        long cursorSequence;
	        Sequence[] updatedSequences;
	        Sequence[] currentSequences;

	        do
	        {
	            currentSequences = updater.get(holder);
	            updatedSequences = copyOf(currentSequences, currentSequences.length + sequencesToAdd.length);
	            cursorSequence = cursor.getCursor();

	            int index = currentSequences.length;
	            for (Sequence sequence : sequencesToAdd)
	            {
	                sequence.set(cursorSequence);
	                updatedSequences[index++] = sequence;
	            }
	        }
	        while (!updater.compareAndSet(holder, currentSequences, updatedSequences));

	        cursorSequence = cursor.getCursor();
	        for (Sequence sequence : sequencesToAdd)
	        {
	            sequence.set(cursorSequence);
	        }
	    }

	    static <T> void addSequence(final T holder,
	            final AtomicReferenceFieldUpdater<T, Sequence[]> updater,
	            final Sequence sequence)
	    {

	        Sequence[] updatedSequences;
	        Sequence[] currentSequences;

	        do
	        {
	            currentSequences = updater.get(holder);
	            updatedSequences = copyOf(currentSequences, currentSequences.length + 1);

	            updatedSequences[currentSequences.length] = sequence;
	        }
	        while (!updater.compareAndSet(holder, currentSequences, updatedSequences));
	    }

	    static <T> boolean removeSequence(final T holder,
	                                      final AtomicReferenceFieldUpdater<T, Sequence[]> sequenceUpdater,
	                                      final Sequence sequence)
	    {
	        int numToRemove;
	        Sequence[] oldSequences;
	        Sequence[] newSequences;

	        do
	        {
	            oldSequences = sequenceUpdater.get(holder);

	            numToRemove = countMatching(oldSequences, sequence);

	            if (0 == numToRemove)
	            {
	                break;
	            }

	            final int oldSize = oldSequences.length;
	            newSequences = new Sequence[oldSize - numToRemove];

	            for (int i = 0, pos = 0; i < oldSize; i++)
	            {
	                final Sequence testSequence = oldSequences[i];
	                if (sequence != testSequence)
	                {
	                    newSequences[pos++] = testSequence;
	                }
	            }
	        }
	        while (!sequenceUpdater.compareAndSet(holder, oldSequences, newSequences));

	        return numToRemove != 0;
	    }

	    private static <T> int countMatching(T[] values, final T toMatch)
	    {
	        int numToRemove = 0;
	        for (T value : values)
	        {
	            if (value == toMatch) // Specifically uses identity
	            {
	                numToRemove++;
	            }
	        }
	        return numToRemove;
	    }
	}
}
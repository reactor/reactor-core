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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import reactor.util.Exceptions;
import sun.misc.Unsafe;


/**
 * Ring based store of reusable entries containing the data representing an event being exchanged between event producer
 * and ringbuffer consumers.
 * @param <E> implementation storing the data for sharing during exchange or parallel coordination of an event.
 */
public abstract class RingBuffer<E> implements LongSupplier {

	/**
	 * Set to -1 as sequence starting point
	 */
	public static final long     INITIAL_CURSOR_VALUE = -1L;

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

		if (hasUnsafe() && QueueSupplier.isPowerOfTwo(bufferSize)) {
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

		if (hasUnsafe() && QueueSupplier.isPowerOfTwo(bufferSize)) {
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
	 * Test if exception is alert
	 * @param t exception checked
	 * @return true if this is an alert signal
	 */
	public static boolean isAlert(Throwable t){
		return t == AlertException.INSTANCE;
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
	public static int log2(int i) {
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
	public static Sequence newSequence(long init) {
		if (hasUnsafe()) {
			return new UnsafeSequence(init);
		}
		else {
			return new AtomicSequence(init);
		}
    }

	/**
	 * Throw a signal singleton exception that can be checked against
	 * {@link #isAlert(Throwable)}
	 */
	public static void throwAlert() {
		throw AlertException.INSTANCE;
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
	 * RingBufferReader#waitFor(long)} call this method with any value greater than that your current consumer sequence
	 * and less than or equal to the value returned from the {@link RingBufferReader#waitFor(long)} method.</p>
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
	 *
	 * @return the current list of read cursors
	 */
	public Sequence[] getSequenceReceivers() {
		return getSequencer().getGatingSequences();
	}

	/**
	 * Create a new {@link RingBufferReader} to track
	 * which
	 * messages are available to be read
	 * from the ring buffer given a list of sequences to track.
	 * @return A sequence barrier that will track the ringbuffer.
	 * @see RingBufferReader
	 */
	abstract public RingBufferReader newReader();

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

	/**
	 * Return {@code true} if {@code sun.misc.Unsafe} was found on the classpath and can be used for acclerated
	 * direct memory access.
	 * @return true if unsafe is present
	 */
	static boolean hasUnsafe() {
		return HAS_UNSAFE;
	}

	static boolean hasUnsafe0() {

		if (isAndroid()) {
			return false;
		}

		try {
			return UnsafeSupport.hasUnsafe();
		} catch (Throwable t) {
			// Probably failed to initialize Reactor0.
			return false;
		}
	}

	static boolean isAndroid() {
		boolean android;
		try {
			Class.forName("android.app.Application", false, UnsafeSupport.getSystemClassLoader());
			android = true;
		} catch (Exception e) {
			// Failed to load the class uniquely available in Android.
			android = false;
		}

		return android;
	}

	private static final boolean HAS_UNSAFE = hasUnsafe0();
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
				try {
					if (unsafe != null) {
						unsafe.getClass().getDeclaredMethod(
								"copyMemory", Object.class, long.class, Object.class, long.class, long.class);
					}
				} catch (NoSuchMethodError | NoSuchMethodException t) {
					throw t;
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

	public static Unsafe getUnsafe(){
		return UNSAFE;
	}

	static boolean hasUnsafe() {
		return UNSAFE != null;
	}

	static <U, W> AtomicReferenceFieldUpdater<U, W> newAtomicReferenceFieldUpdater(
			Class<U> tclass, String fieldName) throws Exception {
		return new UnsafeAtomicReferenceFieldUpdater<>(tclass, fieldName);
	}

	UnsafeSupport() {
	}

	static final class UnsafeAtomicReferenceFieldUpdater<U, M> extends AtomicReferenceFieldUpdater<U, M> {
		private final long offset;

		UnsafeAtomicReferenceFieldUpdater(Class<U> tClass, String fieldName) throws NoSuchFieldException {
			Field field = tClass.getDeclaredField(fieldName);
			if (!Modifier.isVolatile(field.getModifiers())) {
				throw new IllegalArgumentException("Must be volatile");
			}
			offset = UNSAFE.objectFieldOffset(field);
		}

		@Override
		public boolean compareAndSet(U obj, M expect, M update) {
			return UNSAFE.compareAndSwapObject(obj, offset, expect, update);
		}

		@SuppressWarnings("unchecked")
		@Override
		public M get(U obj) {
			return (M) UNSAFE.getObjectVolatile(obj, offset);
		}

		@Override
		public void lazySet(U obj, M newValue) {
			UNSAFE.putOrderedObject(obj, offset, newValue);
		}

		@Override
		public void set(U obj, M newValue) {
			UNSAFE.putObjectVolatile(obj, offset, newValue);
		}

		@Override
		public boolean weakCompareAndSet(U obj, M expect, M update) {
			return UNSAFE.compareAndSwapObject(obj, offset, expect, update);
		}
	}
	private static final Unsafe UNSAFE;
}
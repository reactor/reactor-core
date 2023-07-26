package reactor.core.scheduler;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;

import reactor.util.annotation.Nullable;

public class MpscArrayQueue<E> extends MpscArrayQueueL3Pad<E> implements Queue<E> {

	public MpscArrayQueue(final int capacity) {
		super(capacity);
	}

	/**
	 * {@inheritDoc} <br>
	 * <p>
	 * IMPLEMENTATION NOTES:<br>
	 * Lock free offer using a single CAS. As class name suggests access is permitted to many threads
	 * concurrently.
	 *
	 * @see java.util.Queue#offer
	 */
	@Override
	public boolean offer(final @Nullable E e) {
		if (null == e) {
			throw new NullPointerException();
		}

		// use a cached view on consumer index (potentially updated in loop)
		final long mask = this.mask;
		long producerLimit = this.producerLimit;
		long pIndex;
		do {
			pIndex = this.producerIndex;
			if (pIndex >= producerLimit) {
				final long cIndex = this.consumerIndex;
				producerLimit = cIndex + mask + 1;

				if (pIndex >= producerLimit) {
					return false; // FULL :(
				}
				else {
					// update producer limit to the next index that we must recheck the consumer index
					// this is racy, but the race is benign
					soProducerLimit(producerLimit);
				}
			}
		}
		while (!casProducerIndex(pIndex, pIndex + 1));
		/*
		 * NOTE: the new producer index value is made visible BEFORE the element in the array. If we relied on
		 * the index visibility to poll() we would need to handle the case where the element is not visible.
		 */

		// Won CAS, move on to storing
		final int offset = (int) (pIndex & mask);
		lazySet(offset, e);
		return true; // AWESOME :)
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * IMPLEMENTATION NOTES:<br>
	 * Lock free poll using ordered loads/stores. As class name suggests access is limited to a single thread.
	 *
	 * @see java.util.Queue#poll
	 */
	@Override
	public E poll() {
		final long cIndex = this.consumerIndex;
		final int offset = (int) cIndex & mask;

		// If we can't see the next available element we can't poll
		E e = get(offset);
		if (null == e) {
			/*
			 * NOTE: Queue may not actually be empty in the case of a producer (P1) being interrupted after
			 * winning the CAS on offer but before storing the element in the queue. Other producers may go on
			 * to fill up the queue after this element.
			 */
			if (cIndex != this.producerIndex) {
				do {
					e = get(offset);
				}
				while (e == null);
			}
			else {
				return null;
			}
		}

		lazySet(offset, null);
		soConsumerIndex(cIndex + 1);
		return e;
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * IMPLEMENTATION NOTES:<br>
	 * Lock free peek using ordered loads. As class name suggests access is limited to a single thread.
	 *
	 * @see java.util.Queue#poll
	 */
	@Override
	public E peek() {

		final long cIndex = this.consumerIndex;
		final int offset = (int) cIndex & mask;
		E e = get(offset);
		if (null == e) {
			/*
			 * NOTE: Queue may not actually be empty in the case of a producer (P1) being interrupted after
			 * winning the CAS on offer but before storing the element in the queue. Other producers may go on
			 * to fill up the queue after this element.
			 */
			if (cIndex != this.producerIndex) {
				do {
					e = get(offset);
				}
				while (e == null);
			}
			else {
				return null;
			}
		}
		return e;
	}

	@Override
	public int size() {
		long ci = consumerIndex;
		for (; ; ) {
			long pi = producerIndex;
			long ci2 = consumerIndex;
			if (ci == ci2) {
				return (int) (pi - ci);
			}
			ci = ci2;
		}
	}

	@Override
	public boolean isEmpty() {
		return producerIndex == consumerIndex;
	}

	@Override
	public String toString() {
		return this.getClass()
		           .getName();
	}

	@Override
	public void clear() {
		while (poll() != null) {
			// if you stare into the void
		}
	}

	@Override
	public boolean contains(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<E> iterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Object[] toArray() {
		throw new UnsupportedOperationException();
	}

	@Override
	public <R> R[] toArray(R[] a) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean add(E e) {
		throw new UnsupportedOperationException();
	}

	@Override
	public E remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public E element() {
		throw new UnsupportedOperationException();
	}
}

abstract class MpscArrayQueueCold<T> extends AtomicReferenceArray<T> {

	/**
	 *
	 */
	private static final long serialVersionUID = 8491797459632447132L;

	final int mask;

	public MpscArrayQueueCold(int length) {
		super(length);
		mask = length - 1;
	}
}

abstract class MpscArrayQueueL1Pad<E> extends MpscArrayQueueCold<E> {

	byte pad000, pad001, pad002, pad003, pad004, pad005, pad006, pad007;//  8b
	byte pad010, pad011, pad012, pad013, pad014, pad015, pad016, pad017;// 16b
	byte pad020, pad021, pad022, pad023, pad024, pad025, pad026, pad027;// 24b
	byte pad030, pad031, pad032, pad033, pad034, pad035, pad036, pad037;// 32b
	byte pad040, pad041, pad042, pad043, pad044, pad045, pad046, pad047;// 40b
	byte pad050, pad051, pad052, pad053, pad054, pad055, pad056, pad057;// 48b
	byte pad060, pad061, pad062, pad063, pad064, pad065, pad066, pad067;// 56b
	byte pad070, pad071, pad072, pad073, pad074, pad075, pad076, pad077;// 64b
	byte pad100, pad101, pad102, pad103, pad104, pad105, pad106, pad107;// 72b
	byte pad110, pad111, pad112, pad113, pad114, pad115, pad116, pad117;// 80b
	byte pad120, pad121, pad122, pad123, pad124, pad125, pad126, pad127;// 88b
	byte pad130, pad131, pad132, pad133, pad134, pad135, pad136, pad137;// 96b
	byte pad140, pad141, pad142, pad143, pad144, pad145, pad146, pad147;//104b
	byte pad150, pad151, pad152, pad153, pad154, pad155, pad156, pad157;//112b
	byte pad160, pad161, pad162, pad163, pad164, pad165, pad166, pad167;//120b
	byte pad170, pad171, pad172, pad173, pad174, pad175, pad176, pad177;//128b
	byte pad200, pad201, pad202, pad203;                            //132b

	MpscArrayQueueL1Pad(int capacity) {
		super(capacity);
	}
}

//$gen:ordered-fields
abstract class MpscArrayQueueProducerIndexField<E> extends MpscArrayQueueL1Pad<E> {

	volatile long producerIndex;

	@SuppressWarnings("rawtypes")
	static final AtomicLongFieldUpdater<MpscArrayQueueProducerIndexField> PRODUCER_INDEX =
			AtomicLongFieldUpdater.newUpdater(MpscArrayQueueProducerIndexField.class,
					"producerIndex");

	MpscArrayQueueProducerIndexField(int capacity) {
		super(capacity);
	}

	final boolean casProducerIndex(long expect, long newValue) {
		return PRODUCER_INDEX.compareAndSet(this, expect, newValue);
	}
}

abstract class MpscArrayQueueMidPad<E> extends MpscArrayQueueProducerIndexField<E> {

	byte b000, b001, b002, b003, b004, b005, b006, b007;//  8b
	byte b010, b011, b012, b013, b014, b015, b016, b017;// 16b
	byte b020, b021, b022, b023, b024, b025, b026, b027;// 24b
	byte b030, b031, b032, b033, b034, b035, b036, b037;// 32b
	byte b040, b041, b042, b043, b044, b045, b046, b047;// 40b
	byte b050, b051, b052, b053, b054, b055, b056, b057;// 48b
	byte b060, b061, b062, b063, b064, b065, b066, b067;// 56b
	byte b070, b071, b072, b073, b074, b075, b076, b077;// 64b
	byte b100, b101, b102, b103, b104, b105, b106, b107;// 72b
	byte b110, b111, b112, b113, b114, b115, b116, b117;// 80b
	byte b120, b121, b122, b123, b124, b125, b126, b127;// 88b
	byte b130, b131, b132, b133, b134, b135, b136, b137;// 96b
	byte b140, b141, b142, b143, b144, b145, b146, b147;//104b
	byte b150, b151, b152, b153, b154, b155, b156, b157;//112b
	byte b160, b161, b162, b163, b164, b165, b166, b167;//120b
	byte b170, b171, b172, b173, b174, b175, b176, b177;//128b

	MpscArrayQueueMidPad(int capacity) {
		super(capacity);
	}
}

//$gen:ordered-fields
abstract class MpscArrayQueueProducerLimitField<E> extends MpscArrayQueueMidPad<E> {

	// First unavailable index the producer may claim up to before rereading the consumer index
	volatile long producerLimit;

	@SuppressWarnings("rawtypes")
	static final AtomicLongFieldUpdater<MpscArrayQueueProducerLimitField> PRODUCER_LIMIT =
			AtomicLongFieldUpdater.newUpdater(MpscArrayQueueProducerLimitField.class,
					"producerLimit");

	MpscArrayQueueProducerLimitField(int capacity) {
		super(capacity);
		this.producerLimit = capacity;
	}

	final void soProducerLimit(long newValue) {
		PRODUCER_LIMIT.lazySet(this, newValue);
	}
}

abstract class MpscArrayQueueL2Pad<E> extends MpscArrayQueueProducerLimitField<E> {

	byte b000, b001, b002, b003, b004, b005, b006, b007;//  8b
	byte b010, b011, b012, b013, b014, b015, b016, b017;// 16b
	byte b020, b021, b022, b023, b024, b025, b026, b027;// 24b
	byte b030, b031, b032, b033, b034, b035, b036, b037;// 32b
	byte b040, b041, b042, b043, b044, b045, b046, b047;// 40b
	byte b050, b051, b052, b053, b054, b055, b056, b057;// 48b
	byte b060, b061, b062, b063, b064, b065, b066, b067;// 56b
	byte b070, b071, b072, b073, b074, b075, b076, b077;// 64b
	byte b100, b101, b102, b103, b104, b105, b106, b107;// 72b
	byte b110, b111, b112, b113, b114, b115, b116, b117;// 80b
	byte b120, b121, b122, b123, b124, b125, b126, b127;// 88b
	byte b130, b131, b132, b133, b134, b135, b136, b137;// 96b
	byte b140, b141, b142, b143, b144, b145, b146, b147;//104b
	byte b150, b151, b152, b153, b154, b155, b156, b157;//112b
	byte b160, b161, b162, b163, b164, b165, b166, b167;//120b
	// byte b170,b171,b172,b173,b174,b175,b176,b177;//128b

	MpscArrayQueueL2Pad(int capacity) {
		super(capacity);
	}
}

//$gen:ordered-fields
abstract class MpscArrayQueueConsumerIndexField<E> extends MpscArrayQueueL2Pad<E> {

	volatile long consumerIndex;

	@SuppressWarnings("rawtypes")
	static final AtomicLongFieldUpdater<MpscArrayQueueConsumerIndexField> CONSUMER_INDEX =
			AtomicLongFieldUpdater.newUpdater(MpscArrayQueueConsumerIndexField.class,
					"consumerIndex");

	MpscArrayQueueConsumerIndexField(int capacity) {
		super(capacity);
	}

	final void soConsumerIndex(long newValue) {
		CONSUMER_INDEX.lazySet(this, newValue);
	}
}

abstract class MpscArrayQueueL3Pad<E> extends MpscArrayQueueConsumerIndexField<E> {

	byte b000, b001, b002, b003, b004, b005, b006, b007;//  8b
	byte b010, b011, b012, b013, b014, b015, b016, b017;// 16b
	byte b020, b021, b022, b023, b024, b025, b026, b027;// 24b
	byte b030, b031, b032, b033, b034, b035, b036, b037;// 32b
	byte b040, b041, b042, b043, b044, b045, b046, b047;// 40b
	byte b050, b051, b052, b053, b054, b055, b056, b057;// 48b
	byte b060, b061, b062, b063, b064, b065, b066, b067;// 56b
	byte b070, b071, b072, b073, b074, b075, b076, b077;// 64b
	byte b100, b101, b102, b103, b104, b105, b106, b107;// 72b
	byte b110, b111, b112, b113, b114, b115, b116, b117;// 80b
	byte b120, b121, b122, b123, b124, b125, b126, b127;// 88b
	byte b130, b131, b132, b133, b134, b135, b136, b137;// 96b
	byte b140, b141, b142, b143, b144, b145, b146, b147;//104b
	byte b150, b151, b152, b153, b154, b155, b156, b157;//112b
	byte b160, b161, b162, b163, b164, b165, b166, b167;//120b
	byte b170, b171, b172, b173, b174, b175, b176, b177;//128b

	MpscArrayQueueL3Pad(int capacity) {
		super(capacity);
	}
}
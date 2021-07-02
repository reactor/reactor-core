/*
 * Copyright (c) 2018-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.util.concurrent;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiPredicate;

import reactor.util.annotation.Nullable;

/*
 * The code was inspired by the similarly named JCTools class:
 * https://github.com/JCTools/JCTools/blob/master/jctools-core/src/main/java/org/jctools/queues/atomic
 */
/**
 * A multi-producer single consumer unbounded queue.
 * @param <E> the contained value type
 */
final class MpscLinkedQueue<E> extends AbstractQueue<E> implements BiPredicate<E, E> {
	private volatile LinkedQueueNode<E> producerNode;

	private final static AtomicReferenceFieldUpdater<MpscLinkedQueue, LinkedQueueNode> PRODUCER_NODE_UPDATER
			= AtomicReferenceFieldUpdater.newUpdater(MpscLinkedQueue.class, LinkedQueueNode.class, "producerNode");

	private volatile LinkedQueueNode<E> consumerNode;
	private final static AtomicReferenceFieldUpdater<MpscLinkedQueue, LinkedQueueNode> CONSUMER_NODE_UPDATER
			= AtomicReferenceFieldUpdater.newUpdater(MpscLinkedQueue.class, LinkedQueueNode.class, "consumerNode");

	public MpscLinkedQueue() {
		LinkedQueueNode<E> node = new LinkedQueueNode<>();
		CONSUMER_NODE_UPDATER.lazySet(this, node);
		PRODUCER_NODE_UPDATER.getAndSet(this, node);// this ensures correct construction:
		// StoreLoad
	}


	/**
	 * {@inheritDoc} <br>
	 * <p>
	 * IMPLEMENTATION NOTES:<br>
	 * Offer is allowed from multiple threads.<br>
	 * Offer allocates a new node and:
	 * <ol>
	 * <li>Swaps it atomically with current producer node (only one producer 'wins')
	 * <li>Sets the new node as the node following from the swapped producer node
	 * </ol>
	 * This works because each producer is guaranteed to 'plant' a new node and link the old node. No 2
	 * producers can get the same producer node as part of XCHG guarantee.
	 *
	 * @see java.util.Queue#offer(java.lang.Object)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public final boolean offer(final E e) {
		Objects.requireNonNull(e, "The offered value 'e' must be non-null");

		final LinkedQueueNode<E> nextNode = new LinkedQueueNode<>(e);
		final LinkedQueueNode<E> prevProducerNode = PRODUCER_NODE_UPDATER.getAndSet(this, nextNode);
		// Should a producer thread get interrupted here the chain WILL be broken until that thread is resumed
		// and completes the store in prev.next.
		prevProducerNode.soNext(nextNode); // StoreStore
		return true;
	}

	/**
	 * This is an additional {@link java.util.Queue} extension for
	 * {@link java.util.Queue#offer} which allows atomically offer two elements at once.
	 * <p>
	 * IMPLEMENTATION NOTES:<br>
	 * Offer over {@link #test} is allowed from multiple threads.<br>
	 * Offer over {@link #test} allocates a two new nodes and:
	 * <ol>
	 * <li>Swaps them atomically with current producer node (only one producer 'wins')
	 * <li>Sets the new nodes as the node following from the swapped producer node
	 * </ol>
	 * This works because each producer is guaranteed to 'plant' a new node and link the old node. No 2
	 * producers can get the same producer node as part of XCHG guarantee.
	 *
	 * @see java.util.Queue#offer(java.lang.Object)
	 *
	 * @param e1 first element to offer
	 * @param e2 second element to offer
	 *
	 * @return indicate whether elements has been successfully offered
	 */
	@Override
	@SuppressWarnings("unchecked")
	public boolean test(E e1, E e2) {
		Objects.requireNonNull(e1, "The offered value 'e1' must be non-null");
		Objects.requireNonNull(e2, "The offered value 'e2' must be non-null");

		final LinkedQueueNode<E> nextNode = new LinkedQueueNode<>(e1);
		final LinkedQueueNode<E> nextNextNode = new LinkedQueueNode<>(e2);

		final LinkedQueueNode<E> prevProducerNode = PRODUCER_NODE_UPDATER.getAndSet(this, nextNextNode);
		// Should a producer thread get interrupted here the chain WILL be broken until that thread is resumed
		// and completes the store in prev.next.
		nextNode.soNext(nextNextNode);
		prevProducerNode.soNext(nextNode); // StoreStore

		return true;
	}

	/**
	 * {@inheritDoc} <br>
	 * <p>
	 * IMPLEMENTATION NOTES:<br>
	 * Poll is allowed from a SINGLE thread.<br>
	 * Poll reads the next node from the consumerNode and:
	 * <ol>
	 * <li>If it is null, the queue is assumed empty (though it might not be).
	 * <li>If it is not null set it as the consumer node and return it's now evacuated value.
	 * </ol>
	 * This means the consumerNode.value is always null, which is also the starting point for the queue.
	 * Because null values are not allowed to be offered this is the only node with it's value set to null at
	 * any one time.
	 *
	 * @see java.util.Queue#poll()
	 */
	@Nullable
	@Override
	public E poll() {
		LinkedQueueNode<E> currConsumerNode = consumerNode; // don't load twice, it's alright
		LinkedQueueNode<E> nextNode = currConsumerNode.lvNext();

		if (nextNode != null)
		{
			// we have to null out the value because we are going to hang on to the node
			final E nextValue = nextNode.getAndNullValue();

			// Fix up the next ref of currConsumerNode to prevent promoted nodes from keeping new ones alive.
			// We use a reference to self instead of null because null is already a meaningful value (the next of
			// producer node is null).
			currConsumerNode.soNext(currConsumerNode);
			CONSUMER_NODE_UPDATER.lazySet(this, nextNode);
			// currConsumerNode is now no longer referenced and can be collected
			return nextValue;
		}
		else if (currConsumerNode != producerNode)
		{
			while ((nextNode = currConsumerNode.lvNext()) == null) { }
			// got the next node...
			// we have to null out the value because we are going to hang on to the node
			final E nextValue = nextNode.getAndNullValue();

			// Fix up the next ref of currConsumerNode to prevent promoted nodes from keeping new ones alive.
			// We use a reference to self instead of null because null is already a meaningful value (the next of
			// producer node is null).
			currConsumerNode.soNext(currConsumerNode);
			CONSUMER_NODE_UPDATER.lazySet(this, nextNode);
			// currConsumerNode is now no longer referenced and can be collected
			return nextValue;
		}
		return null;
	}

	@Nullable
	@Override
	public E peek() {
		LinkedQueueNode<E> currConsumerNode = consumerNode; // don't load twice, it's alright
		LinkedQueueNode<E> nextNode = currConsumerNode.lvNext();

		if (nextNode != null)
		{
			return nextNode.lpValue();
		}
		else if (currConsumerNode != producerNode)
		{
			while ((nextNode = currConsumerNode.lvNext()) == null) { }
			// got the next node...
			return nextNode.lpValue();
		}

		return null;
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException();
	}


	@Override
	public void clear() {
		while (poll() != null && !isEmpty()) { } // NOPMD
	}

	@Override
	public int size() {
		// Read consumer first, this is important because if the producer is node is 'older' than the consumer
		// the consumer may overtake it (consume past it) invalidating the 'snapshot' notion of size.
		LinkedQueueNode<E> chaserNode = consumerNode;
		LinkedQueueNode<E> producerNode = this.producerNode;
		int size = 0;
		// must chase the nodes all the way to the producer node, but there's no need to count beyond expected head.
		while (chaserNode != producerNode && // don't go passed producer node
				chaserNode != null && // stop at last node
				size < Integer.MAX_VALUE) // stop at max int
		{
			LinkedQueueNode<E> next;
			next = chaserNode.lvNext();
			// check if this node has been consumed, if so return what we have
			if (next == chaserNode)
			{
				return size;
			}
			chaserNode = next;
			size++;
		}
		return size;
	}

	@Override
	public boolean isEmpty() {
		return consumerNode == producerNode;
	}

	@Override
	public Iterator<E> iterator() {
		throw new UnsupportedOperationException();
	}

	static final class LinkedQueueNode<E>
	{
		private volatile LinkedQueueNode<E> next;
		private final static AtomicReferenceFieldUpdater<LinkedQueueNode, LinkedQueueNode> NEXT_UPDATER
				= AtomicReferenceFieldUpdater.newUpdater(LinkedQueueNode.class, LinkedQueueNode.class, "next");

		private E value;

		LinkedQueueNode()
		{
			this(null);
		}


		LinkedQueueNode(@Nullable E val)
		{
			spValue(val);
		}

		/**
		 * Gets the current value and nulls out the reference to it from this node.
		 *
		 * @return value
		 */
		@Nullable
		public E getAndNullValue()
		{
			E temp = lpValue();
			spValue(null);
			return temp;
		}

		@Nullable
		public E lpValue()
		{
			return value;
		}

		public void spValue(@Nullable E newValue)
		{
			value = newValue;
		}

		public void soNext(@Nullable LinkedQueueNode<E> n)
		{
			NEXT_UPDATER.lazySet(this, n);
		}

		@Nullable
		public LinkedQueueNode<E> lvNext()
		{
			return next;
		}
	}
}

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
package reactor.core.support.rb;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.AlertException;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.support.SignalType;
import reactor.core.support.rb.disruptor.RingBuffer;
import reactor.core.support.rb.disruptor.SequenceBarrier;
import reactor.fn.LongSupplier;

/**
 * Utility methods to perform common tasks associated with {@link
 * Subscriber} handling when the signals are stored in a {@link
 * RingBuffer}.
 */
public enum RingBufferSubscriberUtils {
	;

	public static <E> void onNext(E value, RingBuffer<MutableSignal<E>> ringBuffer) {
		final long seqId = ringBuffer.next();
		final MutableSignal<E> signal = ringBuffer.get(seqId);
		signal.type = SignalType.NEXT;
		signal.value = value;
		ringBuffer.publish(seqId);
	}

	public static <E> void onNext(E value, RingBuffer<MutableSignal<E>> ringBuffer,
			Subscription subscription) {
		final long seqId = ringBuffer.next();

		final MutableSignal<E> signal = ringBuffer.get(seqId);
		signal.type = SignalType.NEXT;
		signal.value = value;

		if (subscription != null) {
			long remaining = ringBuffer.cachedRemainingCapacity();
			if (remaining > 0) {
				subscription.request(remaining);
			}
		}
		ringBuffer.publish(seqId);
	}

	public static <E> MutableSignal<E> next(RingBuffer<MutableSignal<E>> ringBuffer) {
		long seqId = ringBuffer.next();
		MutableSignal<E> signal = ringBuffer.get(seqId);
		signal.type = SignalType.NEXT;
		signal.seqId = seqId;
		return signal;
	}

	public static <E> void publish(RingBuffer<MutableSignal<E>> ringBuffer,
			MutableSignal<E> signal) {
		ringBuffer.publish(signal.seqId);
	}

	public static <E> void onError(Throwable error,
			RingBuffer<MutableSignal<E>> ringBuffer) {
		final long seqId = ringBuffer.next();
		final MutableSignal<E> signal = ringBuffer.get(seqId);

		signal.type = SignalType.ERROR;
		signal.value = null;
		signal.error = error;

		ringBuffer.publish(seqId);
	}

	public static <E> void onComplete(RingBuffer<MutableSignal<E>> ringBuffer) {
		final long seqId = ringBuffer.next();
		final MutableSignal<E> signal = ringBuffer.get(seqId);

		signal.type = SignalType.COMPLETE;
		signal.value = null;
		signal.error = null;

		ringBuffer.publish(seqId);
	}

	public static <E> void tryOnComplete(RingBuffer<MutableSignal<E>> ringBuffer) {
		final long seqId = ringBuffer.tryNext();
		final MutableSignal<E> signal = ringBuffer.get(seqId);

		signal.type = SignalType.COMPLETE;
		signal.value = null;
		signal.error = null;

		ringBuffer.publish(seqId);
	}

	public static <E> void route(MutableSignal<E> task,
			Subscriber<? super E> subscriber) {
		if (task.type == SignalType.NEXT && null != task.value) {
			// most likely case first
			subscriber.onNext(task.value);
		}
		else if (task.type == SignalType.COMPLETE) {
			// second most likely case next
			subscriber.onComplete();
		}
		else if (task.type == SignalType.ERROR) {
			// errors should be relatively infrequent compared to other signals
			subscriber.onError(task.error);
		}

	}

	public static <T> boolean waitRequestOrTerminalEvent(LongSupplier pendingRequest,
			RingBuffer<MutableSignal<T>> ringBuffer, SequenceBarrier barrier,
			Subscriber<? super T> subscriber, AtomicBoolean isRunning,
			LongSupplier nextSequence, Runnable waiter) {
		try {
			long waitedSequence;
			MutableSignal<T> event;
			while (pendingRequest.get() <= 0l) {
				//pause until first request
				waitedSequence = nextSequence.get() + 1;
				if (waiter != null) {
					barrier.waitFor(waitedSequence, waiter);
				}
				else {
					barrier.waitFor(waitedSequence);
				}
				event = ringBuffer.get(waitedSequence);

				if (event.type == SignalType.COMPLETE) {
					try {
						subscriber.onComplete();
						return false;
					}
					catch (Throwable t) {
						Exceptions.throwIfFatal(t);
						subscriber.onError(t);
						return false;
					}
				}
				else if (event.type == SignalType.ERROR) {
					subscriber.onError(event.error);
					return false;
				}
				LockSupport.parkNanos(1l);
			}
		}
		catch (CancelException | AlertException ae) {
			if (!isRunning.get()) {
				return false;
			}
		}
		catch (InterruptedException ie) {
			Thread.currentThread()
			      .interrupt();
		}

		return true;
	}
}

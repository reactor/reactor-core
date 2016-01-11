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
import reactor.core.error.AlertException;
import reactor.core.error.CancelException;
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

	public static <E> void onNext(E value, RingBuffer<RingBuffer.Slot<E>> ringBuffer) {
		final long seqId = ringBuffer.next();
		final RingBuffer.Slot<E> signal = ringBuffer.get(seqId);
		signal.value = value;
		ringBuffer.publish(seqId);
	}

	public static  boolean waitRequestOrTerminalEvent(LongSupplier pendingRequest,
			SequenceBarrier barrier,
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
				if(!isRunning.get()){
					throw CancelException.INSTANCE;
				}
				LockSupport.parkNanos(1L);
			}
		}
		catch (CancelException | AlertException ae) {
			return false;
		}
		catch (InterruptedException ie) {
			Thread.currentThread()
			      .interrupt();
		}

		return true;
	}
}

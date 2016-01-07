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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.AlertException;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.support.WaitStrategy;
import reactor.core.support.rb.disruptor.RingBuffer;
import reactor.fn.Consumer;
import reactor.fn.LongSupplier;

/**
 * An async request client for ring buffer impls
 * @author Stephane Maldini
 */
public final class RequestTask implements Runnable {

	final WaitStrategy waitStrategy;

	final LongSupplier readCount;

	final Subscription upstream;

	final Runnable spinObserver;

	final Consumer<Long> postWaitCallback;

	final Subscriber<?> errorSubscriber;

	final RingBuffer<?> ringBuffer;

	public RequestTask(Subscription upstream, Runnable stopCondition,
			Consumer<Long> postWaitCallback, LongSupplier readCount,
			WaitStrategy waitStrategy, Subscriber<?> errorSubscriber, RingBuffer r) {
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
		final long bufferSize = ringBuffer.getBufferSize();
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
		catch (AlertException e) {
			//completed
		}
		catch (CancelException ce) {
			upstream.cancel();
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		catch (Throwable t) {
			Exceptions.throwIfFatal(t);
			errorSubscriber.onError(t);
		}
	}
}

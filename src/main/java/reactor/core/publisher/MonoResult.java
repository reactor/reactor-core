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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.subscription.BackpressureUtils;
import reactor.core.subscription.CancelledSubscription;
import reactor.core.util.Exceptions;
import reactor.core.util.ReactiveState;
import reactor.core.util.internal.PlatformDependent;

/**
 * @author Stephane Maldini
 */
final class MonoResult<I> implements Subscriber<I>, ReactiveState.ActiveUpstream {

	volatile SignalType   endState;
	volatile I            value;
	volatile Throwable    error;
	volatile Subscription s;

	static final AtomicReferenceFieldUpdater<MonoResult, Subscription> SUBSCRIPTION =
			PlatformDependent.newAtomicReferenceFieldUpdater(MonoResult.class, "s");

	public I await(long timeout, TimeUnit unit) {
		long delay = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(timeout, unit);

		try {
			for (; ; ) {
				SignalType endState = this.endState;
				if (endState != null) {
					switch (endState) {
						case NEXT:
							return value;
						case ERROR:
							if (error instanceof RuntimeException) {
								throw (RuntimeException) error;
							}
							Exceptions.fail(error);
						case COMPLETE:
							return null;
					}
				}
				if (delay < System.currentTimeMillis()) {
					Exceptions.failWithCancel();
				}
				Thread.sleep(1);
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread()
			      .interrupt();
			Exceptions.fail(e);
			return null;
		}
		finally {
			Subscription s = SUBSCRIPTION.getAndSet(this, CancelledSubscription.INSTANCE);

			if (s != null && s != CancelledSubscription.INSTANCE) {
				s.cancel();
			}
		}
	}

	@Override
	public boolean isStarted() {
		return s != null && endState == null;
	}

	@Override
	public boolean isTerminated() {
		return endState != null;
	}

	@Override
	public void onSubscribe(Subscription s) {
		if (BackpressureUtils.validate(this.s, s)) {
			this.s = s;
			s.request(Long.MAX_VALUE);
		}
	}

	@Override
	public void onNext(I i) {
		s.cancel();
		if (endState != null) {
			Exceptions.onNextDropped(i);
		}
		value = i;
		endState = SignalType.NEXT;
	}

	@Override
	public void onError(Throwable t) {
		if (endState != null) {
			Exceptions.onErrorDropped(t);
		}
		error = t;
		endState = SignalType.ERROR;
	}

	@Override
	public void onComplete() {
		if (endState != null) {
			return;
		}
		endState = SignalType.COMPLETE;
	}
}

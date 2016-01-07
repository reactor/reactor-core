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

package reactor.core.subscriber;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;

/**
 * @author Stephane Maldini
 * @since 2.5
 */
public class SubscriberWithDemand<I, O> extends SubscriberBarrier<I, O>
		implements ReactiveState.DownstreamDemand, ReactiveState.FailState {

	protected final static int NOT_TERMINATED = 0;
	protected final static int TERMINATED_WITH_SUCCESS = 1;
	protected final static int TERMINATED_WITH_ERROR = 2;
	protected final static int TERMINATED_WITH_CANCEL = 3;

	@SuppressWarnings("unused")
	private volatile       int                                             terminated = NOT_TERMINATED;
	@SuppressWarnings("rawtypes")
	protected static final AtomicIntegerFieldUpdater<SubscriberWithDemand> TERMINATED =
			AtomicIntegerFieldUpdater.newUpdater(SubscriberWithDemand.class, "terminated");

	@SuppressWarnings("unused")
	private volatile       long                                         requested = 0L;
	@SuppressWarnings("rawtypes")
	protected static final AtomicLongFieldUpdater<SubscriberWithDemand> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(SubscriberWithDemand.class, "requested");


	public SubscriberWithDemand(Subscriber<? super O> subscriber) {
		super(subscriber);
	}

	/**
	 *
	 * @return
	 */
	@Override
	public boolean isTerminated() {
		return terminated != NOT_TERMINATED;
	}

	/**
	 *
	 * @return
	 */
	public final boolean isCompleted() {
		return terminated == TERMINATED_WITH_SUCCESS;
	}

	/**
	 *
	 * @return
	 */
	public final boolean isFailed() {
		return terminated == TERMINATED_WITH_ERROR;
	}

	/**
	 *
	 * @return
	 */
	public final boolean isCancelled() {
		return terminated == TERMINATED_WITH_CANCEL;
	}

	/**
	 *
	 * @return
	 */
	@Override
	public final long requestedFromDownstream() {
		return requested;
	}

	@Override
	protected void doRequest(long n) {
		doRequested(BackpressureUtils.getAndAdd(REQUESTED, this, n), n);
	}

	protected void doRequested(long before, long n){
		requestMore(n);
	}

	protected final void requestMore(long n){
		Subscription s = this.subscription;
		if (s != null) {
			s.request(n);
		}
	}

	@Override
	protected void doComplete() {
		if (TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_SUCCESS)) {
			checkedComplete();
			doTerminate();
		}
	}

	protected void checkedComplete(){
		subscriber.onComplete();
	}

	@Override
	protected void doError(Throwable throwable) {
		if (TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_ERROR)) {
			checkedError(throwable);
			doTerminate();
		}
	}

	protected void checkedError(Throwable throwable){
		subscriber.onError(throwable);
	}

	@Override
	protected void doCancel() {
		if (TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_CANCEL)) {
			checkedCancel();
			doTerminate();
		}
	}

	protected void checkedCancel(){
		super.doCancel();
	}

	protected void doTerminate(){
		//TBD
	}

	@Override
	public String toString() {
		return super.toString() + (requested != 0L ? "{requested=" + requested + "}": "");
	}

	@Override
	public Throwable getError() {
		return isFailed() ? FAILED_SATE : null;
	}

	private static final Exception FAILED_SATE = new RuntimeException("Failed Subscriber"){
		@Override
		public synchronized Throwable fillInStackTrace() {
			return null;
		}
	};
}

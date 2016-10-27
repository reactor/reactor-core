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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.Loopback;
import reactor.core.MultiProducer;
import reactor.core.MultiReceiver;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;

/**
 * @author Stephane Maldini
 */
public class PublisherMock<T> implements Publisher<T> {

	static <T> PublisherMock<T> create() {
		return new PublisherMock<T>();
	}

	final List<DownstreamMock<T>> downstreams = new ArrayList<>();

	public Flux<T> flux() {
		return FluxSource.wrap(this);
	}

	public Mono<T> mono() {
		return MonoSource.wrap(this);
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		DownstreamMock<T> m = new DownstreamMock<>(s);
		synchronized (downstreams) {
			downstreams.add(m);
		}
		s.onSubscribe(m);
	}

	public DownstreamMock<T> get(int i) {
		return downstreams.get(i);
	}

	public void cleanTerminatedDownstreams() {
		synchronized (downstreams) {
			Iterator<DownstreamMock<T>> it = downstreams.iterator();
			DownstreamMock<T> m;
			while (it.hasNext()) {
				m = it.next();
				if (m.getCount() == 0) {
					it.remove();
				}
			}
		}
	}

	public static final class DownstreamMock<T> extends CountDownLatch
			implements Trackable, Subscription, SynchronousSink<T>, MultiReceiver,
			           MultiProducer, Producer, Receiver, Loopback {

		final Subscriber<? super T> s;
		final Trackable             trackable;
		final Loopback              loopback;
		final MultiProducer         multiProducer;
		final MultiReceiver         multiReceiver;
		final Producer              producer;
		final Receiver              receiver;

		volatile long requested;
		static final AtomicLongFieldUpdater<DownstreamMock> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(DownstreamMock.class, "requested");

		volatile boolean cancelled;

		static final Trackable EMPTY_TRACKABLE = new Trackable() {
		};

		static final Loopback EMPTY_LOOPBACK = new Loopback() {
		};

		public DownstreamMock(Subscriber<? super T> s) {
			super(1);
			this.s = s;
			if (s instanceof Trackable) {
				this.trackable = (Trackable) s;
			}
			else {
				this.trackable = EMPTY_TRACKABLE;
			}
			if (s instanceof Loopback) {
				this.loopback = (Loopback) s;
			}
			else {
				this.loopback = EMPTY_LOOPBACK;
			}
			if (s instanceof Receiver) {
				this.receiver = (Receiver) s;
			}
			else {
				this.receiver = null;
			}
			if (s instanceof Producer) {
				this.producer = (Producer) s;
			}
			else {
				this.producer = null;
			}
			if (s instanceof MultiReceiver) {
				this.multiReceiver = (MultiReceiver) s;
			}
			else {
				this.multiReceiver = null;
			}
			if (s instanceof MultiProducer) {
				this.multiProducer = (MultiProducer) s;
			}
			else {
				this.multiProducer = null;
			}
		}

		public boolean isFuseable() {
			return s instanceof Fuseable.QueueSubscription;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
			}
		}

		@Override
		public void cancel() {
			cancelled = true;
			countDown();
		}

		public long expectedFromUpstream() {
			return trackable.expectedFromUpstream();
		}

		public long getCapacity() {
			return trackable.getCapacity();
		}

		public Throwable getError() {
			return trackable.getError();
		}

		public long getPending() {
			return trackable.getPending();
		}

		public boolean isCancelled() {
			return trackable.isCancelled();
		}

		public boolean isStarted() {
			return trackable.isStarted();
		}

		public boolean isTerminated() {
			return trackable.isTerminated();
		}

		public long limit() {
			return trackable.limit();
		}

		public long requestedFromDownstream() {
			return trackable.requestedFromDownstream();
		}

		public long mockRequested() {
			return requested;
		}

		public boolean mockCancelled() {
			return cancelled;
		}

		@Override
		public Object connectedInput() {
			return loopback.connectedInput();
		}

		@Override
		public Object connectedOutput() {
			return loopback.connectedOutput();
		}

		@Override
		public Iterator<?> downstreams() {
			return multiProducer != null ? multiProducer.downstreams() : null;
		}

		@Override
		public long downstreamCount() {
			return multiProducer != null ? multiProducer.downstreamCount() :
					MultiProducer.super.downstreamCount();
		}

		@Override
		public boolean hasDownstreams() {
			return multiProducer != null && multiProducer.hasDownstreams();
		}

		@Override
		public Iterator<?> upstreams() {
			return multiReceiver != null ? multiReceiver.upstreams() : null;
		}

		@Override
		public long upstreamCount() {
			return multiReceiver != null ? multiReceiver.upstreamCount() :
					MultiReceiver.super.upstreamCount();
		}

		@Override
		public Object upstream() {
			return receiver != null ? receiver.upstream() : null;
		}

		@Override
		public Object downstream() {
			return producer != null ? producer.downstream() : null;
		}

		@Override
		public void complete() {
			countDown();
			s.onComplete();
		}

		@Override
		public void error(Throwable e) {
			countDown();
			s.onError(e);
		}

		@Override
		public void next(T o) {
			s.onNext(o);
			Operators.produced(REQUESTED, this, 1);
		}


	}
}

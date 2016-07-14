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

import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Producer;
import reactor.core.flow.Receiver;
import reactor.core.subscriber.SubscriberState;
import reactor.core.util.Exceptions;

/**
 * @author Stephane Maldini
 */
final class DelegateProcessor<IN, OUT> extends FluxProcessor<IN, OUT>
		implements Producer, Receiver {

	private final Publisher<OUT> downstream;
	private final Subscriber<IN> upstream;
	Subscription upstreamSubscription;

	public DelegateProcessor(Publisher<OUT> downstream, Subscriber<IN> upstream) {
		this.downstream = Objects.requireNonNull(downstream, "Downstream must not be null");
		this.upstream = Objects.requireNonNull(upstream, "Upstream must not be null");
	}

	@Override
	public Subscriber<? super IN> downstream() {
		return upstream;
	}

	@Override
	public long getCapacity() {
		return SubscriberState.class.isAssignableFrom(upstream.getClass()) ?
				((SubscriberState) upstream).getCapacity() : Long.MAX_VALUE;
	}

	@Override
	public void onComplete() {
		upstream.onComplete();
	}

	@Override
	public void onError(Throwable t) {
		upstream.onError(t);
	}

	@Override
	public void onNext(IN in) {
		upstream.onNext(in);
	}

	@Override
	public void onSubscribe(Subscription s) {
		upstream.onSubscribe(s);
	}

	@Override
	public void subscribe(Subscriber<? super OUT> s) {
		if (s == null) {
			throw Exceptions.argumentIsNullException();
		}
		downstream.subscribe(s);
	}

	@Override
	public Subscription upstream() {
		return upstreamSubscription;
	}
}

/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Trackable;
import reactor.core.Exceptions;

/**
 * A base processor that exposes {@link Flux} API for {@link Processor}.
 *
 * Implementors include {@link UnicastProcessor}, {@link EmitterProcessor},
 * {@link ReplayProcessor}, {@link WorkQueueProcessor} and {@link TopicProcessor}.
 *
 * @author Stephane Maldini
 *
 * @param <IN> the input value type
 * @param <OUT> the output value type
 */
public abstract class FluxProcessor<IN, OUT> extends Flux<OUT>
		implements Processor<IN, OUT>, Trackable {

	/**
	 * Build a {@link FluxProcessor} whose data are emitted by the most recent emitted {@link Publisher}.
	 * The {@link Flux} will complete once both the publishers source and the last switched to {@link Publisher} have
	 * completed.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.5.RELEASE/src/docs/marble/switchonnext.png" alt="">
	 *
	 * @param <T> the produced type
	 * @return a {@link FluxProcessor} accepting publishers and producing T
	 */
	public static <T> FluxProcessor<Publisher<? extends T>, T> switchOnNext() {
		UnicastProcessor<Publisher<? extends T>> emitter = UnicastProcessor.create();
		FluxProcessor<Publisher<? extends T>, T> p = FluxProcessor.wrap(emitter, switchOnNext(emitter));
		return p;
	}

	/**
	 * Transform a receiving {@link Subscriber} and a producing {@link Publisher} in a logical {@link FluxProcessor}.
	 * The link between the passed upstream and returned downstream will not be created automatically, e.g. not
	 * subscribed together. A {@link Processor} might choose to have orthogonal sequence input and output.
	 *
	 * @param <IN> the receiving type
	 * @param <OUT> the producing type
	 *
	 * @param upstream the upstream subscriber
	 * @param downstream the downstream publisher
	 * @return a new blackboxed {@link FluxProcessor}
	 */
	public static <IN, OUT> FluxProcessor<IN, OUT> wrap(final Subscriber<IN> upstream, final Publisher<OUT> downstream) {
		return new DelegateProcessor<>(downstream, upstream);
	}


	/**
	 * Trigger onSubscribe with a stateless subscription to signal this subscriber it can start receiving
	 * onNext, onComplete and onError calls.
	 * <p>
	 * Doing so MAY allow direct UNBOUNDED onXXX calls and MAY prevent {@link org.reactivestreams.Publisher} to subscribe this
	 * subscriber.
	 *
	 * Note that {@link org.reactivestreams.Processor} can extend this behavior to effectively start its subscribers.
	 *
	 * @return this
	 */
	public FluxProcessor<IN, OUT> connect() {
		onSubscribe(Operators.emptySubscription());
		return this;
	}

	/**
	 * Create a {@link BlockingSink} and attach it via {@link #onSubscribe(Subscription)}.
	 *
	 * @return a new subscribed {@link BlockingSink}
	 */
	public final BlockingSink<IN> connectSink() {
		return connectSink(true);
	}

	/**
	 * Prepare a {@link BlockingSink} and pass it to {@link #onSubscribe(Subscription)} if the autostart flag is
	 * set to true.
	 *
	 * @param autostart automatically start?
	 * @return a new {@link BlockingSink}
	 */
	public final BlockingSink<IN> connectSink(boolean autostart) {
		return BlockingSink.create(this, autostart);
	}

	@Override
	public long getCapacity() {
		return Integer.MAX_VALUE;
	}

	/**
	 * Create a {@link FluxProcessor} that safely gates multi-threaded producer
	 * {@link Subscriber#onNext(Object)}.
	 *
	 * @return a serializing {@link FluxProcessor}
	 */
	public final FluxProcessor<IN, OUT> serialize() {
		return new DelegateProcessor<>(this, Operators.serialize(this));
	}

	@Override
	public void subscribe(Subscriber<? super OUT> s) {
		if (s == null) {
			throw Exceptions.argumentIsNullException();
		}
	}

}

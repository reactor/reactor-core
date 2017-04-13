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

import java.util.Iterator;
import java.util.concurrent.CancellationException;
import java.util.stream.Stream;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.MultiProducer;
import reactor.core.Scannable;
import reactor.core.Trackable;

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
		implements Processor<IN, OUT>, Scannable, Disposable, Trackable, MultiProducer {

	/**
	 * Build a {@link FluxProcessor} whose data are emitted by the most recent emitted {@link Publisher}.
	 * The {@link Flux} will complete once both the publishers source and the last switched to {@link Publisher} have
	 * completed.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.0.6.RELEASE/src/docs/marble/switchonnext.png" alt="">
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
	 * @deprecated Will be removed in 3.1.0 where {@link FluxProcessor#onSubscribe(Subscription)} is not required by
	 * default anymore and brings no benefit given the private scope of the
	 * Subscription.
	 */
	@Deprecated
	public FluxProcessor<IN, OUT> connect() {
		onSubscribe(Operators.emptySubscription());
		return this;
	}

	/**
	 * Create a {@link BlockingSink} and attach it via {@link #onSubscribe(Subscription)}.
	 *
	 * @return a new subscribed {@link BlockingSink}
	 * @deprecated Will be removed in 3.1.0, use {@link #sink()}
	 */
	@Deprecated
	public final BlockingSink<IN> connectSink() {
		return connectSink(true);
	}

	/**
	 * Prepare a {@link BlockingSink} and pass it to {@link #onSubscribe(Subscription)} if the autostart flag is
	 * set to true.
	 *
	 * @param autostart automatically start?
	 * @return a new {@link BlockingSink}
	 * @deprecated Will be removed in 3.1.0, use {@link #sink()}
	 */
	@Deprecated
	public final BlockingSink<IN> connectSink(boolean autostart) {
		return BlockingSink.create(this, autostart);
	}

	@Override
	public void dispose() {
		onError(new CancellationException("Disposed"));
	}

	/**
	 * Return the number of active {@link Subscriber} or {@literal -1} if untracked.
	 *
	 * @return the number of active {@link Subscriber} or {@literal -1} if untracked
	 */
	public long downstreamCount(){
		return inners().count();
	}

	/**
	 * Return the processor buffer capacity if any or {@link Integer#MAX_VALUE}
	 *
	 * @return processor buffer capacity if any or {@link Integer#MAX_VALUE}
	 */
	public int getBufferSize() {
		return Integer.MAX_VALUE;
	}

	@Override
	@Deprecated
	public final long getCapacity() {
		return getBufferSize();
	}

	/**
	 * Current error if any, default to null
	 *
	 * @return Current error if any, default to null
	 */
	public Throwable getError() {
		return null;
	}

	/**
	 * Return true if any {@link Subscriber} is actively subscribed
	 *
	 * @return true if any {@link Subscriber} is actively subscribed
	 */
	public boolean hasDownstreams() {
		return downstreamCount() != 0L;
	}

	@Override
	public Stream<? extends Scannable> inners() {
		return Stream.empty();
	}

	/**
	 * Has this upstream started or "onSubscribed" ?
	 *
	 * @return has this upstream started or "onSubscribed" ?
	 * @deprecated Will be removed in 3.1.0. Processor are stateful and started by default which means you can
	 * onNext them directly
	 */
	@Deprecated
	public boolean isStarted() {
		return true;
	}

	/**
	 * Has this upstream finished or "completed" / "failed" ?
	 *
	 * @return has this upstream finished or "completed" / "failed" ?
	 */
	public boolean isTerminated() {
		return false;
	}

	/**
	 * Return true if this {@link FluxProcessor} supports multithread producing
	 *
	 * @return true if this {@link FluxProcessor} supports multithread producing
	 */
	public boolean isSerialized() {
		return false;
	}

	@Override
	public Object scan(Attr key) {
		switch (key) {
			case TERMINATED:
				return isTerminated();
			case ERROR:
				return getError();
			case CAPACITY:
				return getBufferSize();
		}
		return null;
	}

	@Override
	@Deprecated
	public Iterator<?> downstreams() {
		return inners().iterator();
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

	/**
	 * Create a {@link FluxSink} that safely gates multi-threaded producer
	 * {@link Subscriber#onNext(Object)}.
	 *
	 * <p> The returned {@link FluxSink} will not apply any
	 * {@link FluxSink.OverflowStrategy} and overflowing {@link FluxSink#next(Object)}
	 * will behave in two possible ways depending on the Processor:
	 * <ul>
	 * <li> an unbounded processor will handle the overflow itself by dropping or
	 * buffering </li>
	 * <li> a bounded processor will block/spin</li>
	 * </ul>
	 *
	 * @return a serializing {@link FluxSink}
	 */
	public final FluxSink<IN> sink() {
		FluxCreate.IgnoreSink<IN> s = new FluxCreate.IgnoreSink<>(this);
		onSubscribe(s);
		if(s.isCancelled() ||
				(isSerialized() && getBufferSize() == Integer.MAX_VALUE)){
			return s;
		}
		return new FluxCreate.SerializedSink<>(s);
	}

	/**
	 * Note: From 3.1 this is to be left unimplemented
	 */
	@Override
	public void subscribe(Subscriber<? super OUT> s) {
		if (s == null) {
			throw Exceptions.argumentIsNullException();
		}
	}
}

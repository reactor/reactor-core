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

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Producer;
import reactor.core.flow.Receiver;
import reactor.core.state.Backpressurable;
import reactor.core.subscriber.SignalEmitter;
import reactor.core.util.Assert;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.EmptySubscription;
import reactor.core.util.Exceptions;
import reactor.fn.Consumer;
import reactor.fn.Function;

/**
 * A base processor that expose {@link Flux} API, {@link Processor} and generic {@link Function} or {@link Runnable}
 * scheduling contract.
 *
 * The scheduling contract allows for interoperability with {@link Flux#dispatchOn)} and {@link Flux#publishOn} :
 * {@code
 *  flux.dispatchOn(SchedulerGroup.io()) ou
 *  mono.publishOn(TopicProcessor.create())
 *  stream.dispatchOn(WorkQueueProcessor.create())
 *  promise.publishOn( task -> { Future<T> f = executorService.submit(task); return () -> f.cancel(); } )
 * }
 *
 * @author Stephane Maldini
 * @since 2.0.2, 2.5
 */
public abstract class FluxProcessor<IN, OUT> extends Flux<OUT>
		implements Processor<IN, OUT>, Backpressurable, Receiver, Consumer<IN> {

	/**
	 * @param <IN>
	 * @return
	 */
	public static <IN> FluxProcessor<IN, IN> async(final SchedulerGroup group) {
		FluxProcessor<IN, IN> emitter = EmitterProcessor.create();
		return create(emitter, emitter.dispatchOn(group));
	}

	/**
	 * Create a {@link FluxProcessor} from a receiving {@link Subscriber} and a mapped {@link Publisher}.
	 *
	 *
	 *
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT, E extends Subscriber<IN>> FluxProcessor<IN, OUT> blackbox(
			final E input,
			final Function<E, ? extends Publisher<OUT>> processor) {
		return new DelegateProcessor<>(processor.apply(input), input);
	}
	/**
	 * Prepare a {@link SignalEmitter} and pass it to {@link #onSubscribe(Subscription)} if the autostart flag is
	 * set to true.
	 *
	 * @return a new {@link SignalEmitter}
	 */
	public static <IN> FluxProcessor<IN, IN> blocking() {
		FluxPassthrough<IN> passthrough = new FluxPassthrough<>();
		return create(SignalEmitter.blocking(passthrough), passthrough);
	}

	/**
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> FluxProcessor<IN, OUT> create(final Subscriber<IN> upstream, final Publisher<OUT> downstream) {
		return new DelegateProcessor<>(downstream, upstream);
	}

	Subscription upstreamSubscription;

	protected FluxProcessor() {
	}

	/**
	 * Trigger onSubscribe with a stateless subscription to signal this subscriber it can start receiving
	 * onNext, onComplete and onError calls.
	 * <p>
	 * Doing so MAY allow direct UNBOUNDED onXXX calls and MAY prevent {@link org.reactivestreams.Publisher} to subscribe this
	 * subscriber.
	 *
	 * Note that {@link org.reactivestreams.Processor} can extend this behavior to effectively start its subscribers.
	 */
	public FluxProcessor<IN, OUT> start() {
		onSubscribe(EmptySubscription.INSTANCE);
		return this;
	}

	/**
	 * Create a {@link SignalEmitter} and attach it via {@link #onSubscribe(Subscription)}.
	 *
	 * @return a new subscribed {@link SignalEmitter}
	 */
	public SignalEmitter<IN> startEmitter() {
		return bindEmitter(true);
	}

	/**
	 * Prepare a {@link SignalEmitter} and pass it to {@link #onSubscribe(Subscription)} if the autostart flag is
	 * set to true.
	 *
	 * @return a new {@link SignalEmitter}
	 */
	public SignalEmitter<IN> bindEmitter(boolean autostart) {
		return SignalEmitter.create(this, autostart);
	}


	@Override
	public void onSubscribe(final Subscription s) {
		if (BackpressureUtils.validate(upstreamSubscription, s)) {
			this.upstreamSubscription = s;
			try {
				doOnSubscribe(s);
			}
			catch (Throwable t) {
				Exceptions.throwIfFatal(t);
				s.cancel();
				onError(t);
			}
		}
	}

	@Override
	public void onNext(IN t) {
		if (t == null) {
			throw Exceptions.argumentIsNullException();
		}

	}

	@Override
	public void onError(Throwable t) {
		if (t == null) {
			throw Exceptions.argumentIsNullException();
		}
		Exceptions.throwIfFatal(t);
	}

	protected void doOnSubscribe(Subscription s) {
		//IGNORE
	}

	@Override
	public long getCapacity() {
		return Long.MAX_VALUE;
	}

	@Override
	public void subscribe(Subscriber<? super OUT> s) {
		if (s == null) {
			throw Exceptions.argumentIsNullException();
		}
	}

	@Override
	public void accept(IN runnable) {
		if(runnable == null){
			onComplete();
		}
		else {
			onNext(runnable);
		}
	}

	protected void cancel(Subscription subscription) {
		if (subscription != EmptySubscription.INSTANCE) {
			subscription.cancel();
		}
	}

	@Override
	public Subscription upstream() {
		return upstreamSubscription;
	}

	@Override
	public long getPending() {
		return -1L;
	}

	@Override
	public int getMode() {
		return 0;
	}

	final static class DelegateProcessor<IN, OUT> extends FluxProcessor<IN, OUT>
			implements Producer, Backpressurable {

		private final Publisher<OUT> downstream;
		private final Subscriber<IN> upstream;

		public DelegateProcessor(Publisher<OUT> downstream, Subscriber<IN> upstream) {
			Assert.notNull(upstream, "Upstream must not be null");
			Assert.notNull(downstream, "Downstream must not be null");

			this.downstream = downstream;
			this.upstream = upstream;
		}

		@Override
		public Subscriber<? super IN> downstream() {
			return upstream;
		}

		@Override
		public long getCapacity() {
			return Backpressurable.class.isAssignableFrom(upstream.getClass()) ?
					((Backpressurable) upstream).getCapacity() :
					Long.MAX_VALUE;
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
			if(s == null)
				throw Exceptions.argumentIsNullException();
			downstream.subscribe(s);
		}
	}
}

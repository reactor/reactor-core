/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import org.reactivestreams.Subscriber;

import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

/**
 * A collection of standalone sinks ({@link StandaloneFluxSink} and
 * {@link StandaloneMonoSink}).
 *
 * @author Simon Baslé
 */
public final class Sinks {

	private Sinks() { }

	/**
	 * A {@link StandaloneFluxSink} with the following characteristics:
	 * <ul>
	 *     <li>Multicast</li>
	 *     <li>Backpressure : this sink honors downstream demand by conforming to the lowest demand in case
	 *     of multiple subscribers.</li>
	 *     <li>Replaying: No replay. Only forwards to a {@link Subscriber} the elements that have been
	 *     pushed to the sink AFTER this subscriber was subscribed. To the exception of the first
	 *     subscriber (see below).</li>
	 *     <li>Without {@link Subscriber}: warm up. Remembers up to {@link Queues#SMALL_BUFFER_SIZE}
	 *     elements pushed before the first {@link Subscriber} is registered.</li>
	 * </ul>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/sinkWarmup.svg" alt="">
	 */
	@SuppressWarnings("deprecation")
	public static <T> StandaloneFluxSink<T> multicast() {
		return new FluxProcessorSink<>(EmitterProcessor.create(Queues.SMALL_BUFFER_SIZE));
	}

	/**
	 * A {@link StandaloneFluxSink} with the following characteristics:
	 * <ul>
	 *     <li>Multicast</li>
	 *     <li>Backpressure : this sink honors downstream demand of individual subscribers.</li>
	 *     <li>Replaying: No replay. Only forwards to a {@link Subscriber} the elements that have been
	 *     pushed to the sink AFTER this subscriber was subscribed.</li>
	 *     <li>Without {@link Subscriber}: Discarding. Pushing elements while there are no {@link Subscriber}
	 *     registered will simply discard these elements instead of "warming up" the sink.</li>
	 * </ul>
	 * <p>
	 * <img class="marble" src="doc-files/marbles/sinkNoWarmup.svg" alt="">
	 */
	@SuppressWarnings("deprecation")
	public static <T> StandaloneFluxSink<T> multicastNoWarmup() {
		return new FluxProcessorSink<>(ReplayProcessor.create(0));
	}

	/**
	 * A {@link StandaloneFluxSink} with the following characteristics:
	 * <ul>
	 *     <li>Multicast</li>
	 *     <li>Backpressure : this sink honors downstream demand of individual subscribers.</li>
	 *     <li>Replaying: by {@code historySize}. Keeps the last {@code historySize} elements and
	 *     replays them instantly to new subscribers before continuing with "live" elements.</li>
	 *     <li>Without {@link Subscriber}: buffers enough elements pushed without a subscriber to
	 *     honor the {@code historySize}.</li>
	 * </ul>
	 */
	@SuppressWarnings("deprecation")
	public static <T> StandaloneFluxSink<T> replay(int historySize) {
		return new FluxProcessorSink<>(ReplayProcessor.create(historySize));
	}

	/**
	 * A {@link StandaloneFluxSink} with the following characteristics:
	 * <ul>
	 *     <li>Multicast</li>
	 *     <li>Backpressure : this sink honors downstream demand of individual subscribers.</li>
	 *     <li>Replaying: all elements pushed to this sink are replayed to new subscribers.</li>
	 *     <li>Without {@link Subscriber}: all elements pushed to this sink are remembered,
	 *     even when there is no subscriber.</li>
	 * </ul>
	 */
	@SuppressWarnings("deprecation")
	public static <T> StandaloneFluxSink<T> replayAll() {
		return new FluxProcessorSink<>(ReplayProcessor.create());
	}

	/**
	 * A {@link StandaloneFluxSink} with the following characteristics:
	 * <ul>
	 *     <li><strong>Unicast</strong>: contrary to most other {@link StandaloneFluxSink}, the
	 *     {@link Flux} view rejects {@link Subscriber subscribers} past the first one.</li>
	 *     <li>Backpressure : this sink honors downstream demand of its single {@link Subscriber}.</li>
	 *     <li>Replaying: non-applicable, since only one {@link Subscriber} can register.</li>
	 *     <li>Without {@link Subscriber}: all elements pushed to this sink are remembered and will
	 *     be replayed once the {@link Subscriber} subscribes.</li>
	 * </ul>
	 */
	@SuppressWarnings("deprecation")
	public static <T> StandaloneFluxSink<T> unicast() {
		return new FluxProcessorSink<>(UnicastProcessor.create());
	}

	/**
	 * A {@link StandaloneMonoSink} that works like a conceptual Promise: it can be completed
	 * at any time, but only once. This is done by invoking either of the three completions:
	 * {@link ScalarSink#success(Object) valued completion}, {@link ScalarSink#success() empty completions}
	 * or {@link ScalarSink#error(Throwable) error}. This completion is replayed to late subscribers.
	 */
	public static <T> StandaloneMonoSink<T> promise() {
		return new MonoProcessorSink<>(MonoProcessor.create());
	}

	// == interfaces ==

	/**
	 * A flavor of {@link SequenceSink} that is not attached to a single {@link Subscriber}
	 * but rather viewable {@link #asFlux() as a Flux}. Most likely, such a {@link SequenceSink}
	 * is capable of multicasting to several subscribers. However implementations can chose
	 * to be unicast instead, and only accept one {@link Subscriber} at a time (as allowed
	 * by the {@link org.reactivestreams.Publisher} specification).
	 *
	 * @param <T> the type of elements that can be emitted through this sink
	 */
	public interface StandaloneFluxSink<T> extends SequenceSink<T> {

		@Override
		StandaloneFluxSink<T> next(T t);

		/**
		 * Return the companion {@link Flux} instance that is backed by this sink.
		 * All calls to this method return the same instance.
		 *
		 * @return the {@link Flux} view associated to this {@link StandaloneFluxSink}
		 */
		Flux<T> asFlux();

	}

	/**
	 * A flavor of {@link ScalarSink} that is not attached to a single {@link Subscriber}
	 * but rather viewable {@link #asMono() as a Mono}. Most likely, such a {@link ScalarSink}
	 * is capable of multicasting to several subscribers. However implementations can chose
	 * to be unicast instead, and only accept one {@link Subscriber} at a time (as allowed
	 * by the {@link org.reactivestreams.Publisher} specification).
	 *
	 * @param <T> the type of elements that can be emitted through this sink
	 */
	public interface StandaloneMonoSink<T> extends ScalarSink<T> {

		/**
		 * Return the companion {@link Mono} instance that is backed by this sink.
		 * All calls to this method return the same instance.
		 *
		 * @return the {@link Mono} view associated to this {@link StandaloneMonoSink}
		 */
		Mono<T> asMono();
	}

	// == concrete classes ==

	static final class FluxProcessorSink<T>
			implements StandaloneFluxSink<T> {

		final FluxSink<T>         delegateSink;
		final FluxProcessor<T, T> processor;

		@SuppressWarnings("deprecation")
		FluxProcessorSink(FluxProcessor<T, T> processor) {
			this.processor = processor;
			this.delegateSink = processor.sink();
		}

		@Override
		public Flux<T> asFlux() {
			return processor;
		}

		@Override
		public void complete() {
			delegateSink.complete();
		}

		@Override
		public void error(Throwable e) {
			delegateSink.error(e);
		}

		@Override
		public StandaloneFluxSink<T> next(T t) {
			delegateSink.next(t);
			return this;
		}
	}

	//TODO improve synchronization, prefer CAS ?
	static final class MonoProcessorSink<T> implements StandaloneMonoSink<T> {

		final MonoProcessor<T> processor;
		boolean done;

		MonoProcessorSink(MonoProcessor<T> processor) {
			this.processor = processor;
		}

		@Override
		public Mono<T> asMono() {
			return this.processor;
		}

		@Override
		public void success() {
			synchronized (processor) {
				if (done) {
					return;
				}
				done = true;
			}
			processor.onComplete();
		}

		@Override
		public void success(@Nullable T value) {
			if (value == null) {
				success();
				return;
			}
			synchronized (processor) {
				if (done) {
					Operators.onNextDropped(value, Context.empty()); //FIXME add an API to set the handler?
					return;
				}
				done = true;
			}
			processor.onNext(value);
			processor.onComplete();
		}

		@Override
		public void error(Throwable e) {
			synchronized (processor) {
				if (done) {
					Operators.onErrorDropped(e, Context.empty());
					return;
				}
				done = true;
			}
			processor.onError(e);
		}
	}

}

/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;

/**
 * A stand-alone {@link FluxSink} that wraps a {@link FluxProcessor} for its downstream
 * semantics and that be converted to a {@link Flux} or {@link Mono}.
 * <p>
 * This is a facade over the much more complex API of {@link FluxProcessor} and {@link FluxProcessor#sink()}.
 *
 * @author Simon Baslé
 */
public interface StandaloneFluxSink<T> extends FluxSink<T> {

	/**
	 * Converts the {@link StandaloneFluxSink} to a {@link Flux}, allowing to compose
	 * operators on it.
	 * <p>
	 * When possible, if the concrete implementation already is derived from {@link Flux}
	 * this method should not instantiate any intermediate object.
	 *
	 * @return the {@link StandaloneFluxSink} viewed as a {@link Flux}
	 */
	Flux<T> toFlux();

	/**
	 * Converts the {@link StandaloneFluxSink} to a {@link Mono}, allowing to compose
	 * operators on it.
	 *
	 * @return the {@link StandaloneFluxSink} viewed as a {@link Mono}
	 */
	Mono<T> toMono();



	//== Factory Methods

	//=== Direct
	/**
	 * Create a new direct {@link StandaloneFluxSink}
	 *
	 * @param <O> Type of data signals
	 * @return a direct {@link StandaloneFluxSink}
	 */
	@SuppressWarnings("deprecation")
	static <O> StandaloneFluxSink<O> direct() {
		return new FluxProcessor.SinkFacade<>(DirectProcessor.create());
	}

	//=== Unicast

	/**
	 * Create a new unicast {@link StandaloneFluxSink} that will buffer on an internal
	 * queue in an unbounded fashion.
	 *
	 * @param <O> the data type
	 * @return a unicast {@link StandaloneFluxSink}
	 */
	@SuppressWarnings("deprecation")
	static <O> StandaloneFluxSink<O> unicast() {
		return new FluxProcessor.SinkFacade<>(UnicastProcessor.create());
	}

	/**
	 * Create a new unicast {@link StandaloneFluxSink} that will buffer on a provided
	 * queue in an unbounded fashion.
	 *
	 * @param queue the buffering queue
	 * @param <O> the data type
	 * @return a unicast {@link StandaloneFluxSink}
	 */
	@SuppressWarnings("deprecation")
	static <O> StandaloneFluxSink<O> unicast(Queue<O> queue) {
		return new FluxProcessor.SinkFacade<>(UnicastProcessor.create(queue));
	}

	/**
	 * Create a new unicast {@link StandaloneFluxSink} that will buffer on a provided
	 * queue in an unbounded fashion.
	 *
	 * @param queue the buffering queue
	 * @param endCallback called on any terminal signal
	 * @param <O> the data type
	 * @return a unicast {@link StandaloneFluxSink}
	 */
	@SuppressWarnings("deprecation")
	static <O> StandaloneFluxSink<O> unicast(Queue<O> queue, Disposable endCallback) {
		return new FluxProcessor.SinkFacade<>(UnicastProcessor.create(queue, endCallback));
	}

	/**
	 * Create a new unicast {@link StandaloneFluxSink} that will buffer on a provided
	 * queue in an unbounded fashion.
	 *
	 * @param queue the buffering queue
	 * @param endCallback called on any terminal signal
	 * @param onOverflow called when queue.offer return new FluxProcessor.SinkFacade<>(false and unicastProcessor is
	 * about to emit onError.
	 * @param <O> the data type
	 *
	 * @return a unicast {@link StandaloneFluxSink}
	 */
	@SuppressWarnings("deprecation")
	static <O> StandaloneFluxSink<O> unicast(Queue<O> queue,
			Consumer<? super O> onOverflow,
			Disposable endCallback) {
		return new FluxProcessor.SinkFacade<>(UnicastProcessor.create(queue, onOverflow, endCallback));
	}

	//=== Replay

	/**
	 * Create a {@link StandaloneFluxSink} that caches the last element it has pushed,
	 * replaying it to late subscribers. This is a buffer-based replay processor with
	 * a history size of 1.
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.1.3.RELEASE/src/docs/marble/replaylast.png"
	 * alt="">
	 *
	 * @param <O> the data type
	 *
	 * @return a new {@link StandaloneFluxSink} that replays its last pushed element to each new
	 * {@link Subscriber}
	 */
	@SuppressWarnings("deprecation")
	static <O> StandaloneFluxSink<O> replayLast() {
		return new FluxProcessor.SinkFacade<>(ReplayProcessor.cacheLast());
	}

	/**
	 * Create a {@link StandaloneFluxSink} that caches the last element it has pushed,
	 * replaying it to late subscribers. If a {@link Subscriber} comes in <b>before</b>
	 * any value has been pushed, then the {@code defaultValue} is emitted instead. 
	 * This is a buffer-based replay Processor with a history size of 1.
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.1.3.RELEASE/src/docs/marble/replaylastd.png"
	 * alt="">
	 *
	 * @param value a default value to start the sequence with in case nothing has been
	 * cached yet.
	 * @param <O> the data type
	 *
	 * @return a new {@link StandaloneFluxSink} that replays its last pushed element to each new
	 * {@link Subscriber}, or a default one if nothing was pushed yet
	 */
	@SuppressWarnings("deprecation")
	static <O> StandaloneFluxSink<O> replayLastOrDefault(@Nullable O value) {
		return new FluxProcessor.SinkFacade<>(ReplayProcessor.cacheLastOrDefault(value));
	}

	/**
	 * Create a new {@link StandaloneFluxSink} that replays an unbounded number of elements,
	 * using a default internal {@link Queues#SMALL_BUFFER_SIZE Queue}.
	 *
	 * @param <O> the data type
	 *
	 * @return a new {@link StandaloneFluxSink} that replays the whole history to each new
	 * {@link Subscriber}.
	 */
	@SuppressWarnings("deprecation")
	static <O> StandaloneFluxSink<O> replayAll() {
		return new FluxProcessor.SinkFacade<>(ReplayProcessor.create());
	}

	/**
	 * Create a new {@link StandaloneFluxSink} that replays up to {@code historySize}
	 * elements.
	 *
	 * @param historySize the backlog size, ie. maximum items retained for replay.
	 * @param <O> the data type
	 *
	 * @return a new {@link StandaloneFluxSink} that replays a limited history to each new
	 * {@link Subscriber}.
	 */
	@SuppressWarnings("deprecation")
	static <O> StandaloneFluxSink<O> replaySize(int historySize) {
		return new FluxProcessor.SinkFacade<>(ReplayProcessor.create(historySize, false));
	}

	/**
	 * Create a new {@link StandaloneFluxSink} that either replay all the elements or a
	 * limited amount of elements depending on the {@code unbounded} parameter.
	 *
	 * @param historySize maximum items retained if bounded, or initial link size if unbounded
	 * @param unbounded true if "unlimited" data store must be supplied
	 * @param <O> the data type
	 *
	 * @return a new {@link StandaloneFluxSink} that replays the whole history to each new
	 * {@link Subscriber} if configured as unbounded, a limited history otherwise.
	 */
	@SuppressWarnings("deprecation")
	static <O> StandaloneFluxSink<O> replaySize(int historySize, boolean unbounded) {
		return new FluxProcessor.SinkFacade<>(ReplayProcessor.create(historySize, unbounded));
	}

	/**
	 * Creates a time-bounded replay {@link StandaloneFluxSink}.
	 * <p>
	 * In this setting, the {@link StandaloneFluxSink} internally tags each observed item
	 * with a timestamp value supplied by the {@link Schedulers#parallel()} and keeps only
	 * those whose age is less than the supplied time value converted to milliseconds. For
	 * example, an item arrives at T=0 and the max age is set to 5; at T&gt;=5 this first
	 * item is then evicted by any subsequent item or termination signal, leaving the
	 * buffer empty.
	 * <p>
	 * Once the processor is terminated, subscribers subscribing to it will receive items
	 * that remained in the buffer after the terminal signal, regardless of their age.
	 * <p>
	 * If an subscriber subscribes while the {@link StandaloneFluxSink} is active, it will
	 * observe only those items from within the buffer that have an age less than the
	 * specified time, and each item observed thereafter, even if the buffer evicts items
	 * due to the time constraint in the mean time. In other words, once an subscriber
	 * subscribes, it observes items without gaps in the sequence except for any outdated
	 * items at the beginning of the sequence.
	 * <p>
	 * Note that terminal signals ({@code onError} and {@code onComplete}) trigger
	 * eviction as well. For example, with a max age of 5, the first item is observed at
	 * T=0, then an {@code onComplete} signal arrives at T=10. If an subscriber subscribes
	 * at T=11, it will find an empty {@link StandaloneFluxSink} with just an {@code
	 * onCompleted} signal.
	 *
	 * @param <O> the type of items observed and emitted by the Processor
	 * @param maxAge the maximum age of the contained items
	 *
	 * @return a new {@link StandaloneFluxSink} that replays elements based on their age.
	 */
	@SuppressWarnings("deprecation")
	static <O> StandaloneFluxSink<O> replayTimeout(Duration maxAge) {
		return new FluxProcessor.SinkFacade<>(ReplayProcessor.createTimeout(maxAge));
	}

	/**
	 * Creates a time-bounded replay {@link StandaloneFluxSink}.
	 * <p>
	 * In this setting, the {@link StandaloneFluxSink} internally tags each observed item
	 * with a timestamp value supplied by the {@link Scheduler} and keeps only
	 * those whose age is less than the supplied time value converted to milliseconds. For
	 * example, an item arrives at T=0 and the max age is set to 5; at T&gt;=5 this first
	 * item is then evicted by any subsequent item or termination signal, leaving the
	 * buffer empty.
	 * <p>
	 * Once the processor is terminated, subscribers subscribing to it will receive items
	 * that remained in the buffer after the terminal signal, regardless of their age.
	 * <p>
	 * If an subscriber subscribes while the {@link StandaloneFluxSink} is active, it will
	 * observe only those items from within the buffer that have an age less than the
	 * specified time, and each item observed thereafter, even if the buffer evicts items
	 * due to the time constraint in the mean time. In other words, once an subscriber
	 * subscribes, it observes items without gaps in the sequence except for any outdated
	 * items at the beginning of the sequence.
	 * <p>
	 * Note that terminal signals ({@code onError} and {@code onComplete}) trigger
	 * eviction as well. For example, with a max age of 5, the first item is observed at
	 * T=0, then an {@code onComplete} signal arrives at T=10. If an subscriber subscribes
	 * at T=11, it will find an empty {@link StandaloneFluxSink} with just an {@code
	 * onCompleted} signal.
	 *
	 * @param <O> the type of items observed and emitted by the Processor
	 * @param maxAge the maximum age of the contained items
	 *
	 * @return a new {@link StandaloneFluxSink} that replays elements based on their age.
	 */
	@SuppressWarnings("deprecation")
	static <O> StandaloneFluxSink<O> replayTimeout(Duration maxAge, Scheduler scheduler) {
		return new FluxProcessor.SinkFacade<>(ReplayProcessor.createTimeout(maxAge, scheduler));
	}

	/**
	 * Creates a time- and size-bounded replay {@link StandaloneFluxSink}.
	 * <p>
	 * In this setting, the {@link StandaloneFluxSink} internally tags each received item
	 * with a timestamp value supplied by the {@link Schedulers#parallel()} and holds at
	 * most
	 * {@code size} items in its internal buffer. It evicts items from the start of the
	 * buffer if their age becomes less-than or equal to the supplied age in milliseconds
	 * or the buffer reaches its {@code size} limit.
	 * <p>
	 * When subscribers subscribe to a terminated {@link StandaloneFluxSink}, they observe
	 * the items that remained in the buffer after the terminal signal, regardless of
	 * their age, but at most {@code size} items.
	 * <p>
	 * If an subscriber subscribes while the {@link StandaloneFluxSink} is active, it will
	 * observe only those items from within the buffer that have age less than the
	 * specified time and each subsequent item, even if the buffer evicts items due to the
	 * time constraint in the mean time. In other words, once an subscriber subscribes, it
	 * observes items without gaps in the sequence except for the outdated items at the
	 * beginning of the sequence.
	 * <p>
	 * Note that terminal signals ({@code onError} and {@code onComplete}) trigger
	 * eviction as well. For example, with a max age of 5, the first item is observed at
	 * T=0, then an {@code onComplete} signal arrives at T=10. If an Subscriber subscribes
	 * at T=11, it will find an empty {@link StandaloneFluxSink} with just an {@code
	 * onCompleted} signal.
	 *
	 * @param <O> the type of items observed and emitted by the Processor
	 * @param maxAge the maximum age of the contained items
	 * @param size the maximum number of buffered items
	 *
	 * @return a new {@link StandaloneFluxSink} that replay up to {@code size} elements, but
	 * will evict them from its history based on their age.
	 */
	@SuppressWarnings("deprecation")
	static <O> StandaloneFluxSink<O> replaySizeAndTimeout(int size, Duration maxAge) {
		return new FluxProcessor.SinkFacade<>(ReplayProcessor.createSizeAndTimeout(size, maxAge));
	}

	/**
	 * Creates a time- and size-bounded replay {@link StandaloneFluxSink}.
	 * <p>
	 * In this setting, the {@link StandaloneFluxSink} internally tags each received item
	 * with a timestamp value supplied by the {@link Scheduler} and holds at most
	 * {@code size} items in its internal buffer. It evicts items from the start of the
	 * buffer if their age becomes less-than or equal to the supplied age in milliseconds
	 * or the buffer reaches its {@code size} limit.
	 * <p>
	 * When subscribers subscribe to a terminated {@link StandaloneFluxSink}, they observe
	 * the items that remained in the buffer after the terminal signal, regardless of
	 * their age, but at most {@code size} items.
	 * <p>
	 * If an subscriber subscribes while the {@link StandaloneFluxSink} is active, it will
	 * observe only those items from within the buffer that have age less than the
	 * specified time and each subsequent item, even if the buffer evicts items due to the
	 * time constraint in the mean time. In other words, once an subscriber subscribes, it
	 * observes items without gaps in the sequence except for the outdated items at the
	 * beginning of the sequence.
	 * <p>
	 * Note that terminal signals ({@code onError} and {@code onComplete}) trigger
	 * eviction as well. For example, with a max age of 5, the first item is observed at
	 * T=0, then an {@code onComplete} signal arrives at T=10. If an Subscriber subscribes
	 * at T=11, it will find an empty {@link StandaloneFluxSink} with just an {@code
	 * onCompleted} signal.
	 *
	 * @param <O> the type of items observed and emitted by the Processor
	 * @param maxAge the maximum age of the contained items in milliseconds
	 * @param size the maximum number of buffered items
	 * @param scheduler the {@link Scheduler} that provides the current time
	 *
	 * @return a new {@link StandaloneFluxSink} that replay up to {@code size} elements, but
	 * will evict them from its history based on their age.
	 */
	@SuppressWarnings("deprecation")
	static <O> StandaloneFluxSink<O> replaySizeAndTimeout(int size,
			Duration maxAge,
			Scheduler scheduler) {
		return new FluxProcessor.SinkFacade<>(ReplayProcessor.createSizeAndTimeout(size, maxAge, scheduler));
	}

	//=== Emitter

	/**
	 * Create a new emitter {@link StandaloneFluxSink} using {@link Queues#SMALL_BUFFER_SIZE}
	 * backlog size and auto-cancel.
	 *
	 * @param <O> the data type
	 *
	 * @return an emitter {@link StandaloneFluxSink}
	 */
	@SuppressWarnings("deprecation")
	static <O> StandaloneFluxSink<O> emitter() {
		return new FluxProcessor.SinkFacade<>(EmitterProcessor.create());
	}

	/**
	 * Create a new  emitter {@link StandaloneFluxSink} using {@link Queues#SMALL_BUFFER_SIZE}
	 * backlog size and the provided auto-cancel.
	 *
	 * @param <O> the data type
	 * @param autoCancel automatically cancel
	 *
	 * @return an emitter {@link StandaloneFluxSink}
	 */
	@SuppressWarnings("deprecation")
	static <O> StandaloneFluxSink<O> emitter(boolean autoCancel) {
		return new FluxProcessor.SinkFacade<>(EmitterProcessor.create(autoCancel));
	}

	/**
	 * Create a new  emitter {@link StandaloneFluxSink} using the provided backlog size, with auto-cancel.
	 *
	 * @param <O> the data type
	 * @param bufferSize the internal buffer size to hold signals
	 *
	 * @return an emitter {@link StandaloneFluxSink}
	 */
	@SuppressWarnings("deprecation")
	static <O> StandaloneFluxSink<O> emitter(int bufferSize) {
		return new FluxProcessor.SinkFacade<>(EmitterProcessor.create(bufferSize));
	}

	/**
	 * Create a new  emitter {@link StandaloneFluxSink} using the provided backlog size and auto-cancellation.
	 *
	 * @param <O> the data type
	 * @param bufferSize the internal buffer size to hold signals
	 * @param autoCancel automatically cancel
	 *
	 * @return an emitter {@link StandaloneFluxSink}
	 */
	@SuppressWarnings("deprecation")
	static <O> StandaloneFluxSink<O> emitter(int bufferSize, boolean autoCancel) {
		return new FluxProcessor.SinkFacade<>(EmitterProcessor.create(bufferSize, autoCancel));
	}


	//=== Async

	/**
	 * A {@link TopicProcessor.Builder builder} for a new <strong>shared</strong> async
	 * {@link IdentityProcessor} (when terminating with {@link TopicProcessor.Builder#buildProcessor() buildProcessor})
	 * or new async {@link StandaloneFluxSink} (when terminating with {@link TopicProcessor.Builder#buildSink() buildSink}).
	 * All the configuration can be altered, including the shared property.
	 *
	 * @param <O> the data type
	 * @return a new async Processor builder
	 */
	@SuppressWarnings("deprecation")
	static <O> TopicProcessor.Builder<O> asyncBuilder()  {
		return new TopicProcessor.Builder<O>().share(true);
	}

	/**
	 * Create a new <strong>shared</strong> async {@link StandaloneFluxSink} using
	 * {@link Queues#SMALL_BUFFER_SIZE} backlog size, blockingWait Strategy and auto-cancel.
	 * <p>
	 * A Shared Processor authorizes concurrent onNext calls and is suited for multi-threaded
	 * publisher that will fan-in data.
	 * <p>
	 * A new Cached {@link ThreadPoolExecutor} will be implicitly created.
	 *
	 * @param <O> the data type
	 * @return a new shared async {@link StandaloneFluxSink}
	 */
	@SuppressWarnings("deprecation")
	static <O> StandaloneFluxSink<O> async() {
		return new FluxProcessor.SinkFacade<>(TopicProcessor.<O>builder()
				.share(true)
				.build());
	}

	/**
	 * Create a new async <strong>shared</strong> {@link StandaloneFluxSink} using the
	 * provided backlog size, with a blockingWait Strategy and auto-cancellation.
	 * <p>
	 * A Shared Processor authorizes concurrent onNext calls and is suited for multi-threaded
	 * publisher that will fan-in data.
	 * <p>
	 * A new Cached {@link ThreadPoolExecutor} will be implicitly created and will use the
	 * passed name to qualify the created threads.
	 *
	 * @param name Create a new cached {@link ExecutorService} and assign
	 * this name to the created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <O> the data type
	 * @return a new shared async {@link StandaloneFluxSink}
	 */
	@SuppressWarnings("deprecation")
	static <O> StandaloneFluxSink<O> async(String name, int bufferSize) {
		return new FluxProcessor.SinkFacade<>(TopicProcessor.<O>builder()
				.share(true)
				.name(name)
				.bufferSize(bufferSize)
				.build());
	}

	/**
	 * Create a new <strong>unshared</strong> async {@link StandaloneFluxSink} using the
	 * passed backlog size, with a blockingWait Strategy and auto-cancellation.
	 * <p>
	 * A new Cached {@link ThreadPoolExecutor} will be implicitly created and will use the
	 * passed name to qualify the created threads.
	 *
	 * @param name Create a new Cached {@link ExecutorService} and assign this name to the
	 * created threads
	 * @param bufferSize A Backlog Size to mitigate slow subscribers
	 * @param <O> the data type
	 * @return a new shared async {@link StandaloneFluxSink}
	 */
	@SuppressWarnings("deprecation")
	static <O> StandaloneFluxSink<O> asyncUnshared(String name, int bufferSize) {
		return new FluxProcessor.SinkFacade<>(TopicProcessor.<O>builder()
				.share(false)
				.name(name)
				.bufferSize(bufferSize)
				.build());
	}

}

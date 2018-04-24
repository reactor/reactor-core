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
import java.util.function.Consumer;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.concurrent.WaitStrategy;

/**
 * Utility class to create various flavors of {@link  BalancedFluxProcessor Flux Processors}.
 *
 * @author Simon Baslé
 */
public final class Processors {

	/**
	 * Create a "direct" {@link BalancedFluxProcessor}: it can dispatch signals to zero to many
	 * {@link Subscriber Subscribers}, but has the limitation of not
	 * handling backpressure.
	 * <p>
	 * As a consequence, a direct Processor signals an {@link IllegalStateException} to its
	 * subscribers if you push N elements through it but at least one of its subscribers has
	 * requested less than N.
	 * <p>
	 * Once the Processor has terminated (usually through its {@link BalancedFluxProcessor#sink() Sink}
	 * {@link FluxSink#error(Throwable)} or {@link FluxSink#complete()} methods being called),
	 * it lets more subscribers subscribe but replays the termination signal to them immediately.
	 *
	 * @param <T> the type of the data flowing through the processor
	 * @return a new direct {@link BalancedFluxProcessor}
	 */
	@SuppressWarnings("deprecation")
	public static final <T> BalancedFluxProcessor<T> direct() {
		return DirectProcessor.create();
	}

	/**
	 * Create a builder for a "unicast" {@link BalancedFluxProcessor}, which can deal with
	 * backpressure using an internal buffer, but <strong>can have at most one
	 * {@link Subscriber}</strong>.
	 * <p>
	 * A unicast Processor can be fine tuned through its builder. For instance, by default
	 * it is unbounded: if you push any amount of data through it while its
	 * {@link Subscriber} has not yet requested data, it will buffer
	 * all of the data, which can be changed by providing a {@link Queue} through the
	 * {@link UnicastProcessorBuilder#queue(Queue)} method.
	 *
	 * @param <T>
	 * @return a builder to create a new unicast {@link BalancedFluxProcessor}
	 */
	public static final <T> UnicastProcessorBuilder<T> unicast() {
		return new UnicastProcessorBuilder<>();
	}

	/**
	 * Create a builder for an "emitter" {@link BalancedFluxProcessor}, which is capable
	 * of emitting to several {@link Subscriber Subscribers} while honoring backpressure
	 * for each of its subscribers. It can also subscribe to a {@link Publisher} and relay
	 * its signals synchronously.
	 *
	 * <p>
	 * Initially, when it has no {@link Subscriber}, an emitter Processor can still accept
	 * a few data pushes up to a configurable {@link EmitterProcessorBuilder#bufferSize(int) bufferSize}.
	 * After that point, if no {@link Subscriber} has come in and consumed the data,
	 * calls to {@link BalancedFluxProcessor#onNext(Object) onNext} block until the processor
	 * is drained (which can only happen concurrently by then).
	 *
	 * <p>
	 * Thus, the first {@link Subscriber} to subscribe receives up to {@code bufferSize}
	 * elements upon subscribing. However, after that, the processor stops replaying signals
	 * to additional subscribers. These subsequent subscribers instead only receive the
	 * signals pushed through the processor after they have subscribed. The internal buffer
	 * is still used for backpressure purposes.
	 *
	 * <p>
	 * By default, if all of the emitter Processor's subscribers are cancelled (which
	 * basically means they have all un-subscribed), it will clear its internal buffer and
	 * stop accepting new subscribers. This can be deactivated by using the
	 * {@link EmitterProcessorBuilder#noAutoCancel()} method in the builder.
	 *
	 * @param <T>
	 * @return a builder to create a new emitter {@link BalancedFluxProcessor}
	 */
	public static final <T> EmitterProcessorBuilder<T> emitter() {
		return new EmitterProcessorBuilder<>();
	}

	/**
	 * Create a builder for a "replay" {@link BalancedFluxProcessor}, which caches
	 * elements that are either pushed directly through its {@link BalancedFluxProcessor#sink() sink}
	 * or elements from an upstream {@link Publisher}, and replays them to late
	 * {@link Subscriber Subscribers}.
	 *
	 * <p>
	 * Depending on the methods called on the builder, it can create a replay Processor in
	 * multiple configurations:
	 * <ul>
	 *     <li>
	 *         Caching an unbounded history (no builder configuration and direct call to
	 *         {@link ReplayProcessorBuilder#build()}, or call to {@link ReplayProcessorBuilder#historySize(int, boolean)}
	 *         with {@code true}).
	 *     </li>
	 *     <li>
	 *         Caching a bounded history ({@link ReplayProcessorBuilder#historySize(int)}).
	 *     </li>
	 *     <li>
	 *         Caching time-based replay windows, by only specifying a TTL
	 *         ({@link ReplayProcessorBuilder#maxAge(Duration)}).
	 *     </li>
	 *     <li>
	 *         Caching combination of history size and time window, by specifying both
	 *         TTL ({@link ReplayProcessorBuilder#maxAge(Duration)}) and history size
	 *         ({@link ReplayProcessorBuilder#historySize(int)}).
	 *     </li>
	 * </ul>
	 *
	 * @param <T>
	 * @return a builder to create a new replay {@link BalancedFluxProcessor}
	 */
	public static final <T> ReplayProcessorBuilder<T> replay() {
		return new ReplayProcessorBuilder<>();
	}

	/**
	 * Create a new "replay" {@link BalancedFluxProcessor} (see {@link #replay()}) that
	 * caches the last element it has pushed, replaying it to late {@link Subscriber subscribers}.
	 *
	 * @param <T>
	 * @return a new replay {@link BalancedFluxProcessor} that caches its last pushed element
	 */
	public static final <T> BalancedFluxProcessor<T> cacheLast() {
		return ReplayProcessor.cacheLast();
	}

	/**
	 * Create a new "replay" {@link BalancedFluxProcessor} (see {@link #replay()}) that
	 * caches the last element it has pushed, replaying it to late {@link Subscriber subscribers}.
	 * If a {@link Subscriber} comes in <strong>before</strong> any value has been pushed,
	 * then the {@code defaultValue} is emitted instead.
	 *
	 * @param <T>
	 * @return a new replay {@link BalancedFluxProcessor} that caches its last pushed element
	 */
	public static final <T> BalancedFluxProcessor<T> cacheLastOrDefault(T defaultValue) {
		return ReplayProcessor.cacheLastOrDefault(defaultValue);
	}

	/**
	 * Create a builder for a "fan out" {@link BalancedFluxProcessor}, which is an
	 * <strong>asynchronous</strong> processor optionally capable of relaying elements from multiple
	 * upstream {@link Publisher Publishers} when created in the shared configuration (see the {@link
	 * FanOutProcessorBuilder#share(boolean)} option of the builder).
	 *
	 * <p>
	 * Note that the share option is mandatory if you intend to concurrently call the Processor's
	 * {@link BalancedFluxProcessor#onNext(Object)}, {@link BalancedFluxProcessor#onComplete()}, or
	 * {@link BalancedFluxProcessor#onError(Throwable)} methods directly or from a concurrent upstream
	 * {@link Publisher}.
	 *
	 * <p>
	 * Otherwise, such concurrent calls are illegal, as the processor is then fully compliant with
	 * the Reactive Streams specification.
	 *
	 * <p>
	 * A fan out processor is capable of fanning out to multiple {@link Subscriber Subscribers},
	 * with the added overhead of establishing resources to keep track of each {@link Subscriber}
	 * until an {@link BalancedFluxProcessor#onError(Throwable)} or {@link
	 * BalancedFluxProcessor#onComplete()} signal is pushed through the processor or until the
	 * associated {@link Subscriber} is cancelled.
	 *
	 * <p>
	 * This variant uses a {@link Thread}-per-{@link Subscriber} model.
	 *
	 * <p>
	 * The maximum number of downstream subscribers for this processor is driven by the {@link
	 * FanOutProcessorBuilder#executor(ExecutorService) executor} builder option. Provide a bounded
	 * {@link ExecutorService} to limit it to a specific number.
	 *
	 * <p>
	 * There is also an {@link FanOutProcessorBuilder#autoCancel(boolean) autoCancel} builder
	 * option: If set to {@code true} (the default), it results in the source {@link Publisher
	 * Publisher(s)} being cancelled when all subscribers are cancelled.
	 *
	 * @param <T>
	 * @return a builder to create a new fan out {@link BalancedFluxProcessor}
	 */
	@SuppressWarnings("deprecation")
	public static final <T> FanOutProcessorBuilder<T> fanOut() {
		return new TopicProcessor.Builder<>();
	}

	/**
	 * Create a builder for a "fan out" {@link BalancedFluxProcessor} with relaxed
	 * Reactive Streams compliance. This is an <strong>asynchronous</strong> processor
	 * optionally capable of relaying elements from multiple upstream {@link Publisher Publishers}
	 * when created in the shared configuration (see the {@link FanOutProcessorBuilder#share(boolean)}
	 * option of the builder).
	 *
	 * <p>
	 * Note that the share option is mandatory if you intend to concurrently call the Processor's
	 * {@link BalancedFluxProcessor#onNext(Object)}, {@link BalancedFluxProcessor#onComplete()}, or
	 * {@link BalancedFluxProcessor#onError(Throwable)} methods directly or from a concurrent upstream
	 * {@link Publisher}. Otherwise, such concurrent calls are illegal.
	 *
	 * <p>
	 * A fan out processor is capable of fanning out to multiple {@link Subscriber Subscribers},
	 * with the added overhead of establishing resources to keep track of each {@link Subscriber}
	 * until an {@link BalancedFluxProcessor#onError(Throwable)} or {@link
	 * BalancedFluxProcessor#onComplete()} signal is pushed through the processor or until the
	 * associated {@link Subscriber} is cancelled.
	 *
	 * <p>
	 * This variant uses a RingBuffer and doesn't have the overhead of keeping track of
	 * each {@link Subscriber} in its own Thread, so it scales better. As a trade-off,
	 * its compliance with the Reactive Streams specification is slightly
	 * relaxed, and its distribution pattern is to add up requests from all {@link Subscriber Subscribers}
	 * together and to relay signals to only one {@link Subscriber}, picked in a kind of
	 * round-robin fashion.
	 *
	 * <p>
	 * The maximum number of downstream subscribers for this processor is driven by the {@link
	 * FanOutProcessorBuilder#executor(ExecutorService) executor} builder option. Provide a bounded
	 * {@link ExecutorService} to limit it to a specific number.
	 *
	 * <p>
	 * There is also an {@link FanOutProcessorBuilder#autoCancel(boolean) autoCancel} builder
	 * option: If set to {@code true} (the default), it results in the source {@link Publisher
	 * Publisher(s)} being cancelled when all subscribers are cancelled.
	 *
	 * @param <T>
	 * @return a builder to create a new round-robin fan out {@link BalancedFluxProcessor}
	 */
	@Deprecated
	public static final <T> FanOutProcessorBuilder<T> relaxedFanOut() {
		return new WorkQueueProcessor.Builder<>();
	}

  /**
   * Create a "first" {@link BalancedMonoProcessor}, which can attach to a {@link Publisher} source
   * of which it will propagate only the first incoming {@link Subscriber#onNext(Object) onNext}
   * signal, and cancel the source subscription (unless it is a {@link Mono}). It will then
   * replay that signal to new {@link Subscriber Subscribers}.
   *
   * <p>
   * If no {@link MonoFirstProcessorBuilder#attachToSource(Publisher) source} is initially
   * provided, the processor will wait for a source {@link Subscriber#onSubscribe(Subscription)}.
   *
   * <p>
   * Manual usage by pushing events through the {@link MonoSink} is naturally limited to at most
   * one call to {@link MonoSink#success(Object)}.
   *
   * @param <T>
   * @return a builder to create a new "first" {@link BalancedMonoProcessor}
   */
  public static final <T> MonoFirstProcessorBuilder<T> first() {
		return new MonoFirstProcessorBuilder<>();
	}

	//=== BUILDERS to replace factory method only processors ===

	/**
	 * A builder for the {@link #unicast()} flavor of {@link BalancedFluxProcessor}.
	 *
	 * @param <T>
	 */
	public static final class UnicastProcessorBuilder<T> {

		private Queue<T> queue;
		private Consumer<? super T> onOverflow;
		private Disposable endcallback;

		/**
		 * By default, a unicast Processor is unbounded. This can be changed by using this
		 * method to provide a custom {@link Queue} implementation for the internal buffering.
		 * If that queue is bounded, the processor could reject the push of a value when the
		 * buffer is full and not enough requests from downstream have been received.
		 * <p>
		 * In that bounded case, one can also set a callback to be invoked on each rejected
		 * element, allowing for cleanup of these rejected elements (see {@link #onOverflow(Consumer)}).
		 *
		 * @param q the {@link Queue} to be used for internal buffering
		 * @return the builder
		 */
		public UnicastProcessorBuilder<T> queue(Queue<T> q) {
			this.queue = q;
			return this;
		}

		/**
		 * Set a callback that will be executed by the Processor on any terminating signal.
		 *
		 * @param e the callback
		 * @return the builder
		 */
		public UnicastProcessorBuilder<T> endCallback(Disposable e) {
			this.endcallback = e;
			return this;
		}

		/**
		 * When a bounded {@link #queue(Queue)} has been provided, set up a callback to
		 * be executed on every element rejected by the {@link Queue} once it is already
		 * full.
		 *
		 * @param c the cleanup consumer for overflowing elements in a bounded queue
		 * @return the builder
		 */
		public UnicastProcessorBuilder<T> onOverflow(Consumer<? super T> c) {
			this.onOverflow = c;
			return this;
		}

		/**
		 * Build the unicast {@link BalancedFluxProcessor} according to the builder's
		 * configuration.
		 *
		 * @return a new unicast {@link BalancedFluxProcessor Processor}
		 */
		public BalancedFluxProcessor<T> build() {
			if (queue != null && endcallback != null && onOverflow != null) {
				return UnicastProcessor.create(queue, onOverflow, endcallback);
			}
			else if (queue != null && endcallback != null) {
				return UnicastProcessor.create(queue, endcallback);
			}
			else if (queue != null && onOverflow == null) {
				return UnicastProcessor.create(queue);
			}
			else {
				return UnicastProcessor.create();
			}
		}
	}

	/**
	 * A builder for the {@link #emitter()} flavor of {@link BalancedFluxProcessor}.
	 *
	 * @param <T>
	 */
	public static final class EmitterProcessorBuilder<T> {

		private int bufferSize = -1;
		private boolean autoCancel = true;

		/**
		 * The replay capacity of the emitter Processor, which defines how many elements
		 * it will retain while it has no current {@link Subscriber}. Once that size is
		 * reached, if there still is no {@link Subscriber} the emitter processor will
		 * block its calls to {@link Processor#onNext(Object)} until it is drained (which
		 * can only happen concurrently by then).
		 * <p>
		 * The first {@link Subscriber} to subscribe receives all of these elements, and
		 * further subscribers only see new incoming data after that.
		 *
		 * @param bufferSize the size of the initial no-subscriber bounded buffer
		 * @return the builder
		 */
		public EmitterProcessorBuilder<T> bufferSize(int bufferSize) {
			this.bufferSize = bufferSize;
			return this;
		}

		/**
		 * By default, if all of its {@link Subscriber Subscribers} are cancelled (which
		 * basically means they have all un-subscribed), the emitter Processor will clear
		 * its internal buffer and stop accepting new subscribers. This method changes
		 * that so that the emitter processor keeps on running.
		 *
		 * @return the builder
		 */
		public EmitterProcessorBuilder<T> noAutoCancel() {
			this.autoCancel = false;
			return this;
		}

		/**
		 * Build the emitter {@link BalancedFluxProcessor} according to the builder's
		 * configuration.
		 *
		 * @return a new emitter {@link BalancedFluxProcessor}
		 */
		public BalancedFluxProcessor<T> build() {
			if (bufferSize != -1 && !autoCancel) {
				return EmitterProcessor.create(bufferSize, false);
			}
			else if (bufferSize != -1) {
				return EmitterProcessor.create(bufferSize);
			}
			else if (!autoCancel) {
				return EmitterProcessor.create(false);
			}
			else {
				return EmitterProcessor.create();
			}
		}
	}

	/**
	 * A builder for the {@link #replay()} flavor of {@link BalancedFluxProcessor}.
	 *
	 * @param <T>
	 */
	public static final class ReplayProcessorBuilder<T> {

		private int       size = -1;
		private Duration  maxAge = null;
		private Scheduler scheduler = null;
		private boolean   unbounded = true;

		/**
		 * Set the history capacity to a specific bounded size.
		 *
		 * @param size the history buffer capacity
		 * @return the builder, with a bounded capacity
		 */
		public ReplayProcessorBuilder<T> historySize(int size) {
			this.size = size;
			this.unbounded = false;
			return this;
		}

		/**
		 * Set the history capacity to a specific initial size but allow to still mark
		 * it as unbounded (which is ignored if combined with time-oriented bounds).
		 *
		 * @param size the history buffer capacity
		 * @param unbounded true if the buffer is to be unbounded and the size to be
		 * interpreted as a hint, false to keep it bounded
		 * @return the builder, with an optionally unbounded capacity
		 */
		public ReplayProcessorBuilder<T> historySize(int size, boolean unbounded) {
			this.size = size;
			this.unbounded = unbounded;
			return this;
		}

		/**
		 * Set a time bound to the history.
		 *
		 * @param maxAge the maximum {@link Duration} for which an element is retained and
		 * replayed
		 * @return the builder
		 * @see #maxAge(Duration, Scheduler)
		 */
		public ReplayProcessorBuilder<T> maxAge(Duration maxAge) {
			this.maxAge = maxAge;
			return this;
		}

		/**
		 * Set a time bound for the history, measuring time using the provided
		 * {@link Scheduler}.
		 *
		 * @param maxAge the maximum {@link Duration} for which an element is retained and
		 * replayed
		 * @param ttlScheduler the {@link Scheduler} to be used to enforce the TTL
		 * @return the builder
		 */
		public ReplayProcessorBuilder<T> maxAge(Duration maxAge, Scheduler ttlScheduler) {
			this.maxAge = maxAge;
			this.scheduler = ttlScheduler;
			return this;
		}

		/**
		 * Build the replay {@link BalancedFluxProcessor} according to the builder's configuration.
		 *
		 * @return a new replay {@link BalancedFluxProcessor}
		 */
		public BalancedFluxProcessor<T> build() {
			//replay size and timeout
			if (size != -1 && maxAge != null) {
				if (scheduler != null) {
					return ReplayProcessor.createSizeAndTimeout(size, maxAge, scheduler);
				}
				return ReplayProcessor.createSizeAndTimeout(size, maxAge);
			}

			if (size != -1) {
				if (unbounded) {
					return ReplayProcessor.create(size, true);
				}
				return ReplayProcessor.create(size);
			}

			if (maxAge != null) {
				if (scheduler != null) {
					return ReplayProcessor.createTimeout(maxAge, scheduler);
				}
				return ReplayProcessor.createTimeout(maxAge);
			}

			return ReplayProcessor.create();
		}
	}

	/**
	 * A builder for the {@link #fanOut()} flavor of {@link BalancedFluxProcessor}.
	 *
	 * @param <T>
	 */
	public interface FanOutProcessorBuilder<T> {

		/**
		 * Configures name for this builder. Name is reset to default if the provided
		 * <code>name</code> is null.
		 *
		 * @param name Use a new cached ExecutorService and assign this name to the created threads
		 *             if {@link #executor(ExecutorService)} is not configured.
		 * @return builder with provided name
		 */
		FanOutProcessorBuilder<T> name(@Nullable String name);

		/**
		 * Configures buffer size for this builder. Default value is {@link Queues#SMALL_BUFFER_SIZE}.
		 * @param bufferSize the internal buffer size to hold signals, must be a power of 2
		 * @return builder with provided buffer size
		 */
		FanOutProcessorBuilder bufferSize(int bufferSize);

		/**
		 * Configures wait strategy for this builder. Default value is {@link WaitStrategy#liteBlocking()}.
		 * Wait strategy is push to default if the provided <code>waitStrategy</code> is null.
		 * @param waitStrategy A WaitStrategy to use instead of the default smart blocking wait strategy.
		 * @return builder with provided wait strategy
		 */
		FanOutProcessorBuilder waitStrategy(@Nullable WaitStrategy waitStrategy);

		/**
		 * Configures auto-cancel for this builder. Default value is true.
		 * @param autoCancel automatically cancel
		 * @return builder with provided auto-cancel
		 */
		FanOutProcessorBuilder autoCancel(boolean autoCancel);

		/**
		 * Configures an {@link ExecutorService} to execute as many event-loop consuming the
		 * ringbuffer as subscribers. Name configured using {@link #name(String)} will be ignored
		 * if executor is push.
		 * @param executor A provided ExecutorService to manage threading infrastructure
		 * @return builder with provided executor
		 */
		FanOutProcessorBuilder executor(@Nullable ExecutorService executor);

		/**
		 * Configures an additional {@link ExecutorService} that is used internally
		 * on each subscription.
		 * @param requestTaskExecutor internal request executor
		 * @return builder with provided internal request executor
		 */
		FanOutProcessorBuilder requestTaskExecutor(
				@Nullable ExecutorService requestTaskExecutor);

		/**
		 * Configures sharing state for this builder. A shared Processor authorizes
		 * concurrent onNext calls and is suited for multi-threaded publisher that
		 * will fan-in data.
		 * @param share true to support concurrent onNext calls
		 * @return builder with specified sharing
		 */
		FanOutProcessorBuilder share(boolean share);

		/**
		 * Creates a new fanout {@link BalancedFluxProcessor} according to the builder's
		 * configuration.
		 *
		 * @return a new fanout {@link BalancedFluxProcessor}
		 */
		BalancedFluxProcessor<T> build();
	}

	/**
	 * A builder for the {@link #first()} flavor of {@link BalancedMonoProcessor}.
	 *
	 * @param <T>
	 */
	public static final class MonoFirstProcessorBuilder<T> {

		private Publisher<? extends  T> source;
		private WaitStrategy waitStrategy;

		/**
		 * Attach the resulting {@link BalancedMonoProcessor} to a source.
		 *
		 * @param source the source for the Processor, a {@link Publisher}
		 * @return the builder
		 */
		public MonoFirstProcessorBuilder<T> attachToSource(Publisher<? extends T> source) {
			this.source = source;
			return this;
		}

		/**
		 * Set a {@link WaitStrategy} to be used by the processor.
		 *
		 * @param waitStrategy the {@link WaitStrategy}
		 * @return the builder
		 */
		public MonoFirstProcessorBuilder<T> waitStrategy(WaitStrategy waitStrategy) {
			this.waitStrategy = waitStrategy;
			return this;
		}

		/**
		 * Build the last-{@link BalancedMonoProcessor} according to the builder's configuration.
		 *
		 * @return a new "last" {@link BalancedMonoProcessor}
		 */
		public BalancedMonoProcessor<T> build() {
			if (source != null && waitStrategy != null) {
				return new MonoProcessor<>(source, waitStrategy);
			}
			if (source != null) {
				return new MonoProcessor<>(source);
			}
			if (waitStrategy != null) {
				return MonoProcessor.create(waitStrategy);
			}
			return MonoProcessor.create();
		}
	}
}

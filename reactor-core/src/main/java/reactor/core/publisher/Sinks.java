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

import java.time.Duration;
import java.util.Queue;

import org.reactivestreams.Subscriber;

import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

/**
 * Sinks are constructs through which Reactive Streams signals can be programmatically pushed, with {@link Flux} or {@link Mono}
 * semantics. These standalone sinks expose {@link Many#tryEmitNext(Object) tryEmit} methods that return an {@link Emission} enum,
 * allowing to atomically fail in case the attempted signal is inconsistent with the spec and/or the state of the sink.
 * <p>
 * This class exposes a collection of ({@link Sinks.Many} builders and {@link Sinks.One} factories.
 *
 * @author Simon Baslé
 * @author Stephane Maldini
 */
public final class Sinks {

	private Sinks() {
	}

	/**
	 * A {@link Sinks.Empty} which exclusively produces one terminal signal: error or complete.
	 * It has the following characteristics:
	 * <ul>
	 *     <li>Multicast</li>
	 *     <li>Backpressure : this sink does not need any demand since it can only signal error or completion</li>
	 *     <li>Replaying: Replay the terminal signal (error or complete).</li>
	 * </ul>
	 * Use {@link Sinks.Empty#asMono()} to expose the {@link Mono} view of the sink to downstream consumers.
	 *
	 * @return a new {@link Sinks.Empty}
	 * @see RootSpec#empty()
	 */
	public static <T> Sinks.Empty<T> empty() {
		return SinksSpecs.DEFAULT_ROOT_SPEC.empty();
	}

	/**
	 * A {@link Sinks.One} that works like a conceptual promise: it can be completed
	 * with or without a value at any time, but only once. This completion is replayed to late subscribers.
	 * Calling {@link One#tryEmitValue(Object)} (or {@link One#emitValue(Object, EmitFailureHandler)}) is enough and will
	 * implicitly produce a {@link Subscriber#onComplete()} signal as well.
	 * <p>
	 * Use {@link One#asMono()} to expose the {@link Mono} view of the sink to downstream consumers.
	 *
	 * @return a new {@link Sinks.One}
	 * @see RootSpec#one()
	 */
	public static <T> Sinks.One<T> one() {
		return SinksSpecs.DEFAULT_ROOT_SPEC.one();
	}

	/**
	 * Help building {@link Sinks.Many} sinks that will broadcast multiple signals to one or more {@link Subscriber}.
	 * <p>
	 * Use {@link Many#asFlux()} to expose the {@link Flux} view of the sink to the downstream consumers.
	 *
	 * @return {@link ManySpec}
	 * @see RootSpec#many()
	 */
	public static ManySpec many() {
		return SinksSpecs.DEFAULT_ROOT_SPEC.many();
	}

	/**
	 * Return a {@link RootSpec root spec} for more advanced use cases such as building operators.
	 * Unsafe {@link Sinks.Many}, {@link Sinks.One} and {@link Sinks.Empty} are not serialized nor thread safe,
	 * which implies they MUST be externally synchronized so as to respect the Reactive Streams specification.
	 *
	 * @return {@link RootSpec}
	 */
	public static RootSpec unsafe() {
		return SinksSpecs.UNSAFE_ROOT_SPEC;
	}

	/**
	 * Represents the immediate status of a signal emission. This does not guarantee that a signal is consumed,
	 * it simply refers to the sink state when an emit method is invoked. This is a particularly important
	 * distinction with regards to {@link #FAIL_CANCELLED} which means the sink is -now- interrupted and emission can't
	 * proceed. Consequently, it is possible to emit a signal and obtain an "OK" status even if an in-flight cancellation
	 * is happening. This is due to the async nature of these actions: producer emits while consumer can interrupt independently.
	 */
	public enum Emission {
		/**
		 * Has successfully emitted the signal
		 */
		OK,
		/**
		 * Has failed to emit the signal because the sink was previously terminated successfully or with an error
		 */
		FAIL_TERMINATED,
		/**
		 * Has failed to emit the signal because the sink does not have buffering capacity left
		 */
		FAIL_OVERFLOW,
		/**
		 * Has failed to emit the signal because the sink was previously interrupted by its consumer
		 */
		FAIL_CANCELLED,
		/**
		 * Has failed to emit the signal because the access was not serialized
		 */
		FAIL_NON_SERIALIZED,
		/**
		 * Has failed to emit the signal because the sink has never been subscribed to has no capacity
		 * to buffer the signal.
		 */
		FAIL_ZERO_SUBSCRIBER;

		/**
		 * Has successfully emitted the signal
		 */
		public boolean hasSucceeded() {
			return this == OK;
		}

		/**
		 * Has failed to emit the signal.
		 */
		public boolean hasFailed() {
			return this != OK;
		}

		/**
		 * Easily convert from an {@link Emission} to throwing an exception on {@link #hasFailed() failure cases}.
		 * This is useful if throwing is the most relevant way of dealing with a failed emission attempt.
		 * Note however that generally Reactor code doesn't favor throwing exceptions but rather propagating
		 * them through onError signals.
		 * See also {@link #orThrowWithCause(Throwable)} in case of an {@link One#tryEmitError(Throwable) emitError}
		 * failure for which you want to attach the originally pushed {@link Exception}.
		 *
		 * @see #orThrowWithCause(Throwable)
		 */
		public void orThrow() {
			if (this == OK) return;

			throw new EmissionException(this);
		}

		/**
		 * Easily convert from an {@link Emission} to throwing an exception on {@link #hasFailed() failure cases}.
		 * This is useful if throwing is the most relevant way of dealing with failed {@link One#tryEmitError(Throwable) tryEmitError}
		 * attempt, in which case you probably wants to propagate the originally pushed {@link Exception}.
		 * Note however that generally Reactor code doesn't favor throwing exceptions but rather propagating
		 * them through onError signals.
		 *
		 * @see #orThrow()
		 */
		public void orThrowWithCause(Throwable cause) {
			if (this == OK) return;

			throw new EmissionException(cause, this);
		}
	}

	/**
	 * An exception representing a {@link Emission#hasFailed() failed} {@link Emission}.
	 * The exact type of failure can be found via {@link #getReason()}.
	 */
	public static final class EmissionException extends IllegalStateException {

		final Emission reason;

		public EmissionException(Emission reason) {
			this(reason, "Sink emission failed with " + reason);
		}

		public EmissionException(Throwable cause, Emission reason) {
			super("Sink emission failed with " + reason, cause);
			this.reason = reason;
		}

		public EmissionException(Emission reason, String message) {
			super(message);
			this.reason = reason;
		}

		/**
		 * Get the failure {@link Emission} code that is represented by this exception.
		 *
		 * @return the {@link Emission}
		 */
		public Emission getReason() {
			return this.reason;
		}
	}

	/**
	 * A handler supporting the emit API (eg. {@link Many#emitNext(Object, EmitFailureHandler)}),
	 * checking non-successful emission results from underlying {@link Many#tryEmitNext(Object) tryEmit}
	 * API calls to decide whether or not such calls should be retried.
	 * Other than instructing to retry, the handlers are allowed to have side effects
	 * like parking the current thread for longer retry loops. They don't, however, influence the
	 * exact action taken by the emit API implementations when the handler doesn't allow
	 * the retry to occur.
	 *
	 * @implNote It is expected that the handler may perform side effects (e.g. busy looping)
	 * and should not be considered a plain {@link java.util.function.Predicate}.
	 */
	public interface EmitFailureHandler {

		/**
		 * A pre-made handler that will not instruct to retry any failure
		 * and trigger the failure handling immediately.
		 */
		EmitFailureHandler FAIL_FAST = (signalType, emission) -> false;

		/**
		 * Decide whether the emission should be retried, depending on the provided {@link Emission}
		 * and the type of operation that was attempted (represented as a {@link SignalType}).
		 * Side effects are allowed.
		 *
		 * @param signalType the signal that triggered the emission. Can be either {@link SignalType#ON_NEXT}, {@link SignalType#ON_ERROR} or {@link SignalType#ON_COMPLETE}.
		 * @param emission the result of the emission (a failure)
		 * @return {@code true} if the operation should be retried, {@code false} otherwise.
		 */
		boolean onEmitFailure(SignalType signalType, Emission emission);
	}

	/**
	 * Provides a choice of {@link Sinks.One}/{@link Sinks.Empty} factories and
	 * {@link Sinks.ManySpec further specs} for {@link Sinks.Many}.
	 */
	public interface RootSpec {

		/**
		 * A {@link Sinks.Empty} which exclusively produces one terminal signal: error or complete.
		 * It has the following characteristics:
		 * <ul>
		 *     <li>Multicast</li>
		 *     <li>Backpressure : this sink does not need any demand since it can only signal error or completion</li>
		 *     <li>Replaying: Replay the terminal signal (error or complete).</li>
		 * </ul>
		 * Use {@link Sinks.Empty#asMono()} to expose the {@link Mono} view of the sink to downstream consumers.
		 */
		<T> Sinks.Empty<T> empty();

		/**
		 * A {@link Sinks.One} that works like a conceptual promise: it can be completed
		 * with or without a value at any time, but only once. This completion is replayed to late subscribers.
		 * Calling {@link One#emitValue(Object)} (or {@link One#tryEmitValue(Object)}) is enough and will
		 * implicitly produce a {@link Subscriber#onComplete()} signal as well.
		 * <p>
		 * Use {@link One#asMono()} to expose the {@link Mono} view of the sink to downstream consumers.
		 */
		<T> Sinks.One<T> one();

		/**
		 * Help building {@link Sinks.Many} sinks that will broadcast multiple signals to one or more {@link Subscriber}.
		 * <p>
		 * Use {@link Many#asFlux()} to expose the {@link Flux} view of the sink to the downstream consumers.
		 *
		 * @return {@link ManySpec}
		 */
		ManySpec many();
	}

	/**
	 * Provides {@link Sinks.Many} specs for sinks which can emit multiple elements
	 */
	public interface ManySpec {
		/**
		 * Help building {@link Sinks.Many} that will broadcast signals to a single {@link Subscriber}
		 *
		 * @return {@link UnicastSpec}
		 */
		UnicastSpec unicast();

		/**
		 * Help building {@link Sinks.Many} that will broadcast signals to multiple {@link Subscriber}
		 *
		 * @return {@link MulticastSpec}
		 */
		MulticastSpec multicast();

		/**
		 * Help building {@link Sinks.Many} that will broadcast signals to multiple {@link Subscriber} with the ability to retain
		 * and replay all or an arbitrary number of elements.
		 *
		 * @return {@link MulticastReplaySpec}
		 */
		MulticastReplaySpec replay();
	}

	/**
	 * Provides unicast: 1 sink, 1 {@link Subscriber}
	 */
	public interface UnicastSpec {

		/**
		 * A {@link Sinks.Many} with the following characteristics:
		 * <ul>
		 *     <li><strong>Unicast</strong>: contrary to most other {@link Sinks.Many}, the
		 *     {@link Flux} view rejects {@link Subscriber subscribers} past the first one.</li>
		 *     <li>Backpressure : this sink honors downstream demand of its single {@link Subscriber}.</li>
		 *     <li>Replaying: non-applicable, since only one {@link Subscriber} can register.</li>
		 *     <li>Without {@link Subscriber}: all elements pushed to this sink are remembered and will
		 *     be replayed once the {@link Subscriber} subscribes.</li>
		 * </ul>
		 */
		<T> Sinks.Many<T> onBackpressureBuffer();

		/**
		 * A {@link Sinks.Many} with the following characteristics:
		 * <ul>
		 *     <li><strong>Unicast</strong>: contrary to most other {@link Sinks.Many}, the
		 *     {@link Flux} view rejects {@link Subscriber subscribers} past the first one.</li>
		 *     <li>Backpressure : this sink honors downstream demand of its single {@link Subscriber}.</li>
		 *     <li>Replaying: non-applicable, since only one {@link Subscriber} can register.</li>
		 *    <li>Without {@link Subscriber}: depending on the queue, all elements pushed to this sink are remembered and will
		 * 		  be replayed once the {@link Subscriber} subscribes.</li>
		 * </ul>
		 *
		 * @param queue an arbitrary queue to use that must at least support Single Producer / Single Consumer semantics
		 */
		<T> Sinks.Many<T> onBackpressureBuffer(Queue<T> queue);

		/**
		 * A {@link Sinks.Many} with the following characteristics:
		 * <ul>
		 *     <li><strong>Unicast</strong>: contrary to most other {@link Sinks.Many}, the
		 *     {@link Flux} view rejects {@link Subscriber subscribers} past the first one.</li>
		 *     <li>Backpressure : this sink honors downstream demand of its single {@link Subscriber}.</li>
		 *     <li>Replaying: non-applicable, since only one {@link Subscriber} can register.</li>
		 *     <li>Without {@link Subscriber}: depending on the queue, all elements pushed to this sink are remembered and will
		 *     be replayed once the {@link Subscriber} subscribes.</li>
		 * </ul>
		 *
		 * @param queue an arbitrary queue to use that must at least support Single Producer / Single Consumer semantics
		 * @param endCallback when a terminal signal is observed: error, complete or cancel
		 */
		<T> Sinks.Many<T> onBackpressureBuffer(Queue<T> queue, Disposable endCallback);

		/**
		 * A {@link Sinks.Many} with the following characteristics:
		 * <ul>
		 *     <li><strong>Unicast</strong>: contrary to most other {@link Sinks.Many}, the
		 *     {@link Flux} view rejects {@link Subscriber subscribers} past the first one.</li>
		 *     <li>Backpressure : this sink honors downstream demand of the Subscriber, and will emit {@link Subscriber#onError(Throwable)} if there is a mismatch.</li>
		 *     <li>Replaying: No replay. Only forwards to a {@link Subscriber} the elements that have been
		 *     pushed to the sink AFTER this subscriber was subscribed.</li>
		 * </ul>
		 */
		<T> Sinks.Many<T> onBackpressureError();
	}

	/**
	 * Provides multicast : 1 sink, N {@link Subscriber}
	 */
	public interface MulticastSpec {

		/**
		 * A {@link Sinks.Many} with the following characteristics:
		 * <ul>
		 *     <li>Multicast</li>
		 *     <li>Without {@link Subscriber}: warm up. Remembers up to {@link Queues#SMALL_BUFFER_SIZE}
		 *     elements pushed via {@link Many#tryEmitNext(Object)} before the first {@link Subscriber} is registered.</li>
		 *     <li>Backpressure : this sink honors downstream demand by conforming to the lowest demand in case
		 *     of multiple subscribers.<br>If the difference between multiple subscribers is greater than {@link Queues#SMALL_BUFFER_SIZE}:
		 *          <ul><li>{@link Many#tryEmitNext(Object) tryEmitNext} will return {@link Emission#FAIL_OVERFLOW}</li>
		 * 	        <li>{@link Many#emitNext(Object, EmitFailureHandler) emitNext} will terminate the sink by {@link Many#emitError(Throwable, EmitFailureHandler) emitting}
		 *          an {@link Exceptions#failWithOverflow() overflow error}.</li></ul>
		 * 	   </li>
		 *     <li>Replaying: No replay of values seen by earlier subscribers. Only forwards to a {@link Subscriber}
		 *     the elements that have been pushed to the sink AFTER this subscriber was subscribed, or elements
		 *     that have been buffered due to backpressure/warm up.</li>
		 * </ul>
		 * <p>
		 * <img class="marble" src="doc-files/marbles/sinkWarmup.svg" alt="">
		 */
		<T> Sinks.Many<T> onBackpressureBuffer();

		/**
		 * A {@link Sinks.Many} with the following characteristics:
		 * <ul>
		 *     <li>Multicast</li>
		 *     <li>Without {@link Subscriber}: warm up. Remembers up to {@code bufferSize}
		 *     elements pushed via {@link Many#tryEmitNext(Object)} before the first {@link Subscriber} is registered.</li>
		 *     <li>Backpressure : this sink honors downstream demand by conforming to the lowest demand in case
		 *     of multiple subscribers.<br>If the difference between multiple subscribers is too high compared to {@code bufferSize}:
		 *          <ul><li>{@link Many#tryEmitNext(Object) tryEmitNext} will return {@link Emission#FAIL_OVERFLOW}</li>
		 *          <li>{@link Many#emitNext(Object, EmitFailureHandler) emitNext} will terminate the sink by {@link Many#emitError(Throwable, EmitFailureHandler) emitting}
		 *          an {@link Exceptions#failWithOverflow() overflow error}.</li></ul>
		 *     </li>
		 *     <li>Replaying: No replay of values seen by earlier subscribers. Only forwards to a {@link Subscriber}
		 *     the elements that have been pushed to the sink AFTER this subscriber was subscribed, or elements
		 *     that have been buffered due to backpressure/warm up.</li>
		 * </ul>
		 * <p>
		 * <img class="marble" src="doc-files/marbles/sinkWarmup.svg" alt="">
		 *
		 * @param bufferSize the maximum queue size
		 */
		<T> Sinks.Many<T> onBackpressureBuffer(int bufferSize);

		/**
		 * A {@link Sinks.Many} with the following characteristics:
		 * <ul>
		 *     <li>Multicast</li>
		 *     <li>Without {@link Subscriber}: warm up. Remembers up to {@code bufferSize}
		 *     elements pushed via {@link Many#tryEmitNext(Object)} before the first {@link Subscriber} is registered.</li>
		 *     <li>Backpressure : this sink honors downstream demand by conforming to the lowest demand in case
		 *     of multiple subscribers.<br>If the difference between multiple subscribers is too high compared to {@code bufferSize}:
		 *          <ul><li>{@link Many#tryEmitNext(Object) tryEmitNext} will return {@link Emission#FAIL_OVERFLOW}</li>
		 *          <li>{@link Many#emitNext(Object, EmitFailureHandler) emitNext} will terminate the sink by {@link Many#emitError(Throwable, EmitFailureHandler) emitting}
		 *          an {@link Exceptions#failWithOverflow() overflow error}.</li></ul>
		 *     </li>
		 *     <li>Replaying: No replay of values seen by earlier subscribers. Only forwards to a {@link Subscriber}
		 *     the elements that have been pushed to the sink AFTER this subscriber was subscribed, or elements
		 *     that have been buffered due to backpressure/warm up.</li>
		 * </ul>
		 * <p>
		 * <img class="marble" src="doc-files/marbles/sinkWarmup.svg" alt="">
		 *
		 * @param bufferSize the maximum queue size
		 * @param autoCancel should the sink fully shutdowns (not publishing anymore) when the last subscriber cancels
		 */
		<T> Sinks.Many<T> onBackpressureBuffer(int bufferSize, boolean autoCancel);

		/**
		 A {@link Sinks.Many} with the following characteristics:
		 * <ul>
		 *     <li>Multicast</li>
		 *     <li>Without {@link Subscriber}: fail fast on {@link Many#tryEmitNext(Object) tryEmitNext}.</li>
		 *     <li>Backpressure : notify the caller with {@link Emission#FAIL_OVERFLOW} if any of the subscribers
		 *     cannot process an element, failing fast and backing off from emitting the element at all (all or nothing).
		 * 	   From the perspective of subscribers, data is dropped and never seen but they are not terminated.
		 *     </li>
		 *     <li>Replaying: No replay of elements. Only forwards to a {@link Subscriber} the elements that
		 *     have been pushed to the sink AFTER this subscriber was subscribed, provided all of the subscribers
		 *     have demand.</li>
		 * </ul>
		 * <p>
		 * <img class="marble" src="doc-files/marbles/sinkDirectAllOrNothing.svg" alt="">
		 *
		 * @param <T> the type of elements to emit
		 * @return a multicast {@link Sinks.Many} that "drops" in case any subscriber is too slow
		 */
		<T> Sinks.Many<T> directAllOrNothing();

		/**
		 A {@link Sinks.Many} with the following characteristics:
		 * <ul>
		 *     <li>Multicast</li>
		 *     <li>Without {@link Subscriber}: fail fast on {@link Many#tryEmitNext(Object) tryEmitNext}.</li>
		 *     <li>Backpressure : notify the caller with {@link Emission#FAIL_OVERFLOW} if <strong>none</strong>
		 *     of the subscribers can process an element. Otherwise, it ignores slow subscribers and emits the
		 *     element to fast ones as a best effort. From the perspective of slow subscribers, data is dropped
		 *     and never seen, but they are not terminated.
		 *     </li>
		 *     <li>Replaying: No replay of elements. Only forwards to a {@link Subscriber} the elements that
		 *     have been pushed to the sink AFTER this subscriber was subscribed.</li>
		 * </ul>
		 * <p>
		 * <img class="marble" src="doc-files/marbles/sinkDirectBestEffort.svg" alt="">
		 *
		 * @param <T> the type of elements to emit
		 * @return a multicast {@link Sinks.Many} that "drops" in case of no demand from any subscriber
		 */
		<T> Sinks.Many<T> directBestEffort();
	}

	/**
	 * Provides multicast with history/replay capacity : 1 sink, N {@link Subscriber}
	 */
	public interface MulticastReplaySpec {
		/**
		 * A {@link Sinks.Many} with the following characteristics:
		 * <ul>
		 *     <li>Multicast</li>
		 *     <li>Without {@link Subscriber}: all elements pushed to this sink are remembered,
		 *     even when there is no subscriber.</li>
		 *     <li>Backpressure : this sink honors downstream demand of individual subscribers.</li>
		 *     <li>Replaying: all elements pushed to this sink are replayed to new subscribers.</li>
		 * </ul>
		 */
		<T> Sinks.Many<T> all();

		/**
		 * A {@link Sinks.Many} with the following characteristics:
		 * <ul>
		 *     <li>Multicast</li>
		 *     <li>Without {@link Subscriber}: all elements pushed to this sink are remembered,
		 *     even when there is no subscriber.</li>
		 *     <li>Backpressure : this sink honors downstream demand of individual subscribers.</li>
		 *     <li>Replaying: all elements pushed to this sink are replayed to new subscribers.</li>
		 * </ul>
		 * @param batchSize the underlying buffer will optimize storage by linked arrays of given size
		 */
		<T> Sinks.Many<T> all(int batchSize);

		/**
		 * A {@link Sinks.Many} with the following characteristics:
		 * <ul>
		 *     <li>Multicast</li>
		 *     <li>Without {@link Subscriber}: the latest element pushed to this sink are remembered,
		 *     even when there is no subscriber. Older elements are discarded</li>
		 *     <li>Backpressure : this sink honors downstream demand of individual subscribers.</li>
		 *     <li>Replaying: the latest element pushed to this sink is replayed to new subscribers.</li>
		 * </ul>
		 */
		<T> Sinks.Many<T> latest();

		/**
		 * A {@link Sinks.Many} with the following characteristics:
		 * <ul>
		 *     <li>Multicast</li>
		 *     <li>Without {@link Subscriber}: the latest element pushed to this sink are remembered,
		 *     even when there is no subscriber.</li>
		 *     <li>Backpressure : this sink honors downstream demand of individual subscribers.</li>
		 *     <li>Replaying: the latest element pushed to this sink is replayed to new subscribers. If none the default value is replayed</li>
		 * </ul>
		 *
		 * @param value default value if there is no latest element to replay
		 */
		<T> Sinks.Many<T> latestOrDefault(T value);

		/**
		 * A {@link Sinks.Many} with the following characteristics:
		 * <ul>
		 *     <li>Multicast</li>
		 *     <li>Without {@link Subscriber}: up to {@param historySize} elements pushed to this sink are remembered,
		 *     even when there is no subscriber. Older elements are discarded</li>
		 *     <li>Backpressure : this sink honors downstream demand of individual subscribers.</li>
		 *     <li>Replaying:  up to {@param historySize} elements pushed to this sink are replayed to new subscribers.
		 *     Older elements are discarded.</li>
		 * </ul>
		 *
		 * @param historySize maximum number of elements able to replayed
		 */
		<T> Sinks.Many<T> limit(int historySize);

		/**
		 * A {@link Sinks.Many} with the following characteristics:
		 * <ul>
		 *     <li>Multicast</li>
		 *     <li>Without {@link Subscriber}: up to {@param historySize} elements pushed to this sink are remembered,
		 *     even when there is no subscriber. Older elements are discarded</li>
		 *     <li>Backpressure : this sink honors downstream demand of individual subscribers.</li>
		 *     <li>Replaying:  up to {@param historySize} elements pushed to this sink are replayed to new subscribers.
		 *     Older elements are discarded.</li>
		 * </ul>
		 *
		 * @param maxAge maximum retention time for elements to be retained
		 */
		<T> Sinks.Many<T> limit(Duration maxAge);

		/**
		 * A {@link Sinks.Many} with the following characteristics:
		 * <ul>
		 *     <li>Multicast</li>
		 *     <li>Without {@link Subscriber}: all elements pushed to this sink are remembered until their {@param maxAge} is reached,
		 *     even when there is no subscriber. Older elements are discarded</li>
		 *     <li>Backpressure : this sink honors downstream demand of individual subscribers.</li>
		 *     <li>Replaying:  up to {@param historySize} elements pushed to this sink are replayed to new subscribers.
		 *     Older elements are discarded.</li>
		 * </ul>
		 * Note: Age is checked when a signal occurs, not using a background task.
		 *
		 * @param maxAge maximum retention time for elements to be retained
		 * @param scheduler a {@link Scheduler} to derive the time from
		 */
		<T> Sinks.Many<T> limit(Duration maxAge, Scheduler scheduler);

		/**
		 * A {@link Sinks.Many} with the following characteristics:
		 * <ul>
		 *     <li>Multicast</li>
		 *     <li>Without {@link Subscriber}: up to {@param historySize} elements pushed to this sink are remembered,
		 *     until their {@param maxAge} is reached, even when there is no subscriber. Older elements are discarded</li>
		 *     <li>Backpressure : this sink honors downstream demand of individual subscribers.</li>
		 *     <li>Replaying:  up to {@param historySize} elements pushed to this sink are replayed to new subscribers.
		 *     Older elements are discarded.</li>
		 * </ul>
		 * Note: Age is checked when a signal occurs, not using a background task.
		 *
		 * @param historySize maximum number of elements able to replayed
		 * @param maxAge maximum retention time for elements to be retained
		 */
		<T> Sinks.Many<T> limit(int historySize, Duration maxAge);

		/**
		 * A {@link Sinks.Many} with the following characteristics:
		 * <ul>
		 *     <li>Multicast</li>
		 *     <li>Without {@link Subscriber}: up to {@param historySize} elements pushed to this sink are remembered,
		 *     until their {@param maxAge} is reached, even when there is no subscriber. Older elements are discarded.</li>
		 *     <li>Backpressure : this sink honors downstream demand of individual subscribers.</li>
		 *     <li>Replaying:  up to {@param historySize} elements pushed to this sink are replayed to new subscribers.
		 *     Older elements are discarded.</li>
		 * </ul>
		 * Note: Age is checked when a signal occurs, not using a background task.
		 *
		 * @param historySize maximum number of elements able to replayed
		 * @param maxAge maximum retention time for elements to be retained
		 * @param scheduler a {@link Scheduler} to derive the time from
		 */
		<T> Sinks.Many<T> limit(int historySize, Duration maxAge, Scheduler scheduler);
	}

	/**
	 * A base interface for standalone {@link Sinks} with {@link Flux} semantics.
	 * <p>
	 * The sink can be exposed to consuming code as a {@link Flux} via its {@link #asFlux()} view.
	 *
	 * @author Simon Baslé
	 * @author Stephane Maldini
	 */
	public interface Many<T> extends Scannable {

		/**
		 * Try emitting a non-null element, generating an {@link Subscriber#onNext(Object) onNext} signal.
		 * The result of the attempt is represented as an {@link Emission}, which possibly indicates error cases.
		 * <p>
		 * Might throw an unchecked exception in case of a fatal error downstream which cannot
		 * be propagated to any asynchronous handler (aka a bubbling exception).
		 *
		 * @param t the value to emit, not null
		 * @return {@link Emission}
		 * @see Subscriber#onNext(Object)
		 */
		Emission tryEmitNext(T t);

		/**
		 * Try to terminate the sequence successfully, generating an {@link Subscriber#onComplete() onComplete}
		 * signal. The result of the attempt is represented as an {@link Emission}, which possibly indicates error cases.
		 *
		 * @return {@link Emission}
		 * @see Subscriber#onComplete()
		 */
		Emission tryEmitComplete();

		/**
		 * Try to fail the sequence, generating an {@link Subscriber#onError(Throwable) onError}
		 * signal. The result of the attempt is represented as an {@link Emission}, which possibly indicates error cases.
		 *
		 * @param error the exception to signal, not null
		 * @return {@link Emission}
		 * @see Subscriber#onError(Throwable)
		 */
		Emission tryEmitError(Throwable error);

		/**
		 * Emit a non-null element, generating an {@link Subscriber#onNext(Object) onNext} signal,
		 * or notifies the downstream subscriber(s) of a failure to do so via {@link #emitError(Throwable, EmitFailureHandler)}
		 * (with an {@link Exceptions#isOverflow(Throwable) overflow exception}).
		 * <p>
		 * Generally, {@link #tryEmitNext(Object)} is preferable since it allows a custom handling
		 * of error cases, although this implies checking the returned {@link Emission} and correctly
		 * acting on it (see implementation notes).
		 * <p>
		 * Might throw an unchecked exception in case of a fatal error downstream which cannot
		 * be propagated to any asynchronous handler (aka a bubbling exception).
		 *
		 * @implNote Implementors should typically delegate to {@link #tryEmitNext(Object)} and act on
		 * failures: {@link Emission#FAIL_OVERFLOW} should lead to {@link Operators#onDiscard(Object, Context)} followed
		 * by {@link #emitError(Throwable, EmitFailureHandler)}. {@link Emission#FAIL_CANCELLED} should lead to {@link Operators#onDiscard(Object, Context)}.
		 * {@link Emission#FAIL_TERMINATED} should lead to {@link Operators#onNextDropped(Object, Context)}.
		 * @implNote the duality between this method and {@link #tryEmitNext(Object)} is expected.
		 *
		 * @param t the value to emit, not null
		 * @param failureHandler the failure handler that allows retrying failed {@link Emission}.
		 * @throws EmissionException on non-serialized access
		 * @see #tryEmitNext(Object)
		 * @see Subscriber#onNext(Object)
		 */
		void emitNext(T t, EmitFailureHandler failureHandler);

		/**
		 * Terminate the sequence successfully, generating an {@link Subscriber#onComplete() onComplete}
		 * signal.
		 * <p>
		 * Generally, {@link #tryEmitComplete()} is preferable, since it allows a custom handling
		 * of error cases.
		 *
		 * @implNote Implementors should typically delegate to {@link #tryEmitComplete()}. Failure {@link Emission}
		 * don't need any particular handling where emitComplete is concerned.
		 * @implNote the duality between this method and {@link #tryEmitComplete()} is expected.
		 *
		 * @param failureHandler the failure handler that allows retrying failed {@link Emission}.
		 * @throws EmissionException on non-serialized access
		 * @see #tryEmitComplete()
		 * @see Subscriber#onComplete()
		 */
		void emitComplete(EmitFailureHandler failureHandler);

		/**
		 * Fail the sequence, generating an {@link Subscriber#onError(Throwable) onError}
		 * signal.
		 * <p>
		 * Generally, {@link #tryEmitError(Throwable)} is preferable since it allows a custom handling
		 * of error cases, although this implies checking the returned {@link Emission} and correctly
		 * acting on it (see implementation notes).
		 *
		 * @implNote Implementors should typically delegate to {@link #tryEmitError(Throwable)} and act on
		 * {@link Emission#FAIL_TERMINATED} by calling {@link Operators#onErrorDropped(Throwable, Context)}.
		 * @implNote the duality between this method and {@link #tryEmitError(Throwable)} is expected.
		 *
		 * @param error the exception to signal, not null
		 * @param failureHandler the failure handler that allows retrying failed {@link Emission}.
		 * @throws EmissionException on non-serialized access
		 * @see #tryEmitError(Throwable)
		 * @see Subscriber#onError(Throwable)
		 */
		void emitError(Throwable error, EmitFailureHandler failureHandler);

		/**
		 * Get how many {@link Subscriber Subscribers} are currently subscribed to the sink.
		 * <p>
		 * This is a best effort peek at the sink state, and a subsequent attempt at emitting
		 * to the sink might still return {@link Emission#FAIL_ZERO_SUBSCRIBER} where relevant.
		 * (generally in {@link #tryEmitNext(Object)}). Request (and lack thereof) isn't taken
		 * into account, all registered subscribers are counted.
		 *
		 * @return the number of subscribers at the time of invocation
		 */
		int currentSubscriberCount();

		/**
		 * Return a {@link Flux} view of this sink. Every call returns the same instance.
		 *
		 * @return the {@link Flux} view associated to this {@link Sinks.Many}
		 */
		Flux<T> asFlux();
	}

	/**
	 * A base interface for standalone {@link Sinks} with complete-or-fail semantics.
	 * <p>
	 * The sink can be exposed to consuming code as a {@link Mono} via its {@link #asMono()} view.
	 *
	 * @param <T> a generic type for the {@link Mono} view, allowing composition
	 * @author Simon Baslé
	 * @author Stephane Maldini
	 */
	public interface Empty<T> extends Scannable {

		/**
		 * Try to complete the {@link Mono} without a value, generating only an {@link Subscriber#onComplete() onComplete} signal.
		 * The result of the attempt is represented as an {@link Emission}, which possibly indicates error cases.
		 *
		 * @return {@link Emission}
		 * @see #emitEmpty(EmitFailureHandler)
		 * @see Subscriber#onComplete()
		 */
		Emission tryEmitEmpty();

		/**
		 * Try to fail the {@link Mono}, generating only an {@link Subscriber#onError(Throwable) onError} signal.
		 * The result of the attempt is represented as an {@link Emission}, which possibly indicates error cases.
		 *
		 * @param error the exception to signal, not null
		 * @return {@link Emission}
		 * @see #emitError(Throwable, EmitFailureHandler)
		 * @see Subscriber#onError(Throwable)
		 */
		Emission tryEmitError(Throwable error);

		/**
		 * Terminate the sequence successfully, generating an {@link Subscriber#onComplete() onComplete}
		 * signal.
		 * <p>
		 * Generally, {@link #tryEmitEmpty()} is preferable, since it allows a custom handling
		 * of error cases.
		 *
		 * @implNote Implementors should typically delegate to {@link #tryEmitEmpty()}. Failure {@link Emission}
		 * don't need any particular handling where emitEmpty is concerned.
		 * @implNote the duality between this method and {@link #tryEmitEmpty()} is expected.
		 *
		 * @param failureHandler the failure handler that allows retrying failed {@link Emission}.
		 * @throws EmissionException on non-serialized access
		 * @see #tryEmitEmpty()
		 * @see Subscriber#onComplete()
		 */
		void emitEmpty(EmitFailureHandler failureHandler);

		/**
		 * Fail the sequence, generating an {@link Subscriber#onError(Throwable) onError}
		 * signal.
		 * <p>
		 * Generally, {@link #tryEmitError(Throwable)} is preferable since it allows a custom handling
		 * of error cases, although this implies checking the returned {@link Emission} and correctly
		 * acting on it (see implementation notes).
		 *
		 * @implNote Implementors should typically delegate to {@link #tryEmitError(Throwable)} and act on
		 * {@link Emission#FAIL_TERMINATED} by calling {@link Operators#onErrorDropped(Throwable, Context)}.
		 * @implNote the duality between this method and {@link #tryEmitError(Throwable)} is expected.
		 *
		 * @param error the exception to signal, not null
		 * @param failureHandler the failure handler that allows retrying failed {@link Emission}.
		 * @throws EmissionException on non-serialized access
		 * @see #tryEmitError(Throwable)
		 * @see Subscriber#onError(Throwable)
		 */
		void emitError(Throwable error, EmitFailureHandler failureHandler);

		/**
		 * Get how many {@link Subscriber Subscribers} are currently subscribed to the sink.
		 * <p>
		 * This is a best effort peek at the sink state, and a subsequent attempt at emitting
		 * to the sink might still return {@link Emission#FAIL_ZERO_SUBSCRIBER} where relevant.
		 * Request (and lack thereof) isn't taken into account, all registered subscribers are counted.
		 *
		 * @return the number of active subscribers at the time of invocation
		 */
		int currentSubscriberCount();

		/**
		 * Return a {@link Mono} view of this sink. Every call returns the same instance.
		 *
		 * @return the {@link Mono} view associated to this {@link Sinks.One}
		 */
		Mono<T> asMono();
	}

	/**
	 * A base interface for standalone {@link Sinks} with {@link Mono} semantics.
	 * <p>
	 * The sink can be exposed to consuming code as a {@link Mono} via its {@link #asMono()} view.
	 *
	 * @author Simon Baslé
	 * @author Stephane Maldini
	 */
	public interface One<T> extends Empty<T> {

		/**
		 * Try to complete the {@link Mono} with an element, generating an {@link Subscriber#onNext(Object) onNext} signal
		 * immediately followed by an {@link Subscriber#onComplete() onComplete} signal. A {@code null} value
		 * will only trigger the onComplete. The result of the attempt is represented as an {@link Emission},
		 * which possibly indicates error cases.
		 * <p>
		 * Might throw an unchecked exception in case of a fatal error downstream which cannot
		 * be propagated to any asynchronous handler (aka a bubbling exception).
		 *
		 * @param value the value to emit and complete with, or {@code null} to only trigger an onComplete
		 * @return {@link Emission}
		 * @see #emitValue(Object, EmitFailureHandler)
		 * @see Subscriber#onNext(Object)
		 * @see Subscriber#onComplete()
		 */
		Emission tryEmitValue(@Nullable T value);

		/**
		 * Emit a non-null element, generating an {@link Subscriber#onNext(Object) onNext} signal
		 * immediately followed by an {@link Subscriber#onComplete() onComplete} signal,
		 * or notifies the downstream subscriber(s) of a failure to do so via {@link #emitError(Throwable, EmitFailureHandler)}
		 * (with an {@link Exceptions#isOverflow(Throwable) overflow exception}).
		 * <p>
		 * Generally, {@link #tryEmitValue(Object)} is preferable since it allows a custom handling
		 * of error cases, although this implies checking the returned {@link Emission} and correctly
		 * acting on it (see implementation notes).
		 * <p>
		 * Might throw an unchecked exception in case of a fatal error downstream which cannot
		 * be propagated to any asynchronous handler (aka a bubbling exception).
		 *
		 * @implNote Implementors should typically delegate to {@link #tryEmitValue (Object)} and act on
		 * failures: {@link Emission#FAIL_OVERFLOW} should lead to {@link Operators#onDiscard(Object, Context)} followed
		 * by {@link #emitError(Throwable, EmitFailureHandler)}. {@link Emission#FAIL_CANCELLED} should lead to {@link Operators#onDiscard(Object, Context)}.
		 * {@link Emission#FAIL_TERMINATED} should lead to {@link Operators#onNextDropped(Object, Context)}.
		 * @implNote the duality between this method and {@link #tryEmitValue(Object)} is expected.
		 *
		 * @param value the value to emit and complete with, or {@code null} to only trigger an onComplete
		 * @param failureHandler the failure handler that allows retrying failed {@link Emission}.
		 * @throws EmissionException on non-serialized access
		 * @see #tryEmitValue(Object)
		 * @see Subscriber#onNext(Object)
		 * @see Subscriber#onComplete()
		 */
		void emitValue(@Nullable T value, EmitFailureHandler failureHandler);
	}
}

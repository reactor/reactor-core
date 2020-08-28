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
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

/**
 * Sinks are constructs through which Reactive Streams signals can be programmatically pushed, with {@link Flux} or {@link Mono}
 * semantics. These standalone sinks expose {@code emit} methods that return an {@link Emission} enum, allowing to
 * softly fail in case the attempted signal is inconsistent with the spec and/or the state of the sink.
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
	 */
	public static <T> Sinks.Empty<T> empty() {
		return new VoidProcessor<T>();
	}

	/**
	 * A {@link Sinks.One} that works like a conceptual promise: it can be completed
	 * with or without a value at any time, but only once. This completion is replayed to late subscribers.
	 * Calling {@link One#emitValue(Object)} (or {@link One#tryEmitValue(Object)}) is enough and will
	 * implicitly produce a {@link Subscriber#onComplete()} signal as well.
	 * <p>
	 * Use {@link One#asMono()} to expose the {@link Mono} view of the sink to downstream consumers.
	 */
	public static <T> Sinks.One<T> one() {
		return new NextProcessor<>(null);
	}

	/**
	 * Help building {@link Sinks.Many} sinks that will broadcast multiple signals to one or more {@link Subscriber}.
	 * <p>
	 * Use {@link Many#asFlux()} to expose the {@link Flux} view of the sink to the downstream consumers.
	 *
	 * @return {@link ManySpec}
	 */
	public static ManySpec many() {
		return SinksSpecs.MANY_SPEC;
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
		FAIL_CANCELLED;

		/**
		 * Has successfully emitted the signal
		 */
		public boolean hasSucceeded() {
			return this == OK;
		}

		/**
		 * Has failed to emit the signal because the sink was previously terminated successfully or with an error, or
		 * has been cancelled or has overflowed its buffering capacity in terms of backpressure.
		 */
		public boolean hasFailed() {
			return this != OK;
		}

		/**
		 * Easily convert from an {@link Emission} to throwing an exception on {@link #hasFailed() failure cases}.
		 * This is useful if throwing is the most relevant way of dealing with a failed emission attempt.
		 * See also {@link #orThrowWithCause(Throwable)} in case of an {@link One#emitError(Throwable) emitError}
		 * failure for which you want to propagate the originally pushed {@link Exception}.
		 *
		 * @see #orThrowWithCause(Throwable)
		 */
		public void orThrow() {
			if (this == OK) return;

			throw new EmissionException(this);
		}

		/**
		 * Easily convert from an {@link Emission} to throwing an exception on {@link #hasFailed() failure cases}.
		 * This is useful if throwing is the most relevant way of dealing with failed {@link One#emitError(Throwable)}
		 * attempt, in which case you probably wants to propagate the originally pushed {@link Exception}.
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
			super("Sink emission failed with " + reason);
			this.reason = reason;
		}

		public EmissionException(Throwable cause, Emission reason) {
			super("Sink emission failed with " + reason, cause);
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

		/**
		 * Return a builder for more advanced use cases such as building operators.
		 * Unsafe {@link Sinks.Many} are not serialized and expect usage to be externally synchronized to respect
		 * the Reactive Streams specification.
		 *
		 * @return {@link ManySpec}
		 */
		ManySpec unsafe();
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
		 *     <li>Backpressure : this sink is able to honor downstream demand and will emit `onError` if there is a mismatch.</li>
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
		 *     of multiple subscribers.</li>
		 *     <li>Replaying: No replay. Only forwards to a {@link Subscriber} the elements that have been
		 *     pushed to the sink AFTER this subscriber was subscribed. To the exception of the first
		 *     subscriber (see below).</li>
		 * </ul>
		 * <p>
		 * <img class="marble" src="doc-files/marbles/sinkWarmup.svg" alt="">
		 */
		<T> Sinks.Many<T> onBackpressureBuffer();

		/**
		 * A {@link Sinks.Many} with the following characteristics:
		 * <ul>
		 *     <li>Multicast</li>
		 *     <li>Without {@link Subscriber}: warm up. Remembers up to {@param bufferSize}
		 *     elements pushed via {@link Many#tryEmitNext(Object)} before the first {@link Subscriber} is registered.</li>
		 *     <li>Backpressure : this sink honors downstream demand by conforming to the lowest demand in case
		 *     of multiple subscribers.</li>
		 *     <li>Replaying: No replay. Only forwards to a {@link Subscriber} the elements that have been
		 *     pushed to the sink AFTER this subscriber was subscribed. To the exception of the first
		 *     subscriber (see below).</li>
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
		 *     <li>Without {@link Subscriber}: warm up. Remembers up to {@param bufferSize}
		 *     elements pushed via {@link Many#tryEmitNext(Object)} before the first {@link Subscriber} is registered.</li>
		 *     <li>Backpressure : this sink honors downstream demand by conforming to the lowest demand in case
		 *     of multiple subscribers.</li>
		 *     <li>Replaying: No replay. Only forwards to a {@link Subscriber} the elements that have been
		 *     pushed to the sink AFTER this subscriber was subscribed. To the exception of the first
		 *     subscriber (see below).</li>
		 * </ul>
		 * <p>
		 * <img class="marble" src="doc-files/marbles/sinkWarmup.svg" alt="">
		 *
		 * @param bufferSize the maximum queue size
		 * @param autoCancel should the sink fully shutdowns (not publishing anymore) when the last subscriber cancels
		 */
		<T> Sinks.Many<T> onBackpressureBuffer(int bufferSize, boolean autoCancel);

		/**
		 * A {@link Sinks.Many} with the following characteristics:
		 * <ul>
		 *     <li>Multicast</li>
		 *     <li>Without {@link Subscriber}: elements pushed via {@link Many#tryEmitNext(Object)} are ignored</li>
		 *     <li>Backpressure : this sink is not able to honor downstream demand and will emit `onError` if there is a mismatch.</li>
		 *     <li>Replaying: No replay. Only forwards to a {@link Subscriber} the elements that have been
		 *     pushed to the sink AFTER this subscriber was subscribed.</li>
		 * </ul>
		 */
		<T> Sinks.Many<T> onBackpressureError();
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
	public interface Many<T> {

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
		 * or notifies the downstream subscriber(s) of a failure to do so via {@link #emitError(Throwable)}
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
		 * by {@link #emitError(Throwable)}. {@link Emission#FAIL_CANCELLED} should lead to {@link Operators#onDiscard(Object, Context)}.
		 * {@link Emission#FAIL_TERMINATED} should lead to {@link Operators#onNextDropped(Object, Context)}.
		 *
		 * @param t the value to emit, not null
		 * @see #tryEmitNext(Object)
		 * @see Subscriber#onNext(Object)
		 *
		 */
		void emitNext(T t);

		/**
		 * Terminate the sequence successfully, generating an {@link Subscriber#onComplete() onComplete}
		 * signal.
		 * <p>
		 * Generally, {@link #tryEmitComplete()} is preferable, since it allows a custom handling
		 * of error cases.
		 *
		 * @implNote Implementors should typically delegate to {@link #tryEmitComplete()}. Failure {@link Emission}
		 * don't need any particular handling where emitComplete is concerned.
		 *
		 * @see #tryEmitComplete()
		 * @see Subscriber#onComplete()
		 */
		void emitComplete();

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
		 *
		 * @param error the exception to signal, not null
		 * @see #tryEmitError(Throwable)
		 * @see Subscriber#onError(Throwable)
		 */
		void emitError(Throwable error);

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
	public interface Empty<T> {

		/**
		 * Try to complete the {@link Mono} without a value, generating only an {@link Subscriber#onComplete() onComplete} signal.
		 * The result of the attempt is represented as an {@link Emission}, which possibly indicates error cases.
		 *
		 * @return {@link Emission}
		 * @see #emitEmpty()
		 * @see Subscriber#onComplete()
		 */
		Emission tryEmitEmpty();

		/**
		 * Try to fail the {@link Mono}, generating only an {@link Subscriber#onError(Throwable) onError} signal.
		 * The result of the attempt is represented as an {@link Emission}, which possibly indicates error cases.
		 *
		 * @param error the exception to signal, not null
		 * @return {@link Emission}
		 * @see #emitError(Throwable)
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
		 *
		 * @see #tryEmitEmpty()
		 * @see Subscriber#onComplete()
		 */
		void emitEmpty();

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
		 *
		 * @param error the exception to signal, not null
		 * @see #tryEmitError(Throwable)
		 * @see Subscriber#onError(Throwable)
		 */
		void emitError(Throwable error);

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
		 * @see #emitValue(Object)
		 * @see Subscriber#onNext(Object)
		 * @see Subscriber#onComplete()
		 */
		Emission tryEmitValue(@Nullable T value);

		/**
		 * Emit a non-null element, generating an {@link Subscriber#onNext(Object) onNext} signal
		 * immediately followed by an {@link Subscriber#onComplete() onComplete} signal,
		 * or notifies the downstream subscriber(s) of a failure to do so via {@link #emitError(Throwable)}
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
		 * by {@link #emitError(Throwable)}. {@link Emission#FAIL_CANCELLED} should lead to {@link Operators#onDiscard(Object, Context)}.
		 * {@link Emission#FAIL_TERMINATED} should lead to {@link Operators#onNextDropped(Object, Context)}.
		 *
		 * @param value the value to emit and complete with, or {@code null} to only trigger an onComplete
		 * @see #tryEmitValue(Object)
		 * @see Subscriber#onNext(Object)
		 * @see Subscriber#onComplete()
		 *
		 */
		void emitValue(@Nullable T value);

	}
}
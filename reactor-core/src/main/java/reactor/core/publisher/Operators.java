/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Queue;
import java.util.Spliterator;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.Fuseable.QueueSubscription;
import reactor.core.Scannable;
import reactor.core.Scannable.Attr.RunStyle;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import static reactor.core.Fuseable.NONE;

/**
 * A helper to support "Operator" writing, handle noop subscriptions, validate request
 * size and to cap concurrent additive operations to Long.MAX_VALUE,
 * which is generic to {@link Subscription#request(long)} handling.
 *
 * Combine utils available to operator implementations, @see https://github.com/reactor/reactive-streams-commons
 *
 */
public abstract class Operators {

	/**
	 * Cap an addition to Long.MAX_VALUE
	 *
	 * @param a left operand
	 * @param b right operand
	 *
	 * @return Addition result or Long.MAX_VALUE if overflow
	 */
	public static long addCap(long a, long b) {
		long res = a + b;
		if (res < 0L) {
			return Long.MAX_VALUE;
		}
		return res;
	}

	/**
	 * Concurrent addition bound to Long.MAX_VALUE.
	 * Any concurrent write will "happen before" this operation.
	 *
	 * @param <T> the parent instance type
	 * @param updater  current field updater
	 * @param instance current instance to update
	 * @param toAdd    delta to add
	 * @return value before addition or Long.MAX_VALUE
	 */
	public static <T> long addCap(AtomicLongFieldUpdater<T> updater, T instance, long toAdd) {
		long r, u;
		for (;;) {
			r = updater.get(instance);
			if (r == Long.MAX_VALUE) {
				return Long.MAX_VALUE;
			}
			u = addCap(r, toAdd);
			if (updater.compareAndSet(instance, r, u)) {
				return r;
			}
		}
	}
	/**
	 * Returns the subscription as QueueSubscription if possible or null.
	 * @param <T> the value type of the QueueSubscription.
	 * @param s the source subscription to try to convert.
	 * @return the QueueSubscription instance or null
	 */
	@SuppressWarnings("unchecked")
	@Nullable
	public static <T> QueueSubscription<T> as(Subscription s) {
		if (s instanceof QueueSubscription) {
			return (QueueSubscription<T>) s;
		}
		return null;
	}

	/**
	 * A singleton Subscription that represents a cancelled subscription instance and
	 * should not be leaked to clients as it represents a terminal state. <br> If
	 * algorithms need to hand out a subscription, replace this with a singleton
	 * subscription because there is no standard way to tell if a Subscription is cancelled
	 * or not otherwise.
	 *
	 * @return a singleton noop {@link Subscription} to be used as an inner representation
	 * of the cancelled state
	 */
	public static Subscription cancelledSubscription() {
		return CancelledSubscription.INSTANCE;
	}

	/**
	 * Calls onSubscribe on the target Subscriber with the empty instance followed by a call to onComplete.
	 *
	 * @param s the target subscriber
	 */
	public static void complete(Subscriber<?> s) {
		s.onSubscribe(EmptySubscription.INSTANCE);
		s.onComplete();
	}

	/**
	 * Return a singleton {@link Subscriber} that does not check for double onSubscribe
	 * and purely request Long.MAX. If an error is received it will raise a
	 * {@link Exceptions#errorCallbackNotImplemented(Throwable)} in the receiving thread.
	 *
	 * @return a new {@link Subscriber} whose sole purpose is to request Long.MAX
	 */
	@SuppressWarnings("unchecked")
	public static <T> CoreSubscriber<T> drainSubscriber() {
		return (CoreSubscriber<T>)DrainSubscriber.INSTANCE;
	}

	/**
	 * A {@link Subscriber} that is expected to be used as a placeholder and
	 * never actually be called. All methods log an error.
	 *
	 * @param <T> the type of data (ignored)
	 * @return a placeholder subscriber
	 */
	@SuppressWarnings("unchecked")
	public static <T> CoreSubscriber<T> emptySubscriber() {
		return (CoreSubscriber<T>) EMPTY_SUBSCRIBER;
	}

	/**
	 * A singleton enumeration that represents a no-op Subscription instance that
	 * can be freely given out to clients.
	 * <p>
	 * The enum also implements Fuseable.QueueSubscription so operators expecting a
	 * QueueSubscription from a Fuseable source don't have to double-check their Subscription
	 * received in onSubscribe.
	 *
	 * @return a singleton noop {@link Subscription}
	 */
	public static Subscription emptySubscription() {
		return EmptySubscription.INSTANCE;
	}

	/**
	 * Check whether the provided {@link Subscription} is the one used to satisfy Spec's §1.9 rule
	 * before signalling an error.
	 *
	 * @param subscription the subscription to test.
	 * @return true if passed subscription is a subscription created in {@link #reportThrowInSubscribe(CoreSubscriber, Throwable)}.
	 */
	public static boolean canAppearAfterOnSubscribe(Subscription subscription) {
		return subscription == EmptySubscription.FROM_SUBSCRIBE_INSTANCE;
	}

	/**
	 * Calls onSubscribe on the target Subscriber with the empty instance followed by a call to onError with the
	 * supplied error.
	 *
	 * @param s target Subscriber to error
	 * @param e the actual error
	 */
	public static void error(Subscriber<?> s, Throwable e) {
		s.onSubscribe(EmptySubscription.INSTANCE);
		s.onError(e);
	}

	/**
	 * Report a {@link Throwable} that was thrown from a call to {@link Publisher#subscribe(Subscriber)},
	 * attempting to notify the {@link Subscriber} by:
	 * <ol>
	 *     <li>providing a special {@link Subscription} via {@link Subscriber#onSubscribe(Subscription)}</li>
	 *     <li>immediately delivering an {@link Subscriber#onError(Throwable) onError} signal after that</li>
	 * </ol>
	 * <p>
	 * As at that point the subscriber MAY have already been provided with a {@link Subscription}, we
	 * assume most well formed subscribers will ignore this second {@link Subscription} per Reactive
	 * Streams rule 1.9. Subscribers that don't usually ignore may recognize this special case and ignore
	 * it by checking {@link #canAppearAfterOnSubscribe(Subscription)}.
	 * <p>
	 * Note that if the {@link Subscriber#onSubscribe(Subscription) onSubscribe} attempt throws,
	 * {@link Exceptions#throwIfFatal(Throwable) fatal} exceptions are thrown. Other exceptions
	 * are added as {@link Throwable#addSuppressed(Throwable) suppressed} on the original exception,
	 * which is then directly notified as an {@link Subscriber#onError(Throwable) onError} signal
	 * (again assuming that such exceptions occur because a {@link Subscription} is already set).
	 *
	 * @param subscriber the {@link Subscriber} being subscribed when the error happened
	 * @param e the {@link Throwable} that was thrown from {@link Publisher#subscribe(Subscriber)}
	 * @see #canAppearAfterOnSubscribe(Subscription)
	 */
	public static void reportThrowInSubscribe(CoreSubscriber<?> subscriber, Throwable e) {
		try {
			subscriber.onSubscribe(EmptySubscription.FROM_SUBSCRIBE_INSTANCE);
		}
		catch (Throwable onSubscribeError) {
			Exceptions.throwIfFatal(onSubscribeError);
			e.addSuppressed(onSubscribeError);
		}
		subscriber.onError(onOperatorError(e, subscriber.currentContext()));
	}

	/**
	 * Create a function that can be used to support a custom operator via
	 * {@link CoreSubscriber} decoration. The function is compatible with
	 * {@link Flux#transform(Function)}, {@link Mono#transform(Function)},
	 * {@link Hooks#onEachOperator(Function)} and {@link Hooks#onLastOperator(Function)},
	 * but requires that the original {@link Publisher} be {@link Scannable}.
	 * <p>
	 * This variant attempts to expose the {@link Publisher} as a {@link Scannable} for
	 * convenience of introspection. You should however avoid instanceof checks or any
	 * other processing that depends on identity of the {@link Publisher}, as it might
	 * get hidden if {@link Scannable#isScanAvailable()} returns {@code false}.
	 * Use {@link #liftPublisher(BiFunction)} instead for that kind of use case.
	 *
	 * @param lifter the bifunction taking {@link Scannable} from the enclosing
	 * publisher (assuming it is compatible) and consuming {@link CoreSubscriber}.
	 * It must return a receiving {@link CoreSubscriber} that will immediately subscribe
	 * to the applied {@link Publisher}.
	 *
	 * @param <I> the input type
	 * @param <O> the output type
	 *
	 * @return a new {@link Function}
	 * @see #liftPublisher(BiFunction)
	 */
	public static <I, O> Function<? super Publisher<I>, ? extends Publisher<O>> lift(BiFunction<Scannable, ? super CoreSubscriber<? super O>, ? extends CoreSubscriber<? super I>> lifter) {
		return LiftFunction.liftScannable(null, lifter);
	}

	/**
	 * Create a function that can be used to support a custom operator via
	 * {@link CoreSubscriber} decoration. The function is compatible with
	 * {@link Flux#transform(Function)}, {@link Mono#transform(Function)},
	 * {@link Hooks#onEachOperator(Function)} and {@link Hooks#onLastOperator(Function)},
	 * but requires that the original {@link Publisher} be {@link Scannable}.
	 * <p>
	 * This variant attempts to expose the {@link Publisher} as a {@link Scannable} for
	 * convenience of introspection. You should however avoid instanceof checks or any
	 * other processing that depends on identity of the {@link Publisher}, as it might
	 * get hidden if {@link Scannable#isScanAvailable()} returns {@code false}.
	 * Use {@link #liftPublisher(Predicate, BiFunction)} instead for that kind of use case.
	 *
	 * <p>
	 * The function will be invoked only if the passed {@link Predicate} matches.
	 * Therefore the transformed type O must be the same than the input type since
	 * unmatched predicate will return the applied {@link Publisher}.
	 *
	 * @param filter the predicate to match taking {@link Scannable} from the applied
	 * publisher to operate on. Assumes original is scan-compatible.
	 * @param lifter the bifunction taking {@link Scannable} from the enclosing
	 * publisher and consuming {@link CoreSubscriber}. It must return a receiving
	 * {@link CoreSubscriber} that will immediately subscribe to the applied
	 * {@link Publisher}. Assumes the original is scan-compatible.
	 *
	 * @param <O> the input and output type
	 *
	 * @return a new {@link Function}
	 * @see #liftPublisher(Predicate, BiFunction)
	 */
	public static <O> Function<? super Publisher<O>, ? extends Publisher<O>> lift(
			Predicate<Scannable> filter,
			BiFunction<Scannable, ? super CoreSubscriber<? super O>, ? extends CoreSubscriber<? super O>> lifter) {
		return LiftFunction.liftScannable(filter, lifter);
	}

	/**
	 * Create a function that can be used to support a custom operator via
	 * {@link CoreSubscriber} decoration. The function is compatible with
	 * {@link Flux#transform(Function)}, {@link Mono#transform(Function)},
	 * {@link Hooks#onEachOperator(Function)} and {@link Hooks#onLastOperator(Function)},
	 * and works with the raw {@link Publisher} as input, which is useful if you need to
	 * detect the precise type of the source (eg. instanceof checks to detect Mono, Flux,
	 * true Scannable, etc...).
	 *
	 * @param lifter the bifunction taking the raw {@link Publisher} and
	 * {@link CoreSubscriber}. The publisher can be double-checked (including with
	 * {@code instanceof}, and the function must return a receiving {@link CoreSubscriber}
	 * that will immediately subscribe to the {@link Publisher}.
	 *
	 * @param <I> the input type
	 * @param <O> the output type
	 *
	 * @return a new {@link Function}
	 */
	public static <I, O> Function<? super Publisher<I>, ? extends Publisher<O>> liftPublisher(
			BiFunction<Publisher, ? super CoreSubscriber<? super O>, ? extends CoreSubscriber<? super I>> lifter) {
		return LiftFunction.liftPublisher(null, lifter);
	}

	/**
	 * Create a function that can be used to support a custom operator via
	 * {@link CoreSubscriber} decoration. The function is compatible with
	 * {@link Flux#transform(Function)}, {@link Mono#transform(Function)},
	 * {@link Hooks#onEachOperator(Function)} and {@link Hooks#onLastOperator(Function)},
	 * and works with the raw {@link Publisher} as input, which is useful if you need to
	 * detect the precise type of the source (eg. instanceof checks to detect Mono, Flux,
	 * true Scannable, etc...).
	 *
	 * <p>
	 * The function will be invoked only if the passed {@link Predicate} matches.
	 * Therefore the transformed type O must be the same than the input type since
	 * unmatched predicate will return the applied {@link Publisher}.
	 *
	 * @param filter the {@link Predicate} that the raw {@link Publisher} must pass for
	 * the transformation to occur
	 * @param lifter the {@link BiFunction} taking the raw {@link Publisher} and
	 * {@link CoreSubscriber}. The publisher can be double-checked (including with
	 * {@code instanceof}, and the function must return a receiving {@link CoreSubscriber}
	 * that will immediately subscribe to the {@link Publisher}.
	 *
	 * @param <O> the input and output type
	 *
	 * @return a new {@link Function}
	 */
	public static <O> Function<? super Publisher<O>, ? extends Publisher<O>> liftPublisher(
			Predicate<Publisher> filter,
			BiFunction<Publisher, ? super CoreSubscriber<? super O>, ? extends CoreSubscriber<? super O>> lifter) {
		return LiftFunction.liftPublisher(filter, lifter);
	}

	/**
	 * Cap a multiplication to Long.MAX_VALUE
	 *
	 * @param a left operand
	 * @param b right operand
	 *
	 * @return Product result or Long.MAX_VALUE if overflow
	 */
	public static long multiplyCap(long a, long b) {
		long u = a * b;
		if (((a | b) >>> 31) != 0) {
			if (u / a != b) {
				return Long.MAX_VALUE;
			}
		}
		return u;
	}

	/**
	 * Create an adapter for local onDiscard hooks that check the element
	 * being discarded is of a given {@link Class}. The resulting {@link Function} adds the
	 * hook to the {@link Context}, potentially chaining it to an existing hook in the {@link Context}.
	 *
	 * @param type the type of elements to take into account
	 * @param discardHook the discarding handler for this type of elements
	 * @param <R> element type
	 * @return a {@link Function} that can be used to modify a {@link Context}, adding or
	 * updating a context-local discard hook.
	 */
	static final <R> Function<Context, Context> discardLocalAdapter(Class<R> type, Consumer<? super R> discardHook) {
		Objects.requireNonNull(type, "onDiscard must be based on a type");
		Objects.requireNonNull(discardHook, "onDiscard must be provided a discardHook Consumer");

		final Consumer<Object> safeConsumer = obj -> {
			if (type.isInstance(obj)) {
				discardHook.accept(type.cast(obj));
			}
		};

		return ctx -> {
			Consumer<Object> consumer = ctx.getOrDefault(Hooks.KEY_ON_DISCARD, null);
			if (consumer == null) {
				return ctx.put(Hooks.KEY_ON_DISCARD, safeConsumer);
			}
			else {
				return ctx.put(Hooks.KEY_ON_DISCARD, safeConsumer.andThen(consumer));
			}
		};
	}

	/**
	 * Utility method to activate the onDiscard feature (see {@link Flux#doOnDiscard(Class, Consumer)})
	 * in a target {@link Context}. Prefer using the {@link Flux} API, and reserve this for
	 * testing purposes.
	 *
	 * @param target the original {@link Context}
	 * @param discardConsumer the consumer that will be used to cleanup discarded elements
	 * @return a new {@link Context} that holds (potentially combined) cleanup {@link Consumer}
	 */
	public static final Context enableOnDiscard(@Nullable Context target, Consumer<?> discardConsumer) {
		Objects.requireNonNull(discardConsumer, "discardConsumer must be provided");
		if (target == null) {
			return Context.of(Hooks.KEY_ON_DISCARD, discardConsumer);
		}
		return target.put(Hooks.KEY_ON_DISCARD, discardConsumer);
	}

	/**
	 * Invoke a (local or global) hook that processes elements that get discarded. This
	 * includes elements that are dropped (for malformed sources), but also filtered out
	 * (eg. not passing a {@code filter()} predicate).
	 * <p>
	 * For elements that are buffered or enqueued, but subsequently discarded due to
	 * cancellation or error, see {@link #onDiscardMultiple(Stream, Context)} and
	 * {@link #onDiscardQueueWithClear(Queue, Context, Function)}.
	 *
	 * @param element the element that is being discarded
	 * @param context the context in which to look for a local hook
	 * @param <T> the type of the element
	 * @see #onDiscardMultiple(Stream, Context)
	 * @see #onDiscardMultiple(Collection, Context)
	 * @see #onDiscardQueueWithClear(Queue, Context, Function)
	 */
	public static <T> void onDiscard(@Nullable T element, Context context) {
		Consumer<Object> hook = context.getOrDefault(Hooks.KEY_ON_DISCARD, null);
		if (element != null && hook != null) {
			try {
				hook.accept(element);
			}
			catch (Throwable t) {
				log.warn("Error in discard hook", t);
			}
		}
	}

	/**
	 * Invoke a (local or global) hook that processes elements that get discarded
	 * en masse after having been enqueued, due to cancellation or error. This method
	 * also empties the {@link Queue} (either by repeated {@link Queue#poll()} calls if
	 * a hook is defined, or by {@link Queue#clear()} as a shortcut if no hook is defined).
	 *
	 * @param queue the queue that is being discarded and cleared
	 * @param context the context in which to look for a local hook
	 * @param extract an optional extractor method for cases where the queue doesn't
	 * directly contain the elements to discard
	 * @param <T> the type of the element
	 * @see #onDiscardMultiple(Stream, Context)
	 * @see #onDiscardMultiple(Collection, Context)
	 * @see #onDiscard(Object, Context)
	 */
	public static <T> void onDiscardQueueWithClear(
			@Nullable Queue<T> queue,
			Context context,
			@Nullable Function<T, Stream<?>> extract) {
		if (queue == null) {
			return;
		}

		Consumer<Object> hook = context.getOrDefault(Hooks.KEY_ON_DISCARD, null);
		if (hook == null) {
			queue.clear();
			return;
		}

		try {
			for(;;) {
				T toDiscard = queue.poll();
				if (toDiscard == null) {
					break;
				}

				if (extract != null) {
					try {
						extract.apply(toDiscard)
						       .forEach(elementToDiscard -> {
							       try {
								       hook.accept(elementToDiscard);
							       }
							       catch (Throwable t) {
								       log.warn("Error while discarding item extracted from a queue element, continuing with next item", t);
							       }
						       });
					}
					catch (Throwable t) {
						log.warn("Error while extracting items to discard from queue element, continuing with next queue element", t);
					}
				}
				else {
					try {
						hook.accept(toDiscard);
					}
					catch (Throwable t) {
						log.warn("Error while discarding a queue element, continuing with next queue element", t);
					}
				}
			}
		}
		catch (Throwable t) {
			log.warn("Cannot further apply discard hook while discarding and clearing a queue", t);
		}
	}

  /**
   * Invoke a (local or global) hook that processes elements that get discarded en masse.
   * This includes elements that are buffered but subsequently discarded due to
   * cancellation or error.
   *
   * @param multiple the collection of elements to discard (possibly extracted from other
   * collections/arrays/queues)
   * @param context the {@link Context} in which to look for local hook
   * @see #onDiscard(Object, Context)
   * @see #onDiscardMultiple(Collection, Context)
   * @see #onDiscardQueueWithClear(Queue, Context, Function)
   */
  public static void onDiscardMultiple(Stream<?> multiple, Context context) {
		Consumer<Object> hook = context.getOrDefault(Hooks.KEY_ON_DISCARD, null);
		if (hook != null) {
			try {
				multiple.filter(Objects::nonNull)
				        .forEach(v -> {
				        	try {
				        		hook.accept(v);
					        }
				        	catch (Throwable t) {
				        		log.warn("Error while discarding a stream element, continuing with next element", t);
					        }
				        });
			}
			catch (Throwable t) {
				log.warn("Error while discarding stream, stopping", t);
			}
		}
	}

  /**
   * Invoke a (local or global) hook that processes elements that get discarded en masse.
   * This includes elements that are buffered but subsequently discarded due to
   * cancellation or error.
   *
   * @param multiple the collection of elements to discard
   * @param context the {@link Context} in which to look for local hook
   * @see #onDiscard(Object, Context)
   * @see #onDiscardMultiple(Stream, Context)
   * @see #onDiscardQueueWithClear(Queue, Context, Function)
   */
	public static void onDiscardMultiple(@Nullable Collection<?> multiple, Context context) {
		if (multiple == null) return;
		Consumer<Object> hook = context.getOrDefault(Hooks.KEY_ON_DISCARD, null);
		if (hook != null) {
			try {
				if (multiple.isEmpty()) {
					return;
				}
				for (Object o : multiple) {
					if (o != null) {
						try {
							hook.accept(o);
						}
						catch (Throwable t) {
							log.warn("Error while discarding element from a Collection, continuing with next element", t);
						}
					}
				}
			}
			catch (Throwable t) {
				log.warn("Error while discarding collection, stopping", t);
			}
		}
	}

  /**
   * Invoke a (local or global) hook that processes elements that remains in an {@link java.util.Iterator}.
   * Since iterators can be infinite, this method requires that you explicitly ensure the iterator is
   * {@code knownToBeFinite}. Typically, operating on an {@link Iterable} one can get such a
   * guarantee by looking at the {@link Iterable#spliterator() Spliterator's} {@link Spliterator#getExactSizeIfKnown()}.
   *
   * @param multiple the {@link Iterator} whose remainder to discard
   * @param knownToBeFinite is the caller guaranteeing that the iterator is finite and can be iterated over
   * @param context the {@link Context} in which to look for local hook
   * @see #onDiscard(Object, Context)
   * @see #onDiscardMultiple(Collection, Context)
   * @see #onDiscardQueueWithClear(Queue, Context, Function)
   */
	public static void onDiscardMultiple(@Nullable Iterator<?> multiple, boolean knownToBeFinite, Context context) {
		if (multiple == null) return;
		if (!knownToBeFinite) return;

		Consumer<Object> hook = context.getOrDefault(Hooks.KEY_ON_DISCARD, null);
		if (hook != null) {
			try {
				multiple.forEachRemaining(o -> {
					if (o != null) {
						try {
							hook.accept(o);
						}
						catch (Throwable t) {
							log.warn("Error while discarding element from an Iterator, continuing with next element", t);
						}
					}
				});
			}
			catch (Throwable t) {
				log.warn("Error while discarding Iterator, stopping", t);
			}
		}
	}

	/**
	 * An unexpected exception is about to be dropped.
	 * <p>
	 * If no hook is registered for {@link Hooks#onErrorDropped(Consumer)}, the dropped
	 * error is logged at ERROR level and thrown (via {@link Exceptions#bubble(Throwable)}.
	 *
	 * @param e the dropped exception
	 * @param context a context that might hold a local error consumer
	 */
	public static void onErrorDropped(Throwable e, Context context) {
		Consumer<? super Throwable> hook = context.getOrDefault(Hooks.KEY_ON_ERROR_DROPPED,null);
		if (hook == null) {
			hook = Hooks.onErrorDroppedHook;
		}
		if (hook == null) {
			log.error("Operator called default onErrorDropped", e);
			return;
		}
		hook.accept(e);
	}

	/**
	 * An unexpected event is about to be dropped.
	 * <p>
	 * If no hook is registered for {@link Hooks#onNextDropped(Consumer)}, the dropped
	 * element is just logged at DEBUG level.
	 *
	 * @param <T> the dropped value type
	 * @param t the dropped data
	 * @param context a context that might hold a local next consumer
	 */
	public static <T> void onNextDropped(T t, Context context) {
		Objects.requireNonNull(t, "onNext");
		Objects.requireNonNull(context, "context");
		Consumer<Object> hook = context.getOrDefault(Hooks.KEY_ON_NEXT_DROPPED, null);
		if (hook == null) {
			hook = Hooks.onNextDroppedHook;
		}
		if (hook != null) {
			hook.accept(t);
		}
		else if (log.isDebugEnabled()) {
			log.debug("onNextDropped: " + t);
		}
	}

	/**
	 * Map an "operator" error. The
	 * result error will be passed via onError to the operator downstream after
	 * checking for fatal error via
	 * {@link Exceptions#throwIfFatal(Throwable)}.
	 *
	 * @param error the callback or operator error
	 * @param context a context that might hold a local error consumer
	 * @return mapped {@link Throwable}
	 *
	 */
	public static Throwable onOperatorError(Throwable error, Context context) {
		return onOperatorError(null, error, context);
	}

	/**
	 * Map an "operator" error given an operator parent {@link Subscription}. The
	 * result error will be passed via onError to the operator downstream.
	 * {@link Subscription} will be cancelled after checking for fatal error via
	 * {@link Exceptions#throwIfFatal(Throwable)}.
	 *
	 * @param subscription the linked operator parent {@link Subscription}
	 * @param error the callback or operator error
	 * @param context a context that might hold a local error consumer
	 * @return mapped {@link Throwable}
	 *
	 */
	public static Throwable onOperatorError(@Nullable Subscription subscription,
			Throwable error,
			Context context) {
		return onOperatorError(subscription, error, null, context);
	}

	/**
	 * Map an "operator" error given an operator parent {@link Subscription}. The
	 * result error will be passed via onError to the operator downstream.
	 * {@link Subscription} will be cancelled after checking for fatal error via
	 * {@link Exceptions#throwIfFatal(Throwable)}. Takes an additional signal, which
	 * can be added as a suppressed exception if it is a {@link Throwable} and the
	 * default {@link Hooks#onOperatorError(BiFunction) hook} is in place.
	 *
	 * @param subscription the linked operator parent {@link Subscription}
	 * @param error the callback or operator error
	 * @param dataSignal the value (onNext or onError) signal processed during failure
	 * @param context a context that might hold a local error consumer
	 * @return mapped {@link Throwable}
	 *
	 */
	public static Throwable onOperatorError(@Nullable Subscription subscription,
			Throwable error,
			@Nullable Object dataSignal, Context context) {

		Exceptions.throwIfFatal(error);
		if(subscription != null) {
			subscription.cancel();
		}

		Throwable t = Exceptions.unwrap(error);
		BiFunction<? super Throwable, Object, ? extends Throwable> hook =
				context.getOrDefault(Hooks.KEY_ON_OPERATOR_ERROR, null);
		if (hook == null) {
			hook = Hooks.onOperatorErrorHook;
		}
		if (hook == null) {
			if (dataSignal != null) {
				if (dataSignal != t && dataSignal instanceof Throwable) {
					t = Exceptions.addSuppressed(t, (Throwable) dataSignal);
				}
				//do not wrap original value to avoid strong references
				/*else {
				}*/
			}
			return t;
		}
		return hook.apply(error, dataSignal);
	}

	/**
	 * Return a wrapped {@link RejectedExecutionException} which can be thrown by the
	 * operator. This exception denotes that an execution was rejected by a
	 * {@link reactor.core.scheduler.Scheduler}, notably when it was already disposed.
	 * <p>
	 * Wrapping is done by calling both {@link Exceptions#bubble(Throwable)} and
	 * {@link #onOperatorError(Subscription, Throwable, Object, Context)}.
	 *
	 * @param original the original execution error
	 * @param context a context that might hold a local error consumer
	 *
	 */
	public static RuntimeException onRejectedExecution(Throwable original, Context context) {
		return onRejectedExecution(original, null, null, null, context);
	}

	static final OnNextFailureStrategy onNextErrorStrategy(Context context) {
		OnNextFailureStrategy strategy = null;

		BiFunction<? super Throwable, Object, ? extends Throwable> fn = context.getOrDefault(
				OnNextFailureStrategy.KEY_ON_NEXT_ERROR_STRATEGY, null);
		if (fn instanceof OnNextFailureStrategy) {
			strategy = (OnNextFailureStrategy) fn;
		} else if (fn != null) {
			strategy = new OnNextFailureStrategy.LambdaOnNextErrorStrategy(fn);
		}

		if (strategy == null) strategy = Hooks.onNextErrorHook;
		if (strategy == null) strategy = OnNextFailureStrategy.STOP;
		return strategy;
	}

	public static final BiFunction<? super Throwable, Object, ? extends Throwable> onNextErrorFunction(Context context) {
		return onNextErrorStrategy(context);
	}

	/**
	 * Find the {@link OnNextFailureStrategy} to apply to the calling operator (which could be a local
	 * error mode defined in the {@link Context}) and apply it. For poll(), prefer
	 * {@link #onNextPollError(Object, Throwable, Context)} as it returns a {@link RuntimeException}.
	 * <p>
	 * Cancels the {@link Subscription} and return a {@link Throwable} if errors are
	 * fatal for the error mode, in which case the operator should call onError with the
	 * returned error. On the contrary, if the error mode allows the sequence to
	 * continue, does not cancel the Subscription and returns {@code null}.
	 * <p>
	 * Typical usage pattern differs depending on the calling method:
	 * <ul>
	 *     <li>{@code onNext}: check for a throwable return value and call
	 *     {@link Subscriber#onError(Throwable)} if not null, otherwise perform a direct
	 *     {@link Subscription#request(long) request(1)} on the upstream.</li>
	 *
	 *     <li>{@code tryOnNext}: check for a throwable return value and call
	 *     {@link Subscriber#onError(Throwable)} if not null, otherwise
	 *     return {@code false} to indicate value was not consumed and more must be
	 *     tried.</li>
	 *
	 *     <li>any of the above where the error is going to be propagated through onError but the
	 *     subscription shouldn't be cancelled: use {@link #onNextError(Object, Throwable, Context)} instead.</li>
	 *
	 *     <li>{@code poll} (where the error will be thrown): use {@link #onNextPollError(Object, Throwable, Context)} instead.</li>
	 * </ul>
	 *
	 * @param value The onNext value that caused an error. Can be null.
	 * @param error The error.
	 * @param context The most significant {@link Context} in which to look for an {@link OnNextFailureStrategy}.
	 * @param subscriptionForCancel The mandatory {@link Subscription} that should be cancelled if the
	 * strategy is terminal. See also {@link #onNextError(Object, Throwable, Context)} and
	 * {@link #onNextPollError(Object, Throwable, Context)} for alternatives that don't cancel a subscription
	 * @param <T> The type of the value causing the error.
	 * @return a {@link Throwable} to propagate through onError if the strategy is
	 * terminal and cancelled the subscription, null if not.
	 */
	@Nullable
	public static <T> Throwable onNextError(@Nullable T value, Throwable error, Context context,
			Subscription subscriptionForCancel) {
		error = unwrapOnNextError(error);
		OnNextFailureStrategy strategy = onNextErrorStrategy(context);
		if (strategy.test(error, value)) {
			//some strategies could still return an exception, eg. if the consumer throws
			Throwable t = strategy.process(error, value, context);
			if (t != null) {
				subscriptionForCancel.cancel();
			}
			return t;
		}
		else {
			//falls back to operator errors
			return onOperatorError(subscriptionForCancel, error, value, context);
		}
	}

	/**
	 * Find the {@link OnNextFailureStrategy} to apply to the calling async operator (which could be
	 * a local error mode defined in the {@link Context}) and apply it.
	 * <p>
	 * This variant never cancels a {@link Subscription}. It returns a {@link Throwable} if the error is
	 * fatal for the error mode, in which case the operator should call onError with the
	 * returned error. On the contrary, if the error mode allows the sequence to
	 * continue, this method returns {@code null}.
	 *
	 * @param value The onNext value that caused an error.
	 * @param error The error.
	 * @param context The most significant {@link Context} in which to look for an {@link OnNextFailureStrategy}.
	 * @param <T> The type of the value causing the error.
	 * @return a {@link Throwable} to propagate through onError if the strategy is terminal, null if not.
	 * @see #onNextError(Object, Throwable, Context, Subscription)
	 */
	@Nullable
	public static <T> Throwable onNextError(@Nullable T value, Throwable error, Context context) {
		error = unwrapOnNextError(error);
		OnNextFailureStrategy strategy = onNextErrorStrategy(context);
		if (strategy.test(error, value)) {
			//some strategies could still return an exception, eg. if the consumer throws
			return strategy.process(error, value, context);
		}
		else {
			return onOperatorError(null, error, value, context);
		}
	}

	/**
	 * Find the {@link OnNextFailureStrategy} to apply to the calling operator (which could be a local
	 * error mode defined in the {@link Context}) and apply it.
	 *
	 * @param error The error.
	 * @param context The most significant {@link Context} in which to look for an {@link OnNextFailureStrategy}.
	 * @param subscriptionForCancel The {@link Subscription} that should be cancelled if the
	 * strategy is terminal. Null to ignore (for poll, use {@link #onNextPollError(Object, Throwable, Context)}
	 * rather than passing null).
	 * @param <T> The type of the value causing the error.
	 * @return a {@link Throwable} to propagate through onError if the strategy is
	 * terminal and cancelled the subscription, null if not.
	 */
	public static <T> Throwable onNextInnerError(Throwable error, Context context, @Nullable Subscription subscriptionForCancel) {
		error = unwrapOnNextError(error);
		OnNextFailureStrategy strategy = onNextErrorStrategy(context);
		if (strategy.test(error, null)) {
			//some strategies could still return an exception, eg. if the consumer throws
			Throwable t = strategy.process(error, null, context);
			if (t != null && subscriptionForCancel != null) {
				subscriptionForCancel.cancel();
			}
			return t;
		}
		else {
			return error;
		}
	}

	/**
	 * Find the {@link OnNextFailureStrategy} to apply to the calling async operator (which could be
	 * a local error mode defined in the {@link Context}) and apply it.
	 * <p>
	 * Returns a {@link RuntimeException} if the error is fatal for the error mode, in which
	 * case the operator poll should throw the returned error. On the contrary if the
	 * error mode allows the sequence to continue, returns {@code null} in which case
	 * the operator should retry the {@link Queue#poll() poll()}.
	 * <p>
	 * Note that this method {@link Exceptions#propagate(Throwable) wraps} checked exceptions in order to
	 * return a {@link RuntimeException} that can be thrown from an arbitrary method. If you don't want to
	 * throw the returned exception and this wrapping behavior is undesirable, but you still don't want to
	 * cancel a subscription, you can use {@link #onNextError(Object, Throwable, Context)} instead.
	 *
	 * @param value The onNext value that caused an error.
	 * @param error The error.
	 * @param context The most significant {@link Context} in which to look for an {@link OnNextFailureStrategy}.
	 * @param <T> The type of the value causing the error.
	 * @return a {@link RuntimeException} to be thrown (eg. within {@link Queue#poll()} if the error is terminal in
	 * the strategy, null if not.
	 * @see #onNextError(Object, Throwable, Context)
	 */
	@Nullable
	public static <T> RuntimeException onNextPollError(@Nullable T value, Throwable error, Context context) {
		error = unwrapOnNextError(error);
		OnNextFailureStrategy strategy = onNextErrorStrategy(context);
		if (strategy.test(error, value)) {
			//some strategies could still return an exception, eg. if the consumer throws
			Throwable t = strategy.process(error, value, context);
			if (t != null) return Exceptions.propagate(t);
			return null;
		}
		else {
			Throwable t = onOperatorError(null, error, value, context);
			return Exceptions.propagate(t);
		}
	}

	/**
	 * Applies the hooks registered with {@link Hooks#onLastOperator} and returns
	 * {@link CorePublisher} ready to be subscribed on.
	 *
	 * @param source the original {@link CorePublisher}.
	 * @param <T> the type of the value.
	 * @return a {@link CorePublisher} to subscribe on.
	 */
	@SuppressWarnings("unchecked")
	public static <T> CorePublisher<T> onLastAssembly(CorePublisher<T> source) {
		Function<Publisher, Publisher> hook = Hooks.onLastOperatorHook;
		if (hook == null) {
			return source;
		}

		Publisher<T> publisher = Objects.requireNonNull(hook.apply(source),"LastOperator hook returned null");

		if (publisher instanceof CorePublisher) {
			return (CorePublisher<T>) publisher;
		}
		else {
			return new CorePublisherAdapter<>(publisher);
		}
	}

	private static Throwable unwrapOnNextError(Throwable error) {
		return Exceptions.isBubbling(error) ? error : Exceptions.unwrap(error);
	}

	/**
	 * Return a wrapped {@link RejectedExecutionException} which can be thrown by the
	 * operator. This exception denotes that an execution was rejected by a
	 * {@link reactor.core.scheduler.Scheduler}, notably when it was already disposed.
	 * <p>
	 * Wrapping is done by calling both {@link Exceptions#failWithRejected(Throwable)} and
	 * {@link #onOperatorError(Subscription, Throwable, Object, Context)} (with the passed
	 * {@link Subscription}).
	 *
	 * @param original the original execution error
	 * @param subscription the subscription to pass to onOperatorError.
	 * @param suppressed a Throwable to be suppressed by the {@link RejectedExecutionException} (or null if not relevant)
	 * @param dataSignal a value to be passed to {@link #onOperatorError(Subscription, Throwable, Object, Context)} (or null if not relevant)
	 * @param context a context that might hold a local error consumer
	 */
	public static RuntimeException onRejectedExecution(Throwable original,
			@Nullable Subscription subscription,
			@Nullable Throwable suppressed,
			@Nullable Object dataSignal,
			Context context) {
		//we "cheat" to apply the special key for onRejectedExecution in onOperatorError
		if (context.hasKey(Hooks.KEY_ON_REJECTED_EXECUTION)) {
			context = context.put(Hooks.KEY_ON_OPERATOR_ERROR, context.get(Hooks.KEY_ON_REJECTED_EXECUTION));
		}

		//don't create REE if original is a reactor-produced REE (not including singletons)
		RejectedExecutionException ree = Exceptions.failWithRejected(original);
		if (suppressed != null) {
			ree.addSuppressed(suppressed);
		}
		if (dataSignal != null) {
			return Exceptions.propagate(Operators.onOperatorError(subscription, ree,
					dataSignal, context));
		}
		return Exceptions.propagate(Operators.onOperatorError(subscription, ree, context));
	}

	/**
	 * Concurrent subtraction bound to 0, mostly used to decrement a request tracker by
	 * the amount produced by the operator. Any concurrent write will "happen before"
	 * this operation.
	 *
     * @param <T> the parent instance type
	 * @param updater  current field updater
	 * @param instance current instance to update
	 * @param toSub    delta to subtract
	 * @return value after subtraction or zero
	 */
	public static <T> long produced(AtomicLongFieldUpdater<T> updater, T instance, long toSub) {
		long r, u;
		do {
			r = updater.get(instance);
			if (r == 0 || r == Long.MAX_VALUE) {
				return r;
			}
			u = subOrZero(r, toSub);
		} while (!updater.compareAndSet(instance, r, u));

		return u;
	}

	/**
	 * A generic utility to atomically replace a subscription or cancel the replacement
	 * if the current subscription is marked as already cancelled (as in
	 * {@link #cancelledSubscription()}).
	 *
	 * @param field The Atomic container
	 * @param instance the instance reference
	 * @param s the subscription
	 * @param <F> the instance type
	 *
	 * @return true if replaced
	 */
	public static <F> boolean replace(AtomicReferenceFieldUpdater<F, Subscription> field,
			F instance,
			Subscription s) {
		for (; ; ) {
			Subscription a = field.get(instance);
			if (a == CancelledSubscription.INSTANCE) {
				s.cancel();
				return false;
			}
			if (field.compareAndSet(instance, a, s)) {
				return true;
			}
		}
	}

	/**
	 * Log an {@link IllegalArgumentException} if the request is null or negative.
	 *
	 * @param n the failing demand
	 *
	 * @see Exceptions#nullOrNegativeRequestException(long)
	 */
	public static void reportBadRequest(long n) {
		if (log.isDebugEnabled()) {
			log.debug("Negative request",
					Exceptions.nullOrNegativeRequestException(n));
		}
	}

	/**
	 * Log an {@link IllegalStateException} that indicates more than the requested
	 * amount was produced.
	 *
	 * @see Exceptions#failWithOverflow()
	 */
	public static void reportMoreProduced() {
		if (log.isDebugEnabled()) {
			log.debug("More data produced than requested",
					Exceptions.failWithOverflow());
		}
	}

	/**
	 * Log a {@link Exceptions#duplicateOnSubscribeException() duplicate subscription} error.
	 *
	 * @see Exceptions#duplicateOnSubscribeException()
	 */
	public static void reportSubscriptionSet() {
		if (log.isDebugEnabled()) {
			log.debug("Duplicate Subscription has been detected",
					Exceptions.duplicateOnSubscribeException());
		}
	}

	/**
	 * Represents a fuseable Subscription that emits a single constant value synchronously
	 * to a Subscriber or consumer.
	 *
	 * @param subscriber the delegate {@link Subscriber} that will be requesting the value
	 * @param value the single value to be emitted
	 * @param <T> the value type
	 * @return a new scalar {@link Subscription}
	 */
	public static <T> Subscription scalarSubscription(CoreSubscriber<? super T> subscriber,
			T value){
		return new ScalarSubscription<>(subscriber, value);
	}

	/**
	 * Represents a fuseable Subscription that emits a single constant value synchronously
	 * to a Subscriber or consumer. Also give the subscription a user-defined {@code stepName}
	 * for the purpose of {@link Scannable#stepName()}.
	 *
	 * @param subscriber the delegate {@link Subscriber} that will be requesting the value
	 * @param value the single value to be emitted
	 * @param stepName the {@link String} to represent the {@link Subscription} in {@link Scannable#stepName()}
	 * @param <T> the value type
	 * @return a new scalar {@link Subscription}
	 */
	public static <T> Subscription scalarSubscription(CoreSubscriber<? super T> subscriber,
			T value, String stepName){
		return new ScalarSubscription<>(subscriber, value, stepName);
	}
	
	/**
	 * Safely gate a {@link Subscriber} by making sure onNext signals are delivered
	 * sequentially (serialized).
	 * Serialization uses thread-stealing and a potentially unbounded queue that might
	 * starve a calling thread if races are too important and {@link Subscriber} is slower.
	 *
	 * <p>
	 * <img class="marble" src="https://raw.githubusercontent.com/reactor/reactor-core/v3.1.3.RELEASE/src/docs/marble/serialize.png" alt="">
	 *
	 * @param <T> the relayed type
	 * @param subscriber the subscriber to serialize
	 * @return a serializing {@link Subscriber}
	 */
	public static <T> CoreSubscriber<T> serialize(CoreSubscriber<? super T> subscriber) {
		return new SerializedSubscriber<>(subscriber);
	}

	/**
	 * A generic utility to atomically replace a subscription or cancel the replacement
	 * if current subscription is marked as cancelled (as in {@link #cancelledSubscription()})
	 * or was concurrently updated before.
	 * <p>
	 * The replaced subscription is itself cancelled.
	 *
	 * @param field The Atomic container
	 * @param instance the instance reference
	 * @param s the subscription
	 * @param <F> the instance type
	 *
	 * @return true if replaced
	 */
	public static <F> boolean set(AtomicReferenceFieldUpdater<F, Subscription> field,
			F instance,
			Subscription s) {
		for (; ; ) {
			Subscription a = field.get(instance);
			if (a == CancelledSubscription.INSTANCE) {
				s.cancel();
				return false;
			}
			if (field.compareAndSet(instance, a, s)) {
				if (a != null) {
					a.cancel();
				}
				return true;
			}
		}
	}

	/**
	 * Sets the given subscription once and returns true if successful, false
	 * if the field has a subscription already or has been cancelled.
	 * <p>
	 * If the field already has a subscription, it is cancelled and the duplicate
	 * subscription is reported (see {@link #reportSubscriptionSet()}).
	 *
	 * @param <F> the instance type containing the field
	 * @param field the field accessor
	 * @param instance the parent instance
	 * @param s the subscription to set once
	 * @return true if successful, false if the target was not empty or has been cancelled
	 */
	public static <F> boolean setOnce(AtomicReferenceFieldUpdater<F, Subscription> field, F instance, Subscription s) {
		Objects.requireNonNull(s, "subscription");
		Subscription a = field.get(instance);
		if (a == CancelledSubscription.INSTANCE) {
			s.cancel();
			return false;
		}
		if (a != null) {
			s.cancel();
			reportSubscriptionSet();
			return false;
		}

		if (field.compareAndSet(instance, null, s)) {
			return true;
		}

		a = field.get(instance);

		if (a == CancelledSubscription.INSTANCE) {
			s.cancel();
			return false;
		}

		s.cancel();
		reportSubscriptionSet();
		return false;
	}

	/**
	 * Cap a subtraction to 0
	 *
	 * @param a left operand
	 * @param b right operand
	 * @return Subtraction result or 0 if overflow
	 */
	public static long subOrZero(long a, long b) {
		long res = a - b;
		if (res < 0L) {
			return 0;
		}
		return res;
	}

	/**
	 * Atomically terminates the subscription if it is not already a
	 * {@link #cancelledSubscription()}, cancelling the subscription and setting the field
	 * to the singleton {@link #cancelledSubscription()}.
	 *
	 * @param <F> the instance type containing the field
	 * @param field the field accessor
	 * @param instance the parent instance
	 * @return true if terminated, false if the subscription was already terminated
	 */
	public static <F> boolean terminate(AtomicReferenceFieldUpdater<F, Subscription> field,
			F instance) {
		Subscription a = field.get(instance);
		if (a != CancelledSubscription.INSTANCE) {
			a = field.getAndSet(instance, CancelledSubscription.INSTANCE);
			if (a != null && a != CancelledSubscription.INSTANCE) {
				a.cancel();
				return true;
			}
		}
		return false;
	}

	/**
	 * Check Subscription current state and cancel new Subscription if current is set,
	 * or return true if ready to subscribe.
	 *
	 * @param current current Subscription, expected to be null
	 * @param next new Subscription
	 * @return true if Subscription can be used
	 */
	public static boolean validate(@Nullable Subscription current, Subscription next) {
		Objects.requireNonNull(next, "Subscription cannot be null");
		if (current != null) {
			next.cancel();
			//reportSubscriptionSet();
			return false;
		}

		return true;
	}

	/**
	 * Evaluate if a request is strictly positive otherwise {@link #reportBadRequest(long)}
	 * @param n the request value
	 * @return true if valid
	 */
	public static boolean validate(long n) {
		if (n <= 0) {
			reportBadRequest(n);
			return false;
		}
		return true;
	}

	/**
	 * If the actual {@link Subscriber} is not a {@link CoreSubscriber}, it will apply
	 * safe strict wrapping to apply all reactive streams rules including the ones
	 * relaxed by internal operators based on {@link CoreSubscriber}.
	 *
	 * @param <T> passed subscriber type
	 *
	 * @param actual the {@link Subscriber} to apply hook on
	 * @return an eventually transformed {@link Subscriber}
	 */
	@SuppressWarnings("unchecked")
	public static <T> CoreSubscriber<? super T> toCoreSubscriber(Subscriber<? super T> actual) {

		Objects.requireNonNull(actual, "actual");

		CoreSubscriber<? super T> _actual;

		if (actual instanceof CoreSubscriber){
			_actual = (CoreSubscriber<? super T>) actual;
		}
		else {
			_actual = new StrictSubscriber<>(actual);
		}

		return _actual;
	}

	/**
	 * If the actual {@link CoreSubscriber} is not {@link reactor.core.Fuseable.ConditionalSubscriber},
	 * it will apply an adapter which directly maps all
	 * {@link reactor.core.Fuseable.ConditionalSubscriber#tryOnNext(Object)} to
	 * {@link Subscriber#onNext(Object)}
	 * and always returns true as the result
	 *
	 * @param <T> passed subscriber type
	 *
	 * @param actual the {@link Subscriber} to adapt
	 * @return a potentially adapted {@link reactor.core.Fuseable.ConditionalSubscriber}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Fuseable.ConditionalSubscriber<? super  T> toConditionalSubscriber(CoreSubscriber<? super T> actual) {
		Objects.requireNonNull(actual, "actual");

		Fuseable.ConditionalSubscriber<? super T> _actual;

		if (actual instanceof Fuseable.ConditionalSubscriber) {
			_actual = (Fuseable.ConditionalSubscriber<? super T>) actual;
		}
		else {
			_actual = new ConditionalSubscriberAdapter<>(actual);
		}

		return _actual;
	}



	static Context multiSubscribersContext(InnerProducer<?>[] multicastInners){
		if (multicastInners.length > 0){
			return multicastInners[0].actual().currentContext();
		}
		return Context.empty();
	}

	/**
	 * Add the amount {@code n} to the given field, capped to {@link Long#MAX_VALUE},
	 * unless the field is already at {@link Long#MAX_VALUE} OR {@link Long#MIN_VALUE}.
	 * Return the value before the update.
	 *
	 * @param updater the field to update
	 * @param instance the instance bearing the field
	 * @param n the value to add
	 * @param <T> the type of the field-bearing instance
	 *
	 * @return the old value of the field, before update.
	 */
	static <T> long addCapCancellable(AtomicLongFieldUpdater<T> updater, T instance,
			long n) {
		for (; ; ) {
			long r = updater.get(instance);
			if (r == Long.MIN_VALUE || r == Long.MAX_VALUE) {
				return r;
			}
			long u = addCap(r, n);
			if (updater.compareAndSet(instance, r, u)) {
				return r;
			}
		}
	}

	/**
	 * An unexpected exception is about to be dropped from an operator that has multiple
	 * subscribers (and thus potentially multiple Context with local onErrorDropped handlers).
	 *
	 * @param e the dropped exception
	 * @param multicastInners the inner targets of the multicast
	 * @see #onErrorDropped(Throwable, Context)
	 */
	static void onErrorDroppedMulticast(Throwable e, InnerProducer<?>[] multicastInners) {
		//TODO let this method go through multiple contexts and use their local handlers
		//if at least one has no local handler, also call onErrorDropped(e, Context.empty())
		onErrorDropped(e, multiSubscribersContext(multicastInners));
	}

	/**
	 * An unexpected event is about to be dropped from an operator that has multiple
	 * subscribers (and thus potentially multiple Context with local onNextDropped handlers).
	 * <p>
	 * If no hook is registered for {@link Hooks#onNextDropped(Consumer)}, the dropped
	 * element is just logged at DEBUG level.
	 *
	 * @param <T> the dropped value type
	 * @param t the dropped data
	 * @param multicastInners the inner targets of the multicast
	 * @see #onNextDropped(Object, Context)
	 */
	static <T> void onNextDroppedMulticast(T t,	InnerProducer<?>[] multicastInners) {
		//TODO let this method go through multiple contexts and use their local handlers
		//if at least one has no local handler, also call onNextDropped(t, Context.empty())
		onNextDropped(t, multiSubscribersContext(multicastInners));
	}

	static <T> long producedCancellable(AtomicLongFieldUpdater<T> updater, T instance, long n) {
		for (; ; ) {
			long current = updater.get(instance);
			if (current == Long.MIN_VALUE) {
				return Long.MIN_VALUE;
			}
			if (current == Long.MAX_VALUE) {
				return Long.MAX_VALUE;
			}
			long update = current - n;
			if (update < 0L) {
				reportBadRequest(update);
				update = 0L;
			}
			if (updater.compareAndSet(instance, current, update)) {
				return update;
			}
		}
	}

	static long unboundedOrPrefetch(int prefetch) {
		return prefetch == Integer.MAX_VALUE ? Long.MAX_VALUE : prefetch;
	}

	static int unboundedOrLimit(int prefetch) {
		return prefetch == Integer.MAX_VALUE ? Integer.MAX_VALUE : (prefetch - (prefetch >>	2));
	}

	static int unboundedOrLimit(int prefetch, int lowTide) {
		if (lowTide <= 0) {
			return prefetch;
		}
		if (lowTide >= prefetch) {
			return unboundedOrLimit(prefetch);
		}
		return prefetch == Integer.MAX_VALUE ? Integer.MAX_VALUE : lowTide;
	}

	Operators() {
	}

	static final class CorePublisherAdapter<T> implements CorePublisher<T>,
	                                                      OptimizableOperator<T, T> {

		final Publisher<T> publisher;

		@Nullable
		final OptimizableOperator<?, T> optimizableOperator;

		CorePublisherAdapter(Publisher<T> publisher) {
			this.publisher = publisher;
			if (publisher instanceof OptimizableOperator) {
				@SuppressWarnings("unchecked")
				OptimizableOperator<?, T> optimSource = (OptimizableOperator<?, T>) publisher;
				this.optimizableOperator = optimSource;
			}
			else {
				this.optimizableOperator = null;
			}
		}

		@Override
		public void subscribe(CoreSubscriber<? super T> subscriber) {
			publisher.subscribe(subscriber);
		}

		@Override
		public void subscribe(Subscriber<? super T> s) {
			publisher.subscribe(s);
		}

		@Override
		public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
			return actual;
		}

		@Override
		public final CorePublisher<? extends T> source() {
			return this;
		}

		@Override
		public final OptimizableOperator<?, ? extends T> nextOptimizableSource() {
			return optimizableOperator;
		}
	}

	static final Fuseable.ConditionalSubscriber<?> EMPTY_SUBSCRIBER = new Fuseable.ConditionalSubscriber<Object>() {
		@Override
		public void onSubscribe(Subscription s) {
			Throwable e = new IllegalStateException("onSubscribe should not be used");
			log.error("Unexpected call to Operators.emptySubscriber()", e);
		}

		@Override
		public void onNext(Object o) {
			Throwable e = new IllegalStateException("onNext should not be used, got " + o);
			log.error("Unexpected call to Operators.emptySubscriber()", e);
		}

		@Override
		public boolean tryOnNext(Object o) {
			Throwable e = new IllegalStateException("tryOnNext should not be used, got " + o);
			log.error("Unexpected call to Operators.emptySubscriber()", e);
			return false;
		}

		@Override
		public void onError(Throwable t) {
			Throwable e = new IllegalStateException("onError should not be used", t);
			log.error("Unexpected call to Operators.emptySubscriber()", e);
		}

		@Override
		public void onComplete() {
			Throwable e = new IllegalStateException("onComplete should not be used");
			log.error("Unexpected call to Operators.emptySubscriber()", e);
		}

		@Override
		public Context currentContext() {
			return Context.empty();
		}
	};
	//

	final static class CancelledSubscription implements Subscription, Scannable {
		static final CancelledSubscription INSTANCE = new CancelledSubscription();

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) {
				return true;
			}
			return null;
		}

		@Override
		public void cancel() {
			// deliberately no op
		}

		@Override
		public void request(long n) {
			// deliberately no op
		}

		@Override
		public String stepName() {
			return "cancelledSubscription";
		}
	}

	final static class EmptySubscription implements QueueSubscription<Object>, Scannable {
		static final EmptySubscription INSTANCE = new EmptySubscription();
		static final EmptySubscription FROM_SUBSCRIBE_INSTANCE = new EmptySubscription();

		@Override
		public void cancel() {
			// deliberately no op
		}

		@Override
		public void clear() {
			// deliberately no op
		}

		@Override
		public boolean isEmpty() {
			return true;
		}

		@Override
		@Nullable
		public Object poll() {
			return null;
		}

		@Override
		public void request(long n) {
			// deliberately no op
		}

		@Override
		public int requestFusion(int requestedMode) {
			return NONE; // can't enable fusion due to complete/error possibility
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return true;
			return null;
		}

		@Override
		public int size() {
			return 0;
		}

		@Override
		public String stepName() {
			return "emptySubscription";
		}
	}

	/**
	 * Base class for Subscribers that will receive their Subscriptions at any time, yet
	 * they might also need to be cancelled or requested at any time.
	 */
	public static class DeferredSubscription
			implements Subscription, Scannable {

		static final int STATE_CANCELLED = -2;
		static final int STATE_SUBSCRIBED = -1;

		Subscription s;
		volatile long requested;

		protected boolean isCancelled(){
			return requested == STATE_CANCELLED;
		}

		@Override
		public void cancel() {
			final long state = REQUESTED.getAndSet(this, STATE_CANCELLED);
			if (state == STATE_CANCELLED) {
				return;
			}

			if (state == STATE_SUBSCRIBED) {
				this.s.cancel();
			}
		}

		protected void terminate() {
			REQUESTED.getAndSet(this, STATE_CANCELLED);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			long requested = this.requested; // volatile read to see subscription
			if (key == Attr.PARENT) return s;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested < 0 ? 0 : requested;
			if (key == Attr.CANCELLED) return isCancelled();

			return null;
		}

		@Override
		public void request(long n) {
			long r = this.requested; // volatile read beforehand

			if (r > STATE_SUBSCRIBED) { // works only in case onSubscribe has not happened
				long u;
				for (;;) { // normal CAS loop with overflow protection
					if (r == Long.MAX_VALUE) { // if r == Long.MAX_VALUE then we dont care and we can loose this request just in case of racing
						return;
					}
					u = Operators.addCap(r, n);
					if (REQUESTED.compareAndSet(this, r, u)) { // Means increment happened before onSubscribe
						return;
					}
					else { // Means increment happened after onSubscribe
						r = this.requested; // update new state to see what exactly happened (onSubscribe | cancel | requestN)

						if (r < 0) { // check state (expect -1 | -2 to exit, otherwise repeat)
							break;
						}
					}
				}
			}

			if (r == STATE_CANCELLED) { // if canceled, just exit
				return;
			}

			this.s.request(n); // if onSubscribe -> subscription exists (and we sure of that because volatile read after volatile write) so we can execute requestN on the subscription
		}

		/**
		 * Atomically sets the single subscription and requests the missed amount from it.
		 *
		 * @param s the subscription to set
		 * @return false if this arbiter is cancelled or there was a subscription already set
		 */
		public final boolean set(Subscription s) {
			Objects.requireNonNull(s, "s");
			final long state = this.requested;
			Subscription a = this.s;
			if (state == STATE_CANCELLED) {
				s.cancel();
				return false;
			}
			if (a != null) {
				s.cancel();
				reportSubscriptionSet();
				return false;
			}

			long r;
			long accumulated = 0;
			for (;;) {
				r = this.requested;

				if (r == STATE_CANCELLED || r == STATE_SUBSCRIBED) {
					s.cancel();
					return false;
				}

				this.s = s;

				long toRequest = r - accumulated;
				if (toRequest > 0) { // if there is something,
					s.request(toRequest); // then we do a request on the given subscription
				}
				accumulated += toRequest;

				if (REQUESTED.compareAndSet(this, r, STATE_SUBSCRIBED)) {
					return true;
				}
			}
		}

		static final AtomicLongFieldUpdater<DeferredSubscription> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(DeferredSubscription.class, "requested");

	}

	/**
	 * A Subscriber/Subscription barrier that holds a single value at most and properly gates asynchronous behaviors
	 * resulting from concurrent request or cancel and onXXX signals.
	 * Publisher Operators using this Subscriber can be fused (implement Fuseable).
	 *
	 * @param <I> The upstream sequence type
	 * @param <O> The downstream sequence type
	 */
	public static class MonoSubscriber<I, O>
			implements InnerOperator<I, O>,
			           Fuseable, //for constants only
			           QueueSubscription<O> {

		protected final CoreSubscriber<? super O> actual;

		/**
		 * The value stored by this Mono operator. Strongly prefer using {@link #setValue(Object)}
		 * rather than direct writes to this field, when possible.
		 */
		@Nullable
		protected O   value;
		volatile  int state; //see STATE field updater

		public MonoSubscriber(CoreSubscriber<? super O> actual) {
			this.actual = actual;
		}

		@Override
		public void cancel() {
			O v = value;
			value = null;
			STATE.set(this, CANCELLED);
			discard(v);
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return isCancelled();
			if (key == Attr.TERMINATED) return state == HAS_REQUEST_HAS_VALUE || state == NO_REQUEST_HAS_VALUE;
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public final void clear() {
			STATE.lazySet(this, FUSED_CONSUMED);
			this.value = null;
		}

		/**
		 * Tries to emit the value and complete the underlying subscriber or
		 * stores the value away until there is a request for it.
		 * <p>
		 * Make sure this method is called at most once
		 * @param v the value to emit
		 */
		public final void complete(@Nullable O v) {
			for (; ; ) {
				int state = this.state;
				if (state == FUSED_EMPTY) {
					setValue(v);
					//sync memory since setValue is non volatile
					if (STATE.compareAndSet(this, FUSED_EMPTY, FUSED_READY)) {
						Subscriber<? super O> a = actual;
						a.onNext(v);
						a.onComplete();
						return;
					}
					//refresh state if race occurred so we test if cancelled in the next comparison
					state = this.state;
				}

				// if state is >= HAS_CANCELLED or bit zero is set (*_HAS_VALUE) case, return
				if ((state & ~HAS_REQUEST_NO_VALUE) != 0) {
					this.value = null;
					discard(v);
					return;
				}

				if (state == HAS_REQUEST_NO_VALUE && STATE.compareAndSet(this, HAS_REQUEST_NO_VALUE, HAS_REQUEST_HAS_VALUE)) {
					this.value = null;
					Subscriber<? super O> a = actual;
					a.onNext(v);
					a.onComplete();
					return;
				}
				setValue(v);
				if (state == NO_REQUEST_NO_VALUE && STATE.compareAndSet(this, NO_REQUEST_NO_VALUE, NO_REQUEST_HAS_VALUE)) {
					return;
				}
			}
		}

		/**
		 * Discard the given value, generally this.value field. Lets derived subscriber with further knowledge about
		 * the possible types of the value discard such values in a specific way. Note that fields should generally be
		 * nulled out along the discard call.
		 *
		 * @param v the value to discard
		 */
		protected void discard(@Nullable O v) {
			Operators.onDiscard(v, actual.currentContext());
		}

		@Override
		public final CoreSubscriber<? super O> actual() {
			return actual;
		}

		/**
		 * Returns true if this Subscription has been cancelled.
		 * @return true if this Subscription has been cancelled
		 */
		public final boolean isCancelled() {
			return state == CANCELLED;
		}

		@Override
		public final boolean isEmpty() {
			return this.state != FUSED_READY;
		}

		@Override
		public void onComplete() {
			actual.onComplete();
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		@SuppressWarnings("unchecked")
		public void onNext(I t) {
			setValue((O) t);
		}

		@Override
		public void onSubscribe(Subscription s) {
			//if upstream
		}

		@Override
		@Nullable
		public final O poll() {
			if (STATE.compareAndSet(this, FUSED_READY, FUSED_CONSUMED)) {
				O v = value;
				value = null;
				return v;
			}
			return null;
		}

		@Override
		public void request(long n) {
			if (validate(n)) {
				for (; ; ) {
					int s = state;
					if (s == CANCELLED) {
						return;
					}
					// if the any bits 1-31 are set, we are either in fusion mode (FUSED_*)
					// or request has been called (HAS_REQUEST_*)
					if ((s & ~NO_REQUEST_HAS_VALUE) != 0) {
						return;
					}
					if (s == NO_REQUEST_HAS_VALUE && STATE.compareAndSet(this, NO_REQUEST_HAS_VALUE, HAS_REQUEST_HAS_VALUE)) {
						O v = value;
						if (v != null) {
							value = null;
							Subscriber<? super O> a = actual;
							a.onNext(v);
							a.onComplete();
						}
						return;
					}
					if (STATE.compareAndSet(this, NO_REQUEST_NO_VALUE, HAS_REQUEST_NO_VALUE)) {
						return;
					}
				}
			}
		}

		@Override
		public int requestFusion(int mode) {
			if ((mode & ASYNC) != 0) {
				STATE.lazySet(this, FUSED_EMPTY);
				return ASYNC;
			}
			return NONE;
		}

		/**
		 * Set the value internally, without impacting request tracking state.
		 * This however discards the provided value when detecting a cancellation.
		 *
		 * @param value the new value.
		 * @see #complete(Object)
		 */
		public void setValue(@Nullable O value) {
			if (STATE.get(this) == CANCELLED) {
				discard(value);
				return;
			}
			this.value = value;
		}

		@Override
		public int size() {
			return isEmpty() ? 0 : 1;
		}

		/**
		 * Indicates this Subscription has no value and not requested yet.
		 */
		static final int NO_REQUEST_NO_VALUE   = 0;
		/**
		 * Indicates this Subscription has a value but not requested yet.
		 */
		static final int NO_REQUEST_HAS_VALUE  = 1;
		/**
		 * Indicates this Subscription has been requested but there is no value yet.
		 */
		static final int HAS_REQUEST_NO_VALUE  = 2;
		/**
		 * Indicates this Subscription has both request and value.
		 */
		static final int HAS_REQUEST_HAS_VALUE = 3;
		/**
		 * Indicates the Subscription has been cancelled.
		 */
		static final int CANCELLED = 4;
		/**
		 * Indicates this Subscription is in fusion mode and is currently empty.
		 */
		static final int FUSED_EMPTY    = 8;
		/**
		 * Indicates this Subscription is in fusion mode and has a value.
		 */
		static final int FUSED_READY    = 16;
		/**
		 * Indicates this Subscription is in fusion mode and its value has been consumed.
		 */
		static final int FUSED_CONSUMED = 32;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<MonoSubscriber> STATE =
				AtomicIntegerFieldUpdater.newUpdater(MonoSubscriber.class, "state");
	}


	/**
	 * A subscription implementation that arbitrates request amounts between subsequent Subscriptions, including the
	 * duration until the first Subscription is set.
	 * <p>
	 * The class is thread safe but switching Subscriptions should happen only when the source associated with the current
	 * Subscription has finished emitting values. Otherwise, two sources may emit for one request.
	 * <p>
	 * You should call {@link #produced(long)} or {@link #producedOne()} after each element has been delivered to properly
	 * account the outstanding request amount in case a Subscription switch happens.
	 *
	 * @param <I> the input value type
	 * @param <O> the output value type
	 */
	abstract static class MultiSubscriptionSubscriber<I, O>
			implements InnerOperator<I, O> {

		final CoreSubscriber<? super O> actual;

		protected boolean unbounded;
		/**
		 * The current subscription which may null if no Subscriptions have been set.
		 */
		Subscription subscription;
		/**
		 * The current outstanding request amount.
		 */
		long         requested;
		volatile Subscription missedSubscription;
		volatile long missedRequested;
		volatile long missedProduced;
		volatile int wip;
		volatile boolean cancelled;

		public MultiSubscriptionSubscriber(CoreSubscriber<? super O> actual) {
			this.actual = actual;
		}

		@Override
		public CoreSubscriber<? super O> actual() {
			return actual;
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;

				drain();
			}
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT)
				return missedSubscription != null ? missedSubscription : subscription;
			if (key == Attr.CANCELLED) return isCancelled();
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM)
				return Operators.addCap(requested, missedRequested);

			return InnerOperator.super.scanUnsafe(key);
		}

		public final boolean isUnbounded() {
			return unbounded;
		}

		final boolean isCancelled() {
			return cancelled;
		}

		@Override
		public void onComplete() {
			actual.onComplete();
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		public void onSubscribe(Subscription s) {
			set(s);
		}

		public final void produced(long n) {
			if (unbounded) {
				return;
			}
			if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
				long r = requested;

				if (r != Long.MAX_VALUE) {
					long u = r - n;
					if (u < 0L) {
						reportMoreProduced();
						u = 0;
					}
					requested = u;
				} else {
					unbounded = true;
				}

				if (WIP.decrementAndGet(this) == 0) {
					return;
				}

				drainLoop();

				return;
			}

			addCap(MISSED_PRODUCED, this, n);

			drain();
		}

		final void producedOne() {
			if (unbounded) {
				return;
			}
			if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
				long r = requested;

				if (r != Long.MAX_VALUE) {
					r--;
					if (r < 0L) {
						reportMoreProduced();
						r = 0;
					}
					requested = r;
				} else {
					unbounded = true;
				}

				if (WIP.decrementAndGet(this) == 0) {
					return;
				}

				drainLoop();

				return;
			}

			addCap(MISSED_PRODUCED, this, 1L);

			drain();
		}

		@Override
		public final void request(long n) {
		    if (validate(n)) {
	            if (unbounded) {
	                return;
	            }
	            if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
	                long r = requested;

	                if (r != Long.MAX_VALUE) {
	                    r = addCap(r, n);
	                    requested = r;
	                    if (r == Long.MAX_VALUE) {
	                        unbounded = true;
	                    }
	                }
		            Subscription a = subscription;

	                if (WIP.decrementAndGet(this) != 0) {
	                    drainLoop();
	                }

	                if (a != null) {
	                    a.request(n);
	                }

	                return;
	            }

	            addCap(MISSED_REQUESTED, this, n);

	            drain();
	        }
		}

		public final void set(Subscription s) {
		    if (cancelled) {
	            s.cancel();
	            return;
	        }

	        Objects.requireNonNull(s);

	        if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
		        Subscription a = subscription;

	            if (a != null && shouldCancelCurrent()) {
	                a.cancel();
	            }

		        subscription = s;

	            long r = requested;

	            if (WIP.decrementAndGet(this) != 0) {
	                drainLoop();
	            }

	            if (r != 0L) {
	                s.request(r);
	            }

	            return;
	        }

	        Subscription a = MISSED_SUBSCRIPTION.getAndSet(this, s);
	        if (a != null && shouldCancelCurrent()) {
	            a.cancel();
	        }
	        drain();
		}

		/**
		 * When setting a new subscription via set(), should
		 * the previous subscription be cancelled?
		 * @return true if cancellation is needed
		 */
		protected boolean shouldCancelCurrent() {
			return false;
		}

		final void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}
			drainLoop();
		}

		final void drainLoop() {
		    int missed = 1;

	        long requestAmount = 0L;
	        long alreadyInRequestAmount = 0L;
	        Subscription requestTarget = null;

	        for (; ; ) {

	            Subscription ms = missedSubscription;

	            if (ms != null) {
	                ms = MISSED_SUBSCRIPTION.getAndSet(this, null);
	            }

	            long mr = missedRequested;
	            if (mr != 0L) {
	                mr = MISSED_REQUESTED.getAndSet(this, 0L);
	            }

	            long mp = missedProduced;
	            if (mp != 0L) {
	                mp = MISSED_PRODUCED.getAndSet(this, 0L);
	            }

		        Subscription a = subscription;

	            if (cancelled) {
	                if (a != null) {
	                    a.cancel();
		                subscription = null;
	                }
	                if (ms != null) {
	                    ms.cancel();
	                }
	            } else {
	                long r = requested;
	                if (r != Long.MAX_VALUE) {
	                    long u = addCap(r, mr);

	                    if (u != Long.MAX_VALUE) {
	                        long v = u - mp;
	                        if (v < 0L) {
	                            reportMoreProduced();
	                            v = 0;
	                        }
	                        r = v;
	                    } else {
	                        r = u;
	                    }
	                    requested = r;
	                }

	                if (ms != null) {
	                    if (a != null && shouldCancelCurrent()) {
	                        a.cancel();
	                    }
		                subscription = ms;
		                if (r != 0L) {
			                requestAmount = addCap(requestAmount, r - alreadyInRequestAmount);
	                        requestTarget = ms;
	                    }
	                } else if (mr != 0L && a != null) {
	                    requestAmount = addCap(requestAmount, mr);
	                    alreadyInRequestAmount += mr; 
	                    requestTarget = a;
	                }
	            }

	            missed = WIP.addAndGet(this, -missed);
	            if (missed == 0) {
	                if (requestAmount != 0L) {
	                    requestTarget.request(requestAmount);
	                }
	                return;
	            }
	        }
		}
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MultiSubscriptionSubscriber, Subscription>
				MISSED_SUBSCRIPTION =
		  AtomicReferenceFieldUpdater.newUpdater(MultiSubscriptionSubscriber.class,
			Subscription.class,
			"missedSubscription");
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<MultiSubscriptionSubscriber>
				MISSED_REQUESTED =
		  AtomicLongFieldUpdater.newUpdater(MultiSubscriptionSubscriber.class, "missedRequested");
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<MultiSubscriptionSubscriber> MISSED_PRODUCED =
		  AtomicLongFieldUpdater.newUpdater(MultiSubscriptionSubscriber.class, "missedProduced");
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<MultiSubscriptionSubscriber> WIP =
		  AtomicIntegerFieldUpdater.newUpdater(MultiSubscriptionSubscriber.class, "wip");
	}

	/**
	 * Represents a fuseable Subscription that emits a single constant value synchronously
	 * to a Subscriber or consumer.
	 *
	 * @param <T> the value type
	 */
	static final class ScalarSubscription<T>
			implements Fuseable.SynchronousSubscription<T>, InnerProducer<T> {

		final CoreSubscriber<? super T> actual;

		final T value;

		@Nullable
		final String stepName;

		volatile int once;

		ScalarSubscription(CoreSubscriber<? super T> actual, T value) {
			this(actual, value, null);
		}

		ScalarSubscription(CoreSubscriber<? super T> actual, T value, String stepName) {
			this.value = Objects.requireNonNull(value, "value");
			this.actual = Objects.requireNonNull(actual, "actual");
			this.stepName = stepName;
		}

		@Override
		public void cancel() {
			if (once == 0) {
				Operators.onDiscard(value, actual.currentContext());
			}
			ONCE.lazySet(this, 2);
		}

		@Override
		public void clear() {
			if (once == 0) {
				Operators.onDiscard(value, actual.currentContext());
			}
			ONCE.lazySet(this, 1);
		}

		@Override
		public boolean isEmpty() {
			return once != 0;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public T poll() {
			if (once == 0) {
				ONCE.lazySet(this, 1);
				return value;
			}
			return null;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return once == 1;
			if (key == Attr.CANCELLED) return once == 2;
			if (key == Attr.RUN_STYLE) return RunStyle.SYNC;

			return InnerProducer.super.scanUnsafe(key);
		}

		@Override
		public void request(long n) {
			if (validate(n)) {
				if (ONCE.compareAndSet(this, 0, 1)) {
					Subscriber<? super T> a = actual;
					a.onNext(value);
					if(once != 2) {
						a.onComplete();
					}
				}
			}
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & Fuseable.SYNC) != 0) {
				return Fuseable.SYNC;
			}
			return 0;
		}

		@Override
		public int size() {
			return isEmpty() ? 0 : 1;
		}

		@Override
		public String stepName() {
			return stepName != null ? stepName : ("scalarSubscription(" + value + ")");
		}

		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ScalarSubscription> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(ScalarSubscription.class, "once");
	}

	final static class DrainSubscriber<T> implements CoreSubscriber<T> {

		static final DrainSubscriber INSTANCE = new DrainSubscriber();

		@Override
		public void onSubscribe(Subscription s) {
			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(Object o) {

		}

		@Override
		public void onError(Throwable t) {
			Operators.onErrorDropped(Exceptions.errorCallbackNotImplemented(t), Context.empty());
		}

		@Override
		public void onComplete() {

		}

		@Override
		public Context currentContext() {
			return Context.empty();
		}
	}

	/**
	 * This class wraps any non-conditional {@link CoreSubscriber<T>} so the delegate
	 * can have an emulation of {@link reactor.core.Fuseable.ConditionalSubscriber<T>}
	 * behaviors
	 *
	 * @param <T> passed subscriber type
	 */
	final static class ConditionalSubscriberAdapter<T> implements Fuseable.ConditionalSubscriber<T> {

		final CoreSubscriber<T> delegate;

		ConditionalSubscriberAdapter(CoreSubscriber<T> delegate) {
			this.delegate = delegate;
		}

		@Override
		public Context currentContext() {
			return delegate.currentContext();
		}

		@Override
		public void onSubscribe(Subscription s) {
			delegate.onSubscribe(s);
		}

		@Override
		public void onNext(T t) {
			delegate.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			delegate.onError(t);
		}

		@Override
		public void onComplete() {
			delegate.onComplete();
		}

		@Override
		public boolean tryOnNext(T t) {
			delegate.onNext(t);
			return true;
		}
	}

	final static class LiftFunction<I, O>
			implements Function<Publisher<I>, Publisher<O>> {

		final Predicate<Publisher> filter;
		final String name;

		final BiFunction<Publisher, ? super CoreSubscriber<? super O>,
				? extends CoreSubscriber<? super I>> lifter;

		static final <I, O> LiftFunction<I, O> liftScannable(
				@Nullable Predicate<Scannable> filter,
				BiFunction<Scannable, ? super CoreSubscriber<? super O>, ? extends CoreSubscriber<? super I>> lifter) {
			Objects.requireNonNull(lifter, "lifter");

			Predicate<Publisher> effectiveFilter =  null;
			if (filter != null) {
				effectiveFilter = pub -> filter.test(Scannable.from(pub));
			}

			BiFunction<Publisher, ? super CoreSubscriber<? super O>, ? extends CoreSubscriber<? super I>>
					effectiveLifter = (pub, sub) -> lifter.apply(Scannable.from(pub), sub);

			return new LiftFunction<>(effectiveFilter, effectiveLifter, lifter.toString());
		}

		static final <I, O> LiftFunction<I, O> liftPublisher(
				@Nullable Predicate<Publisher> filter,
				BiFunction<Publisher, ? super CoreSubscriber<? super O>, ? extends CoreSubscriber<? super I>> lifter) {
			Objects.requireNonNull(lifter, "lifter");
			return new LiftFunction<>(filter, lifter, lifter.toString());
		}

		private LiftFunction(@Nullable Predicate<Publisher> filter,
				BiFunction<Publisher, ? super CoreSubscriber<? super O>, ? extends CoreSubscriber<? super I>> lifter,
				String name) {
			this.filter = filter;
			this.lifter = Objects.requireNonNull(lifter, "lifter");
			this.name = Objects.requireNonNull(name, "name");
		}

		@Override
		@SuppressWarnings("unchecked")
		public Publisher<O> apply(Publisher<I> publisher) {
			if (filter != null && !filter.test(publisher)) {
				return (Publisher<O>)publisher;
			}

			if (publisher instanceof Fuseable) {
				if (publisher instanceof Mono) {
					return new MonoLiftFuseable<>(publisher, this);
				}
				if (publisher instanceof ParallelFlux) {
					return new ParallelLiftFuseable<>((ParallelFlux<I>)publisher, this);
				}
				if (publisher instanceof ConnectableFlux) {
					return new ConnectableLiftFuseable<>((ConnectableFlux<I>) publisher, this);
				}
				if (publisher instanceof GroupedFlux) {
					return new GroupedLiftFuseable<>((GroupedFlux<?, I>) publisher, this);
				}
				return new FluxLiftFuseable<>(publisher, this);
			}
			else {
				if (publisher instanceof Mono) {
					return new MonoLift<>(publisher, this);
				}
				if (publisher instanceof ParallelFlux) {
					return new ParallelLift<>((ParallelFlux<I>)publisher, this);
				}
				if (publisher instanceof ConnectableFlux) {
					return new ConnectableLift<>((ConnectableFlux<I>) publisher, this);
				}
				if (publisher instanceof GroupedFlux) {
					return new GroupedLift<>((GroupedFlux<?, I>) publisher, this);
				}
				return new FluxLift<>(publisher, this);
			}
		}
	}

	/*package*/ static class MonoInnerProducerBase<O>
			implements InnerProducer<O> {

		private final CoreSubscriber<? super O> actual;

		/**
		 * The value stored by this Mono operator.
		 */
		private O value;

		private volatile  int state; //see STATE field updater
		@SuppressWarnings("rawtypes")
		private static final AtomicIntegerFieldUpdater<MonoInnerProducerBase> STATE =
				AtomicIntegerFieldUpdater.newUpdater(MonoInnerProducerBase.class, "state");

		public MonoInnerProducerBase(CoreSubscriber<? super O> actual) {
			this.actual = actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.CANCELLED) return isCancelled();
			if (key == Attr.TERMINATED) return hasCompleted(state);
			if (key == Attr.PREFETCH) return Integer.MAX_VALUE;
			return InnerProducer.super.scanUnsafe(key);
		}

		/**
		 * Tries to emit the provided value and complete the underlying subscriber or
		 * stores the value away until there is a request for it.
		 * <p>
		 * Make sure this method is called at most once. Can't be used in addition to {@link #complete()}.
		 * @param v the value to emit
		 */
		public final void complete(O v) {
			for (; ; ) {
				int s = this.state;
				if (isCancelled(s)) {
					discard(v);
					return;
				}

				if (hasRequest(s) && STATE.compareAndSet(this, s, s | (HAS_VALUE | HAS_COMPLETED))) {
					this.value = null;

					doOnComplete(v);

					actual.onNext(v);
					actual.onComplete();
					return;
				}

				this.value = v;
				if ( /*!hasRequest(s) && */ STATE.compareAndSet(this, s, s | (HAS_VALUE | HAS_COMPLETED))) {
					return;
				}
			}
		}

		/**
		 * Tries to emit the {@link #value} stored if any, and complete the underlying subscriber.
		 * <p>
		 * Make sure this method is called at most once. Can't be used in addition to {@link #complete(Object)}.
		 */
		public final void complete() {
			for (; ; ) {
				int s = this.state;
				if (isCancelled(s)) {
					return;
				}
				if (STATE.compareAndSet(this, s, s | HAS_COMPLETED)) {
					if (hasValue(s) && hasRequest(s)) {
						O v = this.value;
						this.value = null; // aggressively null value to prevent strong ref after complete

						doOnComplete(v);

						actual.onNext(v);
						actual.onComplete();
						return;
					}
					if (!hasValue(s)) {
						actual.onComplete();
						return;
					}
					if (!hasRequest(s)) {
						return;
					}
				}
			}
		}

		/**
		 * Hook for subclasses when the actual completion appears
		 *
		 * @param v the value passed to {@code onComplete(Object)}
		 */
		protected void doOnComplete(O v) {

		}

		/**
		 * Discard the given value, if the value to discard is not the one held by this instance
		 * (see {@link #discardTheValue()} for that purpose. Lets derived subscriber with further knowledge about
		 * the possible types of the value discard such values in a specific way. Note that fields should generally be
		 * nulled out along the discard call.
		 *
		 * @param v the value to discard
		 */
		protected final void discard(@Nullable O v) {
			Operators.onDiscard(v, actual.currentContext());
		}

		protected final void discardTheValue() {
			discard(this.value);
			this.value = null; // aggressively null value to prevent strong ref after complete
		}

		@Override
		public final CoreSubscriber<? super O> actual() {
			return actual;
		}

		/**
		 * Returns true if this Subscription has been cancelled.
		 * @return true if this Subscription has been cancelled
		 */
		public final boolean isCancelled() {
			return state == CANCELLED;
		}


		@Override
		public void request(long n) {
			if (validate(n)) {
				for (; ; ) {
					int s = state;
					if (isCancelled(s)) {
						return;
					}
					if (hasRequest(s)) {
						return;
					}
					if (STATE.compareAndSet(this, s, s | HAS_REQUEST)) {
						doOnRequest(n);
						if (hasValue(s) && hasCompleted(s)) {
							O v = this.value;
							this.value = null; // aggressively null value to prevent strong ref after complete

							doOnComplete(v);

							actual.onNext(v);
							actual.onComplete();
						}
						return;
					}
				}
			}
		}

		/**
		 * Hook for subclasses on the first request successfully marked on the state
		 * @param n the value passed to {@code request(long)}
		 */
		protected void doOnRequest(long n) {

		}

		/**
		 * Set the value internally, without impacting request tracking state.
		 * This however discards the provided value when detecting a cancellation.
		 *
		 * @param value the new value.
		 * @see #complete(Object)
		 */
		protected final void setValue(@Nullable O value) {
			this.value = value;
			for (; ; ) {
				int s = this.state;
				if (isCancelled(s)) {
					discardTheValue();
					return;
				}
				if (STATE.compareAndSet(this, s, s | HAS_VALUE)) {
					return;
				}
			}
		}

		@Override
		public final void cancel() {
			int previous = STATE.getAndSet(this, CANCELLED);

			if (isCancelled(previous)) {
				return;
			}

			doOnCancel();

			if (hasValue(previous) // Had a value...
					&& (previous & (HAS_COMPLETED|HAS_REQUEST)) != (HAS_COMPLETED|HAS_REQUEST) // ... but did not use it
			) {
				discardTheValue();
			}
		}

		/**
		 * Hook for subclasses, called as the first operation when {@link #cancel()} is called. Default
		 * implementation is a no-op.
		 */
		protected void doOnCancel() {

		}

		// The following are to be used as bit masks, not as values per se.
		private static final int HAS_VALUE =      0b00000001;
		private static final int HAS_REQUEST =    0b00000010;
		private static final int HAS_COMPLETED =  0b00000100;
		// The following are to be used as value (ie using == or !=).
		private static final int CANCELLED =      0b10000000;

		private static boolean isCancelled(int s) {
			return s == CANCELLED;
		}

		private static boolean hasRequest(int s) {
			return (s & HAS_REQUEST) == HAS_REQUEST;
		}

		private static boolean hasValue(int s) {
			return (s & HAS_VALUE) == HAS_VALUE;
		}

		private static boolean hasCompleted(int s) {
			return (s & HAS_COMPLETED) == HAS_COMPLETED;
		}

	}


	final static Logger log = Loggers.getLogger(Operators.class);
}

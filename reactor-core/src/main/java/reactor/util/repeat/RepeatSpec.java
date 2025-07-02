/*
 * Copyright (c) 2025 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.util.repeat;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

/**
 * A repeat strategy that allows fine-grained control over repeating behavior in {@link Flux#repeatWhen} and
 * {@link reactor.core.publisher.Mono#repeatWhen}. This includes limiting the number of repeats, applying delays,
 * using custom predicates, attaching hooks, and applying jitter.
 * <p>
 * Use static factory methods such as {@link #times(long)} to start building a {@code
 * RepeatSpec}.
 * The builder is immutable and supports a fluent copy-on-write style, so intermediate configurations can be safely reused.
 * <p>
 * Additional configuration includes:
 * <ul>
 *   <li>Conditional repetition via {@link #onlyIf(Predicate)}</li>
 *   <li>Hook functions before and after each repeat trigger via {@link #doBeforeRepeat} and {@link #doAfterRepeat}</li>
 *   <li>Custom delay strategies via {@link #withFixedDelay(Duration)}, {@link #jitter(double)}, and {@link #withScheduler(Scheduler)}</li>
 *   <li>Context propagation via {@link #withRepeatContext(ContextView)}</li>
 * </ul>
 * This strategy does not retry based on error signals, but rather repeats a successful sequence (until termination condition is met).
 *
 * @author Daeho Kwon
 */
public final class RepeatSpec extends Repeat {

	private static final Predicate<RepeatSignal> ALWAYS_TRUE    = __ -> true;
	private static final Consumer<RepeatSignal>  NO_OP_CONSUMER = rs -> {
	};
	private static final RepeatSignal            TERMINATE      =
			new ImmutableRepeatSignal(-1, -1L, Duration.ZERO, Context.empty());

	final long                    maxRepeats;
	final Predicate<RepeatSignal> repeatPredicate;
	final Consumer<RepeatSignal>  beforeRepeatHook;
	final Consumer<RepeatSignal>  afterRepeatHook;
	final Duration                fixedDelay;
	final Scheduler               scheduler;
	final double                  jitterFactor;

	/**
	 * Copy constructor.
	 */
	private RepeatSpec(ContextView repeatContext,
			long maxRepeats,
			Predicate<RepeatSignal> repeatPredicate,
			Consumer<RepeatSignal> beforeRepeatHook,
			Consumer<RepeatSignal> afterRepeatHook,
			Duration fixedDelay,
			Scheduler scheduler,
			double jitterFactor) {
		super(repeatContext);
		this.maxRepeats = maxRepeats;
		this.repeatPredicate = repeatPredicate;
		this.beforeRepeatHook = beforeRepeatHook;
		this.afterRepeatHook = afterRepeatHook;
		this.fixedDelay = fixedDelay;
		this.scheduler = scheduler;
		this.jitterFactor = jitterFactor;
	}

	/**
	 * Creates a {@code RepeatSpec} that repeats once.
	 *
	 * @return a new {@code RepeatSpec} instance
	 */
	public static RepeatSpec once() {
		return times(1L);
	}

	/**
	 * Creates a {@code RepeatSpec} that repeats n times.
	 *
	 * @param n number of repeats
	 * @return a new {@code RepeatSpec} instance
	 */
	public static RepeatSpec times(long n) {
		if (n < 0) {
			throw new IllegalArgumentException("n should be >= 0");
		}

		return new RepeatSpec(Context.empty(),
				n,
				ALWAYS_TRUE,
				NO_OP_CONSUMER,
				NO_OP_CONSUMER,
				java.time.Duration.ZERO,
				Schedulers.parallel(),
				0d);
	}

	/**
	 * Creates a {@code RepeatSpec} that repeats n times, only if the predicate returns true.
	 *
	 * @param predicate Predicate that determines if next repeat is performed
	 * @param n number of repeats
	 * @return a new {@code RepeatSpec} instance
	 */
	public static RepeatSpec create(Predicate<RepeatSignal> predicate, long n) {
		return new RepeatSpec(Context.empty(),
				n,
				predicate,
				NO_OP_CONSUMER,
				NO_OP_CONSUMER,
				java.time.Duration.ZERO,
				Schedulers.parallel(),
				0d);
	}

	/**
	 * Set the user provided {@link Repeat#repeatContext() context} that can be used to
	 * manipulate state on retries.
	 *
	 * @param repeatContext a new snapshot of user provided data
	 * @return a new copy of the {@link RepeatSpec} which can either be further configured
	 * or used as {@link Repeat}
	 */
	public RepeatSpec withRepeatContext(ContextView repeatContext) {
		return new RepeatSpec(repeatContext,
				this.maxRepeats,
				this.repeatPredicate,
				this.beforeRepeatHook,
				this.afterRepeatHook,
				this.fixedDelay,
				this.scheduler,
				this.jitterFactor);
	}

	/**
	 * Sets a predicate that determines whether a repeat should occur based on the current {@link RepeatSignal}.
	 * This allows conditional repetition based on iteration count, companion value, delay, or context.
	 *
	 * @param predicate the condition to test for each {@link RepeatSignal}
	 * @return a new copy of this {@code RepeatSpec} with the updated predicate
	 */
	public RepeatSpec onlyIf(Predicate<RepeatSignal> predicate) {
		return new RepeatSpec(this.repeatContext,
				this.maxRepeats,
				predicate,
				this.beforeRepeatHook,
				this.afterRepeatHook,
				this.fixedDelay,
				this.scheduler,
				this.jitterFactor);
	}

	/**
	 * Adds a synchronous doBeforeRepeat to be executed <strong>before</strong> the repeat trigger.
	 * This is commonly used for side effects like logging or state updates right before each repetition.
	 *
	 * @param doBeforeRepeat the {@link Consumer} to invoke with the current {@link RepeatSignal}
	 * @return a new copy of this {@code RepeatSpec} with the doBeforeRepeat applied
	 */
	public RepeatSpec doBeforeRepeat(Consumer<RepeatSignal> doBeforeRepeat) {
		return new RepeatSpec(this.repeatContext,
				this.maxRepeats,
				this.repeatPredicate,
				this.beforeRepeatHook.andThen(doBeforeRepeat),
				this.afterRepeatHook,
				this.fixedDelay,
				this.scheduler,
				this.jitterFactor);
	}

	/**
	 * Adds a synchronous doAfterRepeat to be executed <strong>after</strong> the repeat trigger has completed.
	 * This is useful for tracking completion, updating metrics, or performing cleanup work after each repeat cycle.
	 *
	 * @param doAfterRepeat the {@link Consumer} to invoke with the current {@link RepeatSignal}
	 * @return a new copy of this {@code RepeatSpec} with the doAfterRepeat applied
	 */
	public RepeatSpec doAfterRepeat(Consumer<RepeatSignal> doAfterRepeat) {
		return new RepeatSpec(this.repeatContext,
				this.maxRepeats,
				this.repeatPredicate,
				this.beforeRepeatHook,
				this.afterRepeatHook.andThen(doAfterRepeat),
				this.fixedDelay,
				this.scheduler,
				this.jitterFactor);
	}

	/**
	 * Applies a fixed delay between repeat iterations.
	 *
	 * @param delay the {@link Duration} of delay to apply between repeat attempts
	 * @return a new copy of this {@code RepeatSpec} with the delay applied
	 */
	public RepeatSpec withFixedDelay(Duration delay) {
		return new RepeatSpec(this.repeatContext,
				this.maxRepeats,
				this.repeatPredicate,
				this.beforeRepeatHook,
				this.afterRepeatHook,
				delay,
				this.scheduler,
				this.jitterFactor);
	}

	/**
	 * Applies jitter to the configured fixed delay.
	 * This randomizes the delay duration within a bounded range for each repeat.
	 *
	 * @param jitter jitter factor as a percentage of the base delay (e.g. 0.1 = ±10%)
	 * @return a new copy of this {@code RepeatSpec} with jitter configured
	 */
	public RepeatSpec jitter(double jitter) {
		return new RepeatSpec(this.repeatContext,
				this.maxRepeats,
				this.repeatPredicate,
				this.beforeRepeatHook,
				this.afterRepeatHook,
				this.fixedDelay,
				this.scheduler,
				jitter);
	}

	/**
	 * Sets a {@link Scheduler} to use for delaying repeat attempts.
	 *
	 * @param scheduler the scheduler to apply
	 * @return a new copy of this {@code RepeatSpec} with the scheduler applied
	 */
	public RepeatSpec withScheduler(Scheduler scheduler) {
		return new RepeatSpec(this.repeatContext,
				this.maxRepeats,
				this.repeatPredicate,
				this.beforeRepeatHook,
				this.afterRepeatHook,
				this.fixedDelay,
				scheduler,
				this.jitterFactor);
	}

	@Override
	public Publisher<Long> generateCompanion(Flux<Long> signals) {
		return Flux.deferContextual(cv -> signals.index()
		                                         .contextWrite(cv)
		                                         .map(tuple -> {
			                                         long iter = tuple.getT1();
			                                         long companionValue = tuple.getT2();

			                                         RepeatSignal signal =
					                                         new ImmutableRepeatSignal(
							                                         iter,
							                                         companionValue,
							                                         calculateDelay(),
							                                         repeatContext());

			                                         if (iter >= maxRepeats || !repeatPredicate.test(
					                                         signal)) {
				                                         return TERMINATE;
			                                         }
			                                         return signal;
		                                         })
		                                         .takeWhile(signal -> signal != TERMINATE)
		                                         .concatMap(signal -> {
			                                         try {
				                                         beforeRepeatHook.accept(signal);
			                                         }
			                                         catch (Throwable e) {
				                                         return Mono.error(e);
			                                         }

			                                         Duration delay = signal.backoff();
			                                         Mono<Long> trigger = delay.isZero() ?
					                                         Mono.just(signal.companionValue()) :
					                                         Mono.delay(delay, scheduler)
					                                             .thenReturn(signal.companionValue());

			                                         return trigger.doOnSuccess(v -> afterRepeatHook.accept(
					                                         signal));
		                                         }));
	}

	Duration calculateDelay() {
		Duration actual = fixedDelay;
		if (!fixedDelay.isZero() && jitterFactor > 0d) {
			long base = fixedDelay.toMillis();
			long spread = (long) (base * jitterFactor);
			long offset = ThreadLocalRandom.current()
			                               .nextLong(-spread, spread + 1);
			actual = Duration.ofMillis(Math.max(0, base + offset));
		}
		return actual;
	}
}
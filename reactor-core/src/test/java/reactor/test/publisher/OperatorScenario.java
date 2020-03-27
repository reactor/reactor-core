/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.test.publisher;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;


import org.reactivestreams.Publisher;
import reactor.test.StepVerifier;
import reactor.util.annotation.Nullable;

import static reactor.core.Fuseable.NONE;

/**
 * @author Stephane Maldini
 */
public class OperatorScenario<I, PI extends Publisher<? extends I>, O, PO extends Publisher<? extends O>> {

	final Function<PI, ? extends PO> body;
	final Exception                  stack;

	RuntimeException producerError                        = null;
	RuntimeException droppedError                         = null;
	I                droppedItem                          = null;
	int              fusionMode                           = NONE;
	int              fusionModeThreadBarrier              = NONE;
	int              prefetch                             = -1;
	boolean          shouldHitDropNextHookAfterTerminate  = true;
	boolean          shouldHitDropErrorHookAfterTerminate = true;
	boolean          shouldAssertPostTerminateState       = true;
	int              producing                            = -1;
	int              demand                               = -1;
	IntFunction<? extends I> producingMapper;

	@Nullable Consumer<? super O>[]          receivers      = null;
	@Nullable O[]                            receiverValues = null;
	@Nullable String                         description    = null;
	@Nullable Consumer<StepVerifier.Step<O>> verifier       = null;

	OperatorScenario(@Nullable Function<PI, ? extends PO> body, @Nullable Exception stack) {
		this.body = body;
		this.stack = stack;
	}

	public OperatorScenario<I, PI, O, PO> applyAllOptions(@Nullable OperatorScenario<I, PI, O, PO> source) {
		if (source == null) {
			return this;
		}
		this.description = source.description;
		this.fusionMode = source.fusionMode;
		this.receivers = source.receivers;
		this.receiverValues = source.receiverValues;
		this.producerError = source.producerError;
		this.droppedItem = source.droppedItem;
		this.droppedError = source.droppedError;
		this.demand = source.demand;
		this.producing = source.producing;
		this.producingMapper = source.producingMapper;
		this.fusionModeThreadBarrier = source.fusionModeThreadBarrier;
		this.prefetch = source.prefetch;
		this.shouldHitDropNextHookAfterTerminate =
				source.shouldHitDropNextHookAfterTerminate;
		this.shouldHitDropErrorHookAfterTerminate =
				source.shouldHitDropErrorHookAfterTerminate;
		this.shouldAssertPostTerminateState = source.shouldAssertPostTerminateState;
		this.verifier = source.verifier;
		return this;
	}

	public OperatorScenario<I, PI, O, PO> description(String description) {
		this.description = description;
		return this;
	}

	public OperatorScenario<I, PI, O, PO> fusionMode(int fusionMode) {
		this.fusionMode = fusionMode;
		return this;
	}

	public OperatorScenario<I, PI, O, PO> fusionModeThreadBarrier(int fusionModeThreadBarrier) {
		this.fusionModeThreadBarrier = fusionModeThreadBarrier;
		return this;
	}

	public OperatorScenario<I, PI, O, PO> prefetch(int prefetch) {
		this.prefetch = prefetch;
		return this;
	}

	public OperatorScenario<I, PI, O, PO> producer(int n,
			IntFunction<? extends I> producer) {
		if (n < 0) {
			throw new IllegalArgumentException("negative number of produced data");
		}
		this.producingMapper = Objects.requireNonNull(producer, "producer");
		this.producing = n;
		return this;
	}

	public OperatorScenario<I, PI, O, PO> producerEmpty() {
		this.producing = 0;
		return this;
	}

	public OperatorScenario<I, PI, O, PO> producerNever() {
		this.producing = -1;
		return this;
	}

	public OperatorScenario<I, PI, O, PO> producerError(RuntimeException producerError) {
		this.producerError = Objects.requireNonNull(producerError, "producerError");
		return this;
	}

	public OperatorScenario<I, PI, O, PO> droppedError(RuntimeException producerError) {
		this.droppedError = Objects.requireNonNull(producerError, "producerError");
		return this;
	}

	public OperatorScenario<I, PI, O, PO> droppedItem(I item) {
		this.droppedItem = Objects.requireNonNull(item, "item");
		return this;
	}

	@SuppressWarnings("unchecked")
	public OperatorScenario<I, PI, O, PO> receive(Consumer<? super O>... receivers) {
		this.receivers = Objects.requireNonNull(receivers, "receivers");
		if (this.demand != 0 && this.demand < receivers.length) {
			this.demand = receivers.length;
		}
		this.receiverValues = null;
		return this;
	}

	public OperatorScenario<I, PI, O, PO> receive(int n,
			IntFunction<? extends O> receiverMapper) {

		if (n < 1) {
			throw new IllegalArgumentException("Minimum 1 receiver expectation");
		}

		@SuppressWarnings("unchecked") O[] receivers = (O[]) new Object[n];

		for (int i = 0; i < n; i++) {
			receivers[i] = receiverMapper.apply(i);
		}
		return receiveValues(receivers);
	}

	@SuppressWarnings("unchecked")
	public OperatorScenario<I, PI, O, PO> receiveValues(O... receivers) {
		this.receiverValues = Objects.requireNonNull(receivers, "receivers");
		if (this.demand != 0 && this.demand < receivers.length) {
			this.demand = receivers.length;
		}
		this.receivers = null;
		return this;
	}

	public OperatorScenario<I, PI, O, PO> receiverDemand(long d) {
		if (d < 0) {
			throw new IllegalArgumentException("demand must be positive, was: " + d);
		}
		this.demand = (int) Math.min(Integer.MAX_VALUE, d);
		return this;
	}

	public OperatorScenario<I, PI, O, PO> receiverEmpty() {
		this.receiverValues = null;
		this.receivers = null;
		return this;
	}

	public OperatorScenario<I, PI, O, PO> shouldAssertPostTerminateState(boolean shouldAssertPostTerminateState) {
		this.shouldAssertPostTerminateState = shouldAssertPostTerminateState;
		return this;
	}

	public OperatorScenario<I, PI, O, PO> shouldHitDropErrorHookAfterTerminate(boolean shouldHitDropErrorHookAfterTerminate) {
		this.shouldHitDropErrorHookAfterTerminate = shouldHitDropErrorHookAfterTerminate;
		return this;
	}

	public OperatorScenario<I, PI, O, PO> shouldHitDropNextHookAfterTerminate(boolean shouldHitDropNextHookAfterTerminate) {
		this.shouldHitDropNextHookAfterTerminate = shouldHitDropNextHookAfterTerminate;
		return this;
	}

	public OperatorScenario<I, PI, O, PO> verifier(Consumer<StepVerifier.Step<O>> verifier) {
		this.verifier = verifier;
		return this;
	}

	@Nullable
	final Consumer<StepVerifier.Step<O>> verifier() {
		return verifier;
	}

	final StepVerifier.Step<O> applySteps(StepVerifier.Step<O> step) {
		return applySteps(0, Integer.MAX_VALUE, step);
	}

	StepVerifier.Step<O> applySteps(int initial, StepVerifier.Step<O> step) {
		if (initial < 0) {
			throw new IllegalArgumentException("initial demand cannot be negative");
		}

		step = applySteps(0, initial, step);

		if (initial == Integer.MAX_VALUE) {
			return step;
		}

		int toRequest = demand == -1 ? producing : demand;

		toRequest = Math.max(toRequest - initial, 0);

		if (toRequest == 0) {
			return step;
		}

		step = step.thenRequest(toRequest);

		if (receiverValues == null && receivers == null) {
			return step;
		}

		return applySteps(initial, Integer.MAX_VALUE, step);
	}

	final StepVerifier.Step<O> applySteps(int start,
			int stop,
			StepVerifier.Step<O> step) {
		if (stop <= start) {
			return step;
		}

		if (receivers != null) {
			for (int i = start; i < receivers.length; i++) {
				step = step.assertNext(receivers[i]);
				if (i == stop - 1) {
					return step;
				}
			}
		}
		else if (receiverValues != null) {
			for (int i = start; i < receiverValues.length; i++) {
				step = step.expectNext(receiverValues[i]);
				if (i == stop - 1) {
					return step;
				}
			}
		}

		return step;
	}

	@Nullable
	final Function<PI, ? extends PO> body() {
		return body;
	}

	final String description() {
		if (description != null) {
			return description;
		}

		if (stack != null) {
			StackTraceElement element = stack.getStackTrace()[2];
			return element.getFileName() + ":" + element.getLineNumber();
		}

		return toString();
	}

	OperatorScenario<I, PI, O, PO> duplicate() {
		return new OperatorScenario<I, PI, O, PO>(body, stack).applyAllOptions(this);
	}

	final int fusionMode() {
		return fusionMode;
	}

	final int prefetch() {
		return prefetch;
	}

	final int producerCount() {
		return producing;
	}

	final int receiverCount() {
		return demand;
	}

	final boolean shouldAssertPostTerminateState() {
		return shouldAssertPostTerminateState;
	}

	final boolean shouldHitDropErrorHookAfterTerminate() {
		return shouldHitDropErrorHookAfterTerminate;
	}

	final boolean shouldHitDropNextHookAfterTerminate() {
		return shouldHitDropNextHookAfterTerminate;
	}
}

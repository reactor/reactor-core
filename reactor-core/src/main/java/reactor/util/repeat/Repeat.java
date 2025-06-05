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

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.util.context.ContextView;

import java.time.Duration;
import java.util.function.Function;


/**
 * Base abstract class for a strategy to control repeat behavior given a companion {@link Flux} of {@link Long} values,
 * for use with {@link Flux#repeatWhen(Function)} and {@link reactor.core.publisher.Mono#repeatWhen(Function)}.
 * This class is typically extended by repeat strategy builders such as {@link RepeatSpec}.
 * <p>
 * The companion {@code Flux<Long>} represents repeat signals, and each value corresponds to a repeat attempt.
 * This class transforms the repeat signals into a {@link Publisher} that determines whether to trigger a repeat.
 *
 * @author Daeho Kwon
 */
abstract class Repeat implements Function<Flux<Long>, Publisher<?>> {

	public final ContextView repeatContext;

	protected Repeat(ContextView repeatContext) {
		this.repeatContext = repeatContext;
	}

	@Override
	public final Publisher<?> apply(Flux<Long> signals) {
		return generateCompanion(signals);
	}

	/**
	 * Generates the companion publisher responsible for reacting to incoming repeat signals,
	 * effectively deciding whether to trigger another repeat cycle.
	 *
	 * @param repeatSignals the incoming repeat signals, where each {@link Long} value indicates the iteration index
	 * @return the companion publisher that determines if a repeat should occur
	 */
	public abstract Publisher<?> generateCompanion(Flux<Long> repeatSignals);

	public ContextView repeatContext() {
		return this.repeatContext;
	}

	/**
	 * State information associated with each repeat signal, used in repeat strategies.
	 */
	interface RepeatSignal {

		/**
		 * Returns the current iteration count, starting from 0.
		 *
		 * @return the iteration index
		 */
		long iteration();

		/**
		 * Returns the value from the companion publisher that triggered this repeat signal.
		 *
		 * @return the companion value
		 */
		Long companionValue();

		/**
		 * Returns the delay before the next repeat attempt.
		 *
		 * @return the backoff duration
		 */
		Duration backoff();

		/**
		 * Returns the read-only context associated with this repeat signal.
		 *
		 * @return the repeat context view
		 */
		ContextView repeatContextView();

		/**
		 * Returns an immutable copy of this {@link RepeatSignal}, capturing the current state.
		 *
		 * @return an immutable copy of the signal
		 */
		RepeatSignal copy();
	}
}

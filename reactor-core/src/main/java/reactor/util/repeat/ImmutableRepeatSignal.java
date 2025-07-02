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

import reactor.util.context.ContextView;

import java.time.Duration;

/**
 * An immutable {@link reactor.util.repeat.RepeatSpec.RepeatSignal} that can be used for
 * retained copies of mutable implementations.
 *
 * @author Daeho Kwon
 */
final class ImmutableRepeatSignal implements RepeatSpec.RepeatSignal {

	private final long        iteration;
	private final Long        companionValue;
	private final Duration    backoff;
	private final ContextView repeatContext;

	ImmutableRepeatSignal(long iteration,
			Long companionValue,
			Duration backoff,
			ContextView repeatContext) {
		this.iteration = iteration;
		this.companionValue = companionValue;
		this.backoff = backoff;
		this.repeatContext = repeatContext;
	}

	@Override
	public long iteration() {
		return iteration;
	}

	@Override
	public Long companionValue() {
		return companionValue;
	}

	@Override
	public Duration backoff() {
		return backoff;
	}

	@Override
	public ContextView repeatContextView() {
		return repeatContext;
	}

	@Override
	public RepeatSpec.RepeatSignal copy() {
		return this;
	}

	@Override
	public String toString() {
		return "RepeatSignal(iteration=" + iteration + ", companionValue=" + companionValue + ", backoff=" + backoff + ", context=" + repeatContext + ")";
	}
}

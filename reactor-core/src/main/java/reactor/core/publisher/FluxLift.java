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
package reactor.core.publisher;

import java.util.Objects;
import java.util.function.BiFunction;

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;

/**
 * @author Stephane Maldini
 */
final class FluxLift<I, O> extends InternalFluxOperator<I, O> {

	final BiFunction<Publisher, ? super CoreSubscriber<? super O>, ? extends CoreSubscriber<? super I>>
			lifter;

	FluxLift(Publisher<I> p,
			BiFunction<Publisher, ? super CoreSubscriber<? super O>, ? extends CoreSubscriber<? super I>> lifter) {
		super(Flux.from(p));
		this.lifter = lifter;
	}

	@Override
	public CoreSubscriber<? super I> subscribeOrReturn(CoreSubscriber<? super O> actual) {
		CoreSubscriber<? super I> input =
				lifter.apply(source, actual);

		Objects.requireNonNull(input, "Lifted subscriber MUST NOT be null");

		return input;
	}
}

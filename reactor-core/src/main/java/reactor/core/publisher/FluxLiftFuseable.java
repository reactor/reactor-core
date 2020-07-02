/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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
import reactor.core.Fuseable;
import reactor.core.Scannable;

/**
 * @author Stephane Maldini
 */
final class FluxLiftFuseable<I, O> extends InternalFluxOperator<I, O>
		implements Fuseable {

	final BiFunction<Publisher, ? super CoreSubscriber<? super O>, ? extends CoreSubscriber<? super I>>
			lifter;

	FluxLiftFuseable(Publisher<I> p,
			BiFunction<Publisher, ? super CoreSubscriber<? super O>, ? extends CoreSubscriber<? super I>> lifter) {
		super(Flux.from(p));
		this.lifter = lifter;
	}

	@Override
	public String stepName() {
		if (source instanceof Scannable) {
			return Scannable.from(source).stepName();
		}
		return super.stepName();
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Scannable.from(source).scanUnsafe(key);
		return super.scanUnsafe(key);
	}

	@Override
	public CoreSubscriber<? super I> subscribeOrReturn(CoreSubscriber<? super O> actual) {
		CoreSubscriber<? super I> input =
				lifter.apply(source, actual);

		Objects.requireNonNull(input, "Lifted subscriber MUST NOT be null");

		if (actual instanceof QueueSubscription
				&& !(input instanceof QueueSubscription)) {
			//user didn't produce a QueueSubscription, original was one
			input = new FluxHide.SuppressFuseableSubscriber<>(input);
		}
		//otherwise QS is not required or user already made a compatible conversion
		return input;
	}
}

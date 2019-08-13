/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
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

import org.reactivestreams.Subscriber;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;

/**
 * Hook into the {@link org.reactivestreams.Publisher#subscribe(Subscriber)} of a
 * {@link Fuseable} {@link Flux} and execute a provided callback before calling
 * {@link org.reactivestreams.Publisher#subscribe(Subscriber)} directly with the
 * {@link CoreSubscriber}.
 *
 * <p>
 * Note that any exceptions thrown by the hook short circuit the subscription process and
 * are forwarded to the {@link Subscriber}'s {@link Subscriber#onError(Throwable)} method.
 *
 * @param <T> the value type
 * @author Simon Baslé
 */
final class FluxDoFirstFuseable<T> extends InternalFluxOperator<T, T> implements Fuseable {

	final Runnable onFirst;

	FluxDoFirstFuseable(Flux<? extends T> fuseableSource, Runnable onFirst) {
		super(fuseableSource);
		this.onFirst = onFirst;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		try {
			onFirst.run();
		}
		catch (Throwable error) {
			Operators.error(actual, error);
			return null;
		}

		return actual;
	}
}

/*
 * Copyright (c) 2015-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.Objects;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;

/**
 * A Stream that emits only one value and then complete.
 * <p>
 * Since the flux retains the value in a final field, any
 * {@link this#subscribe(Subscriber)} will
 * replay the value. This is a "Cold" fluxion.
 * <p>
 * Create such flux with the provided factory, E.g.:
 * <pre>
 * {@code
 * Flux.just(1).subscribe(
 *    log::info,
 *    log::error,
 *    ()-> log.info("complete")
 * )
 * }
 * </pre>
 * Will log:
 * <pre>
 * {@code
 * 1
 * complete
 * }
 * </pre>
 *
 * @author Stephane Maldini
 */
final class FluxJust<T> extends Flux<T>
		implements Fuseable.ScalarCallable<T>, Fuseable,
		           SourceProducer<T> {

	final T value;

	FluxJust(T value) {
		this.value = Objects.requireNonNull(value, "value");
	}

	@Override
	public T call() throws Exception {
		return value;
	}

	@Override
	public void subscribe(final CoreSubscriber<? super T> actual) {
		actual.onSubscribe(Operators.scalarSubscription(actual, value, "just"));
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.BUFFERED) return 1;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null;
	}

}

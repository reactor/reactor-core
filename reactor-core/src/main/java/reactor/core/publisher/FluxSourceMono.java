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

import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;

/**
 * A connecting {@link Flux} Publisher (right-to-left from a composition chain perspective)
 *
 * @param <I> Upstream type
 */
final class FluxSourceMono<I> extends FluxFromMonoOperator<I, I> {


	/**
	 * Build a {@link FluxSourceMono} wrapper around the passed parent {@link Publisher}
	 *
	 * @param source the {@link Publisher} to decorate
	 */
	FluxSourceMono(Mono<? extends I> source) {
		super(source);
	}

	@Override
	public String stepName() {
		if (source instanceof Scannable) {
			return "FluxFromMono(" + Scannable.from(source).stepName() + ")";
		}
		return "FluxFromMono(" + source.toString() + ")";
	}

	/**
	 * Default is simply delegating and decorating with {@link Flux} API. Note this
	 * assumes an identity between input and output types.
	 * @param actual
	 */
	@Override
	public CoreSubscriber<? super I> subscribeOrReturn(CoreSubscriber<? super I> actual) {
		return actual;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Scannable.from(source).scanUnsafe(key);
		return super.scanUnsafe(key);
	}
}

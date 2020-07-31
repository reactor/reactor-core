/*
 * Copyright (c) 2019-Present Pivotal Software Inc, All Rights Reserved.
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
import java.util.function.Function;

import reactor.core.CoreSubscriber;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

/**
 * Defers the creation of the actual Publisher the Subscriber will be subscribed to.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class MonoDeferContextual<T> extends Mono<T> implements SourceProducer<T> {

	final Function<ContextView, ? extends Mono<? extends T>> contextualMonoFactory;

	MonoDeferContextual(Function<ContextView, ? extends Mono<? extends T>> contextualMonoFactory) {
		this.contextualMonoFactory = Objects.requireNonNull(contextualMonoFactory, "contextualMonoFactory");
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		Mono<? extends T> p;

		Context ctx = actual.currentContext();
		try {
			p = Objects.requireNonNull(contextualMonoFactory.apply(ctx),
					"The Mono returned by the contextualMonoFactory is null");
		}
		catch (Throwable e) {
			Operators.error(actual, Operators.onOperatorError(e, ctx));
			return;
		}

		p.subscribe(actual);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return null; //no particular key to be represented, still useful in hooks
	}
}

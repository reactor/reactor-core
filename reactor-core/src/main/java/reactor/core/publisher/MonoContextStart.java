/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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

import org.reactivestreams.Subscriber;
import reactor.core.Fuseable;
import reactor.util.context.Context;

final class MonoContextStart<T> extends MonoOperator<T, T> implements Fuseable {

	final Function<Context, Context> doOnContext;

	MonoContextStart(Mono<? extends T> source,
			Function<Context, Context> doOnContext) {
		super(source);
		this.doOnContext = Objects.requireNonNull(doOnContext, "doOnContext");
	}

	@Override
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		FluxContextStart.ContextStartSubscriber<T> ctxSub =
				new FluxContextStart.ContextStartSubscriber<>(s, doOnContext, ctx);
		Context c;
		try {
			c = doOnContext.apply(ctx);
		}
		catch (Throwable t) {
			Operators.error(s, Operators.onOperatorError(t));
			return;
		}
		ctxSub.context = c;

		source.subscribe(ctxSub, c);
	}

}

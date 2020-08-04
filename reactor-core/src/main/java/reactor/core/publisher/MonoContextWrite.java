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
import java.util.function.Function;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.context.Context;

final class MonoContextWrite<T> extends InternalMonoOperator<T, T> implements Fuseable {

	final Function<Context, Context> doOnContext;

	MonoContextWrite(Mono<? extends T> source,
			Function<Context, Context> doOnContext) {
		super(source);
		this.doOnContext = Objects.requireNonNull(doOnContext, "doOnContext");
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		Context c = doOnContext.apply(actual.currentContext());

		return new FluxContextWrite.ContextWriteSubscriber<>(actual, c);
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;
		return super.scanUnsafe(key);
	}
}

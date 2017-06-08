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

import org.reactivestreams.Subscriber;
import reactor.core.Fuseable;
import reactor.util.context.Context;

/**
 * Emits the last N values the source emitted before its completion.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxTakeLastOne<T> extends FluxOperator<T, T> implements Fuseable {

	FluxTakeLastOne(Flux<? extends T> source) {
		super(source);
	}

	@Override
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		source.subscribe(new MonoTakeLastOne.TakeLastOneSubscriber<>(s, null, false), ctx);
	}

	@Override
	public int getPrefetch() {
		return Integer.MAX_VALUE;
	}

}

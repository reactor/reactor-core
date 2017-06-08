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

import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.util.context.Context;
import reactor.util.context.ContextRelay;

/**
 * Peek into the lifecycle events and signals of a sequence.
 * <p>
 * <p>
 * The callbacks are all optional.
 * <p>
 * <p>
 * Crashes by the lambdas are ignored.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxLogFuseable<T> extends FluxOperator<T, T>
		implements Fuseable {

	final SignalPeek<T> log;

	FluxLogFuseable(Flux<? extends T> source, SignalPeek<T> log) {
		super(source);
		this.log = log;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		if (s instanceof ConditionalSubscriber) {
			source.subscribe(new FluxPeekFuseable.PeekFuseableConditionalSubscriber<>((ConditionalSubscriber<? super T>) s,
					log), ctx);
			return;
		}
		source.subscribe(new FluxPeekFuseable.PeekFuseableSubscriber<>(s, log), ctx);
	}
}

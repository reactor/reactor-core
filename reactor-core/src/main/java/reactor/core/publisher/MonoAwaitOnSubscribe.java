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
import reactor.util.context.Context;

/**
 * Intercepts the onSubscribe call and makes sure calls to Subscription methods
 * only happen after the child Subscriber has returned from its onSubscribe method.
 * 
 * <p>This helps with child Subscribers that don't expect a recursive call from
 * onSubscribe into their onNext because, for example, they request immediately from
 * their onSubscribe but don't finish their preparation before that and onNext
 * runs into a half-prepared state. This can happen with non Rx mentality based Subscribers.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">https://github.com/reactor/reactive-streams-commons</a>
 * @since 3.0
 */
final class MonoAwaitOnSubscribe<T> extends MonoOperator<T, T> {

	MonoAwaitOnSubscribe(Mono<? extends T> source) {
		super(source);
	}
	
	@Override
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		source.subscribe(new FluxAwaitOnSubscribe.PostOnSubscribeSubscriber<>(s), ctx);
	}
}

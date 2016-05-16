/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * Resumes the failed main sequence with another sequence returned by
 * a function for the particular failure exception.
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
final class MonoOtherwise<T> extends MonoSource<T, T> {

	final Function<? super Throwable, ? extends Publisher<? extends T>> nextFactory;

	public MonoOtherwise(Publisher<? extends T> source,
						   Function<? super Throwable, ? extends Mono<? extends T>>
								   nextFactory) {
		super(source);
		this.nextFactory = Objects.requireNonNull(nextFactory, "nextFactory");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new FluxResume.ResumeSubscriber<>(s, nextFactory));
	}
}

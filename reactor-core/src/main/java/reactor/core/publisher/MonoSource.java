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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import javax.annotation.Nullable;
import reactor.util.context.Context;

/**
 * A decorating {@link Mono} {@link Publisher} that exposes {@link Mono} API over an arbitrary {@link Publisher}
 * Useful to create operators which return a {@link Mono}, e.g. :
 * {@code
 *    flux.as(Mono::fromDirect)
 *        .then(d -> Mono.delay(Duration.ofSeconds(1))
 *        .block();
 * }
 * @param <I> delegate {@link Publisher} type
 */
final class MonoSource<I> extends Mono<I> implements Scannable {

	final Publisher<? extends I> source;

	MonoSource(Publisher<? extends I> source) {
		this.source = Objects.requireNonNull(source);
	}

	/**
	 * Default is simply delegating and decorating with {@link Mono} API. Note this
	 * assumes an identity between input and output types.
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(Subscriber<? super I> s, Context context) {
		source.subscribe(s);
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == ScannableAttr.PARENT) {
				return source;
		}
		return null;
	}

}

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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Fuseable;
import reactor.core.Receiver;
import reactor.core.Scannable;


/**
 * A decorating {@link Mono} {@link Publisher} that exposes {@link Mono} API over an arbitrary {@link Publisher}
 * Useful to create operators which return a {@link Mono}, e.g. :
 * {@code
 *    flux.as(f -> MonoSource.wrap(f))
 *        .then(d -> Mono.delay(Duration.ofSeconds(1))
 *        .block();
 * }
 * @param <I> delegate {@link Publisher} type
 * @param <O> produced type
 */
public class MonoSource<I, O> extends Mono<O> implements Scannable, Receiver {

	protected final Publisher<? extends I> source;

	/**
	 * Unchecked wrap of {@link Publisher} as {@link Mono}, supporting {@link Fuseable} sources
	 *
	 * @param source the {@link Publisher} to wrap
	 * @param <I> input upstream type
	 * @return a wrapped {@link Mono}
	 */
	public static <I> Mono<I> wrap(Publisher<? extends I> source){
		if(source instanceof Fuseable){
			return new FuseableMonoSource<>(source);
		}
		return new MonoSource<>(source);
	}

	/**
	 * Build a {@link MonoSource} wrapper around the passed parent {@link Publisher}
	 *
	 * @param source the {@link Publisher} to decorate
	 */
	protected MonoSource(Publisher<? extends I> source) {
		this.source = Objects.requireNonNull(source);
	}

	/**
	 * Default is simply delegating and decorating with {@link Mono} API. Note this
	 * assumes an identity between input and output types.
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(Subscriber<? super O> s) {
		source.subscribe((Subscriber<? super I>) s);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		return sb.append('{')
		         .append(" \"operator\" : ")
		         .append('"')
		         .append(getClass().getSimpleName()
		                           .replaceAll("Mono", ""))
		         .append('"')
		         .append(' ')
		         .append('}')
		         .toString();
	}

	@Override
	public final Publisher<? extends I> upstream() {
		return source;
	}

	@Override
	public Object scan(Scannable.Attr key) {
		switch (key){
			case PARENT:
				return source;
		}
		return null;
	}

	static final class FuseableMonoSource<I> extends MonoSource<I, I>
			implements Fuseable{
		FuseableMonoSource(Publisher<? extends I> source) {
			super(source);
		}
	}
}

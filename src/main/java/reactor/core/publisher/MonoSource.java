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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.flow.Fuseable;
import reactor.core.flow.Receiver;
import reactor.core.util.Exceptions;

/**
 * A decorating {@link Mono} {@link Publisher} that exposes {@link Mono} API over an arbitrary {@link Publisher}
 * Useful to create operators which return a {@link Mono}, e.g. :
 * {@code
 *    flux.as(f -> new MonoSource<>(f))
 *        .then(d -> Mono.delay(1))
 *        .get();
 * }
 *
 * @param <I> delegate {@link Publisher} type
 * @param <O> produced type
 */
public class MonoSource<I, O> extends Mono<O> implements Receiver{

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
			return onAssembly(new FuseableMonoSource<>(source));
		}
		return onAssembly(new MonoSource<>(source));
	}

	protected MonoSource(Publisher<? extends I> source) {
		this.source = Objects.requireNonNull(source);
	}

	/**
	 * Default is delegating and decorating with Mono API
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(Subscriber<? super O> s) {
		try {
			source.subscribe((Subscriber<? super I>) s);
		}
		catch (Exceptions.BubblingException rfe) {
			if(rfe.getCause() instanceof RuntimeException){
				throw (RuntimeException)rfe.getCause();
			}
			throw rfe;
		}
	}

	@Override
	public String toString() {
		return "{" +
				" operator : \"" + getId() + "\" " +
				'}';
	}

	@Override
	public final Publisher<? extends I> upstream() {
		return source;
	}

	static final class FuseableMonoSource<I> extends MonoSource<I, I> implements Fuseable{
		public FuseableMonoSource(Publisher<? extends I> source) {
			super(source);
		}
	}
}

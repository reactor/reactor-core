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
import reactor.core.state.Backpressurable;

/**
 * A connecting {@link Flux} Publisher (right-to-left from a composition chain perspective)
 *
 * @param <I> Upstream type
 * @param <O> Downstream type
 */
public class FluxSource<I, O> extends Flux<O> implements Receiver {

	protected final Publisher<? extends I> source;

	/**
	 * Unchecked wrap of {@link Publisher} as {@link Flux}, supporting {@link Fuseable} sources
	 *
	 * @param source the {@link Publisher} to wrap 
	 * @param <I> input upstream type
	 * @return a wrapped {@link Flux}
	 */
	public static <I> Flux<I> wrap(Publisher<? extends I> source){
		if(source instanceof Fuseable){
			return new FuseableFluxSource<>(source);
		}
		return new FluxSource<>(source);
	}
	
	protected FluxSource(Publisher<? extends I> source) {
		this.source = Objects.requireNonNull(source);
	}

	@Override
	public long getCapacity() {
		return Backpressurable.class.isAssignableFrom(source.getClass()) ?
				((Backpressurable) source).getCapacity() :
				-1L;
	}

	/**
	 * Default is delegating and decorating with {@link Flux} API
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(Subscriber<? super O> s) {
		source.subscribe((Subscriber<? super I>) s);
	}

	@Override
	public String toString() {
		return "{" +
				" operator : \"" + getName() + "\" " +
				'}';
	}

	@Override
	public final Publisher<? extends I> upstream() {
		return source;
	}

	static final class FuseableFluxSource<I> extends FluxSource<I, I> implements Fuseable{
		public FuseableFluxSource(Publisher<? extends I> source) {
			super(source);
		}
	}
}

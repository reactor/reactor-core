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
import reactor.util.context.Context;
import javax.annotation.Nullable;

/**
 * A connecting {@link Flux} Publisher (right-to-left from a composition chain perspective)
 * @deprecated This class will be package scoped in 3.1, consider moving to
 * {@link FluxOperator}
 * @param <I> Upstream type
 * @param <O> Downstream type
 */
@Deprecated
public class FluxSource<I, O> extends Flux<O> implements Scannable {


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

	/**
	 * Build a {@link FluxSource} wrapper around the passed parent {@link Publisher}
	 *
	 * @param source the {@link Publisher} to decorate
	 */
	protected FluxSource(Publisher<? extends I> source) {
		this.source = Objects.requireNonNull(source);
	}

	/**
	 * Default is simply delegating and decorating with {@link Flux} API. Note this
	 * assumes an identity between input and output types.
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(Subscriber<? super O> s, Context context) {
		source.subscribe((Subscriber<? super I>) s);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		return sb.append('{')
		         .append(" \"operator\" : ")
		         .append('"')
		         .append(getClass().getSimpleName()
		                           .replaceAll("Flux", ""))
		         .append('"')
		         .append(' ')
		         .append('}')
		         .toString();
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == IntAttr.PREFETCH) return getPrefetch();
		if (key == ScannableAttr.PARENT) return source;
		return null;
	}

	static final class FuseableFluxSource<I> extends FluxSource<I, I>
			implements Fuseable{
		FuseableFluxSource(Publisher<? extends I> source) {
			super(source);
		}
	}
}

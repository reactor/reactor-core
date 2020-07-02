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

import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

import static reactor.core.Scannable.Attr.RUN_STYLE;
import static reactor.core.Scannable.Attr.RunStyle.SYNC;

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
final class MonoSource<I> extends Mono<I> implements Scannable, SourceProducer<I>,
                                                     OptimizableOperator<I, I> {

	final Publisher<? extends I> source;

	@Nullable
	final OptimizableOperator<?, I> optimizableOperator;

	MonoSource(Publisher<? extends I> source) {
		this.source = Objects.requireNonNull(source);
		if (source instanceof OptimizableOperator) {
			@SuppressWarnings("unchecked")
			OptimizableOperator<?, I> optimSource = (OptimizableOperator<?, I>) source;
			this.optimizableOperator = optimSource;
		}
		else {
			this.optimizableOperator = null;
		}
	}

	/**
	 * Default is simply delegating and decorating with {@link Mono} API. Note this
	 * assumes an identity between input and output types.
	 * @param actual
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(CoreSubscriber<? super I> actual) {
		source.subscribe(actual);
	}

	@Override
	public CoreSubscriber<? super I> subscribeOrReturn(CoreSubscriber<? super I> actual) {
		return actual;
	}

	@Override
	public final CorePublisher<? extends I> source() {
		return this;
	}

	@Override
	public final OptimizableOperator<?, ? extends I> nextOptimizableSource() {
		return optimizableOperator;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) {
				return source;
		}
		if (key == Attr.RUN_STYLE) {
			return Scannable.from(source).scanUnsafe(key);
		}
		return null;
	}

}

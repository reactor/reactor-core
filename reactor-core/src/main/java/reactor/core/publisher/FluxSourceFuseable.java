/*
 * Copyright (c) 2016-2023 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
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
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;

/**
 * @author Stephane Maldini
 */
final class FluxSourceFuseable<I> extends Flux<I> implements Fuseable, SourceProducer<I>,
                                                             OptimizableOperator<I, I> {

	final Publisher<? extends I> source;

	@Nullable
	final OptimizableOperator<?, I> optimizableOperator;

	FluxSourceFuseable(Publisher<? extends I> source) {
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
	 * Default is simply delegating and decorating with {@link Flux} API. Note this
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
	public Object scanUnsafe(Scannable.Attr key) {
		if (key == Scannable.Attr.PREFETCH) return getPrefetch();
		if (key == Scannable.Attr.PARENT) return source;
		if (key == Attr.RUN_STYLE) return Scannable.from(source).scanUnsafe(key);
		return SourceProducer.super.scanUnsafe(key);
	}
}

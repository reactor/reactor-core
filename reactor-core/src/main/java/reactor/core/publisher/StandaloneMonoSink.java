/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.util.function.LongConsumer;

import org.reactivestreams.Subscriber;
import reactor.core.Disposable;
import reactor.util.context.Context;

/**
 * A stand-alone {@link MonoSink} that wraps a {@link MonoProcessor} for its downstream
 * semantics and that be converted to a {@link Flux} or {@link Mono}.
 * <p>
 * This is a facade over the much more complex API of {@link MonoProcessor} that offers
 * {@link MonoSink} methods instead of relying on the {@link Subscriber} side of the
 * processor.
 *
 * @author Simon Baslé
 */
public final class StandaloneMonoSink<T> implements MonoSink<T> {

	final MonoProcessor<T> processor;
	final MonoSink<T> sink;

	/**
	 * Create a standalone {@link MonoSink}
	 */
	static <O> StandaloneMonoSink<O> create() {
		return new StandaloneMonoSink<>();
	}

	StandaloneMonoSink() {
		this.processor = MonoProcessor.create();
		this.sink = new MonoCreate.DefaultMonoSink<>(processor);
	}

	/**
	 * Converts the {@link StandaloneMonoSink} to a {@link Mono}, allowing to compose
	 * operators on it.
	 * <p>
	 * When possible, if the concrete implementation already is derived from {@link Mono}
	 * this method should not instantiate any intermediate object.
	 *
	 * @return the {@link StandaloneMonoSink} viewed as a {@link Mono}
	 */
	public Mono<T> toMono() {
		return processor;
	}

	/**
	 * Converts the {@link StandaloneMonoSink} to a {@link Flux}, allowing to compose
	 * operators on it.
	 *
	 * @return the {@link StandaloneMonoSink} viewed as a {@link Flux}
	 */
	public Flux<T> toFlux() {
		return processor.flux();
	}

	@Override
	public Context currentContext() {
		return processor.currentContext();
	}

	@Override
	public void success() {
		sink.success();
	}

	@Override
	public void success(T value) {
		sink.success(value);
	}

	@Override
	public void error(Throwable e) {
		sink.error(e);
	}

	@Override
	public MonoSink<T> onRequest(LongConsumer consumer) {
		return sink.onRequest(consumer);
	}

	@Override
	public MonoSink<T> onCancel(Disposable d) {
		return sink.onCancel(d);
	}

	@Override
	public MonoSink<T> onDispose(Disposable d) {
		return sink.onDispose(d);
	}

}

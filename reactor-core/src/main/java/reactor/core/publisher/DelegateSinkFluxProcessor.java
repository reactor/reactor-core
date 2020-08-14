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
import java.util.stream.Stream;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * @author Stephane Maldini
 */
final class DelegateSinkFluxProcessor<IN> extends FluxProcessor<IN, IN> {

	final Flux<IN>       flux;
	final Sinks.Many<IN> sink;

	DelegateSinkFluxProcessor(Sinks.Many<IN> sink) {
		this.sink = Objects.requireNonNull(sink, "sink must not be null");
		this.flux = sink.asFlux();
	}

	@Override
	public Context currentContext() {
		if(sink instanceof CoreSubscriber){
			return ((CoreSubscriber<?>) sink).currentContext();
		}
		return Context.empty();
	}

	@Override
	public void onComplete() {
		sink.emitComplete();
	}

	@Override
	public void onError(Throwable t) {
		sink.emitError(t);
	}

	@Override
	public void onNext(IN in) {
		sink.emitNext(in);
	}

	@Override
	public void onSubscribe(Subscription s) {
		s.request(Long.MAX_VALUE);
	}

	@Override
	public void subscribe(CoreSubscriber<? super IN> actual) {
		Objects.requireNonNull(actual, "subscribe");
		flux.subscribe(actual);
	}

	@Override
	public boolean isSerialized() {
		return sink instanceof SerializedManySink;
	}

	@Override
	public Stream<? extends Scannable> inners() {
		return Scannable.from(sink)
		                .inners();
	}

	@Override
	public int getBufferSize() {
		return Scannable.from(sink)
		                .scanOrDefault(Attr.CAPACITY, super.getBufferSize());
	}

	@Override
	@Nullable
	public Throwable getError() {
		//noinspection ConstantConditions
		return Scannable.from(sink)
		                .scanOrDefault(Attr.ERROR, super.getError());
	}

	@Override
	public boolean isTerminated() {
		return Scannable.from(sink)
		                .scanOrDefault(Attr.TERMINATED, super.isTerminated());
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) {
			return flux;
		}
		return Scannable.from(sink)
		                .scanUnsafe(key);
	}
}

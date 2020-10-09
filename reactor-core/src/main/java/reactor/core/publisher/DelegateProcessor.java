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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * @author Stephane Maldini
 */
@Deprecated
final class DelegateProcessor<IN, OUT> extends FluxProcessor<IN, OUT> {

	final Publisher<OUT> downstream;
	final Subscriber<IN> upstream;

	DelegateProcessor(Publisher<OUT> downstream,
			Subscriber<IN> upstream) {
		this.downstream = Objects.requireNonNull(downstream, "Downstream must not be null");
		this.upstream = Objects.requireNonNull(upstream, "Upstream must not be null");
	}

	@Override
	public Context currentContext() {
		if(upstream instanceof CoreSubscriber){
			return ((CoreSubscriber<?>)upstream).currentContext();
		}
		return Context.empty();
	}

	@Override
	public void onComplete() {
		upstream.onComplete();
	}

	@Override
	public void onError(Throwable t) {
		upstream.onError(t);
	}

	@Override
	public void onNext(IN in) {
		upstream.onNext(in);
	}

	@Override
	public void onSubscribe(Subscription s) {
		upstream.onSubscribe(s);
	}

	@Override
	public void subscribe(CoreSubscriber<? super OUT> actual) {
		Objects.requireNonNull(actual, "subscribe");
		downstream.subscribe(actual);
	}

	@Override
	@SuppressWarnings("unchecked")
	public boolean isSerialized() {
		return upstream instanceof SerializedSubscriber ||
				(upstream instanceof FluxProcessor &&
						((FluxProcessor<?, ?>)upstream).isSerialized());
	}

	@Override
	public Stream<? extends Scannable> inners() {
		//noinspection ConstantConditions
		return Scannable.from(upstream)
		                .inners();
	}

	@Override
	public int getBufferSize() {
		//noinspection ConstantConditions
		return Scannable.from(upstream)
		                .scanOrDefault(Attr.CAPACITY, super.getBufferSize());
	}

	@Override
	@Nullable
	public Throwable getError() {
		//noinspection ConstantConditions
		return Scannable.from(upstream)
		                .scanOrDefault(Attr.ERROR, super.getError());
	}

	@Override
	public boolean isTerminated() {
		//noinspection ConstantConditions
		return Scannable.from(upstream)
		                .scanOrDefault(Attr.TERMINATED, super.isTerminated());
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PARENT) {
			return downstream;
		}
		//noinspection ConstantConditions
		return Scannable.from(upstream)
		                .scanUnsafe(key);
	}
}

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
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Scannable;

/**
 * @author Stephane Maldini
 */
final class DelegateProcessor<IN, OUT> extends FluxProcessor<IN, OUT>  {

	final Publisher<OUT> downstream;
	final Subscriber<IN> upstream;

	DelegateProcessor(Publisher<OUT> downstream,
			Subscriber<IN> upstream) {
		this.downstream = Objects.requireNonNull(downstream, "Downstream must not be null");
		this.upstream = Objects.requireNonNull(upstream, "Upstream must not be null");
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
	public void subscribe(Subscriber<? super OUT> s) {
		if (s == null) {
			throw Exceptions.argumentIsNullException();
		}
		downstream.subscribe(s);
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
		return Scannable.from(upstream)
		                .inners();
	}

	@Override
	public int getBufferSize() {
		return Scannable.from(upstream)
		                .scanOrDefault(IntAttr.CAPACITY, super.getBufferSize());
	}

	@Override
	public Throwable getError() {
		return Scannable.from(upstream)
		                .scanOrDefault(ThrowableAttr.ERROR, super.getError());
	}

	@Override
	public boolean isTerminated() {
		return Scannable.from(upstream)
		                .scanOrDefault(BooleanAttr.TERMINATED, super.isTerminated());
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == ScannableAttr.PARENT) {
			return downstream;
		}
		return Scannable.from(upstream)
		                .scanUnsafe(key);
	}
}

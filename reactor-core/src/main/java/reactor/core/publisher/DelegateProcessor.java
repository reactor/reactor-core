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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.util.context.Context;
import reactor.util.context.ContextRelay;
import javax.annotation.Nullable;

/**
 * @author Stephane Maldini
 */
final class DelegateProcessor<IN, OUT> extends FluxProcessor<IN, OUT>
		implements ContextRelay {

	final Publisher<OUT> downstream;
	final Subscriber<IN> upstream;

	volatile Context context;

	static final AtomicReferenceFieldUpdater<DelegateProcessor, Context> C =
			AtomicReferenceFieldUpdater.newUpdater(DelegateProcessor.class, Context
					.class, "context");

	DelegateProcessor(Publisher<OUT> downstream,
			Subscriber<IN> upstream) {
		this.downstream = Objects.requireNonNull(downstream, "Downstream must not be null");
		this.upstream = Objects.requireNonNull(upstream, "Upstream must not be null");
		if (context == null) {
			C.lazySet(this, ContextRelay.getOrEmpty(upstream));
		}
		else {
			C.lazySet(this, context);
		}
	}

	@Override
	public void onContext(Context context) {
		if(context != Context.empty() &&
				C.compareAndSet(this, Context.empty(), context)){
			ContextRelay.set(upstream, context);
		}
	}

	@Override
	public Context currentContext() {
		return ContextRelay.getOrEmpty(upstream);
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
	public void subscribe(Subscriber<? super OUT> s, Context ctx) {
		//noinspection ConstantConditions
		if (s == null) {
			throw Exceptions.argumentIsNullException();
		}
		Context c = context;
		ContextRelay.set(s, c);
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
		//noinspection ConstantConditions
		return Scannable.from(upstream)
		                .inners();
	}

	@Override
	public int getBufferSize() {
		//noinspection ConstantConditions
		return Scannable.from(upstream)
		                .scanOrDefault(IntAttr.CAPACITY, super.getBufferSize());
	}

	@Override
	@Nullable
	public Throwable getError() {
		//noinspection ConstantConditions
		return Scannable.from(upstream)
		                .scanOrDefault(ThrowableAttr.ERROR, super.getError());
	}

	@Override
	public boolean isTerminated() {
		//noinspection ConstantConditions
		return Scannable.from(upstream)
		                .scanOrDefault(BooleanAttr.TERMINATED, super.isTerminated());
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == ScannableAttr.PARENT) {
			return downstream;
		}
		//noinspection ConstantConditions
		return Scannable.from(upstream)
		                .scanUnsafe(key);
	}
}

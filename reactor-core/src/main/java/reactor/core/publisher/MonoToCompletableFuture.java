/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.util.context.Context;

/**
 * @author Stephane Maldini
 */
final class MonoToCompletableFuture<T> extends CompletableFuture<T> implements CoreSubscriber<T> {

	final AtomicReference<Subscription> ref = new AtomicReference<>();
	final boolean cancelSourceOnNext;

	MonoToCompletableFuture(boolean sourceCanEmitMoreThanOnce) {
		this.cancelSourceOnNext = sourceCanEmitMoreThanOnce;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		boolean cancelled = super.cancel(mayInterruptIfRunning);
		if (cancelled) {
			Subscription s = ref.getAndSet(null);
			if (s != null) {
				s.cancel();
			}
		}
		return cancelled;
	}

	@Override
	public void onSubscribe(Subscription s) {
		if (Operators.validate(ref.getAndSet(s), s)) {
			s.request(Long.MAX_VALUE);
		}
		else {
			s.cancel();
		}
	}

	@Override
	public void onNext(T t) {
		Subscription s = ref.getAndSet(null);
		if (s != null) {
			complete(t);
			if (cancelSourceOnNext) {
				s.cancel();
			}
		}
		else {
			Operators.onNextDropped(t, currentContext());
		}
	}

	@Override
	public void onError(Throwable t) {
		if (ref.getAndSet(null) != null) {
			completeExceptionally(t);
		}
	}

	@Override
	public void onComplete() {
		if (ref.getAndSet(null) != null) {
			complete(null);
		}
	}

	@Override
	public Context currentContext() {
		return Context.empty();
	}
}

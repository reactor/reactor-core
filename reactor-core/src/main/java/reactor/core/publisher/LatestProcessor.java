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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Operators.MonoSubscriber;
import reactor.util.context.Context;

/**
 * @author Simon Baslé
 */
final class LatestProcessor<T> extends Mono<T>
		implements IdentityProcessor<T> {

	static <O> LatestProcessor<O> latestOf(Publisher<? extends O> source) {
		LatestProcessor<O> processor = new LatestProcessor<>();
		source.subscribe(processor);
		return processor;
	}

	private static final MonoSubscriber[] EMPTY = new MonoSubscriber[0];
	private static final MonoSubscriber[] TERMINATED = new MonoSubscriber[0];

	private static final int STATE_INIT = 0;
	private static final int STATE_RUNNING = 1;
	private static final int STATE_TERMINATED = 2;

	volatile int state;
	static final AtomicIntegerFieldUpdater<LatestProcessor> STATE =
			AtomicIntegerFieldUpdater.newUpdater(LatestProcessor.class, "state");

	volatile T latest;
	volatile Throwable error;

	volatile Subscription upstream;
	static final AtomicReferenceFieldUpdater<LatestProcessor, Subscription> UPSTREAM =
			AtomicReferenceFieldUpdater.newUpdater(LatestProcessor.class, Subscription.class, "upstream");

	@SuppressWarnings("unchecked")
	volatile MonoSubscriber<T, T>[] earlySubscribers = EMPTY;
	static final AtomicReferenceFieldUpdater<LatestProcessor, MonoSubscriber[]> EARLY_SUBSCRIBERS =
			AtomicReferenceFieldUpdater.newUpdater(LatestProcessor.class, MonoSubscriber[].class, "earlySubscribers");

	@Override
	public void onSubscribe(Subscription subscription) {
		if (Operators.setOnce(UPSTREAM, this, subscription)) {
			upstream.request(Long.MAX_VALUE);
		}
	}

	@Override
	public void onNext(T t) {
		latest = t;
		if (STATE.getAndSet(this, STATE_RUNNING) == STATE_INIT) {
			@SuppressWarnings("unchecked")
			MonoSubscriber<T, T>[] inners = EARLY_SUBSCRIBERS.getAndSet(this, EMPTY);
			for (MonoSubscriber<T, T> inner : inners) {
				inner.complete(t);
			}
		}
	}

	@Override
	public void onError(Throwable throwable) {
		int s = STATE.getAndSet(this, STATE_TERMINATED);
		if (s == STATE_TERMINATED) {
			Operators.onErrorDropped(throwable, Context.empty());
			return;
		}
		this.error = throwable;
		if (s == STATE_INIT) {
			@SuppressWarnings("unchecked")
			MonoSubscriber<T, T>[] inners = EARLY_SUBSCRIBERS.getAndSet(this, TERMINATED);
			for (MonoSubscriber<T, T> inner : inners) {
				inner.onError(throwable);
			}
		}
	}

	@Override
	public void onComplete() {
		if (STATE.getAndSet(this, STATE_TERMINATED) == STATE_INIT) {
			@SuppressWarnings("unchecked")
			MonoSubscriber<T, T>[] inners = EARLY_SUBSCRIBERS.getAndSet(this, TERMINATED);
			for (MonoSubscriber<T, T> inner : inners) {
				inner.onComplete();
			}
		}
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		int s = state;
		if (s >= STATE_RUNNING) {
			if (this.error != null) {
				Operators.error(actual, this.error);
			}
			else if (latest == null) {
				Operators.complete(actual);
			}
			else {
				Operators.ScalarSubscription<T> scalar = new Operators.ScalarSubscription<>(actual, latest);
				actual.onSubscribe(scalar);
			}
		}
		else {
			//we only add an inner if no signal has yet been emitted
			MonoSubscriber<T, T> inner = new MonoSubscriber<>(actual);
			add(inner);
			actual.onSubscribe(inner);
		}
	}

	boolean add(MonoSubscriber<T, T> s) {
		MonoSubscriber<T, T>[] a = earlySubscribers;
		if (a == TERMINATED) {
			return false;
		}

		synchronized (this) {
			a = earlySubscribers;
			if (a == TERMINATED) {
				return false;
			}
			int len = a.length;

			@SuppressWarnings("unchecked")
			MonoSubscriber<T, T>[] b = new MonoSubscriber[len + 1];
			System.arraycopy(a, 0, b, 0, len);
			b[len] = s;

			earlySubscribers = b;

			return true;
		}
	}

	@Override
	public Flux<T> toFlux() {
		return this.flux();
	}

	@Override
	public Mono<T> toMono() {
		return this;
	}

}

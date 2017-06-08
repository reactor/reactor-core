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

import java.util.concurrent.TimeUnit;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Fuseable;
import reactor.core.scheduler.Scheduler;
import reactor.util.context.Context;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;
import javax.annotation.Nullable;

/**
 * @author Stephane Maldini
 */
final class FluxElapsed<T> extends FluxOperator<T, Tuple2<Long, T>> implements Fuseable {

	final Scheduler scheduler;

	FluxElapsed(Flux<T> source, Scheduler scheduler) {
		super(source);
		this.scheduler = scheduler;
	}

	@Override
	public void subscribe(Subscriber<? super Tuple2<Long, T>> s, Context ctx) {
		source.subscribe(new ElapsedSubscriber<T>(s, scheduler), ctx);
	}

	static final class ElapsedSubscriber<T>
			implements InnerOperator<T, Tuple2<Long, T>>,
			           QueueSubscription<Tuple2<Long, T>> {

		final Subscriber<? super Tuple2<Long, T>> actual;
		final Scheduler                           scheduler;

		Subscription      s;
		QueueSubscription<T> qs;

		long lastTime;

		ElapsedSubscriber(Subscriber<? super Tuple2<Long, T>> actual, Scheduler scheduler) {
			this.actual = actual;
			this.scheduler = scheduler;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == ScannableAttr.PARENT) return s;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				lastTime = scheduler.now(TimeUnit.MILLISECONDS);
				this.s = s;
				actual.onSubscribe(this);
			}
		}

		@Override
		public Subscriber<? super Tuple2<Long, T>> actual() {
			return actual;
		}

		@Override
		public void onNext(T t) {
			if(t == null){
				actual.onNext(null);
				return;
			}
			actual.onNext(snapshot(t));
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			actual.onComplete();
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		public int requestFusion(int requestedMode) {
			QueueSubscription<T> qs = Operators.as(s);
			if (qs != null) {
				this.qs = qs;
				return qs.requestFusion(requestedMode);
			}
			return Fuseable.NONE;
		}

		Tuple2<Long, T> snapshot(T data){
			long now = scheduler.now(TimeUnit.MILLISECONDS);
			long last = lastTime;
			lastTime = now;
			long delta = now - last;
			return Tuples.of(delta, data);
		}

		@Override
		@Nullable
		public Tuple2<Long, T> poll() {
			T data = qs.poll();
			if(data != null){
				return snapshot(data);
			}
			return null;
		}

		@Override
		public int size() {
			return qs.size();
		}

		@Override
		public boolean isEmpty() {
			return qs.isEmpty();
		}

		@Override
		public void clear() {
			qs.clear();
		}
	}
}

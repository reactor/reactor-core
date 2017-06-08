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
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.util.context.Context;
import javax.annotation.Nullable;

/**
 * Emits events on a different thread specified by a scheduler callback.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxPublishOn<T> extends FluxOperator<T, T> implements Fuseable {

	final Scheduler scheduler;

	final boolean delayError;

	final Supplier<? extends Queue<T>> queueSupplier;

	final int prefetch;

	FluxPublishOn(Flux<? extends T> source,
			Scheduler scheduler,
			boolean delayError,
			int prefetch,
			Supplier<? extends Queue<T>> queueSupplier) {
		super(source);
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
		this.delayError = delayError;
		this.prefetch = prefetch;
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(Subscriber<? super T> s, Context ctx) {
		Worker worker;

		try {
			worker = Objects.requireNonNull(scheduler.createWorker(),
					"The scheduler returned a null worker");
		}
		catch (Throwable e) {
			Operators.error(s, Operators.onOperatorError(e));
			return;
		}

		if (s instanceof ConditionalSubscriber) {
			ConditionalSubscriber<? super T> cs = (ConditionalSubscriber<? super T>) s;
			source.subscribe(new PublishOnConditionalSubscriber<>(cs,
					scheduler,
					worker,
					delayError,
					prefetch,
					queueSupplier), ctx);
			return;
		}
		source.subscribe(new PublishOnSubscriber<>(s,
				scheduler,
				worker,
				delayError,
				prefetch,
				queueSupplier), ctx);
	}

	static final class PublishOnSubscriber<T>
			implements QueueSubscription<T>, Runnable, InnerOperator<T, T> {

		final Subscriber<? super T> actual;

		final Scheduler scheduler;

		final Worker worker;

		final boolean delayError;

		final int prefetch;

		final int limit;

		final Supplier<? extends Queue<T>> queueSupplier;

		Subscription s;

		Queue<T> queue;

		volatile boolean cancelled;

		volatile boolean done;

		Throwable error;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<PublishOnSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(PublishOnSubscriber.class, "wip");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<PublishOnSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(PublishOnSubscriber.class, "requested");

		int sourceMode;

		long produced;

		boolean outputFused;

		PublishOnSubscriber(Subscriber<? super T> actual,
				Scheduler scheduler,
				Worker worker,
				boolean delayError,
				int prefetch,
				Supplier<? extends Queue<T>> queueSupplier) {
			this.actual = actual;
			this.worker = worker;
			this.scheduler = scheduler;
			this.delayError = delayError;
			this.prefetch = prefetch;
			this.queueSupplier = queueSupplier;
			if (prefetch != Integer.MAX_VALUE) {
				this.limit = prefetch - (prefetch >> 2);
			}
			else {
				this.limit = Integer.MAX_VALUE;
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				if (s instanceof QueueSubscription) {
					@SuppressWarnings("unchecked") QueueSubscription<T> f =
							(QueueSubscription<T>) s;

					int m = f.requestFusion(Fuseable.ANY | Fuseable.THREAD_BARRIER);

					if (m == Fuseable.SYNC) {
						sourceMode = Fuseable.SYNC;
						queue = f;
						done = true;

						actual.onSubscribe(this);
						return;
					}
					else if (m == Fuseable.ASYNC) {
						sourceMode = Fuseable.ASYNC;
						queue = f;

						actual.onSubscribe(this);

						initialRequest();

						return;
					}
				}

				queue = queueSupplier.get();

				actual.onSubscribe(this);

				initialRequest();
			}
		}

		void initialRequest() {
			if (prefetch == Integer.MAX_VALUE) {
				s.request(Long.MAX_VALUE);
			}
			else {
				s.request(prefetch);
			}
		}

		@Override
		public void onNext(T t) {
			if (t == null) {//async fusion
				if (trySchedule() == Scheduler.REJECTED) {
					throw Operators.onRejectedExecution(this, null, t);
				}
				return;
			}

			if (done) {
				Operators.onNextDropped(t);
				return;
			}
			if (!queue.offer(t)) {
				error = Operators.onOperatorError(s,
						Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL),
						t);
				done = true;
			}
			if (trySchedule() == Scheduler.REJECTED) {
				throw Operators.onRejectedExecution(this, null, t);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t);
				return;
			}
			error = t;
			done = true;
			if (trySchedule() == Scheduler.REJECTED) {
				throw Operators.onRejectedExecution(null, t, null);
			}
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			if (trySchedule() == Scheduler.REJECTED && !worker.isDisposed()) {
				throw Operators.onRejectedExecution();
			}
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
				if (trySchedule() == Scheduler.REJECTED && (!worker.isDisposed() || scheduler.isDisposed())) {
					throw Operators.onRejectedExecution(this, null, null);
				}
			}
		}

		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}

			cancelled = true;
			s.cancel();
			worker.dispose();

			if (WIP.getAndIncrement(this) == 0) {
				queue.clear();
			}
		}

		@Nullable
		Disposable trySchedule() {
			if (WIP.getAndIncrement(this) != 0) {
				return null;
			}

			return worker.schedule(this);
		}

		void runSync() {
			int missed = 1;

			final Subscriber<? super T> a = actual;
			final Queue<T> q = queue;

			long e = produced;

			for (; ; ) {

				long r = requested;

				while (e != r) {
					T v;

					try {
						v = q.poll();
					}
					catch (Throwable ex) {
						doError(a, Operators.onOperatorError(s, ex));
						return;
					}

					if (cancelled) {
						return;
					}
					if (v == null) {
						doComplete(a);
						return;
					}

					a.onNext(v);

					e++;
				}

				if (cancelled) {
					return;
				}

				if (q.isEmpty()) {
					doComplete(a);
					return;
				}

				int w = wip;
				if (missed == w) {
					produced = e;
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				}
				else {
					missed = w;
				}
			}
		}

		void runAsync() {
			int missed = 1;

			final Subscriber<? super T> a = actual;
			final Queue<T> q = queue;

			long e = produced;

			for (; ; ) {

				long r = requested;

				while (e != r) {
					boolean d = done;
					T v;

					try {
						v = q.poll();
					}
					catch (Throwable ex) {
						Exceptions.throwIfFatal(ex);
						s.cancel();
						q.clear();

						doError(a, Operators.onOperatorError(ex));
						return;
					}

					boolean empty = v == null;

					if (checkTerminated(d, empty, a)) {
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(v);

					e++;
					if (e == limit) {
						if (r != Long.MAX_VALUE) {
							r = REQUESTED.addAndGet(this, -e);
						}
						s.request(e);
						e = 0L;
					}
				}

				if (e == r && checkTerminated(done, q.isEmpty(), a)) {
					return;
				}

				int w = wip;
				if (missed == w) {
					produced = e;
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				}
				else {
					missed = w;
				}
			}
		}

		void runBackfused() {
			int missed = 1;

			for (; ; ) {

				if (cancelled) {
					return;
				}

				boolean d = done;

				actual.onNext(null);

				if (d) {
					Throwable e = error;
					if (e != null) {
						doError(actual, e);
					}
					else {
						doComplete(actual);
					}
					return;
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void doComplete(Subscriber<?> a) {
			try {
				a.onComplete();
			}
			finally {
				worker.dispose();
			}
		}

		void doError(Subscriber<?> a, Throwable e) {
			try {
				a.onError(e);
			}
			finally {
				worker.dispose();
			}
		}

		@Override
		public void run() {
			if (outputFused) {
				runBackfused();
			}
			else if (sourceMode == Fuseable.SYNC) {
				runSync();
			}
			else {
				runAsync();
			}
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a) {
			if (cancelled) {
				queue.clear();
				return true;
			}
			if (d) {
				if (delayError) {
					if (empty) {
						Throwable e = error;
						if (e != null) {
							doError(a, e);
						}
						else {
							doComplete(a);
						}
						return true;
					}
				}
				else {
					Throwable e = error;
					if (e != null) {
						queue.clear();
						doError(a, e);
						return true;
					}
					else if (empty) {
						doComplete(a);
						return true;
					}
				}
			}

			return false;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM ) return requested;
			if (key == ScannableAttr.PARENT ) return s;
			if (key == BooleanAttr.CANCELLED) return cancelled;
			if (key == BooleanAttr.TERMINATED) return done;
			if (key == IntAttr.BUFFERED) return queue != null ? queue.size() : 0;
			if (key == ThrowableAttr.ERROR) return error;
			if (key == BooleanAttr.DELAY_ERROR) return delayError;
			if (key == IntAttr.PREFETCH) return prefetch;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void clear() {
			queue.clear();
		}

		@Override
		public boolean isEmpty() {
			return queue.isEmpty();
		}

		@Override
		@Nullable
		public T poll() {
			T v = queue.poll();
			if (v != null && sourceMode != SYNC) {
				long p = produced + 1;
				if (p == limit) {
					produced = 0;
					s.request(p);
				}
				else {
					produced = p;
				}
			}
			return v;
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & ASYNC) != 0) {
				outputFused = true;
				return ASYNC;
			}
			return NONE;
		}

		@Override
		public int size() {
			return queue.size();
		}
	}

	static final class PublishOnConditionalSubscriber<T>
			implements QueueSubscription<T>, Runnable, InnerOperator<T, T> {

		final ConditionalSubscriber<? super T> actual;

		final Worker worker;

		final Scheduler scheduler;

		final boolean delayError;

		final int prefetch;

		final int limit;

		final Supplier<? extends Queue<T>> queueSupplier;

		Subscription s;

		Queue<T> queue;

		volatile boolean cancelled;

		volatile boolean done;

		Throwable error;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<PublishOnConditionalSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(PublishOnConditionalSubscriber.class,
						"wip");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<PublishOnConditionalSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(PublishOnConditionalSubscriber.class,
						"requested");

		int sourceMode;

		long produced;

		long consumed;

		boolean outputFused;

		PublishOnConditionalSubscriber(ConditionalSubscriber<? super T> actual,
				Scheduler scheduler,
				Worker worker,
				boolean delayError,
				int prefetch,
				Supplier<? extends Queue<T>> queueSupplier) {
			this.actual = actual;
			this.worker = worker;
			this.scheduler = scheduler;
			this.delayError = delayError;
			this.prefetch = prefetch;
			this.queueSupplier = queueSupplier;
			if (prefetch != Integer.MAX_VALUE) {
				this.limit = prefetch - (prefetch >> 2);
			}
			else {
				this.limit = Integer.MAX_VALUE;
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				if (s instanceof QueueSubscription) {
					@SuppressWarnings("unchecked") QueueSubscription<T> f =
							(QueueSubscription<T>) s;

					int m = f.requestFusion(Fuseable.ANY | Fuseable.THREAD_BARRIER);

					if (m == Fuseable.SYNC) {
						sourceMode = Fuseable.SYNC;
						queue = f;
						done = true;

						actual.onSubscribe(this);
						return;
					}
					else if (m == Fuseable.ASYNC) {
						sourceMode = Fuseable.ASYNC;
						queue = f;

						actual.onSubscribe(this);

						initialRequest();

						return;
					}
				}

				queue = queueSupplier.get();

				actual.onSubscribe(this);

				initialRequest();
			}
		}

		void initialRequest() {
			if (prefetch == Integer.MAX_VALUE) {
				s.request(Long.MAX_VALUE);
			}
			else {
				s.request(prefetch);
			}
		}

		@Override
		public void onNext(T t) {
			if (t == null) {//async fusion
				if (trySchedule() == Scheduler.REJECTED) {
					throw Operators.onRejectedExecution(this, null, null);
				}
				return;
			}

			if (done) {
				Operators.onNextDropped(t);
				return;
			}
			if (!queue.offer(t)) {
				error = Operators.onOperatorError(s, Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL), t);
				done = true;
			}
			if (trySchedule() == Scheduler.REJECTED) {
				throw Operators.onRejectedExecution(this, null, t);
			}
		}

		@Override
		public void onError(Throwable t) {
			if(done){
				Operators.onErrorDropped(t);
				return;
			}
			error = t;
			done = true;
			if (trySchedule() == Scheduler.REJECTED) {
				throw Operators.onRejectedExecution(null, t, null);
			}
		}

		@Override
		public void onComplete() {
			if(done){
				return;
			}
			done = true;
			if (trySchedule() == Scheduler.REJECTED && !worker.isDisposed()) {
				throw Operators.onRejectedExecution();
			}
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.getAndAddCap(REQUESTED, this, n);
				if (trySchedule() == Scheduler.REJECTED && (!worker.isDisposed() || scheduler.isDisposed())) {
					throw Operators.onRejectedExecution(this, null, null);
				}
			}
		}

		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}

			cancelled = true;
			s.cancel();
			worker.dispose();

			if (WIP.getAndIncrement(this) == 0) {
				queue.clear();
			}
		}

		@Nullable
		Disposable trySchedule() {
			if (WIP.getAndIncrement(this) != 0) {
				return null;
			}

			return worker.schedule(this);
		}

		void runSync() {
			int missed = 1;

			final ConditionalSubscriber<? super T> a = actual;
			final Queue<T> q = queue;

			long e = produced;

			for (; ; ) {

				long r = requested;

				while (e != r) {
					T v;
					try {
						v = q.poll();
					}
					catch (Throwable ex) {
						doError(a, Operators.onOperatorError(s, ex));
						return;
					}

					if (cancelled) {
						return;
					}
					if (v == null) {
						doComplete(a);
						return;
					}

					if (a.tryOnNext(v)) {
						e++;
					}
				}

				if (cancelled) {
					return;
				}

				if (q.isEmpty()) {
					doComplete(a);
					return;
				}

				int w = wip;
				if (missed == w) {
					produced = e;
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				}
				else {
					missed = w;
				}
			}
		}

		void runAsync() {
			int missed = 1;

			final ConditionalSubscriber<? super T> a = actual;
			final Queue<T> q = queue;

			long emitted = produced;
			long polled = consumed;

			for (; ; ) {

				long r = requested;

				while (emitted != r) {
					boolean d = done;
					T v;
					try {
						v = q.poll();
					}
					catch (Throwable ex) {
						Exceptions.throwIfFatal(ex);
						s.cancel();
						q.clear();

						doError(a, Operators.onOperatorError(ex));
						return;
					}
					boolean empty = v == null;

					if (checkTerminated(d, empty, a)) {
						return;
					}

					if (empty) {
						break;
					}

					if (a.tryOnNext(v)) {
						emitted++;
					}

					polled++;

					if (polled == limit) {
						s.request(polled);
						polled = 0L;
					}
				}

				if (emitted == r && checkTerminated(done, q.isEmpty(), a)) {
					return;
				}

				int w = wip;
				if (missed == w) {
					produced = emitted;
					consumed = polled;
					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				}
				else {
					missed = w;
				}
			}

		}

		void runBackfused() {
			int missed = 1;

			for (; ; ) {

				if (cancelled) {
					return;
				}

				boolean d = done;

				actual.onNext(null);

				if (d) {
					Throwable e = error;
					if (e != null) {
						doError(actual, e);
					}
					else {
						doComplete(actual);
					}
					return;
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		@Override
		public void run() {
			if (outputFused) {
				runBackfused();
			}
			else if (sourceMode == Fuseable.SYNC) {
				runSync();
			}
			else {
				runAsync();
			}
		}

		@Override
		public Subscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == LongAttr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == ScannableAttr.PARENT) return s;
			if (key == BooleanAttr.CANCELLED) return cancelled;
			if (key == BooleanAttr.TERMINATED) return done;
			if (key == IntAttr.BUFFERED) return queue != null ? queue.size() : 0;
			if (key == ThrowableAttr.ERROR) return error;
			if (key == BooleanAttr.DELAY_ERROR) return delayError;
			if (key == IntAttr.PREFETCH) return prefetch;

			return InnerOperator.super.scanUnsafe(key);
		}

		void doComplete(Subscriber<?> a) {
			try {
				a.onComplete();
			}
			finally {
				worker.dispose();
			}
		}

		void doError(Subscriber<?> a, Throwable e) {
			try {
				a.onError(e);
			}
			finally {
				worker.dispose();
			}
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a) {
			if (cancelled) {
				queue.clear();
				return true;
			}
			if (d) {
				if (delayError) {
					if (empty) {
						Throwable e = error;
						if (e != null) {
							doError(a, e);
						}
						else {
							doComplete(a);
						}
						return true;
					}
				}
				else {
					Throwable e = error;
					if (e != null) {
						queue.clear();
						doError(a, e);
						return true;
					}
					else if (empty) {
						doComplete(a);
						return true;
					}
				}
			}

			return false;
		}

		@Override
		public void clear() {
			queue.clear();
		}

		@Override
		public boolean isEmpty() {
			return queue.isEmpty();
		}

		@Override
		@Nullable
		public T poll() {
			T v = queue.poll();
			if (v != null && sourceMode != SYNC) {
				long p = consumed + 1;
				if (p == limit) {
					consumed = 0;
					s.request(p);
				}
				else {
					consumed = p;
				}
			}
			return v;
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & ASYNC) != 0) {
				outputFused = true;
				return ASYNC;
			}
			return NONE;
		}

		@Override
		public int size() {
			return queue.size();
		}
	}
}

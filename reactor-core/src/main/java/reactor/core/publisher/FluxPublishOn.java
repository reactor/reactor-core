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

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Scheduler.Worker;
import reactor.util.annotation.Nullable;

/**
 * Emits events on a different thread specified by a scheduler callback.
 *
 * @param <T> the value type
 *
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxPublishOn<T> extends InternalFluxOperator<T, T> implements Fuseable {

	final Scheduler scheduler;

	final boolean delayError;

	FluxPublishOn(Flux<? extends T> source, Scheduler scheduler, boolean delayError) {
		super(source);
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
		this.delayError = delayError;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_ON) return scheduler;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

		return super.scanUnsafe(key);
	}

	@Override
	@SuppressWarnings("unchecked")
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		Worker worker = Objects.requireNonNull(scheduler.createWorker(),
				"The scheduler returned a null worker");

		if (actual instanceof ConditionalSubscriber) {
			ConditionalSubscriber<? super T> cs =
					(ConditionalSubscriber<? super T>) actual;
			source.subscribe(new PublishOnConditionalSubscriber<>(cs,
					scheduler,
					worker,
					delayError));
			return null;
		}
		return new PublishOnSubscriber<>(actual, scheduler, worker, delayError);
	}

	static final class PublishOnSubscriber<T>
			implements QueueSubscription<T>, Runnable, InnerOperator<T, T> {

		final CoreSubscriber<? super T> actual;

		final Scheduler scheduler;

		final Worker worker;

		final boolean delayError;

		Subscription s;

		QueueSubscription<T> qs;

		volatile T value;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<PublishOnSubscriber, Object> VALUE =
				AtomicReferenceFieldUpdater.newUpdater(PublishOnSubscriber.class, Object.class, "value");

		volatile boolean cancelled;

		volatile boolean done;

		Throwable error;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<PublishOnSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(PublishOnSubscriber.class, "wip");

		volatile int discardGuard;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<PublishOnSubscriber> DISCARD_GUARD =
				AtomicIntegerFieldUpdater.newUpdater(PublishOnSubscriber.class, "discardGuard");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<PublishOnSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(PublishOnSubscriber.class, "requested");

		int sourceMode;

		long produced;

		boolean outputFused;

		PublishOnSubscriber(CoreSubscriber<? super T> actual,
				Scheduler scheduler,
				Worker worker,
				boolean delayError) {
			this.actual = actual;
			this.worker = worker;
			this.scheduler = scheduler;
			this.delayError = delayError;

			REQUESTED.lazySet(this, Long.MIN_VALUE);
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
						qs = f;
						done = true;
					}
					else if (m == Fuseable.ASYNC) {
						sourceMode = Fuseable.ASYNC;
						qs = f;
					}
				}
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (sourceMode == ASYNC) {
				trySchedule(this, null, null /* t always null */);
				return;
			}

			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			if (cancelled) {
				Operators.onDiscard(t, actual.currentContext());
				return;
			}

			if (!VALUE.compareAndSet(this, null, t)) {
				Operators.onDiscard(t, actual.currentContext());
				error = Operators.onOperatorError(s,
						Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL),
						t, actual.currentContext());
				done = true;
			}
			trySchedule(this, null, t);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			error = t;
			done = true;
			trySchedule(null, t, null);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			//WIP also guards, no competing onNext
			trySchedule(null, null, null);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				long previousState = Operators.addCapFromMin(REQUESTED, this, n);

				// check if this is the first request from the downstream
				if (previousState == Long.MIN_VALUE && this.sourceMode == Fuseable.NONE) {
					this.s.request(1);
				}

				//WIP also guards during request and onError is possible
				trySchedule(this, null, null);
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
				if (sourceMode == ASYNC) {
					// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
					qs.clear();
				}
				else if (!outputFused) {
					if (sourceMode == SYNC) {
						// discard MUST be happening only and only if there is no racing on elements consumption
						// which is guaranteed by the WIP guard here in case non-fused output
						Operators.onDiscardQueueWithClear(qs, actual.currentContext(), null);
					}
					if (sourceMode == NONE) {
						@SuppressWarnings("unchecked") T v = (T) VALUE.getAndSet(this, null);
						Operators.onDiscard(v, actual.currentContext());
					}
				}
			}
		}

		void trySchedule(
				@Nullable Subscription subscription,
				@Nullable Throwable suppressed,
				@Nullable Object dataSignal) {
			if (WIP.getAndIncrement(this) != 0) {
				if (cancelled) {
					if (sourceMode == ASYNC) {
						// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
						qs.clear();
					}
					else {
						// discard given dataSignal since no more is enqueued (spec guarantees serialised onXXX calls)
						Operators.onDiscard(dataSignal, actual.currentContext());
					}
				}
				return;
			}

			try {
				worker.schedule(this);
			}
			catch (RejectedExecutionException ree) {
				if (sourceMode == ASYNC) {
					// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
					qs.clear();
				} else if (outputFused) {
					// We are the holder of the queue, but we still have to perform discarding under the guarded block
					// to prevent any racing done by downstream
					this.clear();
				}
				else {
					if (sourceMode == Fuseable.SYNC) {
						// discard MUST be happening only and only if there is no racing on elements consumption
						// which is guaranteed by the WIP guard here in case non-fused output
						Operators.onDiscardQueueWithClear(qs, actual.currentContext(), null);
					}
					if (sourceMode == Fuseable.NONE) {
						@SuppressWarnings("unchecked") T v = (T) VALUE.getAndSet(this, null);
						Operators.onDiscard(v, actual.currentContext());
					}
				}
				actual.onError(Operators.onRejectedExecution(ree, subscription, suppressed, dataSignal,
						actual.currentContext()));
			}
		}

		void runSync() {
			int missed = 1;

			final Subscriber<? super T> a = actual;
			final Queue<T> q = qs;

			long e = produced;
			for (; ; ) {
				long r = requested & Long.MAX_VALUE;

				while (e != r) {
					T v;
					try {
						v = q.poll();
					}
					catch (Throwable ex) {
						doError(a, Operators.onOperatorError(s, ex, actual.currentContext()));
						return;
					}

					if (cancelled) {
						Operators.onDiscard(v, actual.currentContext());
						Operators.onDiscardQueueWithClear(q, actual.currentContext(), null);
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
					Operators.onDiscardQueueWithClear(q, actual.currentContext(), null);
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
			final Queue<T> q = qs;

			long e = produced;
			for (; ; ) {
				long r = requested & Long.MAX_VALUE;

				while (e != r) {
					boolean d = done;
					T v;

					try {
						v = qs.poll();
					}
					catch (Throwable ex) {
						Exceptions.throwIfFatal(ex);
						s.cancel();
						// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
						qs.clear();
						doError(a, Operators.onOperatorError(ex, actual.currentContext()));
						return;
					}

					boolean empty = v == null;

					if (checkTerminated(d, empty, a, v)) {
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(v);
					e++;
				}

				if (e == r && checkTerminated(done, q.isEmpty(), a, null)) {
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
					// We are the holder of the queue, but we still have to perform discarding under the guarded block
					// to prevent any racing done by downstream
					this.clear();
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

		void runOneByOne() {
			int missed = 1;

			final Subscriber<? super T> a = actual;

			long e = produced;
			for (; ; ) {
				long r = this.requested & Long.MAX_VALUE;

				while (e != r) {
					boolean d = done;

					@SuppressWarnings("unchecked") T v = (T) VALUE.getAndSet(this, null);

					boolean empty = v == null;

					if (checkTerminated(d, empty, a, v)) {
						return;
					}

					if (empty) {
						break;
					}
					a.onNext(v);
					e++;
					s.request(1);
				}

				if (e == r && checkTerminated(done, value == null, a, null)) {
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

		void doComplete(Subscriber<?> a) {
			a.onComplete();
			worker.dispose();
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
			else if (sourceMode == Fuseable.ASYNC) {
				runAsync();
			}
			else {
				runOneByOne();
			}
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, @Nullable T v) {
			if (cancelled) {
				Operators.onDiscard(v, actual.currentContext());
				if (sourceMode == ASYNC) {
					// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
					qs.clear();
				}
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
						Operators.onDiscard(v, actual.currentContext());
						if (sourceMode == ASYNC) {
							// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
							qs.clear();
						}
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
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested & Long.MAX_VALUE;
			if (key == Attr.PARENT) return s;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.ERROR) return error;
			if (key == Attr.DELAY_ERROR) return delayError;
			if (key == Attr.RUN_ON) return worker;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void clear() {
			if (sourceMode == Fuseable.ASYNC) {
				qs.clear();
				return;
			}

			// use guard on the queue instance as the best way to ensure there is no racing on draining
			// the call to this method must be done only during the ASYNC fusion so all the callers will be waiting
			// this should not be performance costly with the assumption the cancel is rare operation
			if (DISCARD_GUARD.getAndIncrement(this) != 0) {
				return;
			}

			int missed = 1;

			for (;;) {
				Operators.onDiscardQueueWithClear(qs, actual.currentContext(), null);

				int dg = discardGuard;
				if (missed == dg) {
					missed = DISCARD_GUARD.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				}
				else {
					missed = dg;
				}
			}
		}

		@Override
		public boolean isEmpty() {
			return qs == null || qs.isEmpty();
		}

		@Override
		public T poll() {
			return qs == null ? null : qs.poll();
		}

		@Override
		public int requestFusion(int requestedMode) {
			if (sourceMode != Fuseable.NONE && (requestedMode & Fuseable.ASYNC) != 0) {
				outputFused = true;
				return Fuseable.ASYNC;
			}
			return Fuseable.NONE;
		}

		@Override
		public int size() {
			return qs == null ? 0 : qs.size();
		}
	}

	static final class PublishOnConditionalSubscriber<T>
			implements QueueSubscription<T>, Runnable, InnerOperator<T, T> {

		final ConditionalSubscriber<? super T> actual;

		final Worker worker;

		final Scheduler scheduler;

		final boolean delayError;

		Subscription s;

		QueueSubscription<T> qs;

		volatile T value;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<PublishOnConditionalSubscriber, Object> VALUE =
				AtomicReferenceFieldUpdater.newUpdater(PublishOnConditionalSubscriber.class, Object.class, "value");

		volatile boolean cancelled;

		volatile boolean done;

		Throwable error;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<PublishOnConditionalSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(PublishOnConditionalSubscriber.class,
						"wip");

		volatile int discardGuard;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<PublishOnConditionalSubscriber> DISCARD_GUARD =
				AtomicIntegerFieldUpdater.newUpdater(PublishOnConditionalSubscriber.class,
						"discardGuard");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<PublishOnConditionalSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(PublishOnConditionalSubscriber.class,
						"requested");

		int sourceMode;

		long produced;

		boolean outputFused;

		PublishOnConditionalSubscriber(ConditionalSubscriber<? super T> actual,
				Scheduler scheduler,
				Worker worker,
				boolean delayError) {
			this.actual = actual;
			this.worker = worker;
			this.scheduler = scheduler;
			this.delayError = delayError;

			REQUESTED.lazySet(this, Long.MIN_VALUE);
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
						qs = f;
						done = true;
					}
					if (m == Fuseable.ASYNC) {
						sourceMode = Fuseable.ASYNC;
						qs = f;
					}
				}
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (sourceMode == ASYNC) {
				trySchedule(this, null, null /* t always null */);
				return;
			}

			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			if (cancelled) {
				Operators.onDiscard(t, actual.currentContext());
				return;
			}

			if (!VALUE.compareAndSet(this, null, t)) {
				Operators.onDiscard(t, actual.currentContext());
				error = Operators.onOperatorError(s, Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL), t,
						actual.currentContext());
				done = true;
			}
			trySchedule(this, null, t);
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			error = t;
			done = true;
			trySchedule(null, t, null);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			// WIP also guards, no competing onNext
			trySchedule(null, null, null);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				long previousState = Operators.addCapFromMin(REQUESTED, this, n);

				// check if this is the first request from the downstream
				if (previousState == Long.MIN_VALUE && this.sourceMode == Fuseable.NONE) {
					this.s.request(1);
				}

				// WIP also guards during request and onError is possible
				trySchedule(this, null, null);
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
				if (sourceMode == ASYNC) {
					// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
					qs.clear();
				}
				else if (!outputFused) {
					if (sourceMode == SYNC) {
						// discard MUST be happening only and only if there is no racing on elements consumption
						// which is guaranteed by the WIP guard here in case non-fused output
						Operators.onDiscardQueueWithClear(qs, actual.currentContext(), null);
					}
					if (sourceMode == NONE) {
						@SuppressWarnings("unchecked") T v = (T) VALUE.getAndSet(this, null);
						Operators.onDiscard(v, actual.currentContext());
					}
				}
			}
		}

		void trySchedule(
				@Nullable Subscription subscription,
				@Nullable Throwable suppressed,
				@Nullable Object dataSignal) {
			if (WIP.getAndIncrement(this) != 0) {
				if (cancelled) {
					if (sourceMode == ASYNC) {
						// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
						qs.clear();
					}
					else {
						// discard given dataSignal since no more is enqueued (spec guarantees serialised onXXX calls)
						Operators.onDiscard(dataSignal, actual.currentContext());
					}
				}
				return;
			}

			try {
				worker.schedule(this);
			}
			catch (RejectedExecutionException ree) {
				if (sourceMode == ASYNC) {
					// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
					qs.clear();
				} else if (outputFused) {
					// We are the holder of the queue, but we still have to perform discarding under the guarded block
					// to prevent any racing done by downstream
					this.clear();
				}
				else {
					if (sourceMode == SYNC) {
						// discard MUST be happening only and only if there is no racing on elements consumption
						// which is guaranteed by the WIP guard here in case non-fused output
						Operators.onDiscardQueueWithClear(qs, actual.currentContext(), null);
					}
					if (sourceMode == NONE) {
						@SuppressWarnings("unchecked") T v = (T) VALUE.getAndSet(this, null);
						Operators.onDiscard(v, actual.currentContext());
					}
				}
				actual.onError(Operators.onRejectedExecution(ree, subscription, suppressed, dataSignal,
						actual.currentContext()));
			}
		}

		void runSync() {
			int missed = 1;

			final ConditionalSubscriber<? super T> a = actual;
			final Queue<T> q = qs;

			long e = produced;
			for (; ; ) {
				long r = requested & Long.MAX_VALUE;

				while (e != r) {
					T v;
					try {
						v = q.poll();
					}
					catch (Throwable ex) {
						doError(a, Operators.onOperatorError(s, ex, actual.currentContext()));
						return;
					}

					if (cancelled) {
						Operators.onDiscard(v, actual.currentContext());
						Operators.onDiscardQueueWithClear(q, actual.currentContext(), null);
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
					Operators.onDiscardQueueWithClear(q, actual.currentContext(), null);
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
			final Queue<T> q = qs;

			long e = produced;
			for (; ; ) {
				long dropped = 0;
				long r = requested & Long.MAX_VALUE;

				while (e != r) {
					boolean d = done;
					T v;
					try {
						v = q.poll();
					}
					catch (Throwable ex) {
						Exceptions.throwIfFatal(ex);
						s.cancel();
						// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
						q.clear();
						doError(a, Operators.onOperatorError(ex, actual.currentContext()));
						return;
					}
					boolean empty = v == null;

					if (checkTerminated(d, empty, a, v)) {
						return;
					}

					if (empty) {
						if (dropped != 0) {
							s.request(dropped);
						}
						break;
					}

					if (a.tryOnNext(v)) {
						e++;
						if (dropped != 0) {
							s.request(dropped);
							dropped = 0;
						}
					}
					else {
						dropped++;
					}
				}

				if (e == r && checkTerminated(done, q.isEmpty(), a, null)) {
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
					// We are the holder of the queue, but we still have to perform discarding under the guarded block
					// to prevent any racing done by downstream
					this.clear();
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

		void runOneByOne() {
			int missed = 1;

			final ConditionalSubscriber<? super T> a = actual;

			long e = produced;
			for (; ; ) {
				long r = requested & Long.MAX_VALUE;

				while (e != r) {
					boolean d = done;

					@SuppressWarnings("unchecked") T v = (T) VALUE.getAndSet(this, null);

					boolean empty = v == null;

					if (checkTerminated(d, empty, a, v)) {
						return;
					}

					if (empty) {
						break;
					}

					if (a.tryOnNext(v)) {
						e++;
					}

					s.request(1);
				}

				if (e == r && checkTerminated(done, value == null, a, null)) {
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

		@Override
		public void run() {
			if (outputFused) {
				runBackfused();
			}
			else if (sourceMode == Fuseable.SYNC) {
				runSync();
			}
			else if (sourceMode == Fuseable.ASYNC) {
				runAsync();
			}
			else {
				runOneByOne();
			}
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested & Long.MAX_VALUE;
			if (key == Attr.PARENT) return s;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.ERROR) return error;
			if (key == Attr.DELAY_ERROR) return delayError;
			if (key == Attr.RUN_ON) return worker;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

			return InnerOperator.super.scanUnsafe(key);
		}

		void doComplete(Subscriber<?> a) {
			a.onComplete();
			worker.dispose();
		}

		void doError(Subscriber<?> a, Throwable e) {
			try {
				a.onError(e);
			}
			finally {
				worker.dispose();
			}
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, @Nullable T v) {
			if (cancelled) {
				Operators.onDiscard(v, actual.currentContext());
				if (sourceMode == ASYNC) {
					// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
					qs.clear();
				}
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
						Operators.onDiscard(v, actual.currentContext());
						if (sourceMode == ASYNC) {
							// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
							qs.clear();
						}
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
			if (sourceMode == Fuseable.ASYNC) {
				qs.clear();
				return;
			}

			// use guard on the queue instance as the best way to ensure there is no racing on draining
			// the call to this method must be done only during the ASYNC fusion so all the callers will be waiting
			// this should not be performance costly with the assumption the cancel is rare operation
			if (DISCARD_GUARD.getAndIncrement(this) != 0) {
				return;
			}

			int missed = 1;

			for (;;) {
				Operators.onDiscardQueueWithClear(qs, actual.currentContext(), null);

				int dg = discardGuard;
				if (missed == dg) {
					missed = DISCARD_GUARD.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				}
				else {
					missed = dg;
				}
			}
		}

		@Override
		public boolean isEmpty() {
			return qs == null || qs.isEmpty();
		}

		@Override
		@Nullable
		public T poll() {
			return qs == null ? null : qs.poll();
		}

		@Override
		public int requestFusion(int requestedMode) {
			if (sourceMode != NONE && (requestedMode & ASYNC) != 0) {
				outputFused = true;
				return ASYNC;
			}
			return NONE;
		}

		@Override
		public int size() {
			return qs == null ? 0 : qs.size();
		}
	}
}

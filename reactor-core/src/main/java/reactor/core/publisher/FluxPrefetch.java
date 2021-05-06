package reactor.core.publisher;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

final class FluxPrefetch<T> extends InternalFluxOperator<T, T> implements Fuseable {

	final int prefetch;

	final int lowTide;

	final Supplier<? extends Queue<T>> queueSupplier;

	final PrefetchMode prefetchMode;

	enum PrefetchMode {
		EAGER, LAZY,
	}

	public FluxPrefetch(Flux<? extends T> source,
			int prefetch,
			int lowTide,
			Supplier<? extends Queue<T>> queueSupplier,
			PrefetchMode prefetchMode) {
		super(source);
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		this.prefetch = prefetch;
		this.lowTide = lowTide;
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
		this.prefetchMode = prefetchMode;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

		return super.scanUnsafe(key);
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	@Override
	@SuppressWarnings("unchecked")
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		if (actual instanceof ConditionalSubscriber) {
			@SuppressWarnings("unchecked") ConditionalSubscriber<? super T> cs =
					(ConditionalSubscriber<? super T>) actual;
			source.subscribe(new PrefetchConditionalSubscriber<>(cs,
					prefetch,
					lowTide,
					queueSupplier,
					prefetchMode));
			return null;
		}
		return new PrefetchSubscriber<>(actual,
				prefetch,
				lowTide,
				queueSupplier,
				prefetchMode);
	}

	static final class PrefetchSubscriber<T>
			implements QueueSubscription<T>, InnerOperator<T, T> {

		final CoreSubscriber<? super T> actual;

		final int prefetch;

		final int limit;

		final Supplier<? extends Queue<T>> queueSupplier;

		final PrefetchMode prefetchMode;

		Subscription s;

		Queue<T> queue;

		volatile boolean cancelled;

		volatile boolean done;

		Throwable error;

		volatile     long                                       requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<PrefetchSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(PrefetchSubscriber.class, "requested");

		volatile     int                                           wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<PrefetchSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(PrefetchSubscriber.class, "wip");

		volatile     int                                           discardGuard;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<PrefetchSubscriber> DISCARD_GUARD =
				AtomicIntegerFieldUpdater.newUpdater(PrefetchSubscriber.class,
						"discardGuard");

		int sourceMode = -1;

		int outputFused;

		boolean firstRequest = true;

		long produced;

		PrefetchSubscriber(CoreSubscriber<? super T> actual,
				int prefetch,
				int lowTide,
				Supplier<? extends Queue<T>> queueSupplier,
				PrefetchMode prefetchMode) {
			this.actual = actual;
			this.prefetch = prefetch;
			this.limit = Operators.unboundedOrLimit(prefetch, lowTide);
			this.queueSupplier = queueSupplier;
			this.prefetchMode = prefetchMode;

			REQUESTED.lazySet(this, Long.MIN_VALUE);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				WIP.lazySet(this, 1);
				actual.onSubscribe(this);

				if (cancelled) {
					if (sourceMode == Fuseable.ASYNC) {
						// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
						queue.clear();
					}
					else {
						// discard MUST be happening only and only if there is no racing on elements consumption
						// which is guaranteed by the WIP guard here
						Operators.onDiscardQueueWithClear(queue,
								actual.currentContext(),
								null);
					}
					return;
				}

				// sourceMode == -1 if downstream was not calling requestFusion
				if (sourceMode == -1) {
					// check if upstream if
					// fuseable so we can fuse at least with the upstream
					if (s instanceof QueueSubscription) {
						@SuppressWarnings("unchecked") QueueSubscription<T> fusion =
								(QueueSubscription<T>) s;
						int fuisonMode = fusion.requestFusion(Fuseable.ANY);

						if (fuisonMode == Fuseable.SYNC) {
							sourceMode = Fuseable.SYNC;
							queue = fusion;
							done = true;
							firstRequest = false;

							// check if downstream requested something
							if (this.wip == 1 && WIP.addAndGet(this, -1) == 0) {
								// exit if nothing was requested yet
								return;
							}

							drainSync();
							return;
						}
						if (fuisonMode == Fuseable.ASYNC) {
							sourceMode = Fuseable.ASYNC;
							queue = fusion;
							firstRequest = false;

							// check if something was requested or delivered in the
							// meantime
							if (wip == 1 && WIP.addAndGet(this, -1) == 0) {
								// exit if non of the mentioned has happened yet
								return;
							}

							drainAsync();
							return;
						}
					}

					sourceMode = Fuseable.NONE;
					queue = queueSupplier.get();
					if (prefetchMode == PrefetchMode.EAGER) {
						firstRequest = false;
						s.request(Operators.unboundedOrPrefetch(prefetch));
					}

					if (wip == 1 && WIP.addAndGet(this, -1) == 0) {
						return;
					}

					if (prefetchMode == PrefetchMode.LAZY) {
						firstRequest = false;
						s.request(Operators.unboundedOrPrefetch(prefetch));
					}

					drainAsync();
				}
				else if (prefetchMode == PrefetchMode.EAGER && sourceMode == Fuseable.NONE) {
					firstRequest = false;
					s.request(Operators.unboundedOrPrefetch(prefetch));
				}
			}
		}

		@Override
		public void onNext(T t) {
			if (sourceMode == Fuseable.ASYNC) {
				drain(null);
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

			if (!queue.offer(t)) {
				Operators.onDiscard(t, actual.currentContext());
				error = Operators.onOperatorError(s,
						Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL),
						t,
						actual.currentContext());
				done = true;
			}
			drain(t);
		}

		@Override
		public void onError(Throwable err) {
			if (done) {
				Operators.onErrorDropped(err, actual.currentContext());
				return;
			}
			error = err;
			done = true;
			drain(null);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			drain(null);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				long previousState;
				for (; ; ) {
					previousState = this.requested;

					long requested = previousState & Long.MAX_VALUE;
					long nextRequested = Operators.addCap(requested, n);

					if (REQUESTED.compareAndSet(this, previousState, nextRequested)) {
						break;
					}
				}

				// check if this is the first request from the downstream
				if (previousState == Long.MIN_VALUE) {
					// check the mode and fusion mode
					if (prefetchMode == PrefetchMode.LAZY) {
						if (sourceMode == -1) {
							// check if sourceMode was setted
							if (WIP.getAndIncrement(this) == 0) {
								if (sourceMode == Fuseable.NONE) {
									firstRequest = false;
									s.request(Operators.unboundedOrPrefetch(this.prefetch));
									drainAsync();
								}
								else if (sourceMode == Fuseable.ASYNC) {
									drainAsync();
								}
								else {
									drainSync();
								}
							}
							return;
						}
						else if (sourceMode == Fuseable.NONE) {
							firstRequest = false;
							s.request(Operators.unboundedOrPrefetch(this.prefetch));
						}
					}
				}

				drain(null);
			}
		}

		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}

			cancelled = true;
			s.cancel();

			if (WIP.getAndIncrement(this) == 0) {
				if (sourceMode == Fuseable.ASYNC) {
					// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
					queue.clear();
				}
				else if (outputFused == Fuseable.NONE) {
					// discard MUST be happening only and only if there is no racing on elements consumption
					// which is guaranteed by the WIP guard here in case non-fused output
					Operators.onDiscardQueueWithClear(queue,
							actual.currentContext(),
							null);
				}
			}
		}

		private void drainAsync() {
			final Subscriber<? super T> a = actual;
			final Queue<T> queue = this.queue;
			final int sourceMode = this.sourceMode;

			long emitted = produced;
			int missed = 1;
			for (; ; ) {
				long requested = this.requested & Long.MAX_VALUE;

				while (emitted != requested) {
					T value;
					try {
						value = queue.poll();
					}
					catch (Throwable err) {
						Exceptions.throwIfFatal(err);
						s.cancel();
						if (sourceMode == Fuseable.ASYNC) {
							// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
							queue.clear();
						}
						else {
							// discard MUST be happening only and only if there is no racing on elements consumption
							// which is guaranteed by the WIP guard here
							Operators.onDiscardQueueWithClear(queue,
									actual.currentContext(),
									null);
						}

						a.onError(Operators.onOperatorError(err,
								actual.currentContext()));
						return;
					}

					boolean empty = value == null;

					if (checkTerminated(done, empty, value)) {
						return;
					}

					if (empty) {
						break;
					}

					a.onNext(value);
					emitted++;

					if (emitted == limit) {
						if (requested != Long.MAX_VALUE) {
							requested = REQUESTED.addAndGet(this, -emitted);
						}
						if (sourceMode == Fuseable.NONE) {
							s.request(emitted);
						}
						emitted = 0L;
					}
				}

				if (emitted == requested && checkTerminated(done,
						queue.isEmpty(),
						null)) {
					return;
				}

				int w = wip;
				if (missed == w) {
					produced = emitted;
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

		private void drainOutput() {
			int missed = 1;
			for (; ; ) {
				if (cancelled) {
					// We are the holder of the queue, but we still have to perform discarding under the guarded block
					// to prevent any racing done by downstream
					clear();
					return;
				}

				actual.onNext(null);

				if (done) {
					Throwable err = error;
					if (err != null) {
						actual.onError(err);
					}
					else {
						actual.onComplete();
					}
					return;
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		private void drainSync() {
			final Subscriber<? super T> a = actual;
			final Queue<T> queue = this.queue;

			long emitted = produced;
			int missed = 1;
			for (; ; ) {
				long requested = this.requested;

				while (emitted != requested) {
					T value;
					try {
						value = queue.poll();
					}
					catch (Throwable err) {
						a.onError(Operators.onOperatorError(s,
								err,
								actual.currentContext()));
						return;
					}

					if (cancelled) {
						Operators.onDiscard(value, actual.currentContext());
						Operators.onDiscardQueueWithClear(queue,
								actual.currentContext(),
								null);
						return;
					}
					if (value == null) {
						a.onComplete();
						return;
					}

					a.onNext(value);
					emitted++;
				}

				if (cancelled) {
					Operators.onDiscardQueueWithClear(queue,
							actual.currentContext(),
							null);
					return;
				}

				if (queue.isEmpty()) {
					a.onComplete();
					return;
				}

				int w = wip;
				if (missed == w) {
					produced = emitted;
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

		private void drain(@Nullable Object dataSignal) {
			if (WIP.getAndIncrement(this) != 0) {
				if (cancelled) {
					if (sourceMode == Fuseable.ASYNC) {
						// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
						queue.clear();
					}
					else {
						// discard given dataSignal since no more is enqueued (spec guarantees serialised onXXX calls)
						Operators.onDiscard(dataSignal, actual.currentContext());
					}
				}
				return;
			}

			if (outputFused != Fuseable.NONE) {
				drainOutput();
			}
			else if (sourceMode == Fuseable.SYNC) {
				drainSync();
			}
			else {
				drainAsync();
			}
		}

		boolean checkTerminated(boolean done, boolean empty, @Nullable T value) {
			if (cancelled) {
				Operators.onDiscard(value, actual.currentContext());
				if (sourceMode == Fuseable.ASYNC) {
					// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
					queue.clear();
				}
				else {
					// discard MUST be happening only and only if there is no racing on elements consumption
					// which is guaranteed by the WIP guard here
					Operators.onDiscardQueueWithClear(queue,
							actual.currentContext(),
							null);
				}
				return true;
			}
			if (done) {
				Throwable err = error;
				if (err != null) {
					Operators.onDiscard(value, actual.currentContext());
					if (sourceMode == Fuseable.ASYNC) {
						// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
						queue.clear();
					}
					else {
						// discard MUST be happening only and only if there is no racing on elements consumption
						// which is guaranteed by the WIP guard here
						Operators.onDiscardQueueWithClear(queue,
								actual.currentContext(),
								null);
					}
					actual.onError(err);
					return true;
				}
				else if (empty) {
					actual.onComplete();
					return true;
				}
			}

			return false;
		}

		@Override
		public void clear() {
			if (sourceMode == Fuseable.ASYNC) {
				queue.clear();
				return;
			}

			// use guard on the queue instance as the best way to ensure there is no racing on draining
			// the call to this method must be done only during the ASYNC fusion so all the callers will be waiting
			// this should not be performance costly with the assumption the cancel is rare operation
			if (DISCARD_GUARD.getAndIncrement(this) != 0) {
				return;
			}

			int missed = 1;

			for (; ; ) {
				Operators.onDiscardQueueWithClear(queue, actual.currentContext(), null);

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
			return queue.isEmpty();
		}

		@Override
		@Nullable
		public T poll() {
			if (firstRequest && sourceMode == Fuseable.NONE && prefetchMode == PrefetchMode.LAZY) {
				firstRequest = false;
				s.request(Operators.unboundedOrPrefetch(this.prefetch));
			}

			T value = queue.poll();
			if (value != null && sourceMode == Fuseable.NONE) {
				long p = produced + 1;
				if (p == limit) {
					produced = 0;
					s.request(p);
				}
				else {
					produced = p;
				}
			}
			return value;
		}

		public int requestFusion(int requestedMode) {
			if (s instanceof QueueSubscription) {
				@SuppressWarnings("unchecked") QueueSubscription<T> fusion =
						(QueueSubscription<T>) s;
				int fusionMode = fusion.requestFusion(requestedMode);

				if (fusionMode == Fuseable.SYNC) {
					sourceMode = fusionMode;
					outputFused = fusionMode;
					queue = fusion;
					done = true;
					firstRequest = false;

					WIP.lazySet(this, 0);
					return fusionMode;
				}
				else if (fusionMode == Fuseable.ASYNC) {
					sourceMode = fusionMode;
					outputFused = fusionMode;
					queue = fusion;
					firstRequest = false;

					WIP.lazySet(this, 0);
					return fusionMode;
				}
			}

			int fusionMode;
			sourceMode = Fuseable.NONE;
			queue = queueSupplier.get();

			if ((requestedMode & Fuseable.ASYNC) != 0) {
				outputFused = Fuseable.ASYNC;
				fusionMode = Fuseable.ASYNC;
			}
			else {
				fusionMode = Fuseable.NONE;
			}

			WIP.lazySet(this, 0);
			return fusionMode;
		}

		@Override
		public int size() {
			return queue.size();
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.ERROR) return error;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.PREFETCH) return prefetch;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.BUFFERED) return queue != null ? queue.size() : 0;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}
	}

	static final class PrefetchConditionalSubscriber<T>
			implements QueueSubscription<T>, InnerOperator<T, T> {

		final ConditionalSubscriber<? super T> actual;

		final int prefetch;

		final int limit;

		final Supplier<? extends Queue<T>> queueSupplier;

		final PrefetchMode prefetchMode;

		Subscription s;

		Queue<T> queue;

		volatile boolean cancelled;

		volatile boolean done;

		Throwable error;

		volatile     long                                                  requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<PrefetchConditionalSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(PrefetchConditionalSubscriber.class,
						"requested");

		volatile     int                                                      wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<PrefetchConditionalSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(PrefetchConditionalSubscriber.class,
						"wip");

		volatile     int discardGuard;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<PrefetchConditionalSubscriber>
		                 DISCARD_GUARD = AtomicIntegerFieldUpdater.newUpdater(
				PrefetchConditionalSubscriber.class,
				"discardGuard");

		int sourceMode = -1;

		int outputFused;

		boolean firstRequest = true;

		long produced;

		long consumed;

		PrefetchConditionalSubscriber(ConditionalSubscriber<? super T> actual,
				int prefetch,
				int lowTide,
				Supplier<? extends Queue<T>> queueSupplier,
				PrefetchMode prefetchMode) {
			this.actual = actual;
			this.prefetch = prefetch;
			this.limit = Operators.unboundedOrLimit(prefetch, lowTide);
			this.queueSupplier = queueSupplier;
			this.prefetchMode = prefetchMode;

			REQUESTED.lazySet(this, Long.MIN_VALUE);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				WIP.lazySet(this, 1);
				actual.onSubscribe(this);

				if (cancelled) {
					if (sourceMode == Fuseable.ASYNC) {
						// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
						queue.clear();
					}
					else {
						// discard MUST be happening only and only if there is no racing on elements consumption
						// which is guaranteed by the WIP guard here
						Operators.onDiscardQueueWithClear(queue,
								actual.currentContext(),
								null);
					}
					return;
				}

				// sourceMode == -1 if downstream was not calling requestFusion
				if (sourceMode == -1) {
					// check if upstream if
					// fuseable so we can fuse at least with the upstream
					if (s instanceof QueueSubscription) {
						@SuppressWarnings("unchecked") QueueSubscription<T> fusion =
								(QueueSubscription<T>) s;
						int fusionMode = fusion.requestFusion(Fuseable.ANY);

						if (fusionMode == Fuseable.SYNC) {
							sourceMode = Fuseable.SYNC;
							queue = fusion;
							done = true;
							firstRequest = false;

							// check if downstream requested something
							if (this.wip == 1 && WIP.addAndGet(this, -1) == 0) {
								// exit if nothing was requested yet
								return;
							}

							drainSync();
							return;
						}
						if (fusionMode == Fuseable.ASYNC) {
							sourceMode = Fuseable.ASYNC;
							queue = fusion;
							firstRequest = false;

							// check if something was requested or delivered in the
							// meantime
							if (wip == 1 && WIP.addAndGet(this, -1) == 0) {
								// exit if non of the mentioned has happened yet
								return;
							}

							drainAsync();
							return;
						}
					}

					sourceMode = Fuseable.NONE;
					queue = queueSupplier.get();
					if (prefetchMode == PrefetchMode.EAGER) {
						firstRequest = false;
						s.request(Operators.unboundedOrPrefetch(prefetch));
					}

					if (wip == 1 && WIP.addAndGet(this, -1) == 0) {
						return;
					}

					if (prefetchMode == PrefetchMode.LAZY) {
						firstRequest = false;
						s.request(Operators.unboundedOrPrefetch(prefetch));
					}

					drainAsync();
				}
				else if (prefetchMode == PrefetchMode.EAGER && sourceMode == Fuseable.NONE) {
					firstRequest = false;
					s.request(Operators.unboundedOrPrefetch(prefetch));
				}
			}
		}

		@Override
		public void onNext(T t) {
			if (sourceMode == Fuseable.ASYNC) {
				drain(null);
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

			if (!queue.offer(t)) {
				Operators.onDiscard(t, actual.currentContext());
				error = Operators.onOperatorError(s,
						Exceptions.failWithOverflow(Exceptions.BACKPRESSURE_ERROR_QUEUE_FULL),
						t,
						actual.currentContext());
				done = true;
			}
			drain(t);
		}

		@Override
		public void onError(Throwable err) {
			if (done) {
				Operators.onErrorDropped(err, actual.currentContext());
				return;
			}
			error = err;
			done = true;
			drain(null);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			drain(null);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				long previousState;
				for (; ; ) {
					previousState = this.requested;

					long requested = previousState & Long.MAX_VALUE;
					long nextRequested = Operators.addCap(requested, n);

					if (REQUESTED.compareAndSet(this, previousState, nextRequested)) {
						break;
					}
				}

				// check if this is the first request from the downstream
				if (previousState == Long.MIN_VALUE) {
					// check the mode and fusion mode
					if (prefetchMode == PrefetchMode.LAZY) {
						if (sourceMode == -1) {
							// check if sourceMode was setted
							if (WIP.getAndIncrement(this) == 0) {
								if (sourceMode == Fuseable.NONE) {
									firstRequest = false;
									s.request(Operators.unboundedOrPrefetch(this.prefetch));
									drainAsync();
								}
								else if (sourceMode == Fuseable.ASYNC) {
									drainAsync();
								}
								else {
									drainSync();
								}
							}
							return;
						}
						else if (sourceMode == Fuseable.NONE) {
							firstRequest = false;
							s.request(Operators.unboundedOrPrefetch(this.prefetch));
						}
					}
				}

				drain(null);
			}
		}

		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}

			cancelled = true;
			s.cancel();

			if (WIP.getAndIncrement(this) == 0) {
				if (sourceMode == Fuseable.ASYNC) {
					// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
					queue.clear();
				}
				else if (outputFused == Fuseable.NONE) {
					// discard MUST be happening only and only if there is no racing on elements consumption
					// which is guaranteed by the WIP guard here in case non-fused output
					Operators.onDiscardQueueWithClear(queue,
							actual.currentContext(),
							null);
				}
			}
		}

		private void drainAsync() {
			final ConditionalSubscriber<? super T> a = actual;
			final Queue<T> queue = this.queue;
			final int sourceMode = this.sourceMode;

			long emitted = produced;
			int missed = 1;
			long polled = consumed;
			for (; ; ) {
				long requested = this.requested & Long.MAX_VALUE;

				while (emitted != requested) {
					T value;
					try {
						value = queue.poll();
					}
					catch (Throwable err) {
						Exceptions.throwIfFatal(err);
						s.cancel();
						if (sourceMode == Fuseable.ASYNC) {
							// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
							queue.clear();
						}
						else {
							// discard MUST be happening only and only if there is no racing on elements consumption
							// which is guaranteed by the WIP guard here
							Operators.onDiscardQueueWithClear(queue,
									actual.currentContext(),
									null);
						}

						a.onError(Operators.onOperatorError(err,
								actual.currentContext()));
						return;
					}

					boolean empty = value == null;

					if (checkTerminated(done, empty, value)) {
						return;
					}

					if (empty) {
						break;
					}

					if (a.tryOnNext(value)) {
						emitted++;
					}

					polled++;

					if (polled == limit) {
						if (sourceMode == Fuseable.NONE) {
							s.request(polled);
						}
						polled = 0L;
					}
				}

				if (emitted == requested && checkTerminated(done,
						queue.isEmpty(),
						null)) {
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

		private void drainOutput() {
			int missed = 1;
			for (; ; ) {
				if (cancelled) {
					// We are the holder of the queue, but we still have to perform discarding under the guarded block
					// to prevent any racing done by downstream
					clear();
					return;
				}

				actual.onNext(null);

				if (done) {
					Throwable err = error;
					if (err != null) {
						actual.onError(err);
					}
					else {
						actual.onComplete();
					}
					return;
				}

				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		private void drainSync() {
			final ConditionalSubscriber<? super T> a = actual;
			final Queue<T> queue = this.queue;

			long emitted = produced;
			int missed = 1;
			for (; ; ) {
				long requested = this.requested;

				while (emitted != requested) {
					T value;
					try {
						value = queue.poll();
					}
					catch (Throwable err) {
						a.onError(Operators.onOperatorError(s,
								err,
								actual.currentContext()));
						return;
					}

					if (cancelled) {
						Operators.onDiscard(value, actual.currentContext());
						Operators.onDiscardQueueWithClear(queue,
								actual.currentContext(),
								null);
						return;
					}
					if (value == null) {
						a.onComplete();
						return;
					}

					if (a.tryOnNext(value)) {
						emitted++;
					}
				}

				if (cancelled) {
					Operators.onDiscardQueueWithClear(queue,
							actual.currentContext(),
							null);
					return;
				}

				if (queue.isEmpty()) {
					a.onComplete();
					return;
				}

				int w = wip;
				if (missed == w) {
					produced = emitted;
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

		private void drain(@Nullable Object dataSignal) {
			if (WIP.getAndIncrement(this) != 0) {
				if (cancelled) {
					if (sourceMode == Fuseable.ASYNC) {
						// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
						queue.clear();
					}
					else {
						// discard given dataSignal since no more is enqueued (spec guarantees serialised onXXX calls)
						Operators.onDiscard(dataSignal, actual.currentContext());
					}
				}
				return;
			}

			if (outputFused != Fuseable.NONE) {
				drainOutput();
			}
			else if (sourceMode == Fuseable.SYNC) {
				drainSync();
			}
			else {
				drainAsync();
			}
		}

		boolean checkTerminated(boolean done, boolean empty, @Nullable T value) {
			if (cancelled) {
				Operators.onDiscard(value, actual.currentContext());
				if (sourceMode == Fuseable.ASYNC) {
					// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
					queue.clear();
				}
				else {
					// discard MUST be happening only and only if there is no racing on elements consumption
					// which is guaranteed by the WIP guard here
					Operators.onDiscardQueueWithClear(queue,
							actual.currentContext(),
							null);
				}
				return true;
			}
			if (done) {
				Throwable err = error;
				if (err != null) {
					Operators.onDiscard(value, actual.currentContext());
					if (sourceMode == Fuseable.ASYNC) {
						// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
						queue.clear();
					}
					else {
						// discard MUST be happening only and only if there is no racing on elements consumption
						// which is guaranteed by the WIP guard here
						Operators.onDiscardQueueWithClear(queue,
								actual.currentContext(),
								null);
					}
					actual.onError(err);
					return true;
				}
				else if (empty) {
					actual.onComplete();
					return true;
				}
			}

			return false;
		}

		@Override
		public void clear() {
			if (sourceMode == Fuseable.ASYNC) {
				queue.clear();
				return;
			}

			// use guard on the queue instance as the best way to ensure there is no racing on draining
			// the call to this method must be done only during the ASYNC fusion so all the callers will be waiting
			// this should not be performance costly with the assumption the cancel is rare operation
			if (DISCARD_GUARD.getAndIncrement(this) != 0) {
				return;
			}

			int missed = 1;

			for (; ; ) {
				Operators.onDiscardQueueWithClear(queue, actual.currentContext(), null);

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
			return queue.isEmpty();
		}

		@Override
		@Nullable
		public T poll() {
			if (firstRequest && sourceMode == Fuseable.NONE && prefetchMode == PrefetchMode.LAZY) {
				firstRequest = false;
				s.request(Operators.unboundedOrPrefetch(this.prefetch));
			}

			T value = queue.poll();
			if (value != null && sourceMode == Fuseable.NONE) {
				long c = consumed + 1;
				if (c == limit) {
					consumed = 0;
					s.request(c);
				}
				else {
					consumed = c;
				}
			}
			return value;
		}

		public int requestFusion(int requestedMode) {
			if (s instanceof QueueSubscription) {
				@SuppressWarnings("unchecked") QueueSubscription<T> fusion =
						(QueueSubscription<T>) s;
				int fusionMode = fusion.requestFusion(requestedMode);

				if (fusionMode == Fuseable.SYNC) {
					sourceMode = fusionMode;
					outputFused = fusionMode;
					queue = fusion;
					done = true;
					firstRequest = false;

					WIP.lazySet(this, 0);
					return fusionMode;
				}
				else if (fusionMode == Fuseable.ASYNC) {
					sourceMode = fusionMode;
					outputFused = fusionMode;
					queue = fusion;
					firstRequest = false;

					WIP.lazySet(this, 0);
					return fusionMode;
				}
			}

			int fusionMode;
			sourceMode = Fuseable.NONE;
			queue = queueSupplier.get();

			if ((requestedMode & Fuseable.ASYNC) != 0) {
				outputFused = Fuseable.ASYNC;
				fusionMode = Fuseable.ASYNC;
			}
			else {
				fusionMode = Fuseable.NONE;
			}

			WIP.lazySet(this, 0);
			return fusionMode;
		}

		@Override
		public int size() {
			return queue.size();
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return s;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.ERROR) return error;
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.PREFETCH) return prefetch;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return requested;
			if (key == Attr.BUFFERED) return queue != null ? queue.size() : 0;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return InnerOperator.super.scanUnsafe(key);
		}
	}
}
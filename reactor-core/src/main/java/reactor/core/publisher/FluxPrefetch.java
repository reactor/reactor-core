package reactor.core.publisher;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Supplier;

import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Fuseable;
import reactor.util.annotation.Nullable;

final class FluxPrefetch<T> extends InternalFluxOperator<T, T> implements Fuseable {

	private final int prefetch;

	private final int lowTide;

	private final Supplier<? extends Queue<T>> queueSupplier;

	private final RequestMode requestMode;

	enum RequestMode {
		EAGER, LAZY,
	}

	FluxPrefetch(Flux<? extends T> source,
			int prefetch,
			int lowTide,
			Supplier<? extends Queue<T>> queueSupplier,
			RequestMode requestMode) {
		super(source);
		if (prefetch <= 0) {
			throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
		}
		this.prefetch = prefetch;
		this.lowTide = lowTide;
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
		this.requestMode = requestMode;
	}

	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.RUN_STYLE) {
			return Attr.RunStyle.ASYNC;
		}
		return super.scanUnsafe(key);
	}

	@Override
	public int getPrefetch() {
		return prefetch;
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		if (actual instanceof ConditionalSubscriber) {
			@SuppressWarnings("unchecked") ConditionalSubscriber<? super T> cs =
					(ConditionalSubscriber<? super T>) actual;
			source.subscribe(new PrefetchConditionalSubscriber<>(cs,
					prefetch,
					lowTide,
					queueSupplier,
					requestMode));
			return null;
		}

		return new PrefetchSubscriber<T>(actual,
				prefetch,
				lowTide,
				queueSupplier,
				requestMode);
	}

	static final class PrefetchSubscriber<T>
			implements QueueSubscription<T>, InnerOperator<T, T> {

		private final CoreSubscriber<? super T> actual;

		private final int prefetch;

		private final int limit;

		private final Supplier<? extends Queue<T>> queueSupplier;

		private final RequestMode requestMode;

		private Subscription s;

		private Queue<T> queue;

		private volatile boolean cancelled;

		private volatile boolean done;

		private Throwable error;

		volatile     int                                           wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<PrefetchSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(PrefetchSubscriber.class, "wip");

		volatile     long                                       requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<PrefetchSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(PrefetchSubscriber.class, "requested");

		private int upstreamFusionMode = -1;

		private boolean outputFused;

		private boolean firstRequest = true;

		private long produced;

		PrefetchSubscriber(CoreSubscriber<? super T> actual,
				int prefetch,
				int lowTide,
				Supplier<? extends Queue<T>> queueSupplier,
				RequestMode requestMode) {
			this.actual = actual;
			this.prefetch = prefetch;
			this.limit = Operators.unboundedOrLimit(prefetch, lowTide);
			this.queueSupplier = queueSupplier;
			this.requestMode = requestMode;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				WIP.lazySet(this, 1);

				actual.onSubscribe(this);

				if (cancelled) {
					terminate();
					return;
				}

				// upstreamFusionMode == -1 if downstream was not calling requestFusion
				if (upstreamFusionMode == -1) {
					if (s instanceof QueueSubscription) {
						@SuppressWarnings("unchecked") QueueSubscription<T> fusion =
								(QueueSubscription<T>) s;

						int mode = fusion.requestFusion(Fuseable.ANY);
						if (mode == Fuseable.SYNC) {
							upstreamFusionMode = Fuseable.SYNC;
							queue = fusion;
							done = true;

							if (wip == 1 && WIP.addAndGet(this, -1) == 0) {
								return;
							}

							drainSync();

							return;
						}
						if (mode == Fuseable.ASYNC) {
							upstreamFusionMode = Fuseable.ASYNC;
							queue = fusion;
							if (requestMode == RequestMode.EAGER) {
								s.request(Operators.unboundedOrPrefetch(prefetch));
							}

							if (wip == 1 && WIP.addAndGet(this, -1) == 0) {
								return;
							}

							firstRequest = false;
							s.request(Operators.unboundedOrPrefetch(prefetch));
							drainAsync();

							return;
						}
					}

					queue = queueSupplier.get();
					if (requestMode == RequestMode.EAGER) {
						s.request(Operators.unboundedOrPrefetch(prefetch));
					}

					if (wip == 1 && WIP.addAndGet(this, -1) == 0) {
						return;
					}

					firstRequest = false;
					s.request(Operators.unboundedOrPrefetch(prefetch));

					drainAsync();
				}
				else if (upstreamFusionMode != SYNC) {
					if (requestMode == RequestMode.EAGER) {
						s.request(Operators.unboundedOrPrefetch(prefetch));
					}
				}
			}
		}

		@Override
		public void onNext(T t) {
			if (upstreamFusionMode == ASYNC) {
				drain();
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
			drain();
		}

		@Override
		public void onError(Throwable err) {
			if (done) {
				Operators.onErrorDropped(err, actual.currentContext());
				return;
			}
			error = err;
			done = true;
			drain();
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			drain();
		}

		private void drain() {
			if (WIP.getAndIncrement(this) != 0) {
//				TODO: ask
				if (cancelled) {
					if (upstreamFusionMode == ASYNC) {
						// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
						queue.clear();
					}
					else {
						// discard given dataSignal since no more is enqueued (spec guarantees serialised onXXX calls)
						clear();
					}
				}
			}

			if (upstreamFusionMode != Fuseable.SYNC && requestMode == RequestMode.LAZY && firstRequest) {
				firstRequest = false;
				s.request(Operators.unboundedOrPrefetch(prefetch));
			}

			if (outputFused) {
				drainOutput();
			}
			else if (upstreamFusionMode == Fuseable.SYNC) {
				drainSync();
			}
			else {
				drainAsync();
			}
		}

		private void drainOutput() {
			int missed = 1;

			do {
				if (cancelled) {
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
			}
			while (missed != 0);
		}

		private void drainSync() {
			int missed = 1;

			final Queue<T> queue = this.queue;
			long e = produced;

			for (; ; ) {

				long r = requested;

				while (e != r) {
					T value;
					try {
						value = queue.poll();
					}
					catch (Throwable err) {
						actual.onError(Operators.onOperatorError(s,
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
						actual.onComplete();
						return;
					}

					actual.onNext(value);

					e++;
				}

				if (cancelled) {
					Operators.onDiscardQueueWithClear(queue,
							actual.currentContext(),
							null);
					return;
				}

				if (queue.isEmpty()) {
					actual.onComplete();
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

		private void drainAsync() {
			int missed = 1;

			final Queue<T> queue = this.queue;
			long e = produced;

			for (; ; ) {

				long r = requested;

				while (e != r) {
					T value;
					try {
						value = queue.poll();
					}
					catch (Throwable err) {
						Exceptions.throwIfFatal(err);
						s.cancel();
						terminate();

						actual.onError(Operators.onOperatorError(err,
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

					actual.onNext(value);

					e++;
					if (e == limit) {
						if (r != Long.MAX_VALUE) {
							r = REQUESTED.addAndGet(this, -e);
						}
						s.request(e);
						e = 0L;
					}
				}

				if (e == r && checkTerminated(done, queue.isEmpty(), null)) {
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

		private void terminate() {
			if (upstreamFusionMode == ASYNC) {
				// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
				queue.clear();
			}
			// TODO: What to do if fusionMode == SYNC?
			else if (!outputFused) {
				// discard MUST be happening only and only if there is no racing on elements consumption
				// which is guaranteed by the WIP guard here in case non-fused output
				Operators.onDiscardQueueWithClear(queue, actual.currentContext(), null);
			}
			// TODO: What to do if outputFused == true?
		}

		boolean checkTerminated(boolean done, boolean empty, @Nullable T value) {
			if (cancelled) {
				Operators.onDiscard(value, actual.currentContext());
				terminate();

				return true;
			}
			if (done) {
				Throwable e = error;
				if (e != null) {
					Operators.onDiscard(value, actual.currentContext());
					terminate();

					actual.onError(e);
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
		public T poll() {
			T value = queue.poll();
			if (value != null && upstreamFusionMode != SYNC) {
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

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
				drain();
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
				terminate();
			}
		}

		@Override
		public int requestFusion(int requestedMode) {
			if (s instanceof QueueSubscription) {
				@SuppressWarnings("unchecked") QueueSubscription<T> fusion =
						(QueueSubscription<T>) s;
				int mode = fusion.requestFusion(requestedMode);

				if (mode == Fuseable.SYNC) {
					upstreamFusionMode = Fuseable.SYNC;
					queue = fusion;
					outputFused = true;
					done = true;
				}
				else if (mode == Fuseable.ASYNC) {
					upstreamFusionMode = Fuseable.ASYNC;
					queue = fusion;
					outputFused = true;
				}
				else {
					queue = queueSupplier.get();
					upstreamFusionMode = Fuseable.NONE;

					if ((requestedMode & ASYNC) != 0) {
						outputFused = true;
						WIP.lazySet(this, 0);

						return ASYNC;
					}
					WIP.lazySet(this, 0);
					return NONE;
				}

				WIP.lazySet(this, 0);

				return mode;
			}
			else {
				upstreamFusionMode = Fuseable.NONE;
// TODO: HERE
				queue = queueSupplier.get();
				WIP.lazySet(this, 0);

				if ((requestedMode & ASYNC) != 0) {
					outputFused = true;
					return ASYNC;
				}

				return NONE;
			}
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) {
				return s;
			}
			if (key == Attr.CANCELLED) {
				return cancelled;
			}
			if (key == Attr.ERROR) {
				return error;
			}
			if (key == Attr.TERMINATED) {
				return done;
			}
			if (key == Attr.PREFETCH) {
				return prefetch;
			}
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) {
				return requested;
			}
			if (key == Attr.BUFFERED) {
				return queue != null ? queue.size() : 0;
			}
			if (key == Attr.RUN_STYLE) {
				return Attr.RunStyle.SYNC;
			}

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void clear() {
//			TODO: DISCARD_GUARD?
			Operators.onDiscardQueueWithClear(queue, actual.currentContext(), null);
		}

		@Override
		public int size() {
			return queue.size();
		}

		@Override
		public boolean isEmpty() {
			return queue.isEmpty();
		}
	}

	static final class PrefetchConditionalSubscriber<T>
			implements QueueSubscription<T>, InnerOperator<T, T> {

		private final ConditionalSubscriber<? super T> actual;

		private final int prefetch;

		private final int limit;

		private final Supplier<? extends Queue<T>> queueSupplier;

		private final RequestMode requestMode;

		private Subscription s;

		private Queue<T> queue;

		private volatile boolean cancelled;

		private volatile boolean done;

		private Throwable error;

		volatile     int                                                      wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<PrefetchConditionalSubscriber> WIP =
				AtomicIntegerFieldUpdater.newUpdater(PrefetchConditionalSubscriber.class,
						"wip");

		volatile     long                                                  requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<PrefetchConditionalSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(PrefetchConditionalSubscriber.class,
						"requested");

		private int fusionMode;

		private boolean outputFused;

		private boolean firstRequest = true;

		private long produced;

		private long consumed;

		PrefetchConditionalSubscriber(ConditionalSubscriber<? super T> actual,
				int prefetch,
				int lowTide,
				Supplier<? extends Queue<T>> queueSupplier,
				RequestMode requestMode) {
			this.actual = actual;
			this.prefetch = prefetch;
			this.limit = Operators.unboundedOrLimit(prefetch, lowTide);
			this.queueSupplier = queueSupplier;
			this.requestMode = requestMode;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return actual;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				if (s instanceof QueueSubscription) {
					@SuppressWarnings("unchecked") QueueSubscription<T> fusion =
							(QueueSubscription<T>) s;

					int mode = fusion.requestFusion(Fuseable.ANY);
					if (mode == Fuseable.SYNC) {
						fusionMode = Fuseable.SYNC;
						queue = fusion;
						done = true;

						actual.onSubscribe(this);
						return;
					}
					if (mode == Fuseable.ASYNC) {
						fusionMode = Fuseable.ASYNC;
						queue = fusion;
					}
				}
			}
			else {
				queue = queueSupplier.get();
			}

			actual.onSubscribe(this);
			if (requestMode == RequestMode.EAGER) {
				s.request(Operators.unboundedOrPrefetch(prefetch));
			}
		}

		@Override
		public void onNext(T t) {
			if (fusionMode == ASYNC) {
				drain();
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
			drain();
		}

		@Override
		public void onError(Throwable err) {
			if (done) {
				Operators.onErrorDropped(err, actual.currentContext());
				return;
			}
			error = err;
			done = true;
			drain();
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;
			drain();
		}

		private void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				if (cancelled) {
					terminate();
				}
				return;
			}

			if (requestMode == RequestMode.LAZY && firstRequest) {
				firstRequest = false;
				s.request(Operators.unboundedOrPrefetch(prefetch));
			}

			if (outputFused) {
				drainOutput();
			}
			else if (fusionMode == Fuseable.SYNC) {
				drainSync();
			}
			else {
				drainAsync();
			}
		}

		private void drainOutput() {
			int missed = 1;

			do {
				if (cancelled) {
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
			}
			while (missed != 0);
		}

		private void drainSync() {
			int missed = 1;

			final Queue<T> queue = this.queue;
			long emitted = produced;

			for (; ; ) {

				long r = requested;

				while (emitted != r) {
					T value;
					try {
						value = queue.poll();
					}
					catch (Throwable err) {
						actual.onError(Operators.onOperatorError(s,
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
						actual.onComplete();
						return;
					}

					if (actual.tryOnNext(value)) {
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
					actual.onComplete();
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

		private void drainAsync() {
			int missed = 1;

			final Queue<T> queue = this.queue;
			long emitted = produced;
			long polled = consumed;

			for (; ; ) {

				long r = requested;

				while (emitted != r) {
					T value;
					try {
						value = queue.poll();
					}
					catch (Throwable err) {
						Exceptions.throwIfFatal(err);
						s.cancel();
						terminate();

						actual.onError(Operators.onOperatorError(err,
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

					if (actual.tryOnNext(value)) {
						emitted++;
					}

					polled++;
					if (polled == limit) {
//						TODO: Does requested need to be updated?
//						if (r != Long.MAX_VALUE) {
//							r = REQUESTED.addAndGet(this, -polled);
//						}
						s.request(polled);
						polled = 0L;
					}
				}

				if (emitted == r && checkTerminated(done, queue.isEmpty(), null)) {
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

		private void terminate() {
			if (fusionMode == ASYNC) {
				// delegates discarding to the queue holder to ensure there is no racing on draining from the SpScQueue
				queue.clear();
			}
			// TODO: What to do if fusionMode == SYNC?
			else if (!outputFused) {
				// discard MUST be happening only and only if there is no racing on elements consumption
				// which is guaranteed by the WIP guard here in case non-fused output
				Operators.onDiscardQueueWithClear(queue, actual.currentContext(), null);
			}
			// TODO: What to do if outputFused == true?
		}

		boolean checkTerminated(boolean done, boolean empty, @Nullable T value) {
			if (cancelled) {
				Operators.onDiscard(value, actual.currentContext());
				terminate();

				return true;
			}
			if (done) {
				Throwable e = error;
				if (e != null) {
					Operators.onDiscard(value, actual.currentContext());
					terminate();

					actual.onError(e);
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
		public T poll() {
			T value = queue.poll();
			if (value != null && fusionMode != SYNC) {
				long polled = consumed + 1;
				if (polled == limit) {
					consumed = 0;
					s.request(polled);
				}
				else {
					consumed = polled;
				}
			}
			return value;
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(REQUESTED, this, n);
				drain();
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
				terminate();
			}
		}

		@Override
		public int requestFusion(int requestedMode) {
			if ((requestedMode & fusionMode) != 0) {
				outputFused = true;
				return fusionMode;
			}
			else if ((requestedMode & ASYNC) != 0) {
				outputFused = true;
				return ASYNC;
			}
			return NONE;
		}

		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) {
				return s;
			}
			if (key == Attr.CANCELLED) {
				return cancelled;
			}
			if (key == Attr.ERROR) {
				return error;
			}
			if (key == Attr.TERMINATED) {
				return done;
			}
			if (key == Attr.PREFETCH) {
				return prefetch;
			}
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) {
				return requested;
			}
			if (key == Attr.BUFFERED) {
				return queue != null ? queue.size() : 0;
			}
			if (key == Attr.RUN_STYLE) {
				return Attr.RunStyle.SYNC;
			}

			return InnerOperator.super.scanUnsafe(key);
		}

		@Override
		public void clear() {
//			TODO: DISCARD_GUARD?
			Operators.onDiscardQueueWithClear(queue, actual.currentContext(), null);
		}

		@Override
		public int size() {
			return queue.size();
		}

		@Override
		public boolean isEmpty() {
			return queue.isEmpty();
		}
	}
}


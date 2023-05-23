package reactor.core.publisher;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

public class FluxMapBlocking<IN, OUT> extends InternalFluxOperator<IN, OUT> {

	final Function<? super IN, ? extends OUT> mapper;
	final boolean                             delayError;
	final int                                 maxConcurrency;
	final Scheduler                           scheduler;

	/**
	 * Build a {@link InternalFluxOperator} wrapper around the passed parent {@link Publisher}
	 *
	 * @param source the {@link Publisher} to decorate
	 */
	protected FluxMapBlocking(Flux<? extends IN> source,
			Function<? super IN, ? extends OUT> mapper,
			boolean delayError,
			int maxConcurrency,
			Scheduler scheduler) {
		super(source);
		this.mapper = mapper;
		this.delayError = delayError;
		this.maxConcurrency = maxConcurrency;
		this.scheduler = scheduler;
	}

	@Override
	public CoreSubscriber<? super IN> subscribeOrReturn(CoreSubscriber<? super OUT> actual) throws Throwable {
		return new MapBlockingMain<>(actual, mapper, delayError, maxConcurrency, scheduler);
	}


	static class MapBlockingMain<T, R> extends FlatMapTracker<MapBlockingTask<T, R>>
			implements InnerOperator<T, R> {

		@SuppressWarnings("rawtypes")
		static final MapBlockingTask[] EMPTY = new MapBlockingTask[0];

		@SuppressWarnings("rawtypes")
		static final MapBlockingTask[] TERMINATED = new MapBlockingTask[0];

		final boolean                          delayError;
		final int                              maxConcurrency;
		final CoreSubscriber<? super R>        actual;
		final Function<? super T, ? extends R> mapper;
		final Scheduler                        scheduler;


		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<MapBlockingMain> WIP =
				AtomicIntegerFieldUpdater.newUpdater(MapBlockingMain.class, "wip");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<MapBlockingMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(MapBlockingMain.class, "requested");

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MapBlockingMain, Throwable> ERROR =
		AtomicReferenceFieldUpdater.newUpdater(MapBlockingMain.class, Throwable.class, "error");

		volatile boolean cancelled;


		boolean done;

		Subscription s;

		int lastIndex;

		MapBlockingMain(
				CoreSubscriber<? super R> actual,
				Function<? super T, ? extends R> mapper,
				boolean delayError,
				int maxConcurrency,
				Scheduler scheduler) {
			this.actual = actual;
			this.mapper = mapper;
			this.delayError = delayError;
			this.maxConcurrency = maxConcurrency;
			this.scheduler = scheduler;
		}

		@Override
		public CoreSubscriber<? super R> actual() {
			return this.actual;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;
				this.actual.onSubscribe(this);
				s.request(Operators.unboundedOrPrefetch(this.maxConcurrency));
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Operators.onNextDropped(t, actual.currentContext());
				return;
			}

			MapBlockingTask<T, R> task = new MapBlockingTask<>(this, mapper, t);
			if (add(task)) {
				Disposable d = scheduler.schedule(task);
				task.set(d);
			} else {
				Operators.onDiscard(t, actual.currentContext());
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, actual.currentContext());
				return;
			}
			if (Exceptions.addThrowable(ERROR, this, t)) {
				done = true;
				drain(null);
			}
			else {
				Operators.onErrorDropped(t, actual.currentContext());
			}
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
			Operators.addCap(REQUESTED, this, n);
			drain(null);
		}

		@Override
		public void cancel() {
			this.cancelled = true;
			drain(null);
		}


		void drain(@Nullable R dataSignal) {
			if (WIP.getAndIncrement(this) != 0) {
				if (dataSignal != null && cancelled) {
					Operators.onDiscard(dataSignal, actual.currentContext());
				}
				return;
			}
			drainLoop();
		}

		void drainLoop() {
			int missed = 1;

			final Subscriber<? super R> a = actual;
			long replenishMain = 0L;
			for (;;) {
				boolean d = done;

				MapBlockingTask<T, R>[] as = get();

				int n = as.length;

				boolean noSources = isEmpty();

				if (checkTerminated(d, noSources, a, null)) {
					return;
				}

				long r = requested;
				long e = 0L;
				if (r != 0L && !noSources) {

					int j = lastIndex;

					for (int i = 0; i < n; i++) {
						if (cancelled) {
							s.cancel();
							unsubscribe();
							return;
						}

						MapBlockingTask<T, R> inner = as[j];
						if (inner != null) {
							boolean done = inner.isDone();
							R result = inner.result;
							if (result == null && done) {
								remove(inner.index);
								replenishMain++;
							}
							else if (result != null) {
								remove(inner.index);
								replenishMain++;


								a.onNext(result);

								e++;

								if (r != Long.MAX_VALUE && e == r) {
									r = REQUESTED.addAndGet(this, -e);
								}
							}
						}

						if (r == 0L) {
							break;
						}

						if (++j == n) {
							j = 0;
						}
					}

					lastIndex = j;
				}

				if (r == 0L && !noSources) {
					as = get();
					n = as.length;

					for (int i = 0; i < n; i++) {
						if (cancelled) {
							s.cancel();
							unsubscribe();
							return;
						}

						MapBlockingTask<T, R> inner = as[i];
						if (inner == null) {
							continue;
						}

						d = inner.isDone();
						R result = inner.result;
						boolean empty = (result == null);

						// if we have a non-empty source then quit the cleanup
						if (!empty) {
							break;
						}

						if (d) {
							remove(inner.index);
							replenishMain++;
						}
					}
				}

				int m = wip;
				if (missed == m) {
					if (replenishMain != 0L && !done && !cancelled) {
						s.request(replenishMain);
						replenishMain = 0L;
					}

					missed = WIP.addAndGet(this, -missed);
					if (missed == 0) {
						break;
					}
				}
				else {
					missed = m;
				}
			}
		}

		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, @Nullable R value) {
			if (cancelled) {
				Context ctx = actual.currentContext();
				Operators.onDiscard(value, ctx);
				s.cancel();
				unsubscribe();

				return true;
			}

			if (delayError) {
				if (d && empty) {
					Throwable e = error;
					if (e != null && e != Exceptions.TERMINATED) {
						e = Exceptions.terminate(ERROR, this);
						a.onError(e);
					}
					else {
						a.onComplete();
					}

					return true;
				}
			}
			else {
				if (d) {
					Throwable e = error;
					if (e != null && e != Exceptions.TERMINATED) {
						e = Exceptions.terminate(ERROR, this);
						Context ctx = actual.currentContext();
						Operators.onDiscard(value, ctx);
						s.cancel();
						unsubscribe();

						a.onError(e);
						return true;
					}
					else if (empty) {
						a.onComplete();
						return true;
					}
				}
			}

			return false;
		}

		void innerError(Throwable e) {
			e = Operators.onNextInnerError(e, currentContext(), s);
			if (e != null) {
				if (Exceptions.addThrowable(ERROR, this, e)) {
					if (!delayError) {
						done = true;
					}
					drain(null);
				}
				else {
					Operators.onErrorDropped(e, actual.currentContext());
				}
			}
		}

		@Override
		MapBlockingTask<T, R>[] empty() {
			return EMPTY;
		}

		@Override
		MapBlockingTask<T, R>[] terminated() {
			return TERMINATED;
		}

		@Override
		MapBlockingTask<T, R>[] newArray(int size) {
			return new MapBlockingTask[size];
		}

		@Override
		void unsubscribeEntry(MapBlockingTask<T, R> entry) {
			entry.terminate();
		}

		@Override
		void setIndex(MapBlockingTask<T, R> entry, int index) {
			entry.index = index;
		}
	}


	static class MapBlockingTask<T, R> implements Runnable {

		static final Disposable DISPOSED = Disposables.disposed();
		static final Disposable TERMINATED = Disposables.disposed();

		final MapBlockingMain<?, R> parent;
		final Function<? super T, ? extends R> mapper;

		final T value;

		volatile Disposable disposable;
		@SuppressWarnings("rawtypes")
		static AtomicReferenceFieldUpdater<MapBlockingTask, Disposable> DISPOSABLE =
				AtomicReferenceFieldUpdater.newUpdater(MapBlockingTask.class, Disposable.class, "disposable");

		int index;

		R result;

		MapBlockingTask(MapBlockingMain<?, R> parent,
				Function<? super T, ? extends R> mapper,
				T value) {
			this.parent = parent;
			this.mapper = mapper;
			this.value = value;
		}

		@Override
		public void run() {
			R result;
			try {
			    result = Objects.requireNonNull(this.mapper.apply(this.value));
				this.result = result;
			} catch (Throwable t) {
				result = null;
				parent.innerError(t);
			}
			Disposable state = this.disposable;
			if (state != DISPOSED && DISPOSABLE.compareAndSet(this, state, TERMINATED)) {
				parent.drain(result);
			} else {
				Operators.onDiscard(result, parent.currentContext());
			}
		}

		void set(Disposable d) {
			if (disposable == null && DISPOSABLE.compareAndSet(this, null, d)) {
				return;
			}

			d.dispose();
		}

		void terminate() {
			Disposable old = DISPOSABLE.getAndSet(this, DISPOSED);
			if (old == null || old == DISPOSED) {
				return;
			}

			if (old == TERMINATED) {
				Operators.onDiscard(this.result, parent.currentContext());
				return;
			}

			old.dispose();

		}

		boolean isDone() {
			return this.disposable == TERMINATED;
		}
	}
}

//abstract class MapBlockingTracker<T> extends AtomicReferenceArray<T> {
//
//	final Queue<Integer> availableIndexes;
//	final Queue<Integer> readyIndexes;
//
//	public MapBlockingTracker(int length) {
//		super(length);
//	}
//
//	abstract T[] empty();
//
//	abstract T[] terminated();
//
//	abstract void unsubscribeEntry(T entry);
//
//	final void unsubscribe() {
//		T[] a;
//		T[] t = terminated();
//		synchronized (this) {
//			a = array;
//			if (a == t) {
//				return;
//			}
//			SIZE.lazySet(this, 0);
//			free = null;
//			array = t;
//		}
//		for (T e : a) {
//			if (e != null) {
//				unsubscribeEntry(e);
//			}
//		}
//	}
//
//	final T[] get() {
//		return array;
//	}
//
//	final boolean add(T entry) {
//		if (a == terminated()) {
//			return false;
//		}
//		synchronized (this) {
//			int idx = pollFree();
//			if (idx < 0) {
//				int n = a.length;
//				T[] b = n != 0 ? newArray(n << 1) : newArray(4);
//				System.arraycopy(a, 0, b, 0, n);
//
//				array = b;
//				a = b;
//
//				int m = b.length;
//				int[] u = new int[m];
//				for (int i = n + 1; i < m; i++) {
//					u[i] = i;
//				}
//				free = u;
//				consumerIndex = n + 1;
//				producerIndex = m;
//
//				idx = n;
//			}
//			setIndex(entry, idx);
//			SIZE.lazySet(this, size); // make sure entry is released
//			a[idx] = entry;
//			SIZE.lazySet(this, size + 1);
//		}
//		return true;
//	}
//
//	final void remove(int index) {
//		synchronized (this) {
//			T[] a = array;
//			if (a != terminated()) {
//				a[index] = null;
//				offerFree(index);
//				SIZE.lazySet(this, size - 1);
//			}
//		}
//	}
//
//	int pollFree() {
//		int[] a = free;
//		int m = a.length - 1;
//		long ci = consumerIndex;
//		if (producerIndex == ci) {
//			return -1;
//		}
//		int offset = (int) ci & m;
//		consumerIndex = ci + 1;
//		return a[offset];
//	}
//
//	void offerFree(int index) {
//		int[] a = free;
//		int m = a.length - 1;
//		long pi = producerIndex;
//		int offset = (int) pi & m;
//		a[offset] = index;
//		producerIndex = pi + 1;
//	}
//
//	final boolean isEmpty() {
//		return size == 0;
//	}
//}

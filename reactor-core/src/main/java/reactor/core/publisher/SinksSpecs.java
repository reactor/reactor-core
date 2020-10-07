package reactor.core.publisher;

import java.time.Duration;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;
import java.util.stream.Stream;

import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.publisher.Sinks.Emission;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

final class SinksSpecs {

	static final Sinks.RootSpec UNSAFE_ROOT_SPEC  = new RootSpecImpl(false);
	static final Sinks.RootSpec DEFAULT_ROOT_SPEC = new RootSpecImpl(true);
}

interface SinkWrapper<WRAPPED> {

	WRAPPED unwrap();

	ContextHolder contextHolder();
}

final class SerializedManySink<T> implements InternalManySink<T>, Scannable, SinkWrapper<Sinks.Many<T>> {

	final Many<T>       sink;
	final ContextHolder contextHolder;

	volatile int wip;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<SerializedManySink> WIP =
			AtomicIntegerFieldUpdater.newUpdater(SerializedManySink.class, "wip");

	volatile Thread lockedAt;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<SerializedManySink, Thread> LOCKED_AT =
			AtomicReferenceFieldUpdater.newUpdater(SerializedManySink.class, Thread.class, "lockedAt");

	SerializedManySink(Many<T> sink, ContextHolder contextHolder) {
		this.sink = sink;
		this.contextHolder = contextHolder;
	}

	@Override
	public int currentSubscriberCount() {
		return sink.currentSubscriberCount();
	}

	@Override
	public Flux<T> asFlux() {
		return sink.asFlux();
	}

	@Override
	public Many<T> unwrap() {
		return sink;
	}

	@Override
	public ContextHolder contextHolder() {
		return contextHolder;
	}

	@Override
	public Context currentContext() {
		return contextHolder.currentContext();
	}

	public boolean isCancelled() {
		return Scannable.from(sink).scanOrDefault(Attr.CANCELLED, false);
	}

	@Override
	public final Emission tryEmitComplete() {
		Thread currentThread = Thread.currentThread();
		if (!tryAcquire(currentThread)) {
			return Emission.FAIL_NON_SERIALIZED;
		}

		try {
			return sink.tryEmitComplete();
		}
		finally {
			if (WIP.decrementAndGet(this) == 0) {
				LOCKED_AT.compareAndSet(this, currentThread, null);
			}
		}
	}

	@Override
	public final Emission tryEmitError(Throwable t) {
		Objects.requireNonNull(t, "t is null in sink.error(t)");

		Thread currentThread = Thread.currentThread();
		if (!tryAcquire(currentThread)) {
			return Emission.FAIL_NON_SERIALIZED;
		}

		try {
			return sink.tryEmitError(t);
		}
		finally {
			if (WIP.decrementAndGet(this) == 0) {
				LOCKED_AT.compareAndSet(this, currentThread, null);
			}
		}
	}

	@Override
	public final Emission tryEmitNext(T t) {
		Objects.requireNonNull(t, "t is null in sink.next(t)");

		Thread currentThread = Thread.currentThread();
		if (!tryAcquire(currentThread)) {
			return Emission.FAIL_NON_SERIALIZED;
		}

		try {
			return sink.tryEmitNext(t);
		}
		finally {
			if (WIP.decrementAndGet(this) == 0) {
				LOCKED_AT.compareAndSet(this, currentThread, null);
			}
		}
	}

	private boolean tryAcquire(Thread currentThread) {
		if (WIP.get(this) == 0 && WIP.compareAndSet(this, 0, 1)) {
			// lazySet in thread A here is ok because:
			// 1. initial state is `null`
			// 2. `LOCKED_AT.get(this) != currentThread` from a different thread B could see outdated null or an outdated old thread
			// 3. but that old thread cannot be B: since we're in thread B, it must have executed the compareAndSet which would have loaded the update from A
			// 4. Seeing `null` or `C` is equivalent from seeing `A` from the perspective of the condition (`!= currentThread` is still true in all three cases)
			LOCKED_AT.lazySet(this, currentThread);
		}
		else {
			if (LOCKED_AT.get(this) != currentThread) {
				return false;
			}
			WIP.incrementAndGet(this);
		}
		return true;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		return sink.scanUnsafe(key);
	}

	@Override
	public Stream<? extends Scannable> inners() {
		return Scannable.from(sink).inners();
	}

	@Override
	public String toString() {
		return sink.toString();
	}
}

class RootSpecImpl implements Sinks.RootSpec,
                              Sinks.ManySpec,
                              Sinks.MulticastSpec,
                              Sinks.MulticastReplaySpec {

	final boolean serialized;
	final Sinks.UnicastSpec unicastSpec; //needed because UnicastSpec method names overlap with MulticastSpec

	RootSpecImpl(boolean serialized) {
		this.serialized = serialized;
		//there will only be as many instances of UnicastSpecImpl as there are RootSpecImpl instances (2)
		this.unicastSpec = new UnicastSpecImpl(serialized);
	}

	<T> Sinks.Empty<T> wrapEmpty(Sinks.Empty<T> original) {
		if (serialized) {
			//FIXME return wrapped
			return original;
		}
		return original;
	}

	<T> Sinks.One<T> wrapOne(Sinks.One<T> original) {
		if (serialized) {
			//FIXME return wrapped
			return original;
		}
		return original;
	}

	<T> Many<T> wrapMany(Many<T> original, Supplier<ContextView> contextViewSupplier) {
		if (serialized) {
			return new SerializedManySink<>(original, contextViewSupplier);
		}
		return original;
	}

	@Override
	public Sinks.ManySpec many() {
		return this;
	}

	@Override
	public <T> Sinks.Empty<T> empty() {
		return wrapEmpty(new SinkEmptyMulticast<>());
	}

	@Override
	public <T> Sinks.One<T> one() {
		return wrapOne(new NextProcessor<>(null));
	}

	@Override
	public Sinks.UnicastSpec unicast() {
		return this.unicastSpec;
	}

	@Override
	public Sinks.MulticastSpec multicast() {
		return this;
	}

	@Override
	public Sinks.MulticastReplaySpec replay() {
		return this;
	}

	@Override
	public <T> Many<T> onBackpressureBuffer() {
		@SuppressWarnings("deprecation")
		final EmitterProcessor<T> original = EmitterProcessor.create();
		return wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> onBackpressureBuffer(int bufferSize) {
		@SuppressWarnings("deprecation")
		final EmitterProcessor<T> original = EmitterProcessor.create(bufferSize);
		return wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> onBackpressureBuffer(int bufferSize, boolean autoCancel) {
		@SuppressWarnings("deprecation")
		final EmitterProcessor<T> original = EmitterProcessor.create(bufferSize, autoCancel);
		return wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> directAllOrNothing() {
		final SinkManyBestEffort<T> original = SinkManyBestEffort.createAllOrNothing();
		return wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> directBestEffort() {
		final SinkManyBestEffort<T> original = SinkManyBestEffort.createBestEffort();
		return wrapMany(original, original::currentContext);
	}


	@Override
	public <T> Many<T> all() {
		@SuppressWarnings("deprecation")
		final ReplayProcessor<T> original = ReplayProcessor.create();
		return wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> all(int batchSize) {
		@SuppressWarnings("deprecation")
		final ReplayProcessor<T> original = ReplayProcessor.create(batchSize, true);
		return wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> latest() {
		@SuppressWarnings("deprecation")
		final ReplayProcessor<T> original = ReplayProcessor.cacheLast();
		return wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> latestOrDefault(T value) {
		@SuppressWarnings("deprecation")
		final ReplayProcessor<T> original = ReplayProcessor.cacheLastOrDefault(value);
		return wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> limit(int historySize) {
		@SuppressWarnings("deprecation")
		final ReplayProcessor<T> original = ReplayProcessor.create(historySize);
		return wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> limit(Duration maxAge) {
		@SuppressWarnings("deprecation")
		final ReplayProcessor<T> original = ReplayProcessor.createTimeout(maxAge);
		return wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> limit(Duration maxAge, Scheduler scheduler) {
		@SuppressWarnings("deprecation")
		final ReplayProcessor<T> original = ReplayProcessor.createTimeout(maxAge, scheduler);
		return wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> limit(int historySize, Duration maxAge) {
		@SuppressWarnings("deprecation")
		final ReplayProcessor<T> original = ReplayProcessor.createSizeAndTimeout(historySize, maxAge);
		return wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> limit(int historySize, Duration maxAge, Scheduler scheduler) {
		@SuppressWarnings("deprecation")
		final ReplayProcessor<T> original = ReplayProcessor.createSizeAndTimeout(historySize, maxAge, scheduler);
		return wrapMany(original, original::currentContext);
	}
}

class UnicastSpecImpl implements Sinks.UnicastSpec {

	final boolean serialized;

	UnicastSpecImpl(boolean serialized) {
		this.serialized = serialized;
	}

	<T> Many<T> wrapMany(Many<T> original, Supplier<ContextView> contextViewSupplier) {
		if (serialized) {
			return new SerializedManySink<>(original, contextViewSupplier);
		}
		return original;
	}

	@Override
	public <T> Many<T> onBackpressureBuffer() {
		@SuppressWarnings("deprecation")
		final UnicastProcessor<T> original = UnicastProcessor.create();
		return wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> onBackpressureBuffer(Queue<T> queue) {
		@SuppressWarnings("deprecation")
		final UnicastProcessor<T> original = UnicastProcessor.create(queue);
		return wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> onBackpressureBuffer(Queue<T> queue, Disposable endCallback) {
		@SuppressWarnings("deprecation")
		final UnicastProcessor<T> original = UnicastProcessor.create(queue, endCallback);
		return wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> onBackpressureError() {
		final UnicastManySinkNoBackpressure<T> original = UnicastManySinkNoBackpressure.create();
		return wrapMany(original, original::currentContext);
	}
}

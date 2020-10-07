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

	static final SinksWrapper WRAPPER_UNSAFE                                       = new SinksWrapper() {

		@Override
		public <T> Sinks.Empty<T> wrapEmpty(Sinks.Empty<T> original, Supplier<ContextView> contextSupplier) {
			return original;
		}

		@Override
		public <T> Many<T> wrapMany(Sinks.Many<T> original, Supplier<ContextView> contextViewSupplier) {
			return original;
		}

		@Override
		public <T> Sinks.One<T> wrapOne(Sinks.One<T> original, Supplier<ContextView> contextViewSupplier) {
			return original;
		}

	};

	static final SinksWrapper WRAPPER_SERIALIZED_FAIL_FAST = new SinksWrapper() {

		@Override
		public <T> Many<T> wrapMany(Many<T> original, Supplier<ContextView> contextViewSupplier) {
			return new SerializedManySink<>(original, contextViewSupplier);
		}

		@Override
		public <T> Sinks.One<T> wrapOne(Sinks.One<T> original, Supplier<ContextView> contextViewSupplier) {
			return original; //FIXME
		}

		@Override
		public <T> Sinks.Empty<T> wrapEmpty(Sinks.Empty<T> original, Supplier<ContextView> contextViewSupplier) {
			return original; //FIXME
		}
	};

	static final Sinks.RootSpec UNSAFE_ROOT_SPEC                            = new RootWrappingSpec(WRAPPER_UNSAFE);
	static final Sinks.RootSpec DEFAULT_ROOT_SPEC                           = new RootWrappingSpec(WRAPPER_SERIALIZED_FAIL_FAST);

	static final Sinks.ManySpec UNSAFE_MANY_SPEC                            = new ManySpecImpl2(WRAPPER_UNSAFE);
	static final Sinks.UnicastSpec UNSAFE_UNICAST_SPEC                      = new UnicastSpecImpl2(WRAPPER_UNSAFE);
	static final Sinks.MulticastSpec UNSAFE_MULTICAST_SPEC                  = new MulticastSpecImpl2(WRAPPER_UNSAFE);
	static final Sinks.MulticastReplaySpec UNSAFE_MULTICAST_REPLAY_SPEC     = new MulticastReplaySpecImpl2(WRAPPER_UNSAFE);

	static final Sinks.ManySpec SERIALIZED_MANY_SPEC                        = new ManySpecImpl2(WRAPPER_SERIALIZED_FAIL_FAST);
	static final Sinks.UnicastSpec SERIALIZED_UNICAST_SPEC                  = new UnicastSpecImpl2(WRAPPER_SERIALIZED_FAIL_FAST);
	static final Sinks.MulticastSpec SERIALIZED_MULTICAST_SPEC              = new MulticastSpecImpl2(WRAPPER_SERIALIZED_FAIL_FAST);
	static final Sinks.MulticastReplaySpec SERIALIZED_MULTICAST_REPLAY_SPEC = new MulticastReplaySpecImpl2(WRAPPER_SERIALIZED_FAIL_FAST);

}

final class SerializedManySink<T> implements InternalManySink<T>, Scannable {

	final Many<T>               sink;
	final Supplier<ContextView> contextHolder;

	volatile int wip;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<SerializedManySink> WIP =
			AtomicIntegerFieldUpdater.newUpdater(SerializedManySink.class, "wip");

	volatile Thread lockedAt;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<SerializedManySink, Thread> LOCKED_AT =
			AtomicReferenceFieldUpdater.newUpdater(SerializedManySink.class, Thread.class, "lockedAt");

	SerializedManySink(Many<T> sink, Supplier<ContextView> contextHolder) {
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
	public Context currentContext() {
		ContextView contextView = contextHolder.get();
		return Context.of(contextView);
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

interface SinksWrapper {

	<T> Sinks.Many<T> wrapMany(Sinks.Many<T> original, Supplier<ContextView> contextSupplier);
	<T> Sinks.One<T> wrapOne(Sinks.One<T> original, Supplier<ContextView> contextSupplier);
	<T> Sinks.Empty<T> wrapEmpty(Sinks.Empty<T> original, Supplier<ContextView> contextSupplier);
}

class RootWrappingSpec implements Sinks.RootSpec {

	final SinksWrapper wrapper;
	final Sinks.ManySpec manySpec;

	RootWrappingSpec(SinksWrapper wrapper) {
		this.wrapper = wrapper;
		if (wrapper == SinksSpecs.WRAPPER_UNSAFE) {
			this.manySpec = SinksSpecs.UNSAFE_MANY_SPEC;
		}
		else if (wrapper == SinksSpecs.WRAPPER_SERIALIZED_FAIL_FAST) {
			this.manySpec = SinksSpecs.SERIALIZED_MANY_SPEC;
		}
		else {
			this.manySpec = new ManySpecImpl2(wrapper);
		}
	}

	@Override
	public <T> Sinks.Empty<T> empty() {
		final SinkEmptyMulticast<T> original = new SinkEmptyMulticast<>();
		return wrapper.wrapEmpty(original, original::currentContext);
	}

	@Override
	public <T> Sinks.One<T> one() {
		final NextProcessor<T> original = new NextProcessor<>(null);
		return wrapper.wrapOne(original, original::currentContext);
	}

	@Override
	public Sinks.ManySpec many() {
		return this.manySpec;
	}
}

class ManySpecImpl2 implements Sinks.ManySpec {

	final SinksWrapper              wrapper;
	final Sinks.UnicastSpec         unicast;
	final Sinks.MulticastSpec       multicast;
	final Sinks.MulticastReplaySpec multicastReplay;

	ManySpecImpl2(SinksWrapper wrapper) {
		this.wrapper = wrapper;
		if (wrapper == SinksSpecs.WRAPPER_UNSAFE) {
			this.unicast = SinksSpecs.UNSAFE_UNICAST_SPEC;
			this.multicast = SinksSpecs.UNSAFE_MULTICAST_SPEC;
			this.multicastReplay = SinksSpecs.UNSAFE_MULTICAST_REPLAY_SPEC;
		}
		else if (wrapper == SinksSpecs.WRAPPER_SERIALIZED_FAIL_FAST) {
			this.unicast = SinksSpecs.SERIALIZED_UNICAST_SPEC;
			this.multicast = SinksSpecs.SERIALIZED_MULTICAST_SPEC;
			this.multicastReplay = SinksSpecs.SERIALIZED_MULTICAST_REPLAY_SPEC;
		}
		else {
			this.unicast = new UnicastSpecImpl2(wrapper);
			this.multicast = new MulticastSpecImpl2(wrapper);
			this.multicastReplay = new MulticastReplaySpecImpl2(wrapper);
		}
	}

	@Override
	public Sinks.UnicastSpec unicast() {
		return this.unicast;
	}

	@Override
	public Sinks.MulticastSpec multicast() {
		return this.multicast;
	}

	@Override
	public Sinks.MulticastReplaySpec replay() {
		return this.multicastReplay;
	}
}

@SuppressWarnings("deprecation")
class UnicastSpecImpl2 implements Sinks.UnicastSpec {

	final SinksWrapper wrapper;

	UnicastSpecImpl2(SinksWrapper wrapper) {
		this.wrapper = wrapper;
	}

	@Override
	public <T> Many<T> onBackpressureBuffer() {
		final UnicastProcessor<T> original = UnicastProcessor.create();
		return wrapper.wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> onBackpressureBuffer(Queue<T> queue) {
		final UnicastProcessor<T> original = UnicastProcessor.create(queue);
		return wrapper.wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> onBackpressureBuffer(Queue<T> queue, Disposable endCallback) {
		final UnicastProcessor<T> original = UnicastProcessor.create(queue, endCallback);
		return wrapper.wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> onBackpressureError() {
		final UnicastManySinkNoBackpressure<T> original =
				UnicastManySinkNoBackpressure.create();
		return wrapper.wrapMany(original, original::currentContext);
	}
}

@SuppressWarnings("deprecation")
class MulticastSpecImpl2 implements Sinks.MulticastSpec {

	final SinksWrapper wrapper;

	MulticastSpecImpl2(SinksWrapper wrapper) {
		this.wrapper = wrapper;
	}

	@Override
	public <T> Many<T> onBackpressureBuffer() {
		final EmitterProcessor<T> original = EmitterProcessor.create();
		return wrapper.wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> onBackpressureBuffer(int bufferSize) {
		final EmitterProcessor<T> original = EmitterProcessor.create(bufferSize);
		return wrapper.wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> onBackpressureBuffer(int bufferSize, boolean autoCancel) {
		final EmitterProcessor<T> original = EmitterProcessor.create(bufferSize, autoCancel);
		return wrapper.wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> directAllOrNothing() {
		final SinkManyBestEffort<T> original = SinkManyBestEffort.createAllOrNothing();
		return wrapper.wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> directBestEffort() {
		final SinkManyBestEffort<T> original = SinkManyBestEffort.createBestEffort();
		return wrapper.wrapMany(original, original::currentContext);
	}
}

@SuppressWarnings("deprecation")
class MulticastReplaySpecImpl2 implements Sinks.MulticastReplaySpec {

	final SinksWrapper wrapper;

	MulticastReplaySpecImpl2(SinksWrapper wrapper) {
		this.wrapper = wrapper;
	}

	@Override
	public <T> Many<T> all() {
		final ReplayProcessor<T> original = ReplayProcessor.create();
		return wrapper.wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> all(int batchSize) {
		final ReplayProcessor<T> original = ReplayProcessor.create(batchSize, true);
		return wrapper.wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> latest() {
		final ReplayProcessor<T> original = ReplayProcessor.cacheLast();
		return wrapper.wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> latestOrDefault(T value) {
		final ReplayProcessor<T> original = ReplayProcessor.cacheLastOrDefault(value);
		return wrapper.wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> limit(int historySize) {
		final ReplayProcessor<T> original = ReplayProcessor.create(historySize);
		return wrapper.wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> limit(Duration maxAge) {
		final ReplayProcessor<T> original = ReplayProcessor.createTimeout(maxAge);
		return wrapper.wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> limit(Duration maxAge, Scheduler scheduler) {
		final ReplayProcessor<T> original =
				ReplayProcessor.createTimeout(maxAge, scheduler);
		return wrapper.wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> limit(int historySize, Duration maxAge) {
		final ReplayProcessor<T> original =
				ReplayProcessor.createSizeAndTimeout(historySize, maxAge);
		return wrapper.wrapMany(original, original::currentContext);
	}

	@Override
	public <T> Many<T> limit(int historySize, Duration maxAge, Scheduler scheduler) {
		final ReplayProcessor<T> original = ReplayProcessor.createSizeAndTimeout(historySize, maxAge, scheduler);
		return wrapper.wrapMany(original, original::currentContext);
	}
}

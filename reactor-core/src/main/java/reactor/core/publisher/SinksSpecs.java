package reactor.core.publisher;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import reactor.core.Disposable;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Scheduler;

final class SinksSpecs {
	static final ManySpecImpl            MANY_SPEC                    = new ManySpecImpl();
	static final UnicastSpecImpl         UNICAST_SPEC                 = new UnicastSpecImpl(true);
	static final MulticastSpecImpl       MULTICAST_SPEC               = new MulticastSpecImpl(true);
	static final MulticastReplaySpecImpl MULTICAST_REPLAY_SPEC        = new MulticastReplaySpecImpl(true);
	static final UnsafeManySpecImpl      UNSAFE_MANY_SPEC             = new UnsafeManySpecImpl();
	static final UnicastSpecImpl         UNSAFE_UNICAST_SPEC          = new UnicastSpecImpl(false);
	static final MulticastSpecImpl       UNSAFE_MULTICAST_SPEC        = new MulticastSpecImpl(false);
	static final MulticastReplaySpecImpl UNSAFE_MULTICAST_REPLAY_SPEC = new MulticastReplaySpecImpl(false);

}

abstract class SerializedSink {

	volatile int wip;
	static final AtomicIntegerFieldUpdater<SerializedSink> WIP =
			AtomicIntegerFieldUpdater.newUpdater(SerializedSink.class, "wip");

	volatile Thread lockedAt;
	static final AtomicReferenceFieldUpdater<SerializedSink, Thread> LOCKED_AT =
			AtomicReferenceFieldUpdater.newUpdater(SerializedSink.class, Thread.class, "lockedAt");

	boolean tryAcquire(Thread currentThread) {
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
}

abstract class SinkSpecImpl {
	final boolean serialized;

	SinkSpecImpl(boolean serialized) {
		this.serialized = serialized;
	}

	final <T, SINKPROC extends Many<T> & ContextHolder> Many<T> toSerializedSink(SINKPROC sink) {
		if (serialized) {
			return new SinkManySerialized<T>(sink, sink);
		}
		return sink;
	}
}

final class ManySpecImpl implements Sinks.ManySpec {

	@Override
	public Sinks.UnicastSpec unicast() {
		return SinksSpecs.UNICAST_SPEC;
	}

	@Override
	public Sinks.MulticastSpec multicast() {
		return SinksSpecs.MULTICAST_SPEC;
	}

	@Override
	public Sinks.MulticastReplaySpec replay() {
		return SinksSpecs.MULTICAST_REPLAY_SPEC;
	}

	@Override
	public Sinks.ManySpec unsafe() {
		return SinksSpecs.UNSAFE_MANY_SPEC;
	}
}

final class UnsafeManySpecImpl implements Sinks.ManySpec {

	@Override
	public Sinks.UnicastSpec unicast() {
		return SinksSpecs.UNSAFE_UNICAST_SPEC;
	}

	@Override
	public Sinks.MulticastSpec multicast() {
		return SinksSpecs.UNSAFE_MULTICAST_SPEC;
	}

	@Override
	public Sinks.MulticastReplaySpec replay() {
		return SinksSpecs.UNSAFE_MULTICAST_REPLAY_SPEC;
	}

	@Override
	public Sinks.ManySpec unsafe() {
		return SinksSpecs.UNSAFE_MANY_SPEC;
	}
}

@SuppressWarnings("deprecation")
final class UnicastSpecImpl extends SinkSpecImpl implements Sinks.UnicastSpec {
	UnicastSpecImpl(boolean serialized) {
		super(serialized);
	}

	@Override
	public <T> Many<T> onBackpressureBuffer() {
		return toSerializedSink(UnicastProcessor.create());
	}

	@Override
	public <T> Many<T> onBackpressureBuffer(Queue<T> queue) {
		return toSerializedSink(UnicastProcessor.create(queue));
	}

	@Override
	public <T> Many<T> onBackpressureBuffer(Queue<T> queue, Disposable endCallback) {
		return toSerializedSink(UnicastProcessor.create(queue, endCallback));
	}

	@Override
	public <T> Many<T> onBackpressureError() {
		return toSerializedSink(UnicastManySinkNoBackpressure.create());
	}
}

@SuppressWarnings("deprecation")
final class MulticastSpecImpl extends SinkSpecImpl implements Sinks.MulticastSpec {
	MulticastSpecImpl(boolean serialized) {
		super(serialized);
	}

	@Override
	public <T> Many<T> onBackpressureBuffer() {
		return toSerializedSink(EmitterProcessor.create());
	}

	@Override
	public <T> Many<T> onBackpressureBuffer(int bufferSize) {
		return toSerializedSink(EmitterProcessor.create(bufferSize));
	}

	@Override
	public <T> Many<T> onBackpressureBuffer(int bufferSize, boolean autoCancel) {
		return toSerializedSink(EmitterProcessor.create(bufferSize, autoCancel));
	}

	@Override
	public <T> Many<T> directAllOrNothing() {
		return toSerializedSink(SinkManyBestEffort.createAllOrNothing());
	}

	@Override
	public <T> Many<T> directBestEffort() {
		return toSerializedSink(SinkManyBestEffort.createBestEffort());
	}
}

@SuppressWarnings("deprecation")
final class MulticastReplaySpecImpl extends SinkSpecImpl implements Sinks.MulticastReplaySpec {
	MulticastReplaySpecImpl(boolean serialized) {
		super(serialized);
	}

	@Override
	public <T> Many<T> all() {
		return toSerializedSink(ReplayProcessor.create());
	}

	@Override
	public <T> Many<T> all(int batchSize) {
		return toSerializedSink(ReplayProcessor.create(batchSize, true));
	}

	@Override
	public <T> Many<T> latest() {
		return toSerializedSink(ReplayProcessor.cacheLast());
	}

	@Override
	public <T> Many<T> latestOrDefault(T value) {
		return toSerializedSink(ReplayProcessor.cacheLastOrDefault(value));
	}

	@Override
	public <T> Many<T> limit(int historySize) {
		return toSerializedSink(ReplayProcessor.create(historySize));
	}

	@Override
	public <T> Many<T> limit(Duration maxAge) {
		return toSerializedSink(ReplayProcessor.createTimeout(maxAge));
	}

	@Override
	public <T> Many<T> limit(Duration maxAge, Scheduler scheduler) {
		return toSerializedSink(ReplayProcessor.createTimeout(maxAge, scheduler));
	}

	@Override
	public <T> Many<T> limit(int historySize, Duration maxAge) {
		return toSerializedSink(ReplayProcessor.createSizeAndTimeout(historySize, maxAge));
	}

	@Override
	public <T> Many<T> limit(int historySize, Duration maxAge, Scheduler scheduler) {
		return toSerializedSink(ReplayProcessor.createSizeAndTimeout(historySize, maxAge, scheduler));
	}
}

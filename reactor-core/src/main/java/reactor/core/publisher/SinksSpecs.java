package reactor.core.publisher;

import java.time.Duration;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Subscription;

import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Sinks.Emission;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

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

final class SerializedManySink<T> implements Many<T>, Scannable {

	final Many<T>       sink;
	final ContextHolder contextHolder;

	volatile     Throwable                                                  error;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<SerializedManySink, Throwable> ERROR =
			AtomicReferenceFieldUpdater.newUpdater(SerializedManySink.class, Throwable.class, "error");

	volatile Thread lockedAt;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<SerializedManySink, Thread> LOCKED_AT =
			AtomicReferenceFieldUpdater.newUpdater(SerializedManySink.class, Thread.class, "lockedAt");

	volatile boolean done;

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

	Context currentContext() {
		return contextHolder.currentContext();
	}

	public boolean isCancelled() {
		return Scannable.from(sink).scanOrDefault(Attr.CANCELLED, false);
	}

	@Override
	public void emitComplete() {
		//no particular error condition handling for onComplete
		@SuppressWarnings("unused")
		Emission emission = tryEmitComplete();
	}

	@Override
	public final Emission tryEmitComplete() {
		if (done) {
			return Sinks.Emission.FAIL_TERMINATED;
		}
		Thread lockedAt = this.lockedAt;
		if (!(lockedAt == null || lockedAt == Thread.currentThread())) {
			return Emission.FAIL_NON_SERIALIZED;
		}

		done = true;
		return sink.tryEmitComplete();
	}

	@Override
	public void emitError(Throwable error) {
		Emission result = tryEmitError(error);
		switch (result) {
			case FAIL_TERMINATED:
			case FAIL_NON_SERIALIZED:
				Operators.onErrorDropped(error, currentContext());
				break;
		}
	}

	@Override
	public final Emission tryEmitError(Throwable t) {
		Objects.requireNonNull(t, "t is null in sink.error(t)");
		if (done) {
			return Sinks.Emission.FAIL_TERMINATED;
		}
		Thread lockedAt = this.lockedAt;
		if (!(lockedAt == null || lockedAt == Thread.currentThread())) {
			return Emission.FAIL_NON_SERIALIZED;
		}
		if (!Exceptions.addThrowable(ERROR, this, t)) {
			return Emission.FAIL_TERMINATED;
		}

		done = true;
		return sink.tryEmitError(t);
	}

	@Override
	public void emitNext(T value) {
		switch (tryEmitNext(value)) {
			case FAIL_ZERO_SUBSCRIBER:
				//we want to "discard" without rendering the sink terminated.
				// effectively NO-OP cause there's no subscriber, so no context :(
				break;
			case FAIL_OVERFLOW: {
				Context ctx = currentContext();
				IllegalStateException overflow = Exceptions.failWithOverflow("Backpressure overflow during Sinks.Many#emitNext");

				Subscription s = sink instanceof Subscription ? (Subscription) sink : null;
				Throwable ex = Operators.onOperatorError(s, overflow, value, ctx);
				//the emitError will onErrorDropped if already terminated
				emitError(ex);
				Operators.onDiscard(value, ctx);
				break;
			}
			case FAIL_CANCELLED:
				Operators.onDiscard(value, currentContext());
				break;
			case FAIL_TERMINATED:
				Operators.onNextDropped(value, currentContext());
				break;
			case FAIL_NON_SERIALIZED: {
				Context ctx = currentContext();
				IllegalStateException overflow = new IllegalStateException(
						"Spec. Rule 1.3 - onSubscribe, onNext, onError and onComplete signaled to a Subscriber MUST be signaled serially."
				);

				Subscription s = sink instanceof Subscription ? (Subscription) sink : null;
				Throwable ex = Operators.onOperatorError(s, overflow, value, ctx);
				//the emitError will onErrorDropped if already terminated
				emitError(ex);
				Operators.onDiscard(value, currentContext());
				break;
			}
			case OK:
				break;
		}
	}

	@Override
	public final Emission tryEmitNext(T t) {
		Objects.requireNonNull(t, "t is null in sink.next(t)");
		if (done) {
			return Sinks.Emission.FAIL_TERMINATED;
		}
		Thread currentThread = Thread.currentThread();
		Thread lockedAt = LOCKED_AT.get(this);
		if (lockedAt != null) {
			if (lockedAt != currentThread) {
				return Emission.FAIL_NON_SERIALIZED;
			}
		}
		else if (!LOCKED_AT.compareAndSet(this, null, currentThread)) {
			return Emission.FAIL_NON_SERIALIZED;
		}

		Emission emission = sink.tryEmitNext(t);
		LOCKED_AT.compareAndSet(this, currentThread, null);
		return emission;
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.ERROR) {
			return error;
		}
		if (key == Attr.TERMINATED) {
			return done;
		}

		return Scannable.from(sink).scanUnsafe(key);
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

abstract class SinkSpecImpl {
	final boolean serialized;

	SinkSpecImpl(boolean serialized) {
		this.serialized = serialized;
	}

	final <T, SINKPROC extends Many<T> & ContextHolder> Many<T> toSerializedSink(SINKPROC sink) {
		if (serialized) {
			return new SerializedManySink<T>(sink, sink);
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
	public <T> Many<T> onBackpressureDropForAll() {
		return toSerializedSink(SinkManyBestEffort.createAllOrNothing());
	}

	@Override
	public <T> Many<T> onBackpressureDropForSlow() {
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
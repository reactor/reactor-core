package reactor.core.publisher;

import java.time.Duration;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Stream;

import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Sinks.Emission;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
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

	final Many<T>           sink;
	final CoreSubscriber<T> contextHolder;

	volatile     Throwable                                                  error;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<SerializedManySink, Throwable> ERROR =
			AtomicReferenceFieldUpdater.newUpdater(SerializedManySink.class, Throwable.class, "error");

	volatile     int                                           wip;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<SerializedManySink> WIP =
			AtomicIntegerFieldUpdater.newUpdater(SerializedManySink.class, "wip");

	final Queue<T> mpscQueue;

	volatile boolean done;

	SerializedManySink(Many<T> sink, CoreSubscriber<T> contextHolder) {
		this.sink = sink;
		this.mpscQueue = Queues.<T>unboundedMultiproducer().get();
		this.contextHolder = contextHolder;
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
		done = true;
		drain();
		return Sinks.Emission.OK;
	}

	@Override
	public void emitError(Throwable error) {
		Emission result = tryEmitError(error);
		if (result == Emission.FAIL_TERMINATED) {
			Operators.onErrorDropped(error, currentContext());
		}
	}

	@Override
	public final Emission tryEmitError(Throwable t) {
		Objects.requireNonNull(t, "t is null in sink.error(t)");
		if (done) {
			return Sinks.Emission.FAIL_TERMINATED;
		}
		if (Exceptions.addThrowable(ERROR, this, t)) {
			done = true;
			drain();
			return Sinks.Emission.OK;
		}

		Context ctx = currentContext();
		//we do deal with the queue inside tryEmitError, but the throwable t is left up to the user
		Operators.onDiscardQueueWithClear(mpscQueue, ctx, null);
		return Sinks.Emission.FAIL_TERMINATED;
	}

	@Override
	public void emitNext(T value) {
		switch(tryEmitNext(value)) {
			case FAIL_OVERFLOW:
				Operators.onDiscard(value, currentContext());
				//the emitError will onErrorDropped if already terminated
				emitError(Exceptions.failWithOverflow("Backpressure overflow during Sinks.Many#emitNext"));
				break;
			case FAIL_CANCELLED:
				Operators.onDiscard(value, currentContext());
				break;
			case FAIL_TERMINATED:
				Operators.onNextDropped(value, currentContext());
				break;
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
		if (WIP.get(this) == 0 && WIP.compareAndSet(this, 0, 1)) {
			Emission emission;
			try {
				emission = sink.tryEmitNext(t);
			}
			catch (Throwable ex) {
				Subscription s = sink instanceof Subscription ? (Subscription) sink : null;
				ex = Operators.onOperatorError(s, ex, t, currentContext());
				tryEmitError(ex);
				//should use sink.dispose
				return Sinks.Emission.FAIL_TERMINATED;
			}
			if (WIP.decrementAndGet(this) == 0) {
				return emission;
			}
		}
		else {
			this.mpscQueue.offer(t);
			if (WIP.getAndIncrement(this) != 0) {
				return Sinks.Emission.OK;
			}
		}
		drainLoop();
		return Sinks.Emission.OK;
	}

	//impl note: don't use sink.isTerminated() in the drain loop,
	//it needs to separately check its own `done` status before calling the base sink
	//complete()/error() methods (which do flip the isTerminated), otherwise it could
	//bypass the terminate handler (in buffer and latest variants notably).
	final void drain() {
		if (WIP.getAndIncrement(this) == 0) {
			drainLoop();
		}
	}

	final void drainLoop() {
		Many<T> e = sink;
		Queue<T> q = mpscQueue;
		for (; ; ) {

			for (; ; ) {
				if (isCancelled()) {
					Operators.onDiscardQueueWithClear(q, currentContext(), null);
					if (WIP.decrementAndGet(this) != 0) {
						continue;
					}
					else {
						return;
					}
				}
				if (ERROR.get(this) != null) {
					Operators.onDiscardQueueWithClear(q, currentContext(), null);
					//noinspection ConstantConditions
					e.emitError(Exceptions.terminate(ERROR, this));
					return;
				}

				boolean d = done;
				T v = q.poll();

				boolean empty = v == null;

				if (d && empty) {
					e.emitComplete();
					return;
				}

				if (empty) {
					break;
				}

				try {
					e.emitNext(v);
				}
				catch (Throwable ex) {
					ex = Operators.onOperatorError(null, ex, v, currentContext());
					emitError(ex);
					break;
				}
			}

			if (WIP.decrementAndGet(this) == 0) {
				break;
			}
		}
	}

	@Override
	@Nullable
	public Object scanUnsafe(Attr key) {
		if (key == Attr.BUFFERED) {
			return mpscQueue.size();
		}
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

	final <T, SINKPROC extends Many<T> & CoreSubscriber<T>> Many<T> toSerializedSink(SINKPROC sink) {
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
	public <T> Many<T> onBackpressureError() {
		return toSerializedSink(DirectProcessor.create());
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
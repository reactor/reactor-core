package reactor.core.publisher;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import reactor.core.Disposable;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.publisher.Sinks.Many;
import reactor.core.publisher.Sinks.One;
import reactor.core.scheduler.Scheduler;

final class SinksSpecs {

	static final Sinks.RootSpec UNSAFE_ROOT_SPEC  = new RootSpecImpl(false);
	static final Sinks.RootSpec DEFAULT_ROOT_SPEC = new RootSpecImpl(true);

	abstract static class AbstractSerializedSink {

		volatile int                                                   wip;
		static final AtomicIntegerFieldUpdater<AbstractSerializedSink> WIP =
				AtomicIntegerFieldUpdater.newUpdater(AbstractSerializedSink.class, "wip");

		volatile Thread                                                          lockedAt;
		static final AtomicReferenceFieldUpdater<AbstractSerializedSink, Thread> LOCKED_AT =
				AtomicReferenceFieldUpdater.newUpdater(AbstractSerializedSink.class, Thread.class, "lockedAt");

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

	static final class RootSpecImpl implements Sinks.RootSpec,
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

		<T, EMPTY extends Empty<T> & ContextHolder> Empty<T> wrapEmpty(EMPTY original) {
			if (serialized) {
				return new SinkEmptySerialized<>(original, original);
			}
			return original;
		}

		<T, ONE extends One<T> & ContextHolder> One<T> wrapOne(ONE original) {
			if (serialized) {
				return new SinkOneSerialized<>(original, original);
			}
			return original;
		}

		<T, MANY extends Many<T> & ContextHolder> Many<T> wrapMany(MANY original) {
			if (serialized) {
				return new SinkManySerialized<>(original, original);
			}
			return original;
		}

		@Override
		public Sinks.ManySpec many() {
			return this;
		}

		@Override
		public <T> Empty<T> empty() {
			return wrapEmpty(new SinkEmptyMulticast<>());
		}

		@Override
		public <T> One<T> one() {
			@SuppressWarnings("deprecation") //TODO NextProcessor will be turned into an internal class only in 3.5
			final NextProcessor<T> original = new NextProcessor<>(null);
			return wrapOne(original);
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
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> onBackpressureBuffer(int bufferSize) {
			@SuppressWarnings("deprecation")
			final EmitterProcessor<T> original = EmitterProcessor.create(bufferSize);
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> onBackpressureBuffer(int bufferSize, boolean autoCancel) {
			@SuppressWarnings("deprecation")
			final EmitterProcessor<T> original = EmitterProcessor.create(bufferSize, autoCancel);
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> directAllOrNothing() {
			final SinkManyBestEffort<T> original = SinkManyBestEffort.createAllOrNothing();
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> directBestEffort() {
			final SinkManyBestEffort<T> original = SinkManyBestEffort.createBestEffort();
			return wrapMany(original);
		}


		@Override
		public <T> Many<T> all() {
			@SuppressWarnings("deprecation")
			final ReplayProcessor<T> original = ReplayProcessor.create();
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> all(int batchSize) {
			@SuppressWarnings("deprecation")
			final ReplayProcessor<T> original = ReplayProcessor.create(batchSize, true);
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> latest() {
			@SuppressWarnings("deprecation")
			final ReplayProcessor<T> original = ReplayProcessor.cacheLast();
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> latestOrDefault(T value) {
			@SuppressWarnings("deprecation")
			final ReplayProcessor<T> original = ReplayProcessor.cacheLastOrDefault(value);
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> limit(int historySize) {
			@SuppressWarnings("deprecation")
			final ReplayProcessor<T> original = ReplayProcessor.create(historySize);
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> limit(Duration maxAge) {
			@SuppressWarnings("deprecation")
			final ReplayProcessor<T> original = ReplayProcessor.createTimeout(maxAge);
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> limit(Duration maxAge, Scheduler scheduler) {
			@SuppressWarnings("deprecation")
			final ReplayProcessor<T> original = ReplayProcessor.createTimeout(maxAge, scheduler);
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> limit(int historySize, Duration maxAge) {
			@SuppressWarnings("deprecation")
			final ReplayProcessor<T> original = ReplayProcessor.createSizeAndTimeout(historySize, maxAge);
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> limit(int historySize, Duration maxAge, Scheduler scheduler) {
			@SuppressWarnings("deprecation")
			final ReplayProcessor<T> original = ReplayProcessor.createSizeAndTimeout(historySize, maxAge, scheduler);
			return wrapMany(original);
		}
	}

	static final class UnicastSpecImpl implements Sinks.UnicastSpec {

		final boolean serialized;

		UnicastSpecImpl(boolean serialized) {
			this.serialized = serialized;
		}

		<T, MANY extends Many<T> & ContextHolder> Many<T> wrapMany(MANY original) {
			if (serialized) {
				return new SinkManySerialized<>(original, original);
			}
			return original;
		}

		@Override
		public <T> Many<T> onBackpressureBuffer() {
			@SuppressWarnings("deprecation")
			final UnicastProcessor<T> original = UnicastProcessor.create();
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> onBackpressureBuffer(Queue<T> queue) {
			@SuppressWarnings("deprecation")
			final UnicastProcessor<T> original = UnicastProcessor.create(queue);
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> onBackpressureBuffer(Queue<T> queue, Disposable endCallback) {
			@SuppressWarnings("deprecation")
			final UnicastProcessor<T> original = UnicastProcessor.create(queue, endCallback);
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> onBackpressureError() {
			final UnicastManySinkNoBackpressure<T> original = UnicastManySinkNoBackpressure.create();
			return wrapMany(original);
		}
	}
}


/*
 * Copyright (c) 2020-2026 VMware Inc. or its affiliates, All Rights Reserved.
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

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.jspecify.annotations.Nullable;
import reactor.core.Disposable;
import reactor.core.publisher.Sinks.Empty;
import reactor.core.publisher.Sinks.Many;
import reactor.core.publisher.Sinks.One;
import reactor.core.scheduler.Scheduler;
import reactor.util.concurrent.Queues;

final class SinksSpecs {

	static final Sinks.RootSpec  UNSAFE_ROOT_SPEC = new UnsafeSpecImpl();
	static final DefaultSinksSpecs DEFAULT_SINKS    = new DefaultSinksSpecs();

	abstract static class AbstractSerializedSink {

		volatile int                                                   wip;
		static final AtomicIntegerFieldUpdater<AbstractSerializedSink> WIP =
				AtomicIntegerFieldUpdater.newUpdater(AbstractSerializedSink.class, "wip");

		volatile @Nullable Thread lockedAt;

		static final AtomicReferenceFieldUpdater<AbstractSerializedSink, @Nullable Thread>LOCKED_AT =
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


	static final class UnsafeSpecImpl
		implements Sinks.RootSpec, Sinks.ManySpec, Sinks.ManyWithUpstreamUnsafeSpec, Sinks.MulticastSpec, Sinks.MulticastReplaySpec {

		final Sinks.UnicastSpec unicastSpec;

		UnsafeSpecImpl() {
			this.unicastSpec = new UnicastSpecImpl(false);
		}

		@Override
		public <T> Empty<T> empty() {
			return new SinkEmptyMulticast<>();
		}

		@Override
		public <T> One<T> one() {
			return new SinkOneMulticast<>();
		}

		@Override
		public Sinks.ManySpec many() {
			return this;
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
		public Sinks.ManyWithUpstreamUnsafeSpec manyWithUpstream() {
			return this;
		}

		@Override
		public <T> Sinks.Many<T> onBackpressureBuffer() {
			return new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE);
		}

		@Override
		public <T> Sinks.Many<T> onBackpressureBuffer(int bufferSize) {
			return new SinkManyEmitterProcessor<>(true, bufferSize);
		}

		@Override
		public <T> Sinks.Many<T> onBackpressureBuffer(int bufferSize, boolean autoCancel) {
			return new SinkManyEmitterProcessor<>(autoCancel, bufferSize);
		}

		@Override
		public <T> Many<T> directAllOrNothing() {
			return new SinkManyBestEffort<>(true);
		}

		@Override
		public <T> Many<T> directBestEffort() {
			return new SinkManyBestEffort<>(false);
		}

		@Override
		public <T> Many<T> all() {
			return SinkManyReplayProcessor.create();
		}

		@Override
		public <T> Many<T> all(int batchSize) {
			return SinkManyReplayProcessor.create(batchSize);
		}

		@Override
		public <T> Many<T> latest() {
			return SinkManyReplayProcessor.cacheLast();
		}

		@Override
		public <T> Many<T> latestOrDefault(T value) {
			return SinkManyReplayProcessor.cacheLastOrDefault(value);
		}

		@Override
		public <T> Many<T> limit(int historySize) {
			return SinkManyReplayProcessor.create(historySize);
		}

		@Override
		public <T> Many<T> limit(Duration maxAge) {
			return SinkManyReplayProcessor.createTimeout(maxAge);
		}

		@Override
		public <T> Many<T> limit(Duration maxAge, Scheduler scheduler) {
			return SinkManyReplayProcessor.createTimeout(maxAge, scheduler);
		}

		@Override
		public <T> Many<T> limit(int historySize, Duration maxAge) {
			return SinkManyReplayProcessor.createSizeAndTimeout(historySize, maxAge);
		}

		@Override
		public <T> Many<T> limit(int historySize, Duration maxAge, Scheduler scheduler) {
			return SinkManyReplayProcessor.createSizeAndTimeout(historySize, maxAge, scheduler);
		}

		@Override
		public <T> Sinks.ManyWithUpstream<T> multicastOnBackpressureBuffer() {
			return new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE);
		}

		@Override
		public <T> Sinks.ManyWithUpstream<T> multicastOnBackpressureBuffer(int bufferSize, boolean autoCancel) {
			return new SinkManyEmitterProcessor<>(autoCancel, bufferSize);
		}
	}

	//Note: RootSpec is now reserved for Sinks.unsafe()
	static final class DefaultSinksSpecs implements Sinks.ManySpec, Sinks.MulticastSpec, Sinks.MulticastReplaySpec {

		final Sinks.UnicastSpec unicastSpec; //needed because UnicastSpec method names overlap with MulticastSpec

		DefaultSinksSpecs() {
			//there will only one instance of serialized UnicastSpecImpl as there is only one instance of  SafeRootSpecImpl
			this.unicastSpec = new UnicastSpecImpl(true);
		}

		<T, EMPTY extends Empty<T> & ContextHolder> Empty<T> wrapEmpty(EMPTY original) {
			return new SinkEmptySerialized<>(original, original);
		}

		<T, ONE extends One<T> & ContextHolder> One<T> wrapOne(ONE original) {
			return new SinkOneSerialized<>(original, original);
		}

		<T, MANY extends Many<T> & ContextHolder> Many<T> wrapMany(MANY original) {
			return new SinkManySerialized<>(original, original);
		}

		Sinks.ManySpec many() {
			return this;
		}

		<T> Empty<T> empty() {
			return wrapEmpty(new SinkEmptyMulticast<>());
		}

		<T> One<T> one() {
			return wrapOne(new SinkOneMulticast<>());
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
			return wrapMany(new SinkManyEmitterProcessor<>(true, Queues.SMALL_BUFFER_SIZE));
		}

		@Override
		public <T> Many<T> onBackpressureBuffer(int bufferSize) {
			return wrapMany(new SinkManyEmitterProcessor<>(true, bufferSize));
		}

		@Override
		public <T> Many<T> onBackpressureBuffer(int bufferSize, boolean autoCancel) {
			return wrapMany(new SinkManyEmitterProcessor<>(autoCancel, bufferSize));
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
			final SinkManyReplayProcessor<T> original = SinkManyReplayProcessor.create();
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> all(int batchSize) {
			final SinkManyReplayProcessor<T> original = SinkManyReplayProcessor.create(batchSize, true);
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> latest() {
			final SinkManyReplayProcessor<T> original = SinkManyReplayProcessor.cacheLast();
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> latestOrDefault(T value) {
			final SinkManyReplayProcessor<T> original = SinkManyReplayProcessor.cacheLastOrDefault(value);
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> limit(int historySize) {
			if (historySize <= 0) {
				throw new IllegalArgumentException("historySize must be > 0");
			}
			final SinkManyReplayProcessor<T> original = SinkManyReplayProcessor.create(historySize);
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> limit(Duration maxAge) {
			final SinkManyReplayProcessor<T> original = SinkManyReplayProcessor.createTimeout(maxAge);
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> limit(Duration maxAge, Scheduler scheduler) {
			final SinkManyReplayProcessor<T> original = SinkManyReplayProcessor.createTimeout(maxAge, scheduler);
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> limit(int historySize, Duration maxAge) {
			if (historySize <= 0) {
				throw new IllegalArgumentException("historySize must be > 0");
			}
			final SinkManyReplayProcessor<T> original = SinkManyReplayProcessor.createSizeAndTimeout(historySize, maxAge);
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> limit(int historySize, Duration maxAge, Scheduler scheduler) {
			if (historySize <= 0) {
				throw new IllegalArgumentException("historySize must be > 0");
			}
			final SinkManyReplayProcessor<T> original = SinkManyReplayProcessor.createSizeAndTimeout(historySize, maxAge, scheduler);
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
			final SinkManyUnicast<T> original = SinkManyUnicast.create();
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> onBackpressureBuffer(Queue<T> queue) {
			final SinkManyUnicast<T> original = SinkManyUnicast.create(queue);
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> onBackpressureBuffer(Queue<T> queue, Disposable endCallback) {
			final SinkManyUnicast<T> original = SinkManyUnicast.create(queue, endCallback);
			return wrapMany(original);
		}

		@Override
		public <T> Many<T> onBackpressureError() {
			final SinkManyUnicastNoBackpressure<T> original = SinkManyUnicastNoBackpressure.create();
			return wrapMany(original);
		}
	}
}
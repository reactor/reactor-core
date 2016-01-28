/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Loopback;
import reactor.core.flow.MultiProducer;
import reactor.core.flow.Producer;
import reactor.core.queue.RingBuffer;
import reactor.core.queue.Slot;
import reactor.core.state.Backpressurable;
import reactor.core.state.Cancellable;
import reactor.core.state.Completable;
import reactor.core.state.Failurable;
import reactor.core.state.Introspectable;
import reactor.core.state.Prefetchable;
import reactor.core.state.Requestable;
import reactor.core.util.Assert;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.EmptySubscription;
import reactor.core.util.Exceptions;
import reactor.core.util.Logger;
import reactor.core.util.PlatformDependent;
import reactor.core.util.Sequence;
import reactor.core.util.WaitStrategy;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;
import reactor.fn.Supplier;

/**
 * A Shared Processor Service is a {@link Processor} factory eventually sharing one or more internal {@link Processor}.
 * <p> Its purpose is to mutualize some threading and event loop resources, thus creating an (a)sync gateway reproducing
 * the input sequence of signals to their attached subscriber context. Its default behavior will be to request a fair
 * share of the internal {@link Processor} to allow many concurrent use of a single async resource. <p> Alongside
 * building Processor, SharedProcessor can generate unbounded dispatchers as: - a {@link BiConsumer} that schedules the
 * data argument over the  {@link Consumer} task argument. - a {@link Consumer} that schedules  {@link Consumer} task
 * argument. - a {@link Executor} that runs an arbitrary {@link Runnable} task. <p> SharedProcessor maintains a
 * reference count on how many artefacts have been built. Therefore it will automatically shutdown the internal async
 * resource after all references have been released. Each reference (consumer, executor or processor) can be used in
 * combination with {@link ProcessorGroup#release(Object...)} to cleanly unregister and eventually shutdown when no more
 * references use that service.
 * @param <T> the default type (not enforced at runtime)
 * @author Anatoly Kadyshev
 * @author Stephane Maldini
 */
public class ProcessorGroup<T> implements Supplier<Processor<T, T>>, Loopback {

	private static final Logger log               = Logger.getLogger(ProcessorGroup.class);

	/**
	 * Default number of processors available to the runtime on init (min 4)
	 * @see Runtime#availableProcessors()
	 */
	public static final int DEFAULT_POOL_SIZE = Math.max(Runtime.getRuntime()
	                                                            .availableProcessors(), 4);

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> io(String name, int bufferSize) {
		return io(name, bufferSize, DEFAULT_POOL_SIZE);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param concurrency
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> io(String name, int bufferSize, int concurrency) {
		return io(name, bufferSize, concurrency, null, null, true);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param concurrency
	 * @param uncaughtExceptionHandler
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> io(String name,
			int bufferSize,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler) {
		return io(name, bufferSize, concurrency, uncaughtExceptionHandler, null, true);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param concurrency
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> io(String name,
			int bufferSize,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler) {
		return io(name, bufferSize, concurrency, uncaughtExceptionHandler, shutdownHandler, true);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param concurrency
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> io(final String name,
			final int bufferSize,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown) {
		return io(name, bufferSize, concurrency, uncaughtExceptionHandler, shutdownHandler, autoShutdown,
				DEFAULT_WAIT_STRATEGY.get());
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param concurrency
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown
	 * @param waitStrategy
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> io(final String name,
			final int bufferSize,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown,
			WaitStrategy waitStrategy) {

		return create(WorkQueueProcessor.<Runnable>share(name, bufferSize,
				waitStrategy, false),
				concurrency, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
	}

	/**
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> io() {
		return io("io", PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> io(String name) {
		return io(name, PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param <E>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <E> ProcessorGroup<E> sync() {
		return (ProcessorGroup<E>) SYNC_SERVICE;
	}

	/**
	 * @param p
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> create(Processor<Runnable, Runnable> p) {
		return create(p, null, null, true);
	}

	/**
	 * @param p
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> create(Processor<Runnable, Runnable> p, int concurrency) {
		return create(p, concurrency, null, null, true);
	}

	/**
	 * @param p
	 * @param concurrency
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> create(Supplier<? extends Processor<Runnable, Runnable>> p, int concurrency) {
		return create(p, concurrency, null, null, true);
	}

	/**
	 * @param p
	 * @param autoShutdown
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> create(Processor<Runnable, Runnable> p, boolean autoShutdown) {
		return create(p, null, null, autoShutdown);
	}

	/**
	 * @param p
	 * @param uncaughtExceptionHandler
	 * @param autoShutdown
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> create(Processor<Runnable, Runnable> p,
			Consumer<Throwable> uncaughtExceptionHandler,
			boolean autoShutdown) {
		return create(p, uncaughtExceptionHandler, null, autoShutdown);
	}

	/**
	 * @param p
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> create(final Processor<Runnable, Runnable> p,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown) {
		return create(p, 1, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
	}

	/**
	 * @param p
	 * @param concurrency
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown
	 * @param <E>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <E> ProcessorGroup<E> create(Supplier<? extends Processor<Runnable, Runnable>> p,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown) {
		if (p != null && concurrency > 1) {
			return new PooledProcessorGroup<>(p, concurrency, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
		}
		else {
			return new SingleProcessorGroup<E>(p, concurrency, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
		}
	}

	/**
	 * @param p
	 * @param concurrency
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown
	 * @param <E>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <E> ProcessorGroup<E> create(final Processor<Runnable, Runnable> p,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown) {
		return new SingleProcessorGroup<E>(new Supplier<Processor<Runnable, Runnable>>() {
			@Override
			public Processor<Runnable, Runnable> get() {
				return p;
			}
		}, concurrency, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
	}

	/**
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> async() {
		return async("async", PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> async(String name) {
		return async(name, PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> async(String name, int bufferSize) {
		return async(name, bufferSize, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> async(String name, int bufferSize, int concurrency) {
		return async(name, bufferSize, concurrency, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> async(String name,
			int bufferSize,
			Consumer<Throwable> uncaughtExceptionHandler) {
		return async(name, bufferSize, uncaughtExceptionHandler, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> async(String name,
			int bufferSize,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler) {
		return async(name, bufferSize, concurrency, uncaughtExceptionHandler, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> async(String name,
			int bufferSize,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler) {
		return async(name, bufferSize, uncaughtExceptionHandler, shutdownHandler, true);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> async(String name,
			int bufferSize,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler) {
		return async(name, bufferSize, concurrency, uncaughtExceptionHandler, shutdownHandler, true);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> async(String name,
			int bufferSize,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown) {
		return async(name, bufferSize, DEFAULT_POOL_SIZE, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
	}

	/**
	 *
	 * Non-Blocking "Asynchronous" Dedicated Pub-Sub (1 Thread by Sub)
	 *
	 *
	 */

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> async(final String name,
			final int bufferSize,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown) {

		return async(name, bufferSize, concurrency, uncaughtExceptionHandler, shutdownHandler, autoShutdown, DEFAULT_WAIT_STRATEGY);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown
	 * @param waitprovider
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> async(final String name,
			final int bufferSize,
			final int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown,
			final Supplier<? extends WaitStrategy> waitprovider) {

		return ProcessorGroup.create(new Supplier<Processor<Runnable, Runnable>>() {
			int i = 1;
			@Override
			public Processor<Runnable, Runnable> get() {
				return TopicProcessor.share(name+(concurrency > 1 ? "-"+(i++) : ""), bufferSize, waitprovider
						.get(), false);
			}
		}, concurrency, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
	}


	/**
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> single() {
		return single("single", PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> single(String name) {
		return single(name, PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> single(String name, int bufferSize) {
		return single(name, bufferSize, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> single(String name, int bufferSize, Consumer<Throwable> errorC) {
		return single(name, bufferSize, errorC, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> single(String name, int bufferSize, Consumer<Throwable> errorC,
			Runnable shutdownC) {
		return single(name, bufferSize, errorC, shutdownC, SINGLE_WAIT_STRATEGY);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param <E>
	 * @return
	 */
	public static <E> ProcessorGroup<E> single(String name, int bufferSize, Consumer<Throwable> errorC,
			Runnable shutdownC, Supplier<? extends WaitStrategy> waitStrategy) {
		return async(name, bufferSize, 1, errorC, shutdownC, true, waitStrategy);
	}

	/**
	 * @param sharedProcessorReferences
	 * @return
	 */
	public static void release(Object... sharedProcessorReferences) {
		if (sharedProcessorReferences == null) {
			return;
		}

		for (Object sharedProcessorReference : sharedProcessorReferences) {
			if (sharedProcessorReference != null && DispatchOn.class.isAssignableFrom(sharedProcessorReference.getClass())) {
				((DispatchOn) sharedProcessorReference).cancel();
			}
		}
	}

	private static final Supplier<? extends WaitStrategy> DEFAULT_WAIT_STRATEGY = new Supplier<WaitStrategy>() {
		@Override
		public WaitStrategy get() {
			return WaitStrategy.phasedOffLiteLock(200, 200, TimeUnit.MILLISECONDS);
		}
	};

	private static final Supplier<? extends WaitStrategy> SINGLE_WAIT_STRATEGY = new Supplier<WaitStrategy>() {
		@Override
		public WaitStrategy get() {
			return WaitStrategy.phasedOffLiteLock(500, 50, TimeUnit.MILLISECONDS);
		}
	};


	final private TailRecurser tailRecurser;

	final private   Processor<Runnable, Runnable>         processor;
	final protected boolean                               autoShutdown;
	final protected int                                   concurrency;
	final           ExecutorProcessor<Runnable, Runnable> executorProcessor;

	@SuppressWarnings("unused")
	private volatile int refCount = 0;

	private static final AtomicIntegerFieldUpdater<ProcessorGroup> REF_COUNT =
			AtomicIntegerFieldUpdater.newUpdater(ProcessorGroup.class, "refCount");

	@Override
	public FluxProcessor<T, T> get() {
		return dispatchOn();
	}

	/**
	 * @return
	 */
	public final FluxProcessor<T, T> dispatchOn() {
		return dispatchOn(null);
	}
	/**
	 * @return
	 */
	public FluxProcessor<T, T> dispatchOn(Publisher<? extends T> source) {
		return createBarrier(false, source);
	}

	/**
	 * @return
	 */
	public final FluxProcessor<T, T> publishOn() {
		return publishOn(null);
	}

	/**
	 *
	 * @param source
	 * @return
	 */
	public FluxProcessor<T, T> publishOn(Publisher<? extends T> source) {
		return createBarrier(true, source);
	}

	/**
	 * @return
	 */
	public Consumer<Runnable> dispatcher() {
		if (processor == null) {
			return SYNC_DISPATCHER;
		}

		return createBarrier(false, null);
	}

	/**
	 * @param clazz
	 * @param <V>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <V> BiConsumer<V, Consumer<? super V>> dataDispatcher(Class<V> clazz) {
		if (processor == null) {
			return (BiConsumer<V, Consumer<? super V>>) SYNC_DATA_DISPATCHER;
		}

		return createBarrier(false, null);
	}

	/**
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public BiConsumer<T, Consumer<? super T>> dataDispatcher() {
		if (processor == null) {
			return (BiConsumer<T, Consumer<? super T>>) SYNC_DATA_DISPATCHER;
		}

		return createBarrier(false, null);
	}

	/**
	 * @return
	 */
	public Executor executor() {
		if (processor == null) {
			return SYNC_EXECUTOR;
		}

		return createBarrier(false, null);
	}

	public boolean awaitAndShutdown() {
		return awaitAndShutdown(-1, TimeUnit.SECONDS);
	}

	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		if (processor == null) {
			return true;
		}
		else if (ExecutorProcessor.class.isAssignableFrom(processor.getClass())) {
			return ((ExecutorProcessor) processor).awaitAndShutdown(timeout, timeUnit);
		}
		throw new UnsupportedOperationException("Underlying Processor doesn't implement Resource");
	}

	public void forceShutdown() {
		if (processor == null) {
			return;
		}
		else if (ExecutorProcessor.class.isAssignableFrom(processor.getClass())) {
			((ExecutorProcessor) processor).forceShutdown();
			return;
		}
		throw new UnsupportedOperationException("Underlying Processor doesn't implement Resource");
	}

	public boolean alive() {
		if (processor == null) {
			return true;
		}
		if (ExecutorProcessor.class.isAssignableFrom(processor.getClass())) {
			return ((ExecutorProcessor) processor).alive();
		}
		throw new UnsupportedOperationException("Underlying Processor doesn't implement Resource");
	}

	public void shutdown() {
		if (processor == null) {
			return;
		}
		try {
			if (ExecutorProcessor.class.isAssignableFrom(processor.getClass())) {
				((ExecutorProcessor) processor).shutdown();
			}
			else {
				processor.onComplete();
			}
		}
		catch (Throwable t) {
			Exceptions.throwIfFatal(t);
			processor.onError(t);
		}
	}

	/* INTERNAL */

	@SuppressWarnings("unchecked")
	private static final ProcessorGroup SYNC_SERVICE = new ProcessorGroup(null, -1, null, null, false);

	/**
	 * Singleton delegating consumer for synchronous data dispatchers
	 */
	private final static BiConsumer SYNC_DATA_DISPATCHER = new BiConsumer() {
		@Override
		@SuppressWarnings("unchecked")
		public void accept(Object o, Object callback) {
			((Consumer) callback).accept(o);
		}
	};

	/**
	 * Singleton delegating consumer for synchronous dispatchers
	 */
	private final static Consumer<Runnable> SYNC_DISPATCHER = new Consumer<Runnable>() {
		@Override
		public void accept(Runnable callback) {
			callback.run();
		}
	};

	/**
	 * Singleton delegating executor for synchronous executor
	 */
	private final static Executor SYNC_EXECUTOR = new Executor() {
		@Override
		public void execute(Runnable command) {
			command.run();
		}
	};

	private final static int LIMIT_BUFFER_SIZE = PlatformDependent.SMALL_BUFFER_SIZE / 2;

	@SuppressWarnings("unchecked")
	protected ProcessorGroup(Supplier<? extends Processor<Runnable, Runnable>> processor,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown) {
		this.autoShutdown = autoShutdown;
		this.concurrency = concurrency;

		if (processor != null) {
			this.processor = processor.get();
			Assert.isTrue(this.processor != null);

			// Managed Processor, providing for tail recursion,
			if (ExecutorProcessor.class.isAssignableFrom(this.processor.getClass())) {

				this.executorProcessor = (ExecutorProcessor<Runnable, Runnable>) this.processor;

				if (concurrency == 1) {
					int bufferSize = (int) Math.min(this.executorProcessor.getCapacity(), LIMIT_BUFFER_SIZE);

					this.tailRecurser = new TailRecurser(bufferSize, new Consumer<Runnable>() {
						@Override
						public void accept(Runnable task) {
							task.run();
						}
					});
				}
				else {
					this.tailRecurser = null;
				}


			}
			else {
				this.executorProcessor = null;
				this.tailRecurser = null;
			}

			for (int i = 0; i < concurrency; i++) {
				this.processor.onSubscribe(EmptySubscription.INSTANCE);
				this.processor.subscribe(new TaskSubscriber(tailRecurser, autoShutdown, uncaughtExceptionHandler,
						shutdownHandler));
			}

		}
		else {
			this.processor = null;
			this.executorProcessor = null;
			this.tailRecurser = null;
		}
	}

	@Override
	public Object connectedInput() {
		return processor;
	}

	@Override
	public Object connectedOutput() {
		return processor;
	}

	protected void decrementReference() {
		if ((processor != null || concurrency > 1) && REF_COUNT.decrementAndGet(this) <= 0 && autoShutdown) {
			shutdown();
		}
	}

	protected void incrementReference() {
		REF_COUNT.incrementAndGet(this);
	}

	private <Y> DispatchOn<Y> createBarrier(boolean forceWork, Publisher<? extends Y> source) {

		if (processor == null) {
			return new SyncProcessorBarrier<>(this);
		}

		if (ExecutorProcessor.class.isAssignableFrom(processor.getClass()) && !((ExecutorProcessor) processor).alive()) {
			throw new IllegalStateException("Internal Processor is shutdown");
		}

		incrementReference();

		if (forceWork || concurrency > 1) {
			return new PublishOn<>(this, source);
		}

		return new DispatchOn<>(true, this, source);
	}

	/**
	 *
	 */

	static class TailRecurser {

		@SuppressWarnings("unchecked")
		private static final Supplier<Slot<Runnable>> EMITTED = RingBuffer.EMITTED;

		private final ArrayList<Slot<Runnable>> pile;

		private final int pileSizeIncrement;

		private final Consumer<Runnable> taskConsumer;

		private int next = 0;

		public TailRecurser(int backlogSize, Consumer<Runnable> taskConsumer) {
			this.pileSizeIncrement = backlogSize * 2;
			this.taskConsumer = taskConsumer;
			this.pile = new ArrayList<>(pileSizeIncrement);
			ensureEnoughTasks();
		}

		private void ensureEnoughTasks() {
			if (next >= pile.size()) {
				pile.ensureCapacity(pile.size() + pileSizeIncrement);
				for (int i = 0; i < pileSizeIncrement; i++) {
					pile.add(EMITTED.get());
				}
			}
		}

		public Slot<Runnable> next() {
			ensureEnoughTasks();
			return pile.get(next++);
		}

		public void consumeTasks() {
			if (next > 0) {
				for (int i = 0; i < next; i++) {
					taskConsumer.accept(pile.get(i).value);
				}

				for (int i = next - 1; i >= pileSizeIncrement; i--) {
					pile.remove(i);
				}
				next = 0;
			}
		}

	}

	private static class DispatchOn<V> extends FluxProcessor<V, V>
			implements Consumer<Runnable>, BiConsumer<V, Consumer<? super V>>, Executor, Subscription, Backpressurable,
			           Loopback, Producer, Cancellable, Completable, Prefetchable, Requestable, Failurable,
			           Runnable {

		final ProcessorGroup service;
		final Publisher<? extends V> source;

		final RingBuffer<Slot<V>> emitBuffer;
		final Sequence            pollCursor;

		volatile Throwable error;

		volatile boolean cancelled;

		int outstanding;

		@SuppressWarnings("unused")
		volatile int running;
		protected static final AtomicIntegerFieldUpdater<DispatchOn> RUNNING =
				AtomicIntegerFieldUpdater.newUpdater(DispatchOn.class, "running");

		@SuppressWarnings("unused")
		volatile int terminated;
		protected static final AtomicIntegerFieldUpdater<DispatchOn> TERMINATED =
				AtomicIntegerFieldUpdater.newUpdater(DispatchOn.class, "terminated");

		@SuppressWarnings("unused")
		volatile long requested;
		protected static final AtomicLongFieldUpdater<DispatchOn> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(DispatchOn.class, "requested");

		Subscriber<? super V> subscriber;

		DispatchOn(boolean buffer, final ProcessorGroup service,
				 final Publisher<? extends V> source ) {
			this.service = service;
			this.source = source;
			if (!buffer) {
				emitBuffer = null;
				pollCursor = null;
			}
			else {
				outstanding = PlatformDependent.SMALL_BUFFER_SIZE;
				emitBuffer = RingBuffer.<V>createSingleProducer(PlatformDependent.SMALL_BUFFER_SIZE);
				pollCursor = RingBuffer.newSequence(-1L);
				emitBuffer.addGatingSequence(pollCursor);
			}
		}

		@Override
		public Object upstream() {
			return upstreamSubscription;
		}

		@Override
		public Subscriber<? super V> downstream() {
			return subscriber;
		}

		@Override
		public final void accept(V data, Consumer<? super V> consumer) {
			if (consumer == null) {
				throw Exceptions.argumentIsNullException();
			}
			dispatch(new ConsumerRunnable<>(data, consumer));
		}

		@Override
		public final void accept(Runnable consumer) {
			if (consumer == null) {
				throw Exceptions.argumentIsNullException();
			}
			dispatch(consumer);
		}

		@Override
		public final void execute(Runnable command) {
			if (command == null) {
				throw Exceptions.argumentIsNullException();
			}
			dispatch(command);
		}

		@Override
		public final void subscribe(Subscriber<? super V> s) {
			if (s == null) {
				throw Exceptions.argumentIsNullException();
			}
			final boolean set, subscribed;
			synchronized (this) {
				if (subscriber == null) {
					if(source == null) {
						subscriber = s;
					}
					set = true;
				}
				else {
					set = false;
				}
				subscribed = source != null || this.upstreamSubscription != null;
			}

			if (!set) {
				EmptySubscription.error(subscriber, new IllegalStateException("Shared Processors do not support multi-subscribe"));
			}
			else if (subscribed) {
				doStart(s);
			}

		}

		@Override
		public final void onSubscribe(Subscription s) {
			Subscriber<? super V> subscriber = null;

			synchronized (this) {
				if (BackpressureUtils.validate(upstreamSubscription, s)) {
					upstreamSubscription = s;
					subscriber = this.subscriber;
				}
			}

			if (subscriber != null && source == null) {
				doStart(subscriber);
			}
		}

		@Override
		public final void onNext(V o) {
			super.onNext(o);

			if (terminated == 1) {
				Exceptions.onNextDropped(o);
			}

			doNext(o);
		}

		@Override
		public final void onError(Throwable t) {
			super.onError(t);
			if (TERMINATED.compareAndSet(this, 0, 1)) {
				if (error == null) {
					error = t;
				}
				if (subscriber == null) {
					return;
				}

				doError(t);
			}
		}

		@Override
		public final void onComplete() {
			if (TERMINATED.compareAndSet(this, 0, 1)) {
				doComplete();
			}
		}

		@Override
		public String getName() {
			return "DispatchOn";
		}

		@SuppressWarnings("unchecked")
		protected void doStart(final Subscriber<? super V> subscriber) {
			RUNNING.incrementAndGet(this);

			this.subscriber = subscriber;
			subscriber.onSubscribe(this);
			if(source != null){
				source.subscribe(this);
			}
			service.processor.onNext(new Runnable() {
				@Override
				public void run() {
					doRequest(PlatformDependent.SMALL_BUFFER_SIZE);
					if (RUNNING.decrementAndGet(DispatchOn.this) != 0) {
						DispatchOn.this.run();
					}
				}
			});
		}

		@Override
		public long requestedFromDownstream() {
			return requested;
		}

		@SuppressWarnings("unchecked")
		protected void doNext(V o) {
			long seq = emitBuffer.next();
			emitBuffer.get(seq).value = o;
			emitBuffer.publish(seq);

			if (RUNNING.getAndIncrement(this) == 0) {
				service.processor.onNext(this);
			}
		}

		@SuppressWarnings("unchecked")
		protected void doError(Throwable t) {
			if (RUNNING.getAndIncrement(this) == 0) {
				service.processor.onNext(this);
			}
		}

		@SuppressWarnings("unchecked")
		protected void doComplete() {
			if (RUNNING.getAndIncrement(this) == 0) {
				service.processor.onNext(this);
			}
		}

		@Override
		@SuppressWarnings("unchecked")
		public void request(final long n) {
			if(BackpressureUtils.checkRequest(n, subscriber)) {
				BackpressureUtils.getAndAdd(REQUESTED, this, n);
				if (RUNNING.getAndIncrement(this) == 0) {
					service.processor.onNext(this);
				}
			}
		}

		protected final void doRequest(long n) {
			if (terminated == 0) {
				Subscription subscription = this.upstreamSubscription;
				if (subscription != null) {
					subscription.request(n);
				}
			}
		}

		@Override
		public Throwable getError() {
			return error;
		}

		@Override
		public long limit() {
			return PlatformDependent.SMALL_BUFFER_SIZE;
		}

		@Override
		public long expectedFromUpstream() {
			return outstanding;
		}

		@Override
		public final void cancel() {
			if(cancelled){
				return;
			}
			cancelled = true;
			Subscription subscription = this.upstreamSubscription;
			if (subscription != null) {
				this.upstreamSubscription = null;
				subscription.cancel();
			}
			this.subscriber = null;
			if (TERMINATED.compareAndSet(this, 0, 1)) {
				if (service != null) {
					service.decrementReference();
				}
			}
		}

		@SuppressWarnings("unchecked")
		protected void dispatch(Runnable runnable) {
			if (shouldTailRecruse()) {
				service.tailRecurser.next().value = runnable;
			}
			else {
				service.processor.onNext(runnable);
			}
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public boolean isStarted() {
			return upstreamSubscription != null;
		}

		@Override
		public long getPending() {
			return emitBuffer != null ? emitBuffer.getPending() : -1L;
		}

		@Override
		public boolean isTerminated() {
			return terminated == 1;
		}

		@Override
		public void run() {
			int missed = 1;
			long cursor = pollCursor.get();
			long r;
			int outstanding;

			for (; ; ) {
				outstanding = this.outstanding;
				long produced = 0L;
				r = requested;

				for (; ; ) {
					if (cancelled) {
						this.upstreamSubscription = null;
						subscriber = null;
						return;
					}

					if (r != 0L && cursor + 1L <= emitBuffer.getCursor()) {
						if (r != Long.MAX_VALUE) {
							r--;
						}
						outstanding--;
						route(emitBuffer.get(++cursor).value, subscriber, SignalType.NEXT);
						produced++;
					}
					else {
						break;
					}
				}

				if (produced != 0L) {
					this.outstanding = outstanding;
					pollCursor.set(cursor);
					if (r != Long.MAX_VALUE) {
						REQUESTED.addAndGet(this, -produced);
					}
				}

				Throwable error;
				if (terminated == 1) {
					if ((error = this.error) != null) {
						route(error, subscriber, SignalType.ERROR);
						this.upstreamSubscription = null;
						subscriber = null;
						return;
					}
					else if (emitBuffer.getPending() == 0) {
						route(null, subscriber, SignalType.COMPLETE);
						this.upstreamSubscription = null;
						subscriber = null;
						return;
					}
				}

				Subscription subscription = upstreamSubscription;
				if (outstanding < LIMIT_BUFFER_SIZE && subscription != null) {
					int k = PlatformDependent.SMALL_BUFFER_SIZE - outstanding;

					this.outstanding = PlatformDependent.SMALL_BUFFER_SIZE;
					subscription.request(k);
				}
//				else{
//					System.out.println(this+ " "+PublisherFactory.fromSubscription(upstreamSubscription));
//				}

				missed = RUNNING.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}


		}

		@Override
		public Object connectedInput() {
			return service.processor;
		}

		@Override
		public Object connectedOutput() {
			return service.processor;
		}

		@SuppressWarnings("unchecked")
		protected final void route(Object payload, Subscriber subscriber, SignalType type) {

			try {
				if (subscriber == null) {
					return;
				}

				if (type == SignalType.NEXT) {
					subscriber.onNext(payload);
				}
				else if (type == SignalType.COMPLETE) {
					subscriber.onComplete();
					service.decrementReference();
				}
				else if (type == SignalType.SUBSCRIPTION) {
					subscriber.onSubscribe((Subscription) payload);
				}
				else {
					subscriber.onError((Throwable) payload);
				}
			}
			catch (Exceptions.CancelException c) {
				service.decrementReference();
				throw c;
			}
			catch (Throwable t) {
				service.decrementReference();
				if (type != SignalType.ERROR) {
					Exceptions.throwIfFatal(t);
					subscriber.onError(t);
				}
				else {
					throw t;
				}
			}
		}

		protected boolean shouldTailRecruse() {
			return service.tailRecurser != null &&
					service.executorProcessor != null &&
					service.executorProcessor.isInContext();
		}

		@Override
		public long getCapacity() {
			return emitBuffer != null ? PlatformDependent.SMALL_BUFFER_SIZE : Long.MAX_VALUE;
		}

		@Override
		public String toString() {
			return getClass().getSimpleName() + "{" +
					"subscription=" + upstreamSubscription +
					(emitBuffer != null ? ", pendingReceive=" + outstanding + ", buffered=" + emitBuffer.getPending() :
							"") +
					(requested != 0 ? ", pendingSend=" + requested : "") +
					'}';
		}
	}

	private static final class PublishOn<V> extends DispatchOn<V> {

		public PublishOn(ProcessorGroup service, Publisher<? extends V> source) {
			super(false, service, source);
		}

		@Override
		protected void doStart(final Subscriber<? super V> subscriber) {
			dispatch(new Runnable() {
				@Override
				public void run() {
					if(source != null) {
						source.subscribe(PublishOn.this);
					}
					if(terminated == 1) {
						if (error != null) {
							EmptySubscription.error(subscriber, error);
							return;
						}
						EmptySubscription.complete(subscriber);
						return;
					}
					PublishOn.this.subscriber = subscriber;
					subscriber.onSubscribe(PublishOn.this);
				}
			});
		}

		@Override
		protected void doComplete() {
			route(null, subscriber, SignalType.COMPLETE);
		}

		@Override
		protected void doNext(V o) {
			route(o, subscriber, SignalType.NEXT);
		}

		@Override
		protected void doError(Throwable t) {
			route(t, subscriber, SignalType.ERROR);
		}

		@Override
		public void run() {
			int missed = 1;
			long r;
			for (; ; ) {
				r = REQUESTED.getAndSet(this, 0);
				if (r == Long.MAX_VALUE) {
					doRequest(Long.MAX_VALUE);
					return;
				}

				if (r != 0L) {
					doRequest(r);
				}

				missed = RUNNING.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		@Override
		@SuppressWarnings("unchecked")
		public void request(final long n) {
			if(BackpressureUtils.checkRequest(n, subscriber)) {
				BackpressureUtils.getAndAdd(REQUESTED, this, n);
				if (RUNNING.getAndIncrement(this) == 0) {
					if (service.executorProcessor != null && !service.executorProcessor.isInContext()) {
						service.processor.onNext(this);
					}
					else {
						run();
					}
				}
			}
		}

		@Override
		public String getName() {
			return "PublishOn";
		}

		@Override
		public long getCapacity() {
			return Long.MAX_VALUE;
		}
	}

	private static final class SyncProcessorBarrier<V> extends DispatchOn<V> {

		public SyncProcessorBarrier(ProcessorGroup service) {
			super(false, service, null);
		}

		@Override
		protected void dispatch(Runnable runnable) {
			runnable.run();
		}

		@Override
		protected void doStart(Subscriber<? super V> subscriber) {
			route(this, subscriber, SignalType.SUBSCRIPTION);
		}

		@Override
		protected void doComplete() {
			upstreamSubscription = null;
			route(null, subscriber, SignalType.COMPLETE);
			subscriber = null;
		}

		@Override
		protected void doNext(V o) {
			route(o, subscriber, SignalType.NEXT);
		}

		@Override
		protected void doError(Throwable t) {
			upstreamSubscription = null;
			route(t, subscriber, SignalType.ERROR);
			subscriber = null;
		}

		@Override
		public long getCapacity() {
			return Long.MAX_VALUE;
		}

		@Override
		public void request(long n) {
			if(BackpressureUtils.checkRequest(n, subscriber)) {
				Subscription subscription = upstreamSubscription;
				if (subscription != null) {
					subscription.request(n);
				}
			}
		}
	}

	private static final class ConsumerRunnable<T> implements Runnable {

		private final Consumer<? super T> consumer;
		private final T                   data;

		public ConsumerRunnable(T data, Consumer<? super T> consumer) {
			this.consumer = consumer;
			this.data = data;
		}

		@Override
		public void run() {
			consumer.accept(data);
		}
	}

	private static class TaskSubscriber implements Subscriber<Runnable>, Introspectable {

		private final Consumer<Throwable> uncaughtExceptionHandler;
		private final Runnable      shutdownHandler;
		private final TailRecurser        tailRecurser;
		private final boolean        autoShutdown;

		public TaskSubscriber(TailRecurser tailRecurser,
				boolean autoShutdown,
				Consumer<Throwable> uncaughtExceptionHandler,
				Runnable shutdownHandler) {
			this.uncaughtExceptionHandler = uncaughtExceptionHandler;
			this.shutdownHandler = shutdownHandler;
			this.tailRecurser = tailRecurser;
			this.autoShutdown = autoShutdown;
		}

		@Override
		public void onSubscribe(Subscription s) {
			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(Runnable task) {
			try {
				task.run();

				if (tailRecurser != null) {
					tailRecurser.consumeTasks();
				}

			}
			catch (Exceptions.CancelException ce) {
				//IGNORE
			}
			catch (Throwable t){
				log.error("Unrouted exception", t);
			}
		}

		@Override
		public int getMode() {
			return TRACE_ONLY;
		}

		@Override
		public String getName() {
			return TaskSubscriber.class.getSimpleName();
		}

		@Override
		public void onError(Throwable t) {
			Exceptions.throwIfFatal(t);
			if (uncaughtExceptionHandler != null) {
				uncaughtExceptionHandler.accept(t);
			}

			//TODO support resubscribe ?
			throw new UnsupportedOperationException("No error handler provided for this ProcessorGroup", t);
		}

		@Override
		public void onComplete() {
			if (shutdownHandler != null) {
				shutdownHandler.run();
			}
		}

	}

	final static class SingleProcessorGroup<T> extends ProcessorGroup<T> {

		public SingleProcessorGroup(Supplier<? extends Processor<Runnable, Runnable>> processor,
				int concurrency,
				Consumer<Throwable> uncaughtExceptionHandler,
				Runnable shutdownHandler,
				boolean autoShutdown) {
			super(processor, concurrency, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
		}
	}

	final static class PooledProcessorGroup<T> extends ProcessorGroup<T> implements MultiProducer {

		final ProcessorGroup[] processorGroups;

		volatile int index = 0;

		public PooledProcessorGroup(Supplier<? extends Processor<Runnable, Runnable>> processor,
				int concurrency,
				Consumer<Throwable> uncaughtExceptionHandler,
				Runnable shutdownHandler,
				boolean autoShutdown) {
			super(null, concurrency, null, null, autoShutdown);

			processorGroups = new ProcessorGroup[concurrency];

			for (int i = 0; i < concurrency; i++) {
				processorGroups[i] =
						new InnerProcessorGroup(processor, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
			}
		}

		@Override
		public void shutdown() {
			for (ProcessorGroup processorGroup : processorGroups) {
				processorGroup.shutdown();
			}
		}

		@Override
		public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
			for (ProcessorGroup processorGroup : processorGroups) {
				if (!processorGroup.awaitAndShutdown(timeout, timeUnit)) {
					return false;
				}
			}
			return true;
		}

		@Override
		public Iterator<?> downstreams() {
			return Arrays.asList(processorGroups).iterator();
		}

		@Override
		public long downstreamCount() {
			return processorGroups.length;
		}

		@Override
		public void forceShutdown() {
			for (ProcessorGroup processorGroup : processorGroups) {
				processorGroup.forceShutdown();
			}
		}

		@Override
		public boolean alive() {
			for (ProcessorGroup processorGroup : processorGroups) {
				if (!processorGroup.alive()) {
					return false;
				}
			}
			return true;
		}

		@SuppressWarnings("unchecked")
		private ProcessorGroup<T> next() {
			int index = this.index++;
			if (index == Integer.MAX_VALUE) {
				this.index -= Integer.MAX_VALUE;
			}
			return (ProcessorGroup<T>) processorGroups[index % concurrency];
		}

		@Override
		public Executor executor() {
			return next().executor();
		}

		@Override
		public BiConsumer<T, Consumer<? super T>> dataDispatcher() {
			return next().dataDispatcher();
		}

		@Override
		public <V> BiConsumer<V, Consumer<? super V>> dataDispatcher(Class<V> clazz) {
			return next().dataDispatcher(clazz);
		}

		@Override
		public FluxProcessor<T, T> dispatchOn(Publisher<? extends T> source) {
			return next().dispatchOn(source);
		}

		@Override
		public FluxProcessor<T, T> publishOn(Publisher<? extends T> source) {
			return next().publishOn(source);
		}

		@Override
		public Consumer<Runnable> dispatcher() {
			return next().dispatcher();
		}

		@Override
		public FluxProcessor<T, T> get() {
			return next().get();
		}

		private class InnerProcessorGroup extends ProcessorGroup<T> implements Introspectable {

			public InnerProcessorGroup(Supplier<? extends Processor<Runnable, Runnable>> processor,
					Consumer<Throwable> uncaughtExceptionHandler,
					Runnable shutdownHandler,
					boolean autoShutdown) {
				super(processor, 1, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
			}

			@Override
			protected void decrementReference() {
				REF_COUNT.decrementAndGet(this);
				PooledProcessorGroup.this.decrementReference();
			}

			@Override
			protected void incrementReference() {
				REF_COUNT.incrementAndGet(this);
				PooledProcessorGroup.this.incrementReference();
			}

			@Override
			public int getMode() {
				return INNER;
			}

			@Override
			public String getName() {
				return InnerProcessorGroup.class.getSimpleName();
			}
		}

	}
}
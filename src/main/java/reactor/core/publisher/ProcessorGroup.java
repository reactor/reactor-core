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

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Loopback;
import reactor.core.flow.MultiProducer;
import reactor.core.flow.Producer;
import reactor.core.queue.QueueSupplier;
import reactor.core.state.Backpressurable;
import reactor.core.state.Completable;
import reactor.core.state.Failurable;
import reactor.core.state.Introspectable;
import reactor.core.util.Assert;
import reactor.core.util.BackpressureUtils;
import reactor.core.util.EmptySubscription;
import reactor.core.util.Exceptions;
import reactor.core.util.Logger;
import reactor.core.util.PlatformDependent;
import reactor.core.util.WaitStrategy;
import reactor.fn.Consumer;
import reactor.fn.Supplier;

/**
 * A Processor Group offers {@link Processor} {@link Supplier} and {@link Consumer} {@link Callable} generators
 *  eventually mutualizing one or more internal {@link Processor}.
 * <p> Its purpose is to save some threading and event loop resources by creating a level of indirection between
 * the number of threads and active publisher/subscriber akin to a {@link java.util.concurrent.ExecutorService} with
 * event-loop support ({@link TopicProcessor} {@link WorkQueueProcessor}).
 *
 *
 * This indirection requires an input identity
 * {@link Processor} of {@link Runnable} that will callback {@link Runnable#run()} on {@link Subscriber#onNext}. The {@link #create}
 * factories allow arbitrary group creation.
 * <p>
 *    In addition to {@link #create}, {@link ProcessorGroup} offers ready-to-use factories :
 *    <ul>
 *        <li>{@link #async} : Optimized for fast {@link Runnable} executions </li>
 *        <li>{@link #io} : Optimized for slow {@link Runnable} executions </li>
 *        <li>{@link #single} : Optimized for low-latency {@link Runnable} executions </li>
 *    </ul>
 * <p>
 * Appropriate thread count strategy must be considered especially when passing a processor that supports pub-sub
 * such as
 * {@link TopicProcessor} or {@link EmitterProcessor}. In effect multiple subscribers will consume the same {@link Runnable}
 * in these situations unlike {@link WorkQueueProcessor} which exclusively distribute to its subscribers.
 *
 *
 * <p> Eventually, {@link ProcessorGroup} can generate typed proxies such as:
 * <ul>
 *     <li>{@link Processor} that schedule OnNext, OnError, OnComplete on the processor group threads</li>
 *     <li>that schedules  {@link Runnable} task argument.</li>
 * </ul>
 * <p> {@link ProcessorGroup} maintains a reference count on how many artefacts have been built. Therefore it will
 * automatically shutdown the internal async resource after all references have been released. Each reference
 * (consumer or processor) can be used in combination with {@link ProcessorGroup#release(Object...)} to
 * cleanly unregister and eventually shutdown when no more references use that {@link ProcessorGroup}.
 *
 * @author Anatoly Kadyshev
 * @author Stephane Maldini
 */
public class ProcessorGroup
		implements Supplier<Processor<Runnable, Runnable>>, Callable<Consumer<Runnable>>, Consumer<Runnable>, Loopback,
		           Completable {

	static final Logger log = Logger.getLogger(ProcessorGroup.class);

	/**
	 * Default number of processors available to the runtime on init (min 4)
	 * @see Runtime#availableProcessors()
	 */
	public static final int DEFAULT_POOL_SIZE = Math.max(Runtime.getRuntime()
	                                                            .availableProcessors(), 4);

	/**
	 *
	 *
	 * @param name
	 * @param bufferSize

	 * @return
	 */
	public static ProcessorGroup io(String name, int bufferSize) {
		return io(name, bufferSize, DEFAULT_POOL_SIZE);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param concurrency

	 * @return
	 */
	public static ProcessorGroup io(String name, int bufferSize, int concurrency) {
		return io(name, bufferSize, concurrency, null, null, true);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param concurrency
	 * @param uncaughtExceptionHandler

	 * @return
	 */
	public static ProcessorGroup io(String name,
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

	 * @return
	 */
	public static ProcessorGroup io(String name,
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

	 * @return
	 */
	public static ProcessorGroup io(final String name,
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

	 * @return
	 */
	public static ProcessorGroup io(final String name,
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

	 * @return
	 */
	public static ProcessorGroup io() {
		return io("io", PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name

	 * @return
	 */
	public static ProcessorGroup io(String name) {
		return io(name, PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**

	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static ProcessorGroup sync() {
		return SYNC_SERVICE;
	}

	/**
	 * @param p

	 * @return
	 */
	public static ProcessorGroup create(Processor<Runnable, Runnable> p) {
		return create(p, null, null, true);
	}

	/**
	 * @param p

	 * @return
	 */
	public static ProcessorGroup create(Processor<Runnable, Runnable> p, int concurrency) {
		return create(p, concurrency, null, null, true);
	}

	/**
	 * @param p
	 * @param concurrency

	 * @return
	 */
	public static ProcessorGroup create(Supplier<? extends Processor<Runnable, Runnable>> p, int concurrency) {
		return create(p, concurrency, null, null, true);
	}

	/**
	 * @param p
	 * @param autoShutdown

	 * @return
	 */
	public static ProcessorGroup create(Processor<Runnable, Runnable> p, boolean autoShutdown) {
		return create(p, null, null, autoShutdown);
	}

	/**
	 * @param p
	 * @param uncaughtExceptionHandler
	 * @param autoShutdown

	 * @return
	 */
	public static ProcessorGroup create(Processor<Runnable, Runnable> p,
			Consumer<Throwable> uncaughtExceptionHandler,
			boolean autoShutdown) {
		return create(p, uncaughtExceptionHandler, null, autoShutdown);
	}

	/**
	 * @param p
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown

	 * @return
	 */
	public static ProcessorGroup create(final Processor<Runnable, Runnable> p,
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

	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static ProcessorGroup create(Supplier<? extends Processor<Runnable, Runnable>> p,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown) {
		if (p != null && concurrency > 1) {
			return new PooledProcessorGroup(p, concurrency, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
		}
		else {
			return new SingleProcessorGroup(p, concurrency, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
		}
	}

	/**
	 * @param p
	 * @param concurrency
	 * @param uncaughtExceptionHandler
	 * @param shutdownHandler
	 * @param autoShutdown

	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static ProcessorGroup create(final Processor<Runnable, Runnable> p,
			int concurrency,
			Consumer<Throwable> uncaughtExceptionHandler,
			Runnable shutdownHandler,
			boolean autoShutdown) {
		return new SingleProcessorGroup(new Supplier<Processor<Runnable, Runnable>>() {
			@Override
			public Processor<Runnable, Runnable> get() {
				return p;
			}
		}, concurrency, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
	}

	/**

	 * @return
	 */
	public static ProcessorGroup async() {
		return async("async", PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name

	 * @return
	 */
	public static ProcessorGroup async(String name) {
		return async(name, PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param bufferSize

	 * @return
	 */
	public static ProcessorGroup async(String name, int bufferSize) {
		return async(name, bufferSize, null);
	}

	/**
	 * @param name
	 * @param bufferSize

	 * @return
	 */
	public static ProcessorGroup async(String name, int bufferSize, int concurrency) {
		return async(name, bufferSize, concurrency, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler

	 * @return
	 */
	public static ProcessorGroup async(String name,
			int bufferSize,
			Consumer<Throwable> uncaughtExceptionHandler) {
		return async(name, bufferSize, uncaughtExceptionHandler, null);
	}

	/**
	 * @param name
	 * @param bufferSize
	 * @param uncaughtExceptionHandler

	 * @return
	 */
	public static ProcessorGroup async(String name,
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

	 * @return
	 */
	public static ProcessorGroup async(String name,
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

	 * @return
	 */
	public static ProcessorGroup async(String name,
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

	 * @return
	 */
	public static ProcessorGroup async(String name,
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

	 * @return
	 */
	public static ProcessorGroup async(final String name,
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

	 * @return
	 */
	public static ProcessorGroup async(final String name,
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

	 * @return
	 */
	public static ProcessorGroup single() {
		return single("single", PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name

	 * @return
	 */
	public static ProcessorGroup single(String name) {
		return single(name, PlatformDependent.MEDIUM_BUFFER_SIZE);
	}

	/**
	 * @param name
	 * @param bufferSize

	 * @return
	 */
	public static ProcessorGroup single(String name, int bufferSize) {
		return single(name, bufferSize, null);
	}

	/**
	 * @param name
	 * @param bufferSize

	 * @return
	 */
	public static ProcessorGroup single(String name, int bufferSize, Consumer<Throwable> errorC) {
		return single(name, bufferSize, errorC, null);
	}

	/**
	 * @param name
	 * @param bufferSize

	 * @return
	 */
	public static ProcessorGroup single(String name, int bufferSize, Consumer<Throwable> errorC,
			Runnable shutdownC) {
		return single(name, bufferSize, errorC, shutdownC, SINGLE_WAIT_STRATEGY);
	}

	/**
	 * @param name
	 * @param bufferSize

	 * @return
	 */
	public static ProcessorGroup single(String name, int bufferSize, Consumer<Throwable> errorC,
			Runnable shutdownC, Supplier<? extends WaitStrategy> waitStrategy) {
		return async(name, bufferSize, 1, errorC, shutdownC, true, waitStrategy);
	}

	/**
	 * @param sharedProcessorReferences
	 */
	public static void release(Object... sharedProcessorReferences) {
		if (sharedProcessorReferences == null) {
			return;
		}

		for (Object sharedProcessorReference : sharedProcessorReferences) {
			if (sharedProcessorReference != null && sharedProcessorReference instanceof ProcessorGroupBarrier) {
				((ProcessorGroupBarrier) sharedProcessorReference).cancel();
			}
		}
	}

	@Override
	public void accept(Runnable runnable) {
		if (runnable == null) {
			decrementReference();
		}
		else if (processor == null) {
			runnable.run();
		}
		else {
			processor.onNext(runnable);
		}
	}

	@Override
	public Consumer<Runnable> call() throws Exception {
		if (processor == null) {
			return NOOP_TASK_SUBSCRIBER;
		}
		incrementReference();
		return this;
	}

	/**
	 *
	 * @param tailRecurse
	 * @return
	 * @throws Exception
	 */
	public Consumer<Runnable> call(boolean tailRecurse) throws Exception {
		if(tailRecurse){
			FluxProcessor<Runnable, Runnable> processor = this.<Runnable>createBarrier();
			processor.subscribe(NOOP_TASK_SUBSCRIBER);
			return processor.start();
		}
		return call();
	}

	/**
	 * Create a {@link Processor} dispatching {@link Subscriber#onNext(Object)},
	 * {@link Subscriber#onError(Throwable)} and {@link Subscriber#onComplete()} asynchronously using
	 * this {@link ProcessorGroup}. This will increment the reference count of this {@link ProcessorGroup} and
	 * will further condition its shutdown (when the processor is terminated).
	 *
	 * The returned {@link FluxProcessor} offers {@link Flux} API and will only support at most one {@link Subscriber}.
	 */
	@Override
	public FluxProcessor<Runnable, Runnable> get() {
		return createBarrier();
	}

	/**
	 * Create a {@link Processor} dispatching {@link Subscriber#onNext(Object)},
	 * {@link Subscriber#onError(Throwable)} and {@link Subscriber#onComplete()} asynchronously using
	 * this {@link ProcessorGroup}. This will increment the reference count of this {@link ProcessorGroup} and
	 * further condition its shutdown (when the processor is terminated).
	 *
	 * The returned {@link FluxProcessor} offers {@link Flux} API and will only support at most one {@link Subscriber}.
	 *
	 * @param <T> The {@link Processor} reified type
	 * @return a new {@link FluxProcessor} reference
	 */
	public <T> FluxProcessor<T, T> processor() {
		return createBarrier();
	}

	/**
	 * Blocking shutdown of the internal {@link ExecutorProcessor} with {@link Processor#onComplete()}. If the
	 * processor doesn't implement.
	 *
	 * The method will only return after shutdown has been confirmed, waiting undefinitely so.
	 *
	 * @return true if successfully shutdown
	 */
	public final boolean awaitAndShutdown() {
		return awaitAndShutdown(-1, TimeUnit.SECONDS);
	}

	/**
	 * Blocking shutdown of the internal {@link ExecutorProcessor} with {@link Processor#onComplete()}. If the
	 * processor doesn't implement
	 * {@link ExecutorProcessor} or if it is synchronous, throw an {@link UnsupportedOperationException}.
	 * @param timeout the time un given unit to wait for
	 * @param timeUnit the unit
	 *
	 * @return true if successfully shutdown
	 */
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		if (processor == null) {
			return true;
		}
		else if (processor instanceof ExecutorProcessor) {
			return ((ExecutorProcessor) processor).awaitAndShutdown(timeout, timeUnit);
		}
		throw new UnsupportedOperationException("Underlying Processor is null or doesn't implement ExecutorProcessor");
	}

	/**
	 * Non-blocking forced shutdown of the internal {@link Processor} with {@link Processor#onComplete()}
	 */
	public void forceShutdown() {
		if (processor == null) {
			return;
		}
		else if (processor instanceof ExecutorProcessor) {
			((ExecutorProcessor) processor).forceShutdown();
			return;
		}
		throw new UnsupportedOperationException("Underlying Processor is null or doesn't implement ExecutorProcessor");
	}

	@Override
	public boolean isTerminated() {
		if (processor == null) {
			return false;
		}
		return processor instanceof ExecutorProcessor && ((ExecutorProcessor) processor).isTerminated();
	}

	@Override
	public boolean isStarted() {
		return processor == null || !(processor instanceof ExecutorProcessor) || ((ExecutorProcessor) processor).isStarted();
	}

	/**
	 * Non-blocking shutdown of the internal {@link Processor} with {@link Processor#onComplete()}
	 */
	public void shutdown() {
		if (processor == null) {
			return;
		}
		try {
			if (processor instanceof ExecutorProcessor) {
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
	static final ProcessorGroup SYNC_SERVICE = new ProcessorGroup(null, -1, null, null, false);

	static final Supplier<? extends WaitStrategy> DEFAULT_WAIT_STRATEGY = new Supplier<WaitStrategy>() {
		@Override
		public WaitStrategy get() {
			return WaitStrategy.phasedOffLiteLock(200, 200, TimeUnit.MILLISECONDS);
		}
	};

	static final Supplier<? extends WaitStrategy> SINGLE_WAIT_STRATEGY = new Supplier<WaitStrategy>() {
		@Override
		public WaitStrategy get() {
			return WaitStrategy.phasedOffLiteLock(500, 50, TimeUnit.MILLISECONDS);
		}
	};

	static final TaskSubscriber NOOP_TASK_SUBSCRIBER = new TaskSubscriber(null, null);

	final Processor<Runnable, Runnable>         processor;
	final ExecutorProcessor<Runnable, Runnable> executorProcessor;
	final boolean                               autoShutdown;
	final int                                   concurrency;

	@SuppressWarnings("unused")
	private volatile int refCount = 0;

	static final AtomicIntegerFieldUpdater<ProcessorGroup> REF_COUNT =
			AtomicIntegerFieldUpdater.newUpdater(ProcessorGroup.class, "refCount");

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
			if (this.processor instanceof ExecutorProcessor) {
				this.executorProcessor = (ExecutorProcessor<Runnable, Runnable>) this.processor;
			}
			else {
				this.executorProcessor = null;
			}

			for (int i = 0; i < concurrency; i++) {
				this.processor.onSubscribe(EmptySubscription.INSTANCE);
				this.processor.subscribe(new TaskSubscriber(uncaughtExceptionHandler, shutdownHandler));
			}

		}
		else {
			this.processor = null;
			this.executorProcessor = null;
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

	private <Y> ProcessorGroupBarrier<Y> createBarrier() {

		if (processor == null) {
			return new ProcessorGroupBarrier<>(this);
		}

		if (processor instanceof ExecutorProcessor && !((ExecutorProcessor) processor).isStarted()) {
			throw new IllegalStateException("Internal Processor is shutdown");
		}

		incrementReference();

		return new ProcessorGroupBarrier<>(this);
	}

	final static class SingleProcessorGroup extends ProcessorGroup {

		public SingleProcessorGroup(Supplier<? extends Processor<Runnable, Runnable>> processor,
				int concurrency,
				Consumer<Throwable> uncaughtExceptionHandler,
				Runnable shutdownHandler,
				boolean autoShutdown) {
			super(processor, concurrency, uncaughtExceptionHandler, shutdownHandler, autoShutdown);
		}
	}

	final static class PooledProcessorGroup extends ProcessorGroup implements MultiProducer {

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
		public boolean isStarted() {
			for (ProcessorGroup processorGroup : processorGroups) {
				if (!processorGroup.isStarted()) {
					return false;
				}
			}
			return true;
		}

		private ProcessorGroup next() {
			int index = this.index++;
			if (index == Integer.MAX_VALUE) {
				this.index -= Integer.MAX_VALUE;
			}
			return processorGroups[index % concurrency];
		}

		@Override
		public void accept(Runnable runnable) {
			next().accept(runnable);
		}

		@Override
		public FluxProcessor<Runnable, Runnable> get() {
			return next().get();
		}

		@Override
		public Consumer<Runnable> call() throws Exception {
			return next().call();
		}

		@Override
		public <T> FluxProcessor<T, T> processor() {
			return next().processor();
		}

		private class InnerProcessorGroup extends ProcessorGroup implements Introspectable {

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

	final static class TaskSubscriber implements Subscriber<Runnable>, Consumer<Runnable>, Introspectable {

		private final Consumer<Throwable> uncaughtExceptionHandler;
		private final Runnable            shutdownHandler;

		public TaskSubscriber(Consumer<Throwable> uncaughtExceptionHandler, Runnable shutdownHandler) {
			this.uncaughtExceptionHandler = uncaughtExceptionHandler;
			this.shutdownHandler = shutdownHandler;
		}

		@Override
		public void onSubscribe(Subscription s) {
			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(Runnable task) {
			try {
				task.run();

			}
			catch (Exceptions.CancelException ce) {
				//IGNORE
			}
			catch (Throwable t) {
				log.error("Unrouted exception", t);
			}
		}

		@Override
		public void accept(Runnable runnable) {
			if(runnable == null){
				onComplete();
			}
			else {
				onNext(runnable);
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

	static final class ProcessorGroupBarrier<V> extends FluxProcessor<V, V>
			implements Backpressurable, Loopback, Producer, Completable, Failurable {

		protected static final AtomicIntegerFieldUpdater<ProcessorGroupBarrier> TERMINATED =
				AtomicIntegerFieldUpdater.newUpdater(ProcessorGroupBarrier.class, "terminated");

		final    ProcessorGroup                         service;
		final    FluxDispatchOn.DispatchOnSubscriber<V> dispatchOn;
		final    DispatchOnOutput<V>                    dispatchOnOutput;
		volatile Throwable                              error;
		volatile int                                    terminated;

		public ProcessorGroupBarrier(ProcessorGroup service) {
			this.service = service;
			this.dispatchOnOutput = new DispatchOnOutput<>(this);
			if (service.processor != null) {
				this.dispatchOn = new FluxDispatchOn.DispatchOnSubscriber<>(dispatchOnOutput,
						service,
						false,
						PlatformDependent.XS_BUFFER_SIZE,
						QueueSupplier.<V>xs(true));
			}
			else {
				this.dispatchOn = null;
			}
		}

		@Override
		public Object connectedInput() {
			return dispatchOn;
		}

		@Override
		public Object connectedOutput() {
			return dispatchOn;
		}

		@Override
		public Subscriber<? super V> downstream() {
			return dispatchOnOutput.target;
		}

		@Override
		public Throwable getError() {
			return error;
		}

		@Override
		public boolean isStarted() {
			return upstreamSubscription != null;
		}

		@Override
		public boolean isTerminated() {
			return terminated == 1;
		}

		@Override
		public final void onComplete() {
			if (TERMINATED.compareAndSet(this, 0, 1)) {
				upstreamSubscription = null;
				if(dispatchOn == null){
					dispatchOnOutput.onComplete();
				}
				else {
					dispatchOn.onComplete();
				}
			}
		}

		@Override
		public void accept(V runnable) {
			if (runnable == null) {
				if(dispatchOn != null) {
					service.decrementReference();
				}
				dispatchOnOutput.cancel();
			}
			else if(dispatchOn == null){
				dispatchOnOutput.onNext(runnable);
			}
			else{
				dispatchOn.onNext(runnable);
			}
		}

		@Override
		public final void onError(Throwable t) {
			super.onError(t);
			if (TERMINATED.compareAndSet(this, 0, 1)) {
				if (error == null) {
					error = t;
				}
				Subscriber<? super V> subscriber = dispatchOnOutput.target;
				if (subscriber == null) {
					return;
				}

				upstreamSubscription = null;
				if(dispatchOn == null){
					dispatchOnOutput.onError(t);
				}
				else {
					dispatchOn.onError(t);
				}
			}
		}

		@Override
		public final void onNext(V o) {
			super.onNext(o);
			if(dispatchOn == null){
				dispatchOnOutput.onNext(o);
			}
			else {
				dispatchOn.onNext(o);
			}
		}

		@Override
		public final void onSubscribe(Subscription s) {
			Subscriber<? super V> subscriber = null;

			synchronized (this) {
				if (BackpressureUtils.validate(upstreamSubscription, s)) {
					upstreamSubscription = s;
					subscriber = dispatchOnOutput.target;
				}
			}

			if (subscriber != null) {
				if(dispatchOn == null){
					dispatchOnOutput.onSubscribe(s);
				}
				else {
					dispatchOn.onSubscribe(s);
				}
			}
		}

		void cancel() {
			dispatchOnOutput.cancel();
		}

		@Override
		public final void subscribe(Subscriber<? super V> s) {
			if (s == null) {
				throw Exceptions.argumentIsNullException();
			}
			if (terminated != 0) {
				if (error != null) {
					EmptySubscription.error(s, error);
				}
				else {
					EmptySubscription.complete(s);
				}
				return;
			}

			final boolean set, subscribed;
			synchronized (this) {
				if (dispatchOnOutput.target == null) {
					dispatchOnOutput.target = s;
					set = true;
				}
				else {
					set = false;
				}
				subscribed = this.upstreamSubscription != null;
			}

			if (!set) {
				EmptySubscription.error(s,
						new IllegalStateException("Shared Processors do not support multi-subscribe"));
			}
			else if (subscribed) {
				if(dispatchOn == null){
					dispatchOnOutput.onSubscribe(upstreamSubscription);
				}
				else {
					dispatchOn.onSubscribe(upstreamSubscription);
				}
			}

		}

		@Override
		public Subscription upstream() {
			return upstreamSubscription;
		}

		@Override
		public String toString() {
			return super.toString() + ":" + dispatchOnOutput.target;
		}

		final static class DispatchOnOutput<V> implements Subscription, Subscriber<V> {

			final ProcessorGroupBarrier<V> parent;

			Subscriber<? super V> target;
			Subscription          s;

			public DispatchOnOutput(ProcessorGroupBarrier<V> parent) {
				this.parent = parent;
			}

			@Override
			public void request(long n) {
				if (BackpressureUtils.checkRequest(n, target)) {
					s.request(n);
				}
			}

			@Override
			public void cancel() {
				if (TERMINATED.compareAndSet(parent, 0, 1)) {
					if(s != null) {
						s.cancel();
					}
				}
				target = null;
			}

			@Override
			public void onSubscribe(Subscription s) {
				this.s = s;
				target.onSubscribe(this);
			}

			@Override
			public void onNext(V v) {
				if (target == null) {
					Exceptions.onNextDropped(v);
				}
				target.onNext(v);
			}

			@Override
			public void onError(Throwable t) {
				target.onError(t);
				target = null;
			}

			@Override
			public void onComplete() {
				target.onComplete();
				target = null;
			}
		}
	}
}
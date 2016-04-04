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

package reactor.core.scheduler;

import java.util.function.Consumer;
import java.util.function.LongSupplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.state.Cancellable;
import reactor.core.state.Pausable;
import reactor.core.state.Timeable;
import reactor.core.subscriber.Subscribers;
import reactor.core.util.Exceptions;
import reactor.core.util.WaitStrategy;

/**
 * @author Stephane Maldini
 * @since 2.5
 */
public class Timer implements Timeable, Cancellable {

	protected final int resolution;


//	 ==============================================================================================================
//	 Static Methods
//	 ==============================================================================================================

	/**
	 * Create a new {@link Timer} using the default resolution (50MS) and backlog size (64). All times
	 * will
	 * rounded up to the closest multiple of this resolution.
	 * <p>
	 * return a new {@link Timer}
	 */
	public static Timer create() {
		return create(50);
	}

	/**
	 * Create a new {@link Timer} using the default resolution (50MS) and backlog size (64). All times
	 * will
	 * rounded up to the closest multiple of this resolution.
	 * @param name timer thread prefix
	 * <p>
	 * return a new {@link Timer}
	 */
	public static Timer create(String name) {
		return create(name, 50);
	}

	/**
	 * Create a new {@link Timer} using the the given timer {@code resolution} and backlog size (64). All times
	 * will
	 * rounded up to the closest multiple of this resolution.
	 *
	 * @param resolution resolution of this timer in milliseconds
	 *                   <p>
	 *                   return a new {@link Timer}
	 */
	public static Timer create(int resolution) {
		return create(resolution, 64);
	}

	/**
	 * Create a new {@link Timer} using the the given timer {@code resolution} and backlog size (64). All times
	 * will
	 * rounded up to the closest multiple of this resolution.
	 *
	 * @param name timer thread prefix
	 * @param resolution resolution of this timer in milliseconds
	 *                   <p>
	 *                   return a new {@link Timer}
	 */
	public static Timer create(String name, int resolution) {
		return create(name, resolution, 64);
	}

	/**
	 * Create a new {@code HashWheelTimer} using the given timer {@code resolution} and {@code bufferSize}. All times
	 * will
	 * rounded up to the closest multiple of this resolution.
	 *
	 * @param resolution resolution of this timer in milliseconds
	 * @param bufferSize size of the wheel supporting the Timer, the larger the wheel, the less the lookup time is
	 *                   for sparse timeouts.
	 *                   <p>
	 *                   return a new {@link Timer}
	 */
	public static Timer create(int resolution, int bufferSize) {
		return create("reactor-timer", resolution, bufferSize);
	}

	/**
	 * Create a new {@code HashWheelTimer} using the given timer {@code resolution} and {@code bufferSize}. All times
	 * will
	 * rounded up to the closest multiple of this resolution.
	 *
	 * @param name timer thread prefix
	 * @param resolution resolution of this timer in milliseconds
	 * @param bufferSize size of the wheel supporting the Timer, the larger the wheel, the less the lookup time is
	 *                   for sparse timeouts.
	 *                   <p>
	 *                   return a new {@link Timer}
	 */
	public static Timer create(String name, int resolution, int bufferSize) {
		Timer timer = new HashWheelTimer(name, resolution, bufferSize, WaitStrategy.sleeping(), null);
		timer.start();
		return timer;
	}

	/**
	 * Read if the context timer has been set
	 *
	 * @return true if context timer is initialized
	 */
	public static boolean hasGlobal() {
		return GlobalTimer.available();
	}

	/**
	 * The returned timer SHOULD always be cancelled after use, however global timer will ignore it.
	 *
	 * @return eventually the global timer or if not set a fresh timer.
	 */
	public static Timer globalOrNew() {
		return GlobalTimer.globalOrNew();
	}

	/**
	 * The returned timer MIGHT be NULL if no global timer set.
	 *
	 * @return eventually the global timer or if not NULL.
	 */
	public static Timer globalOrNull() {
		return GlobalTimer.available() ? GlobalTimer.get() : null;
	}

	/**
	 * Obtain the default global timer from the current context. The globalTimer is created lazily so
	 * it is preferrable to fetch them out of the critical path.
	 * <p>
	 * The global timer will also ignore {@link Timer#cancel()} calls and should be cleaned using {@link
	 * #unregisterGlobal()}.
	 * <p>
	 * The default globalTimer is a {@link HashWheelTimer}. It is suitable for non blocking periodic
	 * work
	 * such as  eventing, memory access, lock-free code, dispatching...
	 *
	 * @return the globalTimer, usually a {@link HashWheelTimer}
	 */
	public static Timer global() {
		return GlobalTimer.get();
	}

	/**
	 * Clean current global timer references and cancel the respective {@link Timer}.
	 * A new global timer can be assigned later with {@link #global()}.
	 */
	public static void unregisterGlobal() {
		GlobalTimer.unregister();
	}


	final static LongSupplier SYSTEM_NOW = System::currentTimeMillis;


//	 ==============================================================================================================
//	 Instance methods
//	 ==============================================================================================================

	Timer() {
		this.resolution = 0;
	}

	protected Timer(int resolution) {
		this.resolution = resolution;
	}

	/**
	 * Schedule a recurring task. The given {@link Consumer} will be invoked once every N time units
	 * after the given delay.
	 *
	 * @param consumer            the {@code Consumer} to invoke each period
	 * @param period              the duration in milliseconds that should elapse between
	 * invocations of the given {@code Consumer}
	 * @param delay               a duration in which to delay any execution of the given
	 * {@code Consumer}
	 * @return a {@link Pausable} that can be used to {@link
	 * Pausable#cancel() cancel}, {@link Pausable#pause() pause} or
	 * {@link Pausable#resume() resume} the given task.
	 */
	public Pausable schedule(Consumer<? super Long> consumer,
	                  long period, long delay) {
		Subscriber<? super Long> s = Subscribers.consumer(consumer);
		Pausable p = interval(s, period, delay);
		s.onSubscribe(p);
		return p;
	}

	/**
	 * Schedule a recurring task. The given {@link Subscriber} will receive an ever incrementing integer from 0 once
	 * every N time units after the given delay. If the {@link Subscription} has not been requested before the timed
	 * signal, a  {@link java.util.concurrent.TimeoutException} will be signalled instead.
	 *
	 * @param subscriber the {@code Subscriber} to invoke each period
	 * @param period the duration in milliseconds that should elapse between invocations
	 * of the given {@code Subscriber}
	 * @param delay a duration in milliseconds in which to delay any execution of the
	 * given {@code Subscriber}
	 *
	 * @return a {@link Subscription} that can be passed onSubscribe
	 */
	public Pausable interval(Subscriber<? super Long> subscriber,
			long period, long delay) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Schedule a recurring task. The given {@link Consumer} will be invoked immediately, as well as
	 * once
	 * every N time units.
	 *
	 * @param consumer the {@code Consumer} to invoke each period
	 * @param period   the duration that should elapse between invocations of the given {@code Consumer}
	 * @return a {@link Pausable} that can be used to {@link
	 * Pausable#cancel() cancel}, {@link Pausable#pause() pause} or
	 * {@link Pausable#resume() resume} the given task.
	 * @see #schedule(Consumer, long, long)
	 */
	public Pausable schedule(Consumer<? super Long> consumer, long period) {
		return schedule(consumer, period, 0);
	}

	/**
	 * Submit a task for arbitrary execution after the given time delay.
	 *
	 * @param consumer the {@code Consumer} to invoke
	 * @param delay    the duration that should elapse before invocations of the given {@code Consumer}
	 * @return a {@link Pausable} that can be used to {@link
	 * Pausable#cancel() cancel}, {@link Pausable#pause() pause} or
	 * {@link Pausable#resume() resume} the given task.
	 */
	public Pausable submit(Consumer<? super Long> consumer, long delay) {
		Subscriber<? super Long> s = Subscribers.consumer(consumer);
		Pausable p = single(Subscribers.consumer(consumer), delay);
		s.onSubscribe(p);
		return p;
	}

	/**
	 * Submit a task for arbitrary execution after the delay of this timer's resolution.
	 *
	 * @param consumer the {@code Consumer} to invoke
	 *
	 * @return {@literal this}
	 */
	public Pausable submit(Consumer<? super Long> consumer) {
		return submit(consumer, resolution);
	}

	/**
	 * Submit a task for arbitrary execution after the given time delay. If the {@link Subscription} has not been
	 * requested before the timed signal, a {@link java.util.concurrent.TimeoutException} will be signalled
	 * instead.
	 *
	 * @param subscriber the {@code Subscriber} to invoke
	 * @param delay    the duration in milliseconds that should elapse before invocations
	 * of the given {@code Subscriber}
	 * @return a {@link Subscription} that can be passed onSubscribe
	 */
	public Pausable single(Subscriber<? super Long> subscriber, long delay) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Start the Timer, may throw an IllegalStateException if already started.
	 */
	public void start(){

	}

	/**
	 * Cancel this timer by interrupting the task thread. No more tasks can be submitted to this timer after
	 * cancellation.
	 */
	public void cancel() {

	}

	@Override
	public boolean isCancelled() {
		return false;
	}

	@Override
	public long period() {
		return resolution;
	}

	static void checkResolution(long time, long resolution) {
		if (time % resolution != 0) {
			throw Exceptions.bubble(new IllegalArgumentException(
					"Period must be a multiple of Timer resolution (e.g. period % resolution == 0 ). " +
							"Resolution for this Timer is: " + resolution + "ms"
			));
		}
	}
}

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
package reactor;

import reactor.core.support.WaitStrategy;
import reactor.core.timer.GlobalTimer;
import reactor.core.timer.HashWheelTimer;
import reactor.core.timer.Timer;

/**
 * @author Stephane Maldini
 * @since 2.5
 */
public final class Timers {

	private Timers() {
	}

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
		Timer timer = new HashWheelTimer(name, resolution, bufferSize, new WaitStrategy.Sleeping(), null);
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


}

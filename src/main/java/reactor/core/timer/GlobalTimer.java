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
package reactor.core.timer;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import reactor.core.support.ReactiveState;
import reactor.core.support.WaitStrategy;
import reactor.core.support.internal.PlatformDependent;

/**
 * A Global Timer
 *
 * @author Stephane Maldini
 * @since 2.5
 */
public class GlobalTimer extends HashWheelTimer implements ReactiveState.Trace {

	private static final class GlobalContext{
		volatile GlobalTimer timer;
	}

	private static final AtomicReferenceFieldUpdater<GlobalContext, GlobalTimer> GLOBAL_TIMER =
		PlatformDependent.newAtomicReferenceFieldUpdater(GlobalContext.class, "timer");

	private static final GlobalContext context = new GlobalContext();

	public GlobalTimer() {
		super("global-timer", 50, DEFAULT_WHEEL_SIZE, new WaitStrategy.Sleeping(), null);
	}

	private void _cancel() {
		super.cancel();
		//TimeUtils.disable();
	}

	@Override
	public void cancel() {
		// IGNORE
	}

	@Override
	public void start() {
		//TimeUtils.enable();
		super.start();
	}

	/**
	 * Obtain the default global timer from the current context. The globalTimer is created lazily so
	 * it is preferrable to fetch them out of the critical path.
	 * <p>
	 * The global timer will also ignore {@link Timer#cancel()} calls and should be cleaned using {@link
	 * #unregister()} ()}.
	 * <p>
	 * The default globalTimer is a {@link HashWheelTimer}. It is suitable for non blocking
	 * periodic
	 * work
	 * such as  eventing, memory access, lock-free code, dispatching...
	 *
	 * @return the globalTimer, usually a {@link HashWheelTimer}
	 */
	public static Timer get() {
		GlobalTimer t = context.timer;
		while (null == t) {
				t = new GlobalTimer();
				if (!GLOBAL_TIMER.compareAndSet(context, null, t)) {
					t = context.timer;
					t._cancel();
				}
				else{
					t.start();
					break;
				}
		}
		return t;
	}

	/**
	 * Clean current global timer references and cancel the respective {@link Timer}.
	 * A new global timer can be assigned later with {@link #get()}.
	 */
	public static void unregister() {
		GlobalTimer timer;
		while ((timer = GLOBAL_TIMER.getAndSet(context, null)) != null) {
			timer._cancel();
		}
	}

	/**
	 * Read if the context timer has been set
	 *
	 * @return true if context timer is initialized
	 */
	public static boolean available() {
		return context.timer != null;
	}

	/**
	 * The returned timer SHOULD always be cancelled after use, however global timer will ignore it.
	 *
	 * @return eventually the global timer or if not set a fresh timer.
	 */
	public static Timer globalOrNew() {
		Timer timer = context.timer;

		if (timer == null) {
			timer = new HashWheelTimer(50, 64, new WaitStrategy.Sleeping());
			timer.start();
		}
		return timer;
	}

}

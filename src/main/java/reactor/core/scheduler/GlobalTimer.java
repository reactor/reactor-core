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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import reactor.core.state.Introspectable;
import reactor.core.util.PlatformDependent;
import reactor.core.util.WaitStrategy;

/**
 * A Global Timer
 *
 * @author Stephane Maldini
 * @since 2.5
 */
final class GlobalTimer extends Timer implements Introspectable {


	/**
	 * The returned timer SHOULD always be cancelled after use, however global timer will ignore it.
	 *
	 * @return eventually the global timer or if not set a fresh timer.
	 */
	public static Timer globalOrNew() {
		Timer timer = context.timer;

		if (timer == null) {
			timer = new Timer(50, 64, WaitStrategy.sleeping());
			timer.start();
		}
		return timer;
	}


	static final class GlobalContext{
		volatile GlobalTimer timer;
	}

	static final AtomicReferenceFieldUpdater<GlobalContext, GlobalTimer> GLOBAL_TIMER =
		PlatformDependent.newAtomicReferenceFieldUpdater(GlobalContext.class, "timer");

	static final GlobalContext context = new GlobalContext();

	GlobalTimer() {
		super("global-timer", 50, DEFAULT_WHEEL_SIZE, WaitStrategy.sleeping(), null);
	}

	void _cancel() {
		super.shutdown();
		//TimeUtils.disable();
	}

	@Override
	public void shutdown() {
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
	 * The global timer will also ignore {@link Timer#shutdown()} calls and should be cleaned using {@link
	 * #unregister()} ()}.
	 * <p>
	 * It is suitable for non blocking
	 * periodic
	 * work
	 * such as  eventing, memory access, lock-free code, dispatching...
	 *
	 * @return the global {@link Timer}
	 */
	static Timer get() {
		GlobalTimer t = context.timer;
		while (null == t) {
				t = new GlobalTimer();
				if (!GLOBAL_TIMER.compareAndSet(context, null, t)) {
					t._cancel();
					t = context.timer;
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
	static void unregister() {
		GlobalTimer timer;
		while ((timer = GLOBAL_TIMER.getAndSet(context, null)) != null) {
			timer._cancel();
		}
	}

	@Override
	public int getMode() {
		return TRACE_ONLY;
	}


	/**
	 * Read if the context timer has been set
	 *
	 * @return true if context timer is initialized
	 */
	static boolean available() {
		return context.timer != null;
	}

}

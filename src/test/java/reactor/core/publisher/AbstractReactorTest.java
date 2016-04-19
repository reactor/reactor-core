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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Timer;

/**
 * @author Stephane Maldini
 */
public abstract class AbstractReactorTest {

	protected static Scheduler      asyncGroup;
	protected static Scheduler ioGroup;
	protected static Timer          timer;

	protected final Map<Thread, AtomicLong> counters = new ConcurrentHashMap<>();

	@BeforeClass
	public static void loadEnv() {
		timer = Timer.global();
		ioGroup = Computations.concurrent("work", 2048, 4, Throwable::printStackTrace, null, false);
		asyncGroup = Computations.parallel("parallel", 2048, 4, Throwable::printStackTrace, null, false);
	}

	@AfterClass
	public static void closeEnv() {
		timer = null;
		ioGroup.shutdown();
		asyncGroup.shutdown();
		//Timer.unregisterGlobal();
	}

	static {
		System.setProperty("reactor.trace.cancel", "true");
	}

	protected void monitorThreadUse() {
		monitorThreadUse(null);
	}

	protected void monitorThreadUse(Object val) {
		AtomicLong counter = counters.get(Thread.currentThread());
		if (counter == null) {
			counter = new AtomicLong();
			AtomicLong prev = counters.putIfAbsent(Thread.currentThread(), counter);
			if(prev != null){
				counter = prev;
			}
		}
		counter.incrementAndGet();
	}
}

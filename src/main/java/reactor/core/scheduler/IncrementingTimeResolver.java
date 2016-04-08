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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.LongSupplier;

import reactor.core.queue.RingBuffer;
import reactor.core.util.PlatformDependent;
import reactor.core.util.Sequence;
import reactor.core.util.WaitStrategy;

/**
 * @author Stephane Maldini
 * @since 2.5
 */
final class IncrementingTimeResolver {

	final static private Timer NOOP = new Timer();

	static final int      DEFAULT_RESOLUTION = 100;
	static final Sequence now                = RingBuffer.newSequence(-1);

	static final IncrementingTimeResolver INSTANCE = new IncrementingTimeResolver();

	volatile Timer timer = NOOP;

	static final AtomicReferenceFieldUpdater<IncrementingTimeResolver, Timer> REF =
			PlatformDependent.newAtomicReferenceFieldUpdater(IncrementingTimeResolver.class, "timer");

	protected IncrementingTimeResolver() {
	}

	public static long approxCurrentTimeMillis() {
		INSTANCE.getTimer();
		return now.getAsLong();
	}

	public static boolean isEnabled(){
		return INSTANCE.timer != NOOP;
	}

	public static void enable(){
		INSTANCE.getTimer();
	}

	public static void disable(){
		INSTANCE.cancelTimer();
	}

	void cancelTimer(){
		Timer timer;
		for(;;){
			timer = this.timer;
			if(REF.compareAndSet(this, timer, NOOP)){
				timer.cancel();
				break;
			}
		}
	}

	Timer getTimer() {
		Timer timer = this.timer;
		if (NOOP == timer) {
			timer = new HashWheelTimer("time-utils", DEFAULT_RESOLUTION, HashWheelTimer.DEFAULT_WHEEL_SIZE, WaitStrategy.sleeping(), null);
			if(!REF.compareAndSet(this, NOOP, timer)){
				timer.cancel();
				timer = this.timer;
			}
			else {
				timer.start();
			}
		}
		return timer;
	}

	/**
	 * Settable Time Supplier that could be used for Testing purposes or
	 * in systems where time doesn't correspond to the wall clock.
	 */
	public static class SettableTimeSupplier implements LongSupplier {

	    private volatile long                                         current;
	    private final    AtomicLongFieldUpdater<SettableTimeSupplier> fieldUpdater;
	    private final    AtomicBoolean                                initialValueRead;
	    private final    long                                         initialTime;


	    public SettableTimeSupplier(long initialTime) {
	        this.initialValueRead = new AtomicBoolean(false);
	        this.initialTime = initialTime;
	        this.fieldUpdater = AtomicLongFieldUpdater.newUpdater(SettableTimeSupplier.class, "current");
	    }

	    @Override
	    public long getAsLong() {
	        if (initialValueRead.get()) {
	            return current;
	        } else {
	            initialValueRead.set(true);
	            return initialTime;
	        }

	    }

	    public void set(long newCurrent) throws InterruptedException {
	        this.fieldUpdater.set(this, newCurrent);
	    }
	}
}

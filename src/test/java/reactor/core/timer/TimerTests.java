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

import org.junit.Assert;
import org.junit.Test;
import reactor.Timers;
import reactor.core.support.ReactiveState;
import reactor.core.support.WaitStrategy;
import reactor.fn.Consumer;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author @masterav10
 */
public class TimerTests {

	@Test
	public void verifyPause() throws InterruptedException {
		Timer timer = Timers.create();

		AtomicInteger count = new AtomicInteger();

		int tasks = 10;
		Phaser phaser = new Phaser(tasks);

		AtomicLong sysTime = new AtomicLong();

		ReactiveState.Pausable pausable = timer.schedule((time) -> {
			if (phaser.getPhase() == 0) {
				phaser.arrive();
				sysTime.set(System.nanoTime());
			}
			count.getAndIncrement();
		}, 100, TimeUnit.MILLISECONDS, 500);

		phaser.awaitAdvance(0);

		pausable.pause();
		long time = System.nanoTime() - sysTime.get();
		Thread.sleep(1000);
		HashWheelTimer.TimedSubscription<?> registration = (HashWheelTimer.TimedSubscription<?>) pausable;
		Assert.assertTrue(registration.isPaused());
		Assert.assertTrue(time < TimeUnit.MILLISECONDS.toNanos(100));
		Assert.assertEquals(tasks, count.get());
		timer.cancel();
	}

    @Test
    public void timeTravelWithBusySpinStrategyTest() throws InterruptedException {
        timeTravelTest(new WaitStrategy.BusySpin(), 1);
        timeTravelTest(new WaitStrategy.BusySpin(), 5);
        timeTravelTest(new WaitStrategy.BusySpin(), 10);
    }

    @Test
    public void timeTravelWithYieldingWaitStrategyTest() throws InterruptedException {
        timeTravelTest(new WaitStrategy.Yielding(), 1);
        timeTravelTest(new WaitStrategy.Yielding(), 5);
        timeTravelTest(new WaitStrategy.Yielding(), 10);
    }

    @Test
    public void timeTravelWithSleepingWaitStrategyTest() throws InterruptedException {
        timeTravelTest(new WaitStrategy.Sleeping(), 1);
        timeTravelTest(new WaitStrategy.Sleeping(), 5);
        timeTravelTest(new WaitStrategy.Sleeping(), 10);
    }

    private void timeTravelTest(WaitStrategy waitStrategy, int iterations) throws InterruptedException {
        AtomicInteger timesCalled = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(iterations);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        TimeUtils.SettableTimeSupplier timeTravellingSupplier = new TimeUtils.SettableTimeSupplier(0L);

        Timer timer = new HashWheelTimer("time-travelling-timer" + waitStrategy,
                                         500,
                                         512,
                                         waitStrategy,
                                         executor,
                                         timeTravellingSupplier
                                         );
        timer.start();

        timer.schedule(new Consumer<Long>() {
                           @Override
                           public void accept(Long aLong) {
                               timesCalled.incrementAndGet();
                               latch.countDown();
                           }
                       }, 1, TimeUnit.SECONDS);

        timeTravellingSupplier.set(iterations * 1000L);

        latch.await(5, TimeUnit.SECONDS);
        Assert.assertEquals(0, latch.getCount());
        Assert.assertEquals(iterations, timesCalled.get());

        timer.cancel();
    }
}

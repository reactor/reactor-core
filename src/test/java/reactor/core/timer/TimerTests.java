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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.state.Pausable;
import reactor.core.util.WaitStrategy;

/**
 * @author @masterav10
 */
public class TimerTests {

	@Test
	public void verifyPause() throws InterruptedException {
		Timer timer = Timer.create();

		AtomicInteger count = new AtomicInteger();

		int tasks = 10;
		Phaser phaser = new Phaser(tasks);

		AtomicLong sysTime = new AtomicLong();

		Pausable pausable = timer.schedule((time) -> {
			if (phaser.getPhase() == 0) {
				phaser.arrive();
				sysTime.set(System.nanoTime());
			}
			count.getAndIncrement();
		}, 100, 500);

		phaser.awaitAdvance(0);

		pausable.pause();
		long time = System.nanoTime() - sysTime.get();
		Thread.sleep(1000);
		HashWheelTimer.IntervalSubscription registration = (HashWheelTimer.IntervalSubscription) pausable;
		Assert.assertTrue(registration.isPaused());
		Assert.assertTrue(time < TimeUnit.MILLISECONDS.toNanos(100));
		Assert.assertEquals(tasks, count.get());
		timer.cancel();
	}

    @Test
    public void timeTravelWithBusySpinStrategyTest() throws InterruptedException {
        timeTravelTest(WaitStrategy.busySpin(), 1);
        timeTravelTest(WaitStrategy.busySpin(), 5);
        timeTravelTest(WaitStrategy.busySpin(), 10);
    }

    @Test
    public void timeTravelWithYieldingWaitStrategyTest() throws InterruptedException {
        timeTravelTest(WaitStrategy.yielding(), 1);
        timeTravelTest(WaitStrategy.yielding(), 5);
        timeTravelTest(WaitStrategy.yielding(), 10);
    }

    @Test
    public void timeTravelWithSleepingWaitStrategyTest() throws InterruptedException {
        timeTravelTest(WaitStrategy.sleeping(), 1);
        timeTravelTest(WaitStrategy.sleeping(), 5);
        timeTravelTest(WaitStrategy.sleeping(), 10);
    }

    private void timeTravelTest(WaitStrategy waitStrategy, int iterations) throws InterruptedException {
        AtomicInteger timesCalled = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(iterations);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        IncrementingTimeResolver.SettableTimeSupplier timeTravellingSupplier = new IncrementingTimeResolver.SettableTimeSupplier(0L);

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
                       }, 1000);

        timeTravellingSupplier.set(iterations * 1000L);

        latch.await(5, TimeUnit.SECONDS);
        Assert.assertEquals(0, latch.getCount());
        Assert.assertEquals(iterations, timesCalled.get());

        timer.cancel();
    }
}

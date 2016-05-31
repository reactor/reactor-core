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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.scheduler.TimedScheduler.TimedWorker;

public class HashWheelTimerTest {

    @Test
    public void independentWorkers() throws InterruptedException {
        TimedScheduler timer = Schedulers.newTimer("test-timer");
        
        try {
            TimedWorker w1 = timer.createWorker();
            
            TimedWorker w2 = timer.createWorker();
            
            CountDownLatch cdl = new CountDownLatch(1);
            
            w1.shutdown();
            
            try {
                w1.schedule(() -> { });
                Assert.fail("Failed to reject task");
            } catch (Throwable ex) {
                // ingoring
            }
            
            w2.schedule(cdl::countDown);
            
            if (!cdl.await(1, TimeUnit.SECONDS)) {
                Assert.fail("Worker 2 didn't execute in time");
            }
            w2.shutdown();
        } finally {
            timer.shutdown();
        }
    }

    @Test
    public void massCancel() throws InterruptedException {
        TimedScheduler timer = Schedulers.newTimer("test-timer");
        
        try {
            TimedWorker w1 = timer.createWorker();
    
            AtomicInteger counter = new AtomicInteger();
            
            Runnable task = counter::getAndIncrement;
            
            for (int i = 0; i < 10; i++) {
                w1.schedule(task, 500, TimeUnit.MILLISECONDS);
            }
            
            w1.shutdown();
            
            Thread.sleep(1000);
            
            Assert.assertEquals(0, counter.get());
        } finally {
            timer.shutdown();
        }
    }

}

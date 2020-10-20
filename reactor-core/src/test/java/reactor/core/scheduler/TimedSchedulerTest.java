/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
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

import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler.Worker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.fail;

public class TimedSchedulerTest extends AbstractSchedulerTest {

	@Override
	protected Scheduler scheduler() {
		return Schedulers.newSingle("TimedSchedulerTest");
	}

	@Test
	public void independentWorkers() throws InterruptedException {
		Scheduler timer = Schedulers.newSingle("test-timer");
        
        try {
            Worker w1 = timer.createWorker();
            
            Worker w2 = timer.createWorker();
            
            CountDownLatch cdl = new CountDownLatch(1);
            
            w1.dispose();

			assertThatExceptionOfType(Throwable.class).isThrownBy(() -> {
                w1.schedule(() -> { });
			});

            w2.schedule(cdl::countDown);
            
            if (!cdl.await(1, TimeUnit.SECONDS)) {
                fail("Worker 2 didn't execute in time");
            }
            w2.dispose();
        } finally {
            timer.dispose();
        }
    }

    @Test
    public void massCancel() throws InterruptedException {
        Scheduler timer = Schedulers.newSingle("test-timer");
        
        try {
            Worker w1 = timer.createWorker();
    
            AtomicInteger counter = new AtomicInteger();
            
            Runnable task = counter::getAndIncrement;

	        int tasks = 10;

	        Disposable[] c = new Disposable[tasks];

	        for (int i = 0; i < tasks; i++) {
		        c[i] = w1.schedulePeriodically(task, 500, 500, TimeUnit.MILLISECONDS);
	        }
            
            w1.dispose();

	        for (int i = 0; i < tasks; i++) {
		        assertThat(c[i].isDisposed()).isTrue();
	        }
            
            assertThat(counter).hasValue(0);
        }
        finally {
	        timer.dispose();
        }
    }

}

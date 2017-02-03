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
import reactor.core.Cancellation;
import reactor.core.Disposable;
import reactor.core.scheduler.TimedScheduler.TimedWorker;

import static org.assertj.core.api.Assertions.assertThat;

public class SingleTimedSchedulerTest extends AbstractSchedulerTest {

	@Override
	protected TimedScheduler scheduler() {
		return Schedulers.timer();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void unsupportedStart() throws Exception {
		Schedulers.timer()
		          .start();
	}

	@Test(timeout = 10000)
	final public void directScheduleAndDisposeDelay() throws Exception {
		TimedScheduler s = scheduler();

		try {
			assertThat(s.isDisposed()).isFalse();
			CountDownLatch latch = new CountDownLatch(1);
			CountDownLatch latch2 = new CountDownLatch(1);
			Cancellation c = s.schedule(() -> {
				try {
					latch.countDown();
					latch2.await(10, TimeUnit.SECONDS);
				}
				catch (InterruptedException e) {
				}
			}, 10, TimeUnit.MILLISECONDS);
			Disposable d = (Disposable) c;

			latch.await();
			assertThat(d.isDisposed()).isFalse();
			d.dispose();

			Thread.yield();

			latch2.countDown();

			s.shutdown();
			assertThat(s.isDisposed()).isFalse();

			c = s.schedule(() -> {
			});

			d = (Disposable) c;
			assertThat(d.isDisposed()).isFalse();
			d.dispose();
			assertThat(d.isDisposed()).isTrue();
		}
		finally {
			s.shutdown();
		}
	}

	@Test(timeout = 10000)
	final public void workerScheduleAndDisposeDelay() throws Exception {
		TimedScheduler s = scheduler();
		try {
			TimedScheduler.TimedWorker w = s.createWorker();

			assertThat(w.isDisposed()).isFalse();
			CountDownLatch latch = new CountDownLatch(1);
			CountDownLatch latch2 = new CountDownLatch(1);
			Cancellation c = w.schedule(() -> {
				try {
					latch.countDown();
					latch2.await(10, TimeUnit.SECONDS);
				}
				catch (InterruptedException e) {
				}
			}, 10, TimeUnit.MILLISECONDS);
			Disposable d = (Disposable) c;

			latch.await();
			assertThat(d.isDisposed()).isFalse();
			d.dispose();

			Thread.yield();

			latch2.countDown();

			w.shutdown();
			assertThat(w.isDisposed()).isTrue();

			c = w.schedule(() -> {
			});

			assertThat(c).isEqualTo(Scheduler.REJECTED);

			d = (Disposable) c;
			assertThat(d.isDisposed()).isTrue();
			d.dispose();
			assertThat(d.isDisposed()).isTrue();
		}
		finally {
			s.shutdown();
		}
	}

	@Test(timeout = 10000)
	final public void directScheduleAndDisposePeriod() throws Exception {
		TimedScheduler s = scheduler();

		try {
			assertThat(s.isDisposed()).isFalse();
			CountDownLatch latch = new CountDownLatch(2);
			CountDownLatch latch2 = new CountDownLatch(1);
			Cancellation c = s.schedulePeriodically(() -> {
				try {
					latch.countDown();
					if (latch.getCount() == 0) {
						latch2.await(10, TimeUnit.SECONDS);
					}
				}
				catch (InterruptedException e) {
				}
			}, 10, 10, TimeUnit.MILLISECONDS);
			Disposable d = (Disposable) c;

			latch.await();
			assertThat(d.isDisposed()).isFalse();
			d.dispose();

			Thread.yield();

			latch2.countDown();

			s.shutdown();
			assertThat(s.isDisposed()).isFalse();

			c = s.schedule(() -> {
			});

			d = (Disposable) c;
			assertThat(d.isDisposed()).isFalse();
			d.dispose();
			assertThat(d.isDisposed()).isTrue();
		}
		finally {
			s.shutdown();
		}
	}

	@Test(timeout = 10000)
	final public void workerScheduleAndDisposePeriod() throws Exception {
		TimedScheduler s = scheduler();
		try {
			TimedScheduler.TimedWorker w = s.createWorker();

			assertThat(w.isDisposed()).isFalse();
			CountDownLatch latch = new CountDownLatch(1);
			CountDownLatch latch2 = new CountDownLatch(1);
			Cancellation c = w.schedulePeriodically(() -> {
				try {
					latch.countDown();
					latch2.await(10, TimeUnit.SECONDS);
				}
				catch (InterruptedException e) {
				}
			}, 10, 10, TimeUnit.MILLISECONDS);
			Disposable d = (Disposable) c;

			latch.await();
			assertThat(d.isDisposed()).isFalse();
			d.dispose();

			Thread.yield();

			latch2.countDown();

			w.shutdown();
			assertThat(w.isDisposed()).isTrue();

			c = w.schedule(() -> {
			});

			assertThat(c).isEqualTo(Scheduler.REJECTED);

			d = (Disposable) c;
			assertThat(d.isDisposed()).isTrue();
			d.dispose();
			assertThat(d.isDisposed()).isTrue();
		}
		finally {
			s.shutdown();
		}
	}

	@Test
	public void independentWorkers() throws InterruptedException {
		TimedScheduler timer = Schedulers.newTimer("test-timer");
        
        try {
            TimedWorker w1 = timer.createWorker();
            
            TimedWorker w2 = timer.createWorker();
            
            CountDownLatch cdl = new CountDownLatch(1);
            
            w1.dispose();
            
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
            w2.dispose();
        } finally {
            timer.dispose();
        }
    }

    @Test
    public void massCancel() throws InterruptedException {
        TimedScheduler timer = Schedulers.newTimer("test-timer");
        
        try {
            TimedWorker w1 = timer.createWorker();
    
            AtomicInteger counter = new AtomicInteger();
            
            Runnable task = counter::getAndIncrement;

	        int tasks = 10;

	        Cancellation[] c = new Cancellation[tasks];

	        for (int i = 0; i < tasks; i++) {
		        c[i] = w1.schedulePeriodically(task, 500, 500, TimeUnit.MILLISECONDS);
	        }
            
            w1.dispose();

	        for (int i = 0; i < tasks; i++) {
		        assertThat(((Disposable) c[i]).isDisposed()).isTrue();
	        }
            
            Assert.assertEquals(0, counter.get());
        }
        finally {
	        timer.shutdown();
        }
    }

}

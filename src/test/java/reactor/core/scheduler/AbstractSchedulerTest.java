/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import org.junit.Assert;
import org.junit.Test;
import reactor.core.Cancellation;
import reactor.core.Disposable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Stephane Maldini
 */
public abstract class AbstractSchedulerTest {

	protected abstract Scheduler scheduler();

	protected boolean shouldCheckInterrupted(){
		return false;
	}

	protected boolean shouldCheckDisposeTask(){
		return true;
	}

	protected boolean shouldCheckMassWorkerDispose(){
		return true;
	}

	@Test(timeout = 10000)
	final public void directScheduleAndDispose() throws Exception {
		Scheduler s = scheduler();
		Scheduler unwrapped = null;
		if(s instanceof Schedulers.CachedScheduler){
			unwrapped = ((Schedulers.CachedScheduler)s).get();
			assertThat(unwrapped).isNotNull();
			if(!(s instanceof TimedScheduler)){
				try{
					((Schedulers.CachedScheduler)s).asTimedScheduler();
					fail("cached as non timed-scheduler");
				}
				catch (UnsupportedOperationException e){
					assertThat(e).hasMessage("Scheduler is not Timed");
				}
			}
		}

		try {
			assertThat(s.isDisposed()).isFalse();
			CountDownLatch latch = new CountDownLatch(1);
			CountDownLatch latch2 = shouldCheckDisposeTask() ? new CountDownLatch(1)
					: null;
			Cancellation c = s.schedule(() -> {
				try{
					latch.countDown();
					if(latch2 != null && !latch2.await(10, TimeUnit.SECONDS) &&
							shouldCheckInterrupted()){
						Assert.fail("Should have interrupted");
					}
				}
				catch (InterruptedException e){
				}
			});
			Disposable d = (Disposable) c;

			latch.await();
			if(shouldCheckDisposeTask()) {
				assertThat(d.isDisposed()).isFalse();
			}
			else{
				d.isDisposed(); //noop
			}
			d.dispose();
			d.dispose();//noop

			Thread.yield();

			if(latch2 != null) {
				latch2.countDown();
			}


			s.shutdown();
			s.dispose();//noop
			if(shouldCheckDisposeTask()) {
				assertThat(s.isDisposed()).isFalse();
			}

			c = s.schedule(() -> {
			});

			d = (Disposable) c;
			assertThat(d.isDisposed()).isFalse();
			d.dispose();
			assertThat(d.isDisposed()).isFalse();
		}
		finally {
			s.shutdown();
			s.dispose();//noop
		}
	}

	@Test(timeout = 10000)
	final public void workerScheduleAndDispose() throws Exception {
		Scheduler s = scheduler();
		try {
			Scheduler.Worker w = s.createWorker();

			assertThat(w.isDisposed()).isFalse();
			CountDownLatch latch = new CountDownLatch(1);
			CountDownLatch latch2 = shouldCheckDisposeTask() ? new CountDownLatch(1)
					: null;
			Cancellation c = w.schedule(() -> {
				try{
					latch.countDown();
					if(latch2 != null && !latch2.await(10, TimeUnit.SECONDS) &&
							shouldCheckInterrupted()){
						Assert.fail("Should have interrupted");
					}
				}
				catch (InterruptedException e){
				}
			});
			Disposable d = (Disposable) c;

			latch.await();
			if(shouldCheckDisposeTask()) {
				assertThat(d.isDisposed()).isFalse();
			}
			d.dispose();
			d.dispose();//noop

			Thread.yield();

			if(latch2 != null) {
				latch2.countDown();
			}

			Disposable[] massCancel;
			if(shouldCheckMassWorkerDispose()) {
				int n = 10;
				massCancel = new Disposable[n];
				Thread current = Thread.currentThread();
				for(int i = 0; i< n; i++){
					massCancel[i] = (Disposable) w.schedule(() -> {
						if(current == Thread.currentThread()){
							return;
						}
						try{
							Thread.sleep(5000);
						}
						catch (InterruptedException ie){

						}
					});
				}
			}
			else{
				massCancel = null;
			}
			w.shutdown();
			w.dispose(); //noop
			assertThat(w.isDisposed()).isTrue();

			if(massCancel != null){
				for(Disposable _d : massCancel){
					assertThat(_d.isDisposed()).isTrue();
				}
			}

			c = w.schedule(() -> {});

			assertThat(c).isEqualTo(Scheduler.REJECTED);

			d = (Disposable) c;
			assertThat(d.isDisposed()).isFalse();
			d.dispose();
			assertThat(d.isDisposed()).isFalse();
		}
		finally {
			s.shutdown();
			s.dispose();//noop
		}
	}
}

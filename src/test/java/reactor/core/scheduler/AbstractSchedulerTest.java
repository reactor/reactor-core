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

import org.assertj.core.api.Condition;
import org.assertj.core.condition.AnyOf;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import reactor.core.Cancellation;
import reactor.core.Disposable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Stephane Maldini
 */
public abstract class AbstractSchedulerTest {

	static final Condition<Scheduler> DISPOSED_OR_CACHED =
			new Condition<>(sched -> sched instanceof Schedulers.CachedScheduler || sched.isDisposed(),
			"a %s scheduler", "disposed or cached");

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
		Scheduler unwrapped;
		if(s instanceof Schedulers.CachedScheduler){
			unwrapped = ((Schedulers.CachedScheduler)s).get();
			assertThat(unwrapped).isNotNull();
		}
		else {
			unwrapped = null;
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

			if(s == ImmediateScheduler.instance()){
				return;
			}
			if(unwrapped == null) {
				assertThat(s.isDisposed()).isTrue();
			}


			c = s.schedule(() -> {
				if(unwrapped == null && shouldCheckInterrupted()){
					try {
						Thread.sleep(10000);
					}
					catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
			});

			d = (Disposable) c;
			if(unwrapped == null) {
				assertThat(c).isEqualTo(Scheduler.REJECTED);
			}
			d.dispose();
			assertThat(d.isDisposed()).isTrue();
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
			d = (Disposable) c;

			assertThat(c).isEqualTo(Scheduler.REJECTED);
			assertThat(d.isDisposed()).isTrue();
		}
		finally {
			s.shutdown();
			s.dispose();//noop
		}
	}

	@Test(timeout = 10000)
	final public void directScheduleAndDisposeDelay() throws Exception {
		Scheduler s = scheduler();

		try {
			Assume.assumeTrue("Scheduler marked as not supporting time scheduling", scheduler().isTimeCapable());
			assertThat(s.isDisposed()).isFalse();
			CountDownLatch latch = new CountDownLatch(1);
			CountDownLatch latch2 = new CountDownLatch(1);
			Disposable d = (Disposable) s.schedule(() -> {
				try {
					latch.countDown();
					latch2.await(10, TimeUnit.SECONDS);
				}
				catch (InterruptedException e) {
				}
			}, 10, TimeUnit.MILLISECONDS);
			assertThat(d).isNotSameAs(Scheduler.REJECTED);

			latch.await();
			assertThat(d.isDisposed()).isFalse();
			d.dispose();

			Thread.yield();

			latch2.countDown();

			s.dispose();
			assertThat(s).is(DISPOSED_OR_CACHED);

			d = (Disposable) s.schedule(() -> { });

			if (!(s instanceof Schedulers.CachedScheduler)) {
				assertThat(d).isSameAs(Scheduler.REJECTED);
			}
			d.dispose();
			assertThat(d.isDisposed()).isTrue();
		}
		finally {
			s.dispose();
		}
	}

	@Test(timeout = 10000)
	final public void workerScheduleAndDisposeDelay() throws Exception {
		Scheduler s = scheduler();
		Scheduler.Worker w = s.createWorker();

		try {
			Assume.assumeTrue("Worker marked as not supporting time scheduling", w.isTimeCapable());

			assertThat(w.isDisposed()).isFalse();
			CountDownLatch latch = new CountDownLatch(1);
			CountDownLatch latch2 = new CountDownLatch(1);
			Disposable d = (Disposable) w.schedule(() -> {
				try {
					latch.countDown();
					latch2.await(10, TimeUnit.SECONDS);
				}
				catch (InterruptedException e) {
				}
			}, 10, TimeUnit.MILLISECONDS);
			assertThat(d).isNotSameAs(Scheduler.REJECTED);

			latch.await();
			assertThat(d.isDisposed()).isFalse();
			d.dispose();

			Thread.yield();

			latch2.countDown();

			w.dispose();
			assertThat(w.isDisposed()).isTrue();

			d = (Disposable) w.schedule(() -> { });

			assertThat(d).isEqualTo(Scheduler.REJECTED);
			assertThat(d.isDisposed()).isTrue();
			d.dispose();
			assertThat(d.isDisposed()).isTrue();
		}
		finally {
			w.dispose();
			s.dispose();
		}
	}

	@Test(timeout = 10000)
	final public void directScheduleAndDisposePeriod() throws Exception {
		Scheduler s = scheduler();

		try {
			Assume.assumeTrue("Scheduler marked as not supporting time scheduling", s.isTimeCapable());
			assertThat(s.isDisposed()).isFalse();
			CountDownLatch latch = new CountDownLatch(2);
			CountDownLatch latch2 = new CountDownLatch(1);
			Disposable d = (Disposable) s.schedulePeriodically(() -> {
				try {
					latch.countDown();
					if (latch.getCount() == 0) {
						latch2.await(10, TimeUnit.SECONDS);
					}
				}
				catch (InterruptedException e) {
				}
			}, 10, 10, TimeUnit.MILLISECONDS);
			assertThat(d).isNotSameAs(Scheduler.REJECTED);

			assertThat(d.isDisposed()).isFalse();

			latch.await();
			d.dispose();

			Thread.yield();

			latch2.countDown();

			s.dispose();
			assertThat(s).is(DISPOSED_OR_CACHED);

			d = (Disposable) s.schedule(() -> { });

			d.dispose();
			assertThat(d.isDisposed()).isTrue();
		}
		finally {
			s.dispose();
		}
	}

	@Test(timeout = 10000)
	final public void workerScheduleAndDisposePeriod() throws Exception {
		Scheduler s = scheduler();
		Scheduler.Worker w = s.createWorker();

		try {
			Assume.assumeTrue("Worker marked as not supporting time scheduling", w.isTimeCapable());

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
			assertThat(d).isNotSameAs(Scheduler.REJECTED);

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
			w.dispose();
			s.dispose();
		}
	}
}

/*
 * Copyright (c) 2019-2026 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.scheduler;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.blockhound.BlockHound;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

@Timeout(5)
public class ReactorBlockHoundIntegrationTest {

	static {
		// Use the builder to load only our integration to avoid false positives
		BlockHound.builder()
		          .with(new ReactorBlockHoundIntegration())
		          .install();
	}

	@Test
	public void shouldDetectBlockingCalls() {
		expectBlockingCall("java.lang.Thread.sleep", future -> {
			Schedulers.parallel()
			          .schedule(() -> {
				          try {
					          Thread.sleep(10);
					          future.complete(null);
				          }
				          catch (Throwable e) {
					          future.completeExceptionally(e);
				          }
			          });
		});
	}

	@Test
	public void shouldDetectBlockingCallsOnSubscribe() {
		expectBlockingCall("java.lang.Thread.yield", future -> {
			Mono.fromRunnable(Thread::yield)
			    .subscribeOn(Schedulers.parallel())
			    .subscribe(future::complete, future::completeExceptionally);
		});
	}

	@Test
	public void shouldDetectBlockingCallsInOperators() {
		expectBlockingCall("java.lang.Thread.yield", future -> {
			Mono.delay(Duration.ofMillis(10))
			    .doOnNext(__ -> Thread.yield())
			    .subscribe(future::complete, future::completeExceptionally);
		});
	}

	@Test
	public void shouldNotReportSchedulerScheduledFutureTask() throws InterruptedException {
		Scheduler scheduler = Schedulers.newSingle("foo");
		try {
			disposeOnNonBlockingThreadWhileQueueLockIsHeld(scheduler::schedule);
		}
		finally {
			scheduler.dispose();
		}
	}

	@Test
	public void shouldNotReportWorkerScheduledFutureTask() throws InterruptedException {
		Scheduler scheduler = Schedulers.newSingle("foo");
		Scheduler.Worker worker = scheduler.createWorker();
		try {
			disposeOnNonBlockingThreadWhileQueueLockIsHeld(worker::schedule);
		}
		finally {
			worker.dispose();
			scheduler.dispose();
		}
	}

	interface DelayedScheduling {

		Disposable schedule(Runnable task, long delay, TimeUnit unit);
	}

	// Disposing a delayed task removes it from the executor's DelayedWorkQueue,
	// which takes the queue's lock. When another thread holds that lock, the
	// disposing thread parks, and BlockHound reports the park unless it happens
	// inside an allowed method (SchedulerTask#dispose, WorkerTask#dispose).
	void disposeOnNonBlockingThreadWhileQueueLockIsHeld(DelayedScheduling scheduling) throws InterruptedException {
		// The tasks are scheduled an hour ahead so that they never run: they only
		// exist to be added to the queue and removed from it again.
		Runnable neverRuns = () -> {
		};

		// Keeps the queue's lock busy from a regular thread, so that the dispose
		// calls on the NonBlocking thread below regularly find it held and park.
		AtomicBoolean done = new AtomicBoolean();
		Thread lockContender = new Thread(() -> {
			while (!done.get()) {
				scheduling.schedule(neverRuns, 1, TimeUnit.HOURS)
				          .dispose();
			}
		}, "lockContender");
		lockContender.start();

		try {
			// Schedulers.parallel() threads are NonBlocking, so BlockHound checks
			// every dispose call made here.
			Mono.fromRunnable(() -> {
				    for (int i = 0; i < 10_000; i++) {
					    scheduling.schedule(neverRuns, 1, TimeUnit.HOURS)
					              .dispose();
				    }
			    })
			    .subscribeOn(Schedulers.parallel())
			    .block(Duration.ofSeconds(4));
		}
		finally {
			done.set(true);
			lockContender.join();
		}
	}

	void expectBlockingCall(String desc, Consumer<CompletableFuture<Object>> callable) {
		Assertions
				.assertThatThrownBy(() -> {
					CompletableFuture<Object> future = new CompletableFuture<>();
					callable.accept(future);
					future.join();
				})
				.hasMessageContaining("Blocking call! " + desc);
	}
}

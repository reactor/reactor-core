/*
 * Copyright (c) 2026 VMware Inc. or its affiliates, All Rights Reserved.
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
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SchedulerStateTest {

	// see https://github.com/reactor/reactor-core/issues/4478
	@Test
	void transitionAwaitsOnDaemonThread() {
		AtomicReference<Thread> awaitingThread = new AtomicReference<>();
		SchedulerState<String> state = SchedulerState.transition("initial", "next",
				(resource, timeout, unit) -> {
					awaitingThread.set(Thread.currentThread());
					return true;
				});

		state.onDispose.block(Duration.ofSeconds(5));

		Thread thread = awaitingThread.get();
		assertThat(thread).isNotNull();
		assertThat(thread.isDaemon()).as("daemon").isTrue();
		assertThat(thread.getName()).startsWith("reactor-dispose-awaiter-");
	}

	@Test
	void awaiterThreadHasNormalPriorityRegardlessOfCaller() throws InterruptedException {
		AtomicReference<Thread> created = new AtomicReference<>();
		Thread caller = new Thread(() -> created.set(
				SchedulerState.DisposeAwaiterRunnable.newAwaiterThread(() -> {})));
		caller.setPriority(Thread.MIN_PRIORITY);
		caller.start();
		caller.join(5000);

		Thread thread = created.get();
		assertThat(thread).isNotNull();
		assertThat(thread.getPriority()).as("priority").isEqualTo(Thread.NORM_PRIORITY);
		assertThat(thread.isDaemon()).as("daemon").isTrue();
		assertThat(thread.getName()).startsWith("reactor-dispose-awaiter-");
	}

	@Test
	void disposeGracefullyLeavesNoNonDaemonAwaiterThread() {
		Schedulers.newSingle("disposeGracefullyLeavesNoNonDaemonAwaiterThread")
		          .disposeGracefully()
		          .block(Duration.ofSeconds(5));

		List<Thread> awaiterThreads = Thread.getAllStackTraces()
		                                    .keySet()
		                                    .stream()
		                                    .filter(t -> t.getName().startsWith("reactor-dispose-awaiter-"))
		                                    .collect(Collectors.toList());

		assertThat(awaiterThreads).isNotEmpty()
		                          .allMatch(Thread::isDaemon, "daemon");
	}
}

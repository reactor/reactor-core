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

package reactor.core.publisher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

class SinksVirtualThreadTest {

	@Test
	@Timeout(10)
	void busyLoopingDoesNotStarveVirtualThreads() throws InterruptedException {
		int carrierThreads = Runtime.getRuntime().availableProcessors();
		Sinks.Many<String> sink = Sinks.many().multicast().directAllOrNothing();
		CountDownLatch guardHeld = new CountDownLatch(1);
		CountDownLatch releaseGuard = new CountDownLatch(1);
		List<Thread> emitters = new ArrayList<>();

		sink.asFlux().subscribe(value -> {
			guardHeld.countDown();
			try {
				releaseGuard.await();
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});

		Thread guardHolder = Thread.ofPlatform().name("sink-guard-holder").start(() ->
				sink.emitNext("guard", Sinks.EmitFailureHandler.FAIL_FAST));
		assertThat(guardHeld.await(5, TimeUnit.SECONDS)).isTrue();

		try {
			for (int i = 0; i < carrierThreads; i++) {
				emitters.add(Thread.ofVirtual().name("sink-busy-loop-" + i).start(() ->
						sink.emitNext("value", Sinks.EmitFailureHandler.busyLooping(Duration.ofSeconds(5)))));
			}

			CountDownLatch probeRan = new CountDownLatch(1);
			Thread.ofVirtual().name("sink-probe").start(probeRan::countDown);

			assertThat(probeRan.await(2, TimeUnit.SECONDS)).isTrue();
		}
		finally {
			releaseGuard.countDown();
			guardHolder.join();
			for (Thread emitter : emitters) {
				emitter.join();
			}
		}
	}
}

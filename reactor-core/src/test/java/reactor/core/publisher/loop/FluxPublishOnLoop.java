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
package reactor.core.publisher.loop;

import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxPublishOnTest;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

/**
 * @author Stephane Maldini
 */
public class FluxPublishOnLoop {

	final FluxPublishOnTest publishOnTest = new FluxPublishOnTest();

	@BeforeAll
	public static void before() {
		FluxPublishOnTest.exec = Executors.newSingleThreadExecutor();
	}

	@AfterAll
	public static void after() {
		FluxPublishOnTest.exec.shutdownNow();
	}

	@Test
	public void prefetchAmountOnlyLoop() {
		for (int i = 0; i < 1000; i++) {
			publishOnTest.prefetchAmountOnly();
		}
	}

	@Test
	public void diamondLoop() {
		for (int i = 0; i < 1000; i++) {
			publishOnTest.diamond();
		}
	}

	@Test
	public void boundedQueueLoop() {
		for (int i = 0; i < 1000; i++) {
			publishOnTest.boundedQueue();
		}
	}

	@Test
	public void boundedQueueFilterLoop() {
		for (int i = 0; i < 1000; i++) {
			publishOnTest.boundedQueueFilter();
		}
	}
	@Test
	public void withFlatMapLoop() {
		for (int i = 0; i < 200; i++) {
			publishOnTest.withFlatMap();
		}
	}

	@Test
	public void crossRangeMaxHiddenLoop() throws Exception {
		for (int i = 0; i < 10; i++) {
			publishOnTest.crossRangeMaxHidden();
		}
	}

	@Test
	public void crossRangeMaxLoop() {
		for (int i = 0; i < 50; i++) {
			publishOnTest.crossRangeMax();
		}
	}

	@Test
	public void crossRangeMaxUnboundedLoop() {
		for (int i = 0; i < 50; i++) {
			publishOnTest.crossRangeMaxUnbounded();
		}
	}

	@Test
	public void crossRangePerfDefaultLoop() {
		for (int i = 0; i < 100000; i++) {
			if (i % 2000 == 0) {
				publishOnTest.crossRangePerfDefault();
			}
		}
	}

	@Test
	public void crossRangePerfDefaultLoop2() {
		Scheduler scheduler = Schedulers.fromExecutorService(FluxPublishOnTest.exec);

		int count = 1000;

		for (int j = 1; j < 256; j *= 2) {

			Flux<Integer> source = Flux.range(1, count)
			                           .flatMap(v -> Flux.range(v, 2), 128, j)
			                           .publishOn(scheduler);

			StepVerifier.Step<Integer> v = StepVerifier.create(source)
			            .expectNextCount(count * 2);

			for (int i = 0; i < 10000; i++) {
				v.verifyComplete();
			}
		}
	}

	@Test
	public void mapNotifiesOnceConsistent() throws InterruptedException {
		for (int i = 0; i < 100; i++) {
			publishOnTest.mapNotifiesOnce();
		}
	}

	@Test
	public void mapManyFlushesAllValuesConsistently() throws InterruptedException {
		int iterations = 5;
		for (int i = 0; i < iterations; i++) {
			publishOnTest.mapManyFlushesAllValuesThoroughly();
		}
	}
}

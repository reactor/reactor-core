/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.util;

import java.util.concurrent.Executors;

import org.junit.jupiter.api.Test;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * This test case should be OK to run in the normal test source set and is intended
 * for a test sourceset that doesn't include Micrometer dependency, and which only runs
 * tests validating the likes of {@link Flux#metrics()} are NO-OP in that context.
 *
 * <p>For tests that validate metrics usage when micrometer <em>is</em> present,
 * please have a look at the {@code withMicrometerTest} sourceset.</p>
 *
 * @author Simon Baslé
 */
public class MetricsNoMicrometerTest {

	@Test
	public void isMicrometerAvailable() {
		assertThat(Metrics.isInstrumentationAvailable()).isFalse();
	}

	@Test
	public void FluxMetricsNoOp() {
		assertThatCode(() -> Flux.just("foo").hide().metrics().blockLast())
				.doesNotThrowAnyException();
	}

	@Test
	public void FluxMetricsFusedNoOp() {
		assertThatCode(() -> Flux.just("foo").metrics().blockLast())
				.doesNotThrowAnyException();
	}

	@Test
	public void MonoMetricsNoOp() {
		assertThatCode(() -> Mono.just("foo").hide().metrics().block())
				.doesNotThrowAnyException();
	}

	@Test
	public void MonoMetricsFusedNoOp() {
		assertThatCode(() -> Mono.just("foo").metrics().block())
				.doesNotThrowAnyException();
	}

	@Test
	public void schedulersInstrumentation() {
		try {
			assertThatCode(() -> {
				Schedulers.enableMetrics();
				Scheduler s = Schedulers.newSingle("foo");
				Schedulers.decorateExecutorService(s,
						Executors.newSingleThreadScheduledExecutor());
				s.schedule(() -> System.out.println("schedulers instrumentation no micrometer"));
			})
					.doesNotThrowAnyException();
		}
		finally {
			Schedulers.disableMetrics();
		}
	}

}

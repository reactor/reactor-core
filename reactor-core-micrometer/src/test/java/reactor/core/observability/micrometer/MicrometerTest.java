/*
 * Copyright (c) 2022-2023 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.observability.micrometer;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import reactor.core.scheduler.Scheduler;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
class MicrometerTest {

	@Test
	void metricsUsesSpecifiedClockAndRegistry() {
		SimpleMeterRegistry customLocalRegistry = new SimpleMeterRegistry();
		MicrometerMeterListenerFactory<?> factory = (MicrometerMeterListenerFactory<?>) Micrometer.metrics(customLocalRegistry);

		assertThat(factory.registry).as("registry").isSameAs(customLocalRegistry).isNotSameAs(Metrics.globalRegistry);
	}

	@Test
	void observationContextKeySmokeTest() {
		assertThat(MicrometerObservationListener.CONTEXT_KEY_OBSERVATION)
			.isEqualTo(ObservationThreadLocalAccessor.KEY);
	}

	@Test
	void timedSchedulerReturnsAConfiguredTimedScheduler() {
		Scheduler mockScheduler = Mockito.mock(Scheduler.class);
		Scheduler.Worker mockWorker = Mockito.mock(Scheduler.Worker.class);
		Mockito.when(mockScheduler.createWorker()).thenReturn(mockWorker);

		final MeterRegistry registry = new SimpleMeterRegistry();
		final Tags tags = Tags.of("1", "A", "2", "B");
		final String prefix =  "testSchedulerMetrics";

		Scheduler test = Micrometer.timedScheduler(mockScheduler, registry, prefix, tags);

		assertThat(test).isInstanceOfSatisfying(TimedScheduler.class, ts -> {
			assertThat(ts.delegate).as("delegate").isSameAs(mockScheduler);
			assertThat(ts.registry).as("registry").isSameAs(registry);
			//we verify the tags and prefix we passed made it to at least one meter.
			//this is more about the Micrometer passing down the params than it is about checking _all_ meters in the actual class.
			Meter.Id id = ts.submittedDirect.getId();
			assertThat(id.getName()).as("prefix used")
				.isEqualTo("testSchedulerMetrics.scheduler.tasks.submitted");
			assertThat(id.getTags()).as("tags")
				.containsExactlyElementsOf(tags.and("submission.type", "direct"));
		});
	}
}
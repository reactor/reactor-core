/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.jetbrains.annotations.TestOnly;
import org.junit.Test;
import org.reactivestreams.Subscription;

import static org.assertj.core.api.Assertions.assertThat;


public class MicrometerMetricsTest {

	@Test
	public void testUsesMicrometer() {
		AtomicReference<Subscription> subRef = new AtomicReference<>();

		new FluxMetrics<>(Flux.just("foo"))
				.doOnSubscribe(subRef::set)
				.subscribe();

		assertThat(subRef.get()).isInstanceOf(FluxMetrics.MicrometerMetricsSubscriber.class);
	}


	//TODO add proper tests for metrics

	@Test
	public void pickupMetrics() {
		Metrics.addRegistry(new SimpleMeterRegistry());

		final Flux<Integer> test = Flux.range(1, 100)
		                               .delayElements(Duration.ofMillis(100))
		                               .log()
		                               .metrics()
		                               .limitRate(2)
		                               .take(10);

		test.subscribe();
		test.subscribe();
		test.subscribe();
		test.blockLast();

		Metrics.globalRegistry.forEachMeter(
				m -> {
					StringBuilder sb = new StringBuilder(m.getId().getName());
					m.measure().forEach(measure -> sb.append(' ').append(measure.toString()));
					System.out.println(sb);
				});
	}

}

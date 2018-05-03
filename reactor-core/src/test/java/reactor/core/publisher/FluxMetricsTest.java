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

import java.util.concurrent.atomic.AtomicReference;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;

/**
 * @author Simon Baslé
 */
public class FluxMetricsTest {

	private MeterRegistry registry;

	@Before
	public void setupRegistry() {
		registry = new SimpleMeterRegistry();
		Metrics.addRegistry(registry);
	}

	@After
	public void removeRegistry() {
		Metrics.removeRegistry(registry);
	}

	@Test
	public void sequenceNameFromScanUnavailable() {
		Flux<String> delegate = Flux.just("foo");
		Flux<String> source = new Flux<String>() {

			@Override
			public void subscribe(CoreSubscriber<? super String> actual) {
				delegate.subscribe(actual);
			}
		};
		FluxMetrics<String> test = new FluxMetrics<>(source);

		assertThat(Scannable.from(source).isScanAvailable())
				.as("source scan unavailable").isFalse();
		assertThat(test.name).isEqualTo(FluxMetrics.REACTOR_DEFAULT_NAME);
	}

	@Test
	public void sequenceNameFromScannableNoName() {
		Flux<String> source = Flux.just("foo");
		FluxMetrics<String> test = new FluxMetrics<>(source);

		assertThat(Scannable.from(source).isScanAvailable())
				.as("source scan available").isTrue();
		assertThat(test.name).isEqualTo(FluxMetrics.REACTOR_DEFAULT_NAME);
	}

	@Test
	public void sequenceNameFromScannableName() {
		Flux<String> source = Flux.just("foo").name("foo").hide().hide();
		FluxMetrics<String> test = new FluxMetrics<>(source);

		assertThat(Scannable.from(source).isScanAvailable())
				.as("source scan available").isTrue();
		assertThat(test.name).isEqualTo("foo");
	}

	@Test
	public void testUsesMicrometer() {
		AtomicReference<Subscription> subRef = new AtomicReference<>();

		new FluxMetrics<>(Flux.just("foo").hide())
				.doOnSubscribe(subRef::set)
				.subscribe();

		assertThat(subRef.get()).isInstanceOf(FluxMetrics.MicrometerMetricsSubscriber.class);
	}

	@Test
	public void splitMetricsOnName() {
		final Flux<Integer> unnamed = Flux.<Integer>error(new IllegalStateException("boom"))
				.metrics()
				.onErrorResume(e -> Mono.empty());
		final Flux<Integer> named = Flux.range(1, 40)
		                                .name("foo")
		                                .metrics();
		Mono.when(unnamed, named).block();

		Timer unnamedMeter = registry
				.find(FluxMetrics.METER_SUBSCRIBE_TO_TERMINATE)
				.tag(FluxMetrics.TAG_SEQUENCE_NAME, FluxMetrics.REACTOR_DEFAULT_NAME)
				.timer();

		Timer namedMeter = registry
				.find(FluxMetrics.METER_SUBSCRIBE_TO_TERMINATE)
				.tag(FluxMetrics.TAG_TERMINATION_TYPE, FluxMetrics.TAGVALUE_ON_ERROR)
				.tag(FluxMetrics.TAG_SEQUENCE_NAME, "foo")
				.timer();

		assertThat(unnamedMeter).isNotNull();
		assertThat(namedMeter).isNotNull();

		assertThat(unnamedMeter.count()).isEqualTo(1L);
		assertThat(namedMeter.count()).isZero();
	}

	@Test //FIXME
	public void usesTags() {
		Flux.range(1, 8)
		    .tag("tag1", "A")
		    .name("usesTags")
		    .tag("tag2", "B")
		    .metrics()
		    .blockLast();

		Flux.range(1, 100)
		    .tag("tag1", "A")
		    .name("usesTags")
		    .tag("tag2", "A")
		    .metrics()
		    .blockLast();

		Timer meterGlobal = registry
				.find(FluxMetrics.METER_ON_NEXT_DELAY)
				.tag(FluxMetrics.TAG_SEQUENCE_NAME, "usesTags")
				.timer();

		Timer meterTag1 = registry
				.find(FluxMetrics.METER_ON_NEXT_DELAY)
				.tag(FluxMetrics.TAG_SEQUENCE_NAME, "usesTags")
				.tag("tag1", "A")
				.timer();

		Timer meterTag1And2 = registry
				.find(FluxMetrics.METER_ON_NEXT_DELAY)
				.tag(FluxMetrics.TAG_SEQUENCE_NAME, "usesTags")
				.tag("tag1", "A")
				.tag("tag2", "A")
				.timer();

		assertThat(meterGlobal).isNotNull();
		assertThat(meterTag1).isNotNull();
		assertThat(meterTag1And2).isNotNull();

		assertSoftly(softly -> {
			softly.assertThat(meterGlobal.count()).as("meterGlobal").isEqualTo(108L);
			softly.assertThat(meterGlobal.count()).as("meterTag1").isEqualTo(8L);
			softly.assertThat(meterGlobal.count()).as("meterTag1And2").isEqualTo(100L);
		});
	}

	@Test
	public void onNextTimerCounts() {
		Flux.range(1, 123)
		    .metrics()
		    .blockLast();

		Timer nextMeter = registry
				.find(FluxMetrics.METER_ON_NEXT_DELAY)
				.timer();

		assertThat(nextMeter).isNotNull();
		assertThat(nextMeter.count()).isEqualTo(123L);

		Flux.range(1, 10)
		    .metrics()
		    .take(3)
		    .blockLast();

		assertThat(nextMeter.count()).isEqualTo(126L);

		Flux.range(1, 1000)
		    .name("foo")
		    .metrics()
		    .blockLast();

		assertThat(nextMeter.count())
				.as("notTakingNamedIntoAccount")
				.isEqualTo(126L);
	}


	//TODO add further tests for metrics

}
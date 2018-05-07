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
		assertThat(test.name).isEqualTo(reactor.util.Metrics.REACTOR_DEFAULT_NAME);
	}

	@Test
	public void sequenceNameFromScannableNoName() {
		Flux<String> source = Flux.just("foo");
		FluxMetrics<String> test = new FluxMetrics<>(source);

		assertThat(Scannable.from(source).isScanAvailable())
				.as("source scan available").isTrue();
		assertThat(test.name).isEqualTo(reactor.util.Metrics.REACTOR_DEFAULT_NAME);
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
				.find(reactor.util.Metrics.METER_SUBSCRIBE_TO_TERMINATE)
				.tag(reactor.util.Metrics.TAG_SEQUENCE_NAME, reactor.util.Metrics.REACTOR_DEFAULT_NAME)
				.timer();

		Timer namedMeter = registry
				.find(reactor.util.Metrics.METER_SUBSCRIBE_TO_TERMINATE)
				.tag(reactor.util.Metrics.TAG_TERMINATION_TYPE, reactor.util.Metrics.TAGVALUE_ON_ERROR)
				.tag(reactor.util.Metrics.TAG_SEQUENCE_NAME, "foo")
				.timer();

		assertThat(unnamedMeter).isNotNull();
		assertThat(namedMeter).isNotNull();

		assertThat(unnamedMeter.count()).isEqualTo(1L);
		assertThat(namedMeter.count()).isZero();
	}

	@Test
	public void usesTags() {
		Flux.range(1, 8)
		    .tag("tag1", "A")
		    .name("usesTags")
		    .tag("tag2", "foo")
		    .metrics()
		    .blockLast();

		Timer meter = registry
				.find(reactor.util.Metrics.METER_ON_NEXT_DELAY)
				.tag(reactor.util.Metrics.TAG_SEQUENCE_NAME, "usesTags")
				.tag("tag1", "A")
				.tag("tag2", "foo")
				.timer();

		assertThat(meter).isNotNull();
		assertThat(meter.count()).isEqualTo(8L);
	}

	@Test
	public void onNextTimerCounts() {
		Flux.range(1, 123)
		    .metrics()
		    .blockLast();

		Timer nextMeter = registry
				.find(reactor.util.Metrics.METER_ON_NEXT_DELAY)
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
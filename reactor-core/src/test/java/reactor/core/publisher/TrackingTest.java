/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.publisher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;

import static org.assertj.core.api.Assertions.assertThat;

public class TrackingTest {

	@Rule
	public TestName testName = new TestName();

	@After
	public void tearDown() {
		new HashSet<>(Hooks.getTrackers().keySet()).forEach(Hooks::removeTracker);
		Tracker.Marker.COUNTER.set(0);
	}

	@Test
	public void shouldCreateMarker() {
		TestTracker tracker = new TestTracker();
		Hooks.addTracker(testName.getMethodName(), tracker);

		Flux.just(1).blockLast();

		assertThat(tracker.getMarkersRecorded()).hasSize(1);
	}

	@Test
	public void shouldCreateMarkerFromThread() throws InterruptedException {
		TestTracker tracer = new TestTracker();
		Hooks.addTracker(testName.getMethodName(), tracer);

		CountDownLatch latch = new CountDownLatch(1);
		Flux
			.just(1, 2, 3)
			.flatMap(it -> {
				return Mono.delay(Duration.ofMillis(1), Schedulers.elastic())
				           .doOnNext(__ -> {
					           Tracker.Marker currentMarker = tracer.getCurrentMarker();
					           assertThat(currentMarker).isNotNull();
				           });
			})
			.subscribe(new Subscriber<Object>() {

				@Override
				public void onSubscribe(Subscription s) {
					new Thread(() -> s.request(3)).start();
				}

				@Override
				public void onNext(Object integer) {

				}

				@Override
				public void onError(Throwable t) {

				}

				@Override
				public void onComplete() {
					latch.countDown();
				}
			});

		latch.await(2, TimeUnit.SECONDS);

		assertThat(tracer.getMarkersRecorded()).hasSize(4);
	}

	@Test
	public void shouldPropagateMarkerToThreads() {
		TestTracker tracer = new TestTracker();
		Hooks.addTracker(testName.getMethodName(), tracer);

		Flux.just(1, 2, 3)
		    .publishOn(Schedulers.parallel())
		    .concatMap(it -> {
			    return Mono.delay(Duration.ofMillis(1), Schedulers.elastic())
			               .doOnNext(__ -> {
				               Tracker.Marker currentMarker = tracer.getCurrentMarker();
				               assertThat(currentMarker)
						               .isNotNull();
			               });
		    })
		    .publishOn(Schedulers.parallel())
		    .concatMap(it -> {
			    return Mono.delay(Duration.ofMillis(1), Schedulers.elastic())
			               .doOnNext(__ -> {
				               Tracker.Marker currentMarker = tracer.getCurrentMarker();
				               assertThat(currentMarker)
						               .isNotNull();
			               });
		    })
				.blockLast();

		assertThat(tracer.getMarkersRecorded())
				.hasSize(7);

		// 0 <- 1 <- 2 <- 4
		assertThat(tracer.getMarkersRecorded().get(1).getParent())
				.isEqualTo(tracer.getMarkersRecorded().get(0));

		assertThat(tracer.getMarkersRecorded().get(2).getParent())
				.isEqualTo(tracer.getMarkersRecorded().get(1));

		assertThat(tracer.getMarkersRecorded().get(4).getParent())
				.isEqualTo(tracer.getMarkersRecorded().get(2));

		/////////////////////////////////////////////////////////////
		// 0 <- 1 <- 3 <- 5 <- 6
		assertThat(tracer.getMarkersRecorded().get(3).getParent())
				.isEqualTo(tracer.getMarkersRecorded().get(1));

		assertThat(tracer.getMarkersRecorded().get(5).getParent())
				.isEqualTo(tracer.getMarkersRecorded().get(3));

		assertThat(tracer.getMarkersRecorded().get(6).getParent())
				.isEqualTo(tracer.getMarkersRecorded().get(5));
	}

	private static class TestTracker implements Tracker {

		final List<Marker>        markersRecorded = new ArrayList<>();
		final ThreadLocal<Marker> currentMarker   = new ThreadLocal<>();

		boolean shouldTrace = true;

		List<Marker> getMarkersRecorded() {
			return markersRecorded;
		}

		Marker getCurrentMarker() {
			return currentMarker.get();
		}

		@Override
		public boolean shouldCreateMarker() {
			return shouldTrace;
		}

		@Override
		public void onMarkerCreated(Marker marker) {
			markersRecorded.add(marker);
		}

		@Override
		public Disposable onScopePassing(Marker marker) {
			currentMarker.set(marker);
			return () -> currentMarker.set(null);
		}
	}
}
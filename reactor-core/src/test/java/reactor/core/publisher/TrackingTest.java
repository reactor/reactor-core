/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.publisher;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;

public class TrackingTest {

	@Rule
	public TestName testName = new TestName();

	@After
	public void tearDown() {
		new HashSet<>(Hooks.getContextTrackers().keySet()).forEach(Hooks::removeContextTracker);
		TestContextTracker.Marker.COUNTER.set(0);
	}

	@Test
	public void shouldCreateMarker() {
		TestContextTracker tracker = new TestContextTracker();
		Hooks.addContextTracker(testName.getMethodName(), tracker);

		Flux.just(1).blockLast();

		assertThat(tracker.getMarkersRecorded()).hasSize(1);
	}

	@Test
	public void shouldCreateMarkerFromThread() throws InterruptedException {
		TestContextTracker tracer = new TestContextTracker();
		Hooks.addContextTracker(testName.getMethodName(), tracer);

		CountDownLatch latch = new CountDownLatch(1);
		Flux
			.just(1, 2, 3)
			.flatMap(it -> {
				return Mono.delay(Duration.ofMillis(1), Schedulers.elastic())
				           .doOnNext(__ -> {
					           TestContextTracker.Marker currentMarker = tracer.getCurrentMarker();
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
		TestContextTracker tracer = new TestContextTracker();
		Hooks.addContextTracker(testName.getMethodName(), tracer);

		Flux.just(1, 2, 3)
		    .publishOn(Schedulers.parallel())
		    .concatMap(it -> {
			    return Mono.delay(Duration.ofMillis(1), Schedulers.elastic())
			               .doOnNext(__ -> {
				               TestContextTracker.Marker currentMarker = tracer.getCurrentMarker();
				               assertThat(currentMarker).isNotNull();
			               });
		    })
		    .publishOn(Schedulers.parallel())
		    .concatMap(it -> {
			    return Mono.delay(Duration.ofMillis(1), Schedulers.elastic())
			               .doOnNext(__ -> {
				               TestContextTracker.Marker currentMarker = tracer.getCurrentMarker();
				               assertThat(currentMarker).isNotNull();
			               });
		    })
		    .blockLast();

		assertThat(tracer.getMarkersRecorded())
				.hasSize(7);

		TestContextTracker.Marker root = tracer
				.getMarkersRecorded()
				.stream()
				.filter(it -> it.getParent() == null)
				.findFirst()
				.orElseThrow(() -> {
					return new AssertionError("Parentless marker is missing");
				});

		assertThat(tracer.getMarkersRecorded()).allSatisfy(marker -> {
			TestContextTracker.Marker parent = marker;
			while (parent.getParent() != null) {
				parent = parent.getParent();
			}

			assertThat(parent).as("parent of " + marker).isEqualTo(root);
		});
	}

	private static class TestContextTracker implements ContextTracker {

		static final Logger log = LoggerFactory.getLogger(TestContextTracker.class);

		final List<Marker>        markersRecorded = new ArrayList<>();
		final ThreadLocal<Marker> currentMarker   = new ThreadLocal<>();

		List<Marker> getMarkersRecorded() {
			return markersRecorded;
		}

		Marker getCurrentMarker() {
			return currentMarker.get();
		}

		@Override
		public Context onSubscribe(Context context) {
			Marker parent = context.getOrDefault(Marker.class, null);
			Marker marker = new Marker(parent);
			markersRecorded.add(marker);
			currentMarker.set(marker);
			return context.put(Marker.class, marker);
		}

		@Override
		public Disposable onContextPassing(Context context) {
			Marker marker = context.get(Marker.class);

			log.info("onContextPassing(" + marker + ")");
			currentMarker.set(marker);
			return () -> {
				log.info("onContextPassingCleanup(" + marker + ")");
				currentMarker.set(null);
			};
		}

		static final class Marker {

			static final AtomicLong COUNTER = new AtomicLong();

			@Nullable
			private final Marker parent;

			private final long id = COUNTER.getAndIncrement();

			Marker(@Nullable Marker parent) {
				this.parent = parent;
			}

			@Nullable
			public Marker getParent() {
				return parent;
			}

			@Override
			public String toString() {
				return "Marker{id=" + id + ", parent=" + parent + "}";
			}
		}
	}
}
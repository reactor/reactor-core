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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;

public class TrackingTest {

	@Rule
	public final TestContextTracker tracker = new TestContextTracker();

	@Test
	public void shouldCreateMarker() {
		assertThat(tracker.getMarkersRecorded()).hasSize(0);

		Flux.just(1).map(Object::toString).blockLast();

		assertThat(tracker.getMarkersRecorded()).hasSize(1);
	}

	@Test
	public void shouldCreateNestedMarkers() {
		Flux.just(1, 2, 3)
		    .flatMap(it -> Mono.just(it).hide())
		    .blockLast();

		assertThat(tracker.getMarkersRecorded()).hasSize(4);
	}

	@Test
	public void shouldCreateMultipleMarkers() {
		for (int i = 0; i < 3; i++) {
			Flux.just(1, 2, 3)
			    .flatMap(it -> Mono.just(it).hide())
			    .blockLast();
		}

		assertThat(tracker.getRootMarkers())
				.hasSize(3)
				.allSatisfy(root -> {
					assertThat(tracker.getMarkersRecorded())
							.filteredOn(it -> root.equals(it.getParent()))
							.as("Children of %s", root)
							.hasSize(3);
				});
	}

	@Test
	public void shouldKeepParentChildRelationships() {
		Flux.just(1, 2, 3)
		    .flatMap(it -> Mono.just(it).hide())
		    .blockLast();

		TestContextTracker.Marker root = tracker.getRootMarkers()
		                                        .findFirst()
		                                        .orElseThrow(() -> {
			                                        return new AssertionError("Parentless marker is missing");
		                                        });

		assertThat(tracker.getMarkersRecorded())
				.hasSize(4)
				.allSatisfy(marker -> {
					assertThat(marker.getRoot())
							.as("root of " + marker)
							.isEqualTo(root);
				});
	}

	@Test
	public void shouldCreateMarkerFromThread() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);
		Flux
			.just(1, 2, 3)
			.flatMap(it -> {
				return Mono.delay(Duration.ofMillis(1), Schedulers.elastic())
				           .doOnNext(__ -> {
					           TestContextTracker.Marker currentMarker = tracker.getCurrentMarker();
					           assertThat(currentMarker).isNotNull();
				           });
			})
			.subscribe(
					null,
					null,
					latch::countDown,
					s -> new Thread(() -> s.request(3)).start()
			);

		latch.await(2, TimeUnit.SECONDS);

		assertThat(tracker.getMarkersRecorded()).hasSize(4);
	}

	@Test
	public void shouldPropagateMarkerToThreads() {
		Flux.just(1, 2, 3)
		    .publishOn(Schedulers.parallel())
		    .concatMap(it -> {
			    return Mono.delay(Duration.ofMillis(1), Schedulers.elastic())
			               .doOnNext(__ -> {
				               TestContextTracker.Marker currentMarker = tracker.getCurrentMarker();
				               assertThat(currentMarker).isNotNull();
			               });
		    })
		    .publishOn(Schedulers.parallel())
		    .concatMap(it -> {
			    return Mono.delay(Duration.ofMillis(1), Schedulers.elastic())
			               .doOnNext(__ -> {
				               TestContextTracker.Marker currentMarker = tracker.getCurrentMarker();
				               assertThat(currentMarker).isNotNull();
			               });
		    })
		    .blockLast();

		assertThat(tracker.getMarkersRecorded()).hasSize(7);

		TestContextTracker.Marker root = tracker.getRootMarkers()
		                                        .findFirst()
		                                        .orElseThrow(() -> {
			                                        return new AssertionError("Parentless marker is missing");
		                                        });

		assertThat(tracker.getMarkersRecorded()).allSatisfy(marker -> {
			assertThat(marker.getRoot()).as("root of " + marker).isEqualTo(root);
		});
	}

	private static class TestContextTracker implements ContextTracker, TestRule {

		static final Logger log = LoggerFactory.getLogger(TestContextTracker.class);

		final List<Marker>        markersRecorded = new ArrayList<>();
		final ThreadLocal<Marker> currentMarker   = new ThreadLocal<>();
		final AtomicLong          idGenerator     = new AtomicLong();

		List<Marker> getMarkersRecorded() {
			return markersRecorded;
		}

		Marker getCurrentMarker() {
			return currentMarker.get();
		}

		Stream<Marker> getRootMarkers() {
			return getMarkersRecorded()
					.stream()
					.filter(it -> it.getParent() == null);
		}

		public Statement apply(Statement base, Description description) {
			return new Statement() {
				@Override
				public void evaluate() throws Throwable {
					String key = description.getMethodName();
					Hooks.addContextTracker(key, TestContextTracker.this);
					try {
						base.evaluate();
					}
					finally {
						Hooks.removeContextTracker(key);
					}
				}
			};
		}

		@Override
		public Context onSubscribe(Context context) {
			Marker parent = context.getOrDefault(Marker.class, null);
			Marker marker = new Marker(idGenerator.getAndIncrement(), parent);
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

			@Nullable
			private final Marker parent;

			private final long id;

			Marker(long id, @Nullable Marker parent) {
				this.id = id;
				this.parent = parent;
			}

			@Nullable
			public Marker getParent() {
				return parent;
			}

			public Marker getRoot() {
				TestContextTracker.Marker parent = this;
				while (parent.getParent() != null) {
					parent = parent.getParent();
				}
				return parent;
			}

			@Override
			public String toString() {
				return "Marker{id=" + id + ", parent=" + parent + "}";
			}
		}
	}
}
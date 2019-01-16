/*
 * Copyright (c) 2011-2019 Pivotal Software Inc, All Rights Reserved.
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

import java.util.Collection;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Disposables;

class TrackingReactorPublisherTransformer implements ReactorPublisherTransformer {

	private Disposable onSubscribe() {
		Collection<Tracker> allTrackers = Hooks.getTrackers().values();
		if (allTrackers.stream().noneMatch(Tracker::shouldCreateMarker)) {
			return Disposables.disposed();
		}

		Tracker.Marker parent = Tracker.Marker.CURRENT.get();

		Tracker.Marker current = new Tracker.Marker(parent);
		Tracker.Marker.CURRENT.set(current);

		for (Tracker tracker : allTrackers) {
			tracker.onMarkerCreated(current);
		}

		return () -> Tracker.Marker.CURRENT.set(parent);
	}

	@Override
	public Mono<Object> transform(Mono<Object> source) {
		return new MonoOperator<Object, Object>(source) {

			@Override
			public void subscribe(CoreSubscriber<? super Object> actual) {
				Disposable wrap = onSubscribe();
				try {
					source.subscribe(actual);
				}
				finally {
					wrap.dispose();
				}
			}
		};
	}

	@Override
	public Flux<Object> transform(Flux<Object> source) {
		return new FluxOperator<Object, Object>(source) {

			@Override
			public void subscribe(CoreSubscriber<? super Object> actual) {
				Disposable wrap = onSubscribe();
				try {
					source.subscribe(actual);
				}
				finally {
					wrap.dispose();
				}
			}
		};
	}

	@Override
	public ParallelFlux<Object> transform(ParallelFlux<Object> source) {
		return new ParallelFlux<Object>() {

			@Override
			public int parallelism() {
				return source.parallelism();
			}

			@Override
			protected void subscribe(CoreSubscriber<? super Object>[] subscribers) {
				Disposable wrap = onSubscribe();
				try {
					source.subscribe(subscribers);
				}
				finally {
					wrap.dispose();
				}
			}
		};
	}

	@Override
	public Publisher<Object> transform(Publisher<Object> source) {
		return new Publisher<Object>() {
			@Override
			public void subscribe(Subscriber<? super Object> s) {
				Disposable wrap = onSubscribe();
				try {
					source.subscribe(s);
				}
				finally {
					wrap.dispose();
				}
			}
		};
	}
}

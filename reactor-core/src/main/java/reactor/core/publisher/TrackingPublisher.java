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

import java.util.Collection;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.FluxContextStart.ContextStartSubscriber;
import reactor.util.context.Context;

class TrackingPublisher implements CorePublisher<Object> {

	private final Publisher<Object> source;

	final Collection<Tracker> trackers;

	TrackingPublisher(Publisher<Object> source, Collection<Tracker> trackers) {
		this.source = source;
		this.trackers = trackers;
	}

	@Override
	public void subscribe(CoreSubscriber<? super Object> subscriber) {
		if (trackers.stream().noneMatch(Tracker::shouldCreateMarker)) {
			if (source instanceof CorePublisher) {
				((CorePublisher<Object>) source).subscribe(subscriber);
			}
			else {
				source.subscribe(subscriber);
			}
			return;
		}

		Context context = subscriber.currentContext();
		Tracker.Marker parent = context.getOrDefault(Tracker.Marker.class, null);

		Tracker.Marker current = new Tracker.Marker(parent);
		context = context.put(Tracker.Marker.class, current);

		for (Tracker tracker : trackers) {
			tracker.onMarkerCreated(current);
		}

		if (source instanceof CorePublisher) {
			((CorePublisher<Object>) source).subscribe(new ContextStartSubscriber<>(subscriber, context));
		}
		else {
			source.subscribe(new ContextStartSubscriber<>(subscriber, context));
		}
	}

	@Override
	public void subscribe(Subscriber<? super Object> s) {
		source.subscribe(s);
	}
}

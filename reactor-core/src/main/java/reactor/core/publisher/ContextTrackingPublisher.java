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

class ContextTrackingPublisher implements CorePublisher<Object> {

	private final Publisher<Object> source;

	final Collection<ContextTracker> trackers;

	ContextTrackingPublisher(Publisher<Object> source, Collection<ContextTracker> trackers) {
		this.source = source;
		this.trackers = trackers;
	}

	@Override
	public void subscribe(CoreSubscriber<? super Object> subscriber) {
		Context originalContext = subscriber.currentContext();
		Context context = originalContext;

		for (ContextTracker tracker : trackers) {
			context = tracker.onSubscribe(context);
		}

		if (context != originalContext) {
			if (!context.hasKey(Hooks.KEY_CONTEXT_TRACKING)) {
				context = context.put(Hooks.KEY_CONTEXT_TRACKING, true);
			}
			subscriber = new ContextStartSubscriber<>(subscriber, context);
		}

		if (source instanceof CorePublisher) {
			((CorePublisher<Object>) source).subscribe(subscriber);
		}
		else {
			source.subscribe(subscriber);
		}
	}

	@Override
	public void subscribe(Subscriber<? super Object> s) {
		source.subscribe(s);
	}
}

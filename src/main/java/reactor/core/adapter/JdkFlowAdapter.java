/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.adapter;

import java.util.concurrent.Flow;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

/**
 * Convert a Java 9+ {@literal Flow.Publisher} to/from a Reactive Streams {@link Publisher}.
 *
 * @author Stephane Maldini
 * @author Sebastien Deleuze
 * @since 2.5
 */
public enum JdkFlowAdapter {
	;

	public static <T> Flow.Publisher<T> publisherToFlowPublisher(final Publisher<T>
			publisher) {
		return new PublisherAsFlowPublisher<>(publisher);
	}

	public static <T> Flux flowPublisherToPublisher(Flow.Publisher<T> publisher) {
		return new FlowPublisherAsFlux<>(publisher);
	}

	private static class FlowPublisherAsFlux<T> extends Flux<T> {
        private final java.util.concurrent.Flow.Publisher<T> pub;

        private FlowPublisherAsFlux(java.util.concurrent.Flow.Publisher<T> pub) {
            this.pub = pub;
        }

        @Override
        public void subscribe(final Subscriber<? super T> s) {
        	pub.subscribe(new SubscriberToRS<>(s));
        }
    }

    private static class PublisherAsFlowPublisher<T> implements Flow.Publisher<T> {
        private final Publisher<T> pub;

        private PublisherAsFlowPublisher(Publisher<T> pub) {
            this.pub = pub;
        }

        @Override
        public void subscribe(Flow.Subscriber<? super T> subscriber) {
        	pub.subscribe(new FlowSubscriber<>(subscriber));
        }
    }

    private static class FlowSubscriber<T> implements Subscriber<T> {

		private final Flow.Subscriber<? super T> subscriber;

		public FlowSubscriber(Flow.Subscriber<? super T> subscriber) {
			this.subscriber = subscriber;
		}

		@Override
		public void onSubscribe(final Subscription s) {
			subscriber.onSubscribe(new Flow.Subscription() {
				@Override
				public void request(long l) {
					s.request(l);
				}

				@Override
				public void cancel() {
					s.cancel();
				}
			});
		}

		@Override
		public void onNext(T o) {
			subscriber.onNext(o);
		}

		@Override
		public void onError(Throwable t) {
			subscriber.onError(t);
		}

		@Override
		public void onComplete() {
			subscriber.onComplete();
		}
	}

	private static class SubscriberToRS<T> implements Flow.Subscriber<T> {

		private final Subscriber<? super T> s;

		public SubscriberToRS(Subscriber<? super T> s) {
			this.s = s;
		}

		@Override
		public void onSubscribe(final Flow.Subscription subscription) {
			s.onSubscribe(new Subscription() {
				@Override
				public void request(long n) {
					subscription.request(n);
				}

				@Override
				public void cancel() {
					subscription.cancel();
				}
			});
		}

		@Override
		public void onNext(T o) {
			s.onNext(o);
		}

		@Override
		public void onError(Throwable throwable) {
			s.onError(throwable);
		}

		@Override
		public void onComplete() {
			s.onComplete();
		}
	}
}

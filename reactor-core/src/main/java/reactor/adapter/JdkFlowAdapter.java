/*
 * Copyright (c) 2016-2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.adapter;

import java.util.concurrent.Flow;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;

/**
 * Convert a Java 9+ {@literal Flow.Publisher} to/from a Reactive Streams {@link Publisher}.
 *
 * @author Stephane Maldini
 * @author Sebastien Deleuze
 */
@SuppressWarnings("Since15")
public abstract class JdkFlowAdapter {

	/**
	 * Return a java {@code Flow.Publisher} from a {@link Flux}
	 * @param publisher the source Publisher to convert
	 * @param <T> the type of the publisher
	 * @return a java {@code Flow.Publisher} from the given {@link Publisher}
	 */
	public static <T> Flow.Publisher<T> publisherToFlowPublisher(final Publisher<T>
			publisher) {
		return new PublisherAsFlowPublisher<>(publisher);
	}

	/**
	 * Return a {@link Flux} from a java {@code Flow.Publisher}
	 *
	 * @param publisher the source Publisher to convert
	 * @param <T> the type of the publisher
	 * @return a {@link Flux} from a java {@code Flow.Publisher}
	 */
	public static <T> Flux<T> flowPublisherToFlux(Flow.Publisher<T> publisher) {
		return new FlowPublisherAsFlux<>(publisher);
	}

	private static class FlowPublisherAsFlux<T> extends Flux<T> implements Scannable {
        private final java.util.concurrent.Flow.Publisher<T> pub;

        private FlowPublisherAsFlux(java.util.concurrent.Flow.Publisher<T> pub) {
            this.pub = pub;
        }

        @Override
        public void subscribe(final CoreSubscriber<? super T> actual) {
        	pub.subscribe(new SubscriberToRS<>(actual));
        }

		@Override
		public Object scanUnsafe(Attr key) {
			return null; //no particular key to be represented, still useful in hooks
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

    private static class FlowSubscriber<T> implements CoreSubscriber<T>, Flow.Subscription {

		private final Flow.Subscriber<? super T> subscriber;

		Subscription subscription;

		public FlowSubscriber(Flow.Subscriber<? super T> subscriber) {
			this.subscriber = subscriber;
		}

		@Override
		public void onSubscribe(final Subscription s) {
		    this.subscription = s;
			subscriber.onSubscribe(this);
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

		@Override
		public void request(long n) {
		    subscription.request(n);
		}

		@Override
		public void cancel() {
		    subscription.cancel();
		}
	}

	private static class SubscriberToRS<T> implements Flow.Subscriber<T>, Subscription {

		private final Subscriber<? super T> s;

		Flow.Subscription subscription;

		public SubscriberToRS(Subscriber<? super T> s) {
			this.s = s;
		}

		@Override
		public void onSubscribe(final Flow.Subscription subscription) {
		    this.subscription = subscription;
			s.onSubscribe(this);
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

		@Override
		public void request(long n) {
		    subscription.request(n);
		}

		@Override
		public void cancel() {
		    subscription.cancel();
		}
	}

	JdkFlowAdapter(){}
}

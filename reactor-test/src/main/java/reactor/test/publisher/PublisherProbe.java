/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

package reactor.test.publisher;

import java.util.concurrent.atomic.AtomicLongArray;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.publisher.TestPublisher.Violation;

/**
 * A test utility that allow to easily produce a {@link Publisher} ({@link Mono} or {@link Flux})
 * for tests involving control flow. For instance, say you have a {@link Mono#switchIfEmpty(Mono)}
 * and you want to make sure that your code branched into the "if empty" case.
 * <p>
 * The publisher acts as a probe capturing subscription, cancellation and request events.
 * Later, the {@link PublisherProbe} can be used post completion to check if that
 * particular probe was hit.
 * <p>
 * This is very close to a {@link TestPublisher} but avoids the mental overhead of remembering
 * to 1) use the {@link TestPublisher#emit(Object[]) TestPublisher emit methods} and
 * 2) the {@link Violation#CLEANUP_ON_TERMINATE} so that the subscription state can be checked
 * post-completion...
 *
 * @author Simon Baslé
 */
public interface PublisherProbe<T> {

	/**
	 * Check that the probe was never subscribed to, or throw an {@link AssertionError}.
	 */
	default void assertWasNotSubscribed() {
		if (wasSubscribed()) {
			throw new AssertionError("ControlFlowProbe should not have been subscribed but it was");
		}
	}

	/**
	 * Check that the probe was subscribed to at least once, or throw an {@link AssertionError}.
	 */
	default void assertWasSubscribed() {
		if (!wasSubscribed()) {
			throw new AssertionError("ControlFlowProbe should have been subscribed but it wasn't");
		}
	}

	/**
	 * Check that the probe was never cancelled, or throw an {@link AssertionError}.
	 */
	default void assertWasNotCancelled() {
		if (wasCancelled()) {
			throw new AssertionError("ControlFlowProbe should not have been cancelled but it was");
		}
	}

	/**
	 * Check that the probe was cancelled at least once, or throw an {@link AssertionError}.
	 */
	default void assertWasCancelled() {
		if (!wasCancelled()) {
			throw new AssertionError("ControlFlowProbe should have been cancelled but it wasn't");
		}
	}

	/**
	 * Check that the probe was never requested, or throw an {@link AssertionError}.
	 */
	default void assertWasNotRequested() {
		if (wasRequested()) {
			throw new AssertionError("ControlFlowProbe should not have been requested but it was");
		}
	}

	/**
	 * Check that the probe was requested at least once, or throw an {@link AssertionError}.
	 */
	default void assertWasRequested() {
		if (!wasRequested()) {
			throw new AssertionError("ControlFlowProbe should have been requested but it wasn't");
		}
	}

	/**
	 * Return a {@link Mono} version of the probe. Note all calls to mono() and
	 * {@link #flux()} are backed by the same {@link PublisherProbe} and as such influence
	 * a single state.
	 *<p>
	 * If the probe was {@link #of(Publisher) created out of a Publisher}, the {@link Flux}
	 * will forward the signals from this publisher (up to one {@link Subscriber#onNext(Object)} though).
	 * {@link #empty() Otherwise} it will simply complete.
	 *
	 * @return a {@link Mono} version of the probe.
	 */
	Mono<T> mono();

	/**
	 * Return a {@link Flux} version of the probe. Note all calls to {@link #mono()} and
	 * flux() are backed by the same {@link PublisherProbe} and as such influence
	 * a single state.
	 * <p>
	 * If the probe was {@link #of(Publisher) created out of a Publisher}, the {@link Flux}
	 * will forward the signals from this publisher
	 * {@link #empty() Otherwise} it will simply complete.
	 *
	 * @return a {@link Flux} version of the probe.
	 */
	Flux<T> flux();

	/**
	 * @return true if the probe was subscribed to at least once.
	 */
	boolean wasSubscribed();
	/**
	 * @return true if the probe was cancelled to at least once.
	 */
	boolean wasCancelled();

	/**
	 * @return true if the probe was requested at least once.
	 */
	boolean wasRequested();

	/**
	 * Create a {@link PublisherProbe} out of a {@link Publisher}, ensuring that its
	 * {@link #flux()} and {@link #mono()} versions will propagate signals from this
	 * publisher while capturing subscription, cancellation and request events around it.
	 *
	 * @param source the source publisher to mimic and probe.
	 * @param <T> the type of the source publisher.
	 * @return a probe that mimics the publisher.
	 */
	static <T> PublisherProbe<T> of(Publisher<? extends T> source) {
		return new DefaultPublisherProbe<>(source);
	}

	/**
	 * Create a {@link PublisherProbe} of which {@link #flux()} and {@link #mono()}
	 * versions will simply complete, capturing subscription, cancellation and request
	 * events around them.
	 *
	 * @param <T> the type of the empty probe.
	 * @return a probe that mimics an empty publisher.
	 */
	static <T> PublisherProbe<T> empty() {
		return new DefaultPublisherProbe<>(Mono.empty());
	}

	final class DefaultPublisherProbe<T>
			extends AtomicLongArray
			implements PublisherProbe<T> {

		private static final int SUBSCRIBED = 0;
		private static final int CANCELLED = 1;
		private static final int REQUESTED = 2;

		final Publisher<T> delegate;

		@SuppressWarnings("unchecked")
		DefaultPublisherProbe(Publisher<? extends T> delegate) {
			super(3);
			this.delegate = (Publisher<T>) delegate;
		}

		@Override
		public Mono<T> mono() {
			return Mono.from(delegate)
			           .doOnSubscribe(sub -> incrementAndGet(SUBSCRIBED))
			           .doOnCancel(() -> incrementAndGet(CANCELLED))
			           .doOnRequest(l -> incrementAndGet(REQUESTED));
		}

		@Override
		public Flux<T> flux() {
			return Flux.from(delegate)
			           .doOnSubscribe(sub -> incrementAndGet(SUBSCRIBED))
			           .doOnCancel(() -> incrementAndGet(CANCELLED))
			           .doOnRequest(l -> incrementAndGet(REQUESTED));
		}

		@Override
		public boolean wasSubscribed() {
			return get(SUBSCRIBED) > 0;
		}

		@Override
		public boolean wasCancelled() {
			return get(CANCELLED) > 0;
		}

		@Override
		public boolean wasRequested() {
			return get(REQUESTED) > 0;
		}
	}
}

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

package reactor.core.publisher;

import java.util.Iterator;

import org.reactivestreams.Subscriber;
import reactor.core.subscriber.SubscriberWithContext;
import reactor.core.support.ReactiveState;
import reactor.fn.Consumer;
import reactor.fn.Function;

/**
 * Simple iterating consumer for {@link FluxFactory#create(Consumer, Function)} and its alias
 *
 * @param <T>
 *
 * @author Ben Hale
 * @author Stephane Maldini
 * @since 2.5
 */
public abstract class ForEachSequencer<T>
		implements Consumer<SubscriberWithContext<T, Iterator<? extends T>>>, ReactiveState.Trace,
		           ReactiveState.Upstream {

	@Override
	public final void accept(SubscriberWithContext<T, Iterator<? extends T>> subscriber) {
		final Iterator<? extends T> iterator = subscriber.context();
		if (iterator.hasNext()) {
			subscriber.onNext(iterator.next());
			//peek next
			if (!iterator.hasNext()) {
				subscriber.onComplete();
			}
		}
		else {
			subscriber.onComplete();
		}
	}

	/**
	 * Simple Publisher implementations
	 */
	public static final class IterableSequencer<T> extends ForEachSequencer<T>
			implements Function<Subscriber<? super T>, Iterator<? extends T>> {

		private final Iterable<? extends T> defaultValues;

		public IterableSequencer(Iterable<? extends T> defaultValues) {
			this.defaultValues = defaultValues;
		}

		@Override
		public Iterator<? extends T> apply(Subscriber<? super T> subscriber) {
			if (defaultValues == null) {
				throw FluxFactory.PrematureCompleteException.INSTANCE;
			}
			Iterator<? extends T> it = defaultValues.iterator();
			if (!it.hasNext()) {
				throw FluxFactory.PrematureCompleteException.INSTANCE;
			}
			return it;
		}

		@Override
		public Object upstream() {
			return defaultValues;
		}

		@Override
		public String toString() {
			return "{iterable : " + defaultValues + " }";
		}
	}

	/**
	 * Simple Publisher implementations
	 */
	public static final class IteratorSequencer<T> extends ForEachSequencer<T>
			implements Function<Subscriber<? super T>, Iterator<? extends T>> {

		private final Iterator<? extends T> defaultValues;

		public IteratorSequencer(Iterator<? extends T> defaultValues) {
			this.defaultValues = defaultValues;
		}

		@Override
		public Iterator<? extends T> apply(Subscriber<? super T> subscriber) {
			if (defaultValues == null) {
				throw FluxFactory.PrematureCompleteException.INSTANCE;
			}
			Iterator<? extends T> it = defaultValues;
			if (!it.hasNext()) {
				throw FluxFactory.PrematureCompleteException.INSTANCE;
			}
			return it;
		}

		@Override
		public Object upstream() {
			return defaultValues;
		}
	}
}

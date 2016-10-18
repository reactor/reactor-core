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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.function.Function;

/**
 * @author Stephane Maldini
 * @since 2.5
 */
final class FluxLiftOld<I, O> extends FluxSource<I, O>
		implements Function<Subscriber<? super O>, Subscriber<? super I>>  {

	final private Function<Subscriber<? super O>, Subscriber<? super I>> barrierProvider;

	public FluxLiftOld(Publisher<I> source, Function<Subscriber<? super O>, Subscriber<? super I>> barrierProvider) {
		super(source);
		this.barrierProvider = barrierProvider;
	}

	@Override
	public void subscribe(Subscriber<? super O> s) {
		source.subscribe(apply(s));
	}

	@Override
	public Subscriber<? super I> apply(Subscriber<? super O> subscriber) {
		return barrierProvider.apply(subscriber);
	}

	/**
	 * Mono lift inlined
	 * @param <I>
	 * @param <O>
	 */
	static final class MonoLift<I, O> extends MonoSource<I, O>
			implements Function<Subscriber<? super O>, Subscriber<? super I>> {
		final private Function<Subscriber<? super O>, Subscriber<? super I>> barrierProvider;

		public MonoLift(Publisher<I> source, Function<Subscriber<? super O>, Subscriber<? super I>> barrierProvider) {
			super(source);
			this.barrierProvider = barrierProvider;
		}

		@Override
		public void subscribe(Subscriber<? super O> s) {
			source.subscribe(apply(s));
		}

		@Override
		public Subscriber<? super I> apply(Subscriber<? super O> subscriber) {
			return barrierProvider.apply(subscriber);
		}

	}
}

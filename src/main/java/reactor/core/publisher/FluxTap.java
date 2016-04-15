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

import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.flow.Fuseable;
import reactor.core.flow.Producer;
import reactor.core.flow.Receiver;

/**
 *  A
 *  {@link FluxTap} provides a peek access into the last element visible of any sequence observed by the tap. Using a {@code Tap} one can
 * inspect the current event passing through a fluxion. A {@code StreamTap}'s value will be
 * continually updated as data passes through the fluxion, so a call to {@link #get()} will
 * return the last value seen by the event fluxion.
 *
 * <p>
 * <img width="640" src="https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/marble/tap.png" alt="">
 *
 * @param <T> the type of values that this Tap can consume and supply
 * @author Stephane Maldini
 */
public class FluxTap<T> extends FluxSource<T, T> implements Supplier<T> {

	volatile T value;

	/**
	 * Allow to continue assembling the chain while having access to {@link #get()} to peek the last observed item.
	 * If multiple subscribers are sharing the same {@link FluxTap}, they will all participate in updating the
	 * observed tap.
	 *
	 * @param <T> the tapped type
	 * @return a new Tapping {@link FluxTap}
	 */
	public static <T> FluxTap<T> tap(Publisher<? extends T> source){
		if(source instanceof Fuseable){
			return new FluxTapFuseable<>(source);
		}
		return new FluxTap<>(source);
	}

	/**
	 * Create a {@code Tap}.
	 */
	FluxTap(Publisher<? extends T> source) {
		super(source);
	}

	/**
	 * Get the value of this {@code Tap}, which is the current value of the event fluxion this
	 * tap is consuming.
	 *
	 * @return the value
	 */
	@Override
	public T get() {
		return value;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new Tap<>(s, this));
	}

	static final class Tap<O> implements Subscriber<O>, Subscription, Receiver, Producer {

		final Subscriber<? super O> actual;
		final FluxTap<O>            parent;
		Subscription s;

		public Tap(Subscriber<? super O> actual, FluxTap<O> parent) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object upstream() {
			return s;
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		public void onSubscribe(Subscription s) {
			this.s = s;
			actual.onSubscribe(this);
		}

		@Override
		public void onNext(O o) {
			parent.value = o;
			actual.onNext(o);
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			actual.onComplete();
		}
	}

	static final class TapFuseable<O>
			implements Subscriber<O>, Receiver, Producer, Fuseable.SynchronousSubscription<O> {
		final Subscriber<? super O> actual;
		final FluxTap<O>            parent;

		Fuseable.QueueSubscription<O> s;

		public TapFuseable(Subscriber<? super O> actual, FluxTap<O> parent) {
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		@SuppressWarnings("unchecked")
		public void onSubscribe(Subscription s) {
			this.s = (Fuseable.QueueSubscription<O>)s;
			actual.onSubscribe(this);
		}

		@Override
		public void onNext(O o) {
			parent.value = o;
			actual.onNext(o);
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			actual.onComplete();
		}

		@Override
		public O poll() {
			O v = s.poll();
			if(v != null){
				parent.value = v;
			}
			return v;
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object upstream() {
			return s;
		}

		@Override
		public int size() {
			return s.size();
		}

		@Override
		public boolean isEmpty() {
			return s.isEmpty();
		}

		@Override
		public void clear() {
			s.clear();
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		public int requestFusion(int requestedMode) {
			return s.requestFusion(requestedMode);
		}
	}

	static final class FluxTapFuseable<O> extends FluxTap<O> implements Fuseable {

		FluxTapFuseable(Publisher<? extends O> source) {
			super(source);
		}

		@Override
		public void subscribe(Subscriber<? super O> s) {
			source.subscribe(new TapFuseable<>(s, this));
		}
	}
}

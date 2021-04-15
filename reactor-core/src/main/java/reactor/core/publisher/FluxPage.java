/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;

/**
 * @author Simon Baslé
 */
final class FluxPage<P, T> extends Flux<T> implements SourceProducer<T> {

	final Supplier<? extends P>                       initialPageSupplier;
	final Function<? super P, Mono<P>>                nextPageFunction;
	final Function<? super P, ? extends Publisher<T>> pageContentFunction;

	public FluxPage(Supplier<? extends P> initialPageSupplier,
	                Function<? super P, Mono<P>> nextPageFunction,
	                Function<? super P, ? extends Publisher<T>> pageContentFunction) {
		this.initialPageSupplier = initialPageSupplier;
		this.nextPageFunction = nextPageFunction;
		this.pageContentFunction = pageContentFunction;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		P initialPage = initialPageSupplier.get();
		Mono.just(initialPage).subscribe(new PageMain<>(actual, nextPageFunction, pageContentFunction));
	}

	static final class PageMain<P, T> implements InnerOperator<P, T> {

		final CoreSubscriber<? super T> actual;
		final Function<? super P, Mono<P>> nextPageFunction;
		final Function<? super P, ? extends Publisher<T>> pageContentFunction;

		boolean      valued = false;
		Subscription pageSubscription;
		Subscription pageContentSubscription;

		volatile     long                             contentRequested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<PageMain> CONTENT_REQUESTED =
				AtomicLongFieldUpdater.newUpdater(PageMain.class, "contentRequested");

		PageMain(CoreSubscriber<? super T> actual,
		         Function<? super P, Mono<P>> nextPageFunction,
		         Function<? super P, ? extends Publisher<T>> pageContentFunction) {
			this.actual = actual;
			this.nextPageFunction = nextPageFunction;
			this.pageContentFunction = pageContentFunction;

			actual.onSubscribe(this);
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return this.actual;
		}

		@Override
		public void onSubscribe(Subscription s) {
			synchronized (this) {
				this.valued = false;
				this.pageSubscription = s;
				this.pageSubscription.request(1);
			}
		}

		@Override
		public void onNext(P p) {
			this.valued = true;
			Publisher<? extends T> pageContentPublisher = pageContentFunction.apply(p);
			pageContentPublisher.subscribe(new PageContentSubscriber<>(this, p));
		}

		@Override
		public void onComplete() {
			if (!this.valued) {
				actual.onComplete(); //FIXME called everytime
			}
			// else do nothing
		}

		@Override
		public void onError(Throwable t) {
			//FIXME
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(CONTENT_REQUESTED, this, n);
				synchronized (this) {
					if (pageContentSubscription != null) {
						pageContentSubscription.request(n);
					}
				}
			}
		}

		@Override
		public void cancel() {
			pageContentSubscription.cancel();
			pageSubscription.cancel();
		}
	}

	static final class PageContentSubscriber<P, T> implements InnerConsumer<T> {

		final PageMain<P, T> parent;
		final P page;

		PageContentSubscriber(PageMain<P, T> parent, P page) {
			this.parent = parent;
			this.page = page;
		}

		@Override
		public void onSubscribe(Subscription s) {
			long previouslyRequested = PageMain.CONTENT_REQUESTED.get(parent);
			synchronized (parent) {
				parent.pageContentSubscription = s;
				if (Operators.validate(previouslyRequested)) {
					s.request(previouslyRequested);
				}
			}
		}

		@Override
		public void onNext(T t) {
			PageMain.CONTENT_REQUESTED.decrementAndGet(parent);
			parent.actual.onNext(t);
		}

		@Override
		public void onComplete() {
			parent.nextPageFunction.apply(this.page)
			                       .subscribe(parent);
		}

		@Override
		public void onError(Throwable t) {
			parent.actual.onError(t);
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			//FIXME
			return null;
		}
	}

}

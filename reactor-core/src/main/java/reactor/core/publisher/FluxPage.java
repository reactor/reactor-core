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
		PageMain<P, T> pageMainSubscription = new PageMain<>(actual, nextPageFunction, pageContentFunction);
		pageMainSubscription.prepareNextPage(Mono.fromSupplier(initialPageSupplier));
		actual.onSubscribe(pageMainSubscription);
	}

	static final class PageMain<P, T> implements InnerProducer<T> {

		final CoreSubscriber<? super T> actual;
		final Function<? super P, Mono<P>> nextPageFunction;
		final Function<? super P, ? extends Publisher<T>> pageContentFunction;

		@Nullable
		Subscription pageContentSubscription;

		@Nullable
		Mono<P> preparedNextPage = null;

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
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return this.actual;
		}

		void prepareNextPage(Mono<P> pageMono) {
			if (CONTENT_REQUESTED.get(this) > 0) {
				synchronized (this) {
					this.pageContentSubscription = null;
					this.preparedNextPage = null;
				}
				pageMono.subscribe(new PageSubscriber<>(this));
			}
			else {
				synchronized (this) {
					this.pageContentSubscription = null;
					this.preparedNextPage = pageMono;
				}
			}
		}



		void nextPage(P page) {
			//if we get a next page, it means we have at least outstanding request of 1
			Publisher<? extends T> pageContentPublisher = pageContentFunction.apply(page);
			pageContentPublisher.subscribe(new PageContentSubscriber<>(this, page));
		}

		void noMorePages() {
			actual.onComplete();
		}

		void nextContent(T content) {
			if (contentRequested != Long.MAX_VALUE) {
				CONTENT_REQUESTED.decrementAndGet(this);
			}
			actual.onNext(content);
		}

		void errorContent(Throwable error) {
			actual.onError(error);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(CONTENT_REQUESTED, this, n);

				Mono<P> prepNextPage;
				Subscription pageContentSub;
				synchronized (this) {
					prepNextPage = this.preparedNextPage;
					this.preparedNextPage = null;
					pageContentSub = pageContentSubscription;
				}

				if (prepNextPage != null) {
					prepNextPage.subscribe(new PageSubscriber<>(this));
					return;
				}
				if (pageContentSub != null) {
					long contentToRequest = CONTENT_REQUESTED.get(this) == Long.MAX_VALUE ? Long.MAX_VALUE : n;
					pageContentSub.request(contentToRequest);
				}
			}
		}

		@Override
		public void cancel() {
			Subscription pageContentSub;
			synchronized (this) {
				pageContentSub = this.pageContentSubscription;
			}
			if (pageContentSub != null) {
				pageContentSub.cancel();
			}
		}
	}

	static final class PageSubscriber<P, T> implements InnerConsumer<P> {

		final PageMain<P, T> parent;

		boolean done;

		PageSubscriber(PageMain<P, T> parent) {
			this.parent = parent;
		}

		@Override
		public void onSubscribe(Subscription s) {
			s.request(1);
		}

		@Override
		public void onNext(P p) {
			done = true;
			parent.nextPage(p);
			//no need to cancel, we enforce upstream being a Mono
		}

		public void onComplete() {
			if (!done) {
				done = true;
				parent.noMorePages();
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, parent.actual.currentContext());
			}
			done = true;
			parent.actual.onError(t);
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			return null; //FIXME
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
			long previouslyRequested = PageMain.CONTENT_REQUESTED.get(this.parent);
			synchronized (parent) {
				parent.pageContentSubscription = s;
				parent.preparedNextPage = null;
			}
			if (previouslyRequested > 0) {
				s.request(previouslyRequested);
			}
		}

		@Override
		public void onNext(T t) {
			parent.nextContent(t);
		}

		@Override
		public void onComplete() {
			parent.prepareNextPage(parent.nextPageFunction.apply(this.page));
		}

		@Override
		public void onError(Throwable t) {
			parent.errorContent(t);
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			//FIXME
			return null;
		}
	}

}

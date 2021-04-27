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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * @author Simon Baslé
 */
final class FluxPaging<P, T> extends Flux<T> implements SourceProducer<T> {

	final P                                           initialPage;
	final Function<? super P, Mono<P>>                nextPageFunction;
	final Function<? super P, ? extends Publisher<T>> pageContentFunction;

	public FluxPaging(P initialPage,
	                  Function<? super P, ? extends Publisher<T>> pageContentFunction,
	                  Function<? super P, Mono<P>> nextPageFunction) {
		this.initialPage = initialPage;
		this.nextPageFunction = nextPageFunction;
		this.pageContentFunction = pageContentFunction;
	}

	@Override
	public void subscribe(CoreSubscriber<? super T> actual) {
		PageMain<P, T> pageMainSubscription = new PageMain<>(actual, pageContentFunction, nextPageFunction);
		pageMainSubscription.prepareNextPage(Mono.just(initialPage));
		actual.onSubscribe(pageMainSubscription);
	}

	static final class PageMain<P, T> implements InnerProducer<T> {

		final CoreSubscriber<? super T> actual;
		final Function<? super P, Mono<P>> nextPageFunction;
		final Function<? super P, ? extends Publisher<T>> pageContentFunction;

		@Nullable
		Subscription pageContentSubscription;

		@Nullable
		PageSubscriber<P, T> nextPageSubscriber;

		volatile     long                             contentRequested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<PageMain> CONTENT_REQUESTED =
				AtomicLongFieldUpdater.newUpdater(PageMain.class, "contentRequested");

		PageMain(CoreSubscriber<? super T> actual,
		         Function<? super P, ? extends Publisher<T>> pageContentFunction,
		         Function<? super P, Mono<P>> nextPageFunction) {
			this.actual = actual;
			this.nextPageFunction = nextPageFunction;
			this.pageContentFunction = pageContentFunction;
		}

		@Override
		public CoreSubscriber<? super T> actual() {
			return this.actual;
		}

		void prepareNextPage(Mono<P> pageMono) {
			PageSubscriber<P, T> next = new PageSubscriber<>(this);
			synchronized (this) {
				this.pageContentSubscription = null;
				this.nextPageSubscriber = next;
			}
			pageMono.subscribe(next);

			if (CONTENT_REQUESTED.get(this) > 0 && next.get() < 2) {
				next.requestPageOnce();
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

				PageSubscriber<P, T> prepNextPage;
				Subscription pageContentSub;
				synchronized (this) {
					prepNextPage = this.nextPageSubscriber;
					pageContentSub = pageContentSubscription;
				}

				if (prepNextPage != null) {
					prepNextPage.requestPageOnce();
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

	static final class PageSubscriber<P, T> extends AtomicInteger implements InnerConsumer<P> {

		final PageMain<P, T> parent;

		boolean done;
		Subscription s;

		PageSubscriber(PageMain<P, T> parent) {
			this.parent = parent;
		}

		@Override
		public Context currentContext() {
			return parent.actual.currentContext();
		}

		//0: no Subscription, no request
		//1: subscription, no request
		//2: no Subscription, prerequested
		//3: subscription, requested
		// onSubscribe transitions
		// 2 -> 3
		// 0 -> 1
		// request transitions
		// 0 -> 2
		// 1 -> 3

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				for (;;) {
					if (compareAndSet(2, 3)) {
						s.request(1);
						return;
					}
					if (compareAndSet(0, 1)) {
						return;
					}
					//that second cas could see 2, so we loop back
				}
			}
		}

		void requestPageOnce() {
			for (;;) {
				if (get() > 2) {
					return;
				}
				if (compareAndSet(1, 3)) {
					//the subscription was set but never requested
					this.s.request(1);
					return;
				}
				//try to transition from 0 to 2
				if (compareAndSet(0, 2)) {
					return;
				}
				//otherwise second CAS has probably seen sub update (1), we loop back
			}
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
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return get() >= 2 ? 1 : 0;
			if (key == Attr.ACTUAL) return parent.actual;
			if (key == Attr.PARENT) return parent;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return null;
		}
	}

	static final class PageContentSubscriber<P, T> implements InnerConsumer<T> {

		final PageMain<P, T> parent;
		final P page;

		boolean done;

		PageContentSubscriber(PageMain<P, T> parent, P page) {
			this.parent = parent;
			this.page = page;
		}

		@Override
		public Context currentContext() {
			return parent.actual.currentContext();
		}

		@Override
		public void onSubscribe(Subscription s) {
			long previouslyRequested = PageMain.CONTENT_REQUESTED.get(this.parent);
			synchronized (parent) {
				parent.pageContentSubscription = s;
				parent.nextPageSubscriber = null;
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
			if (done) {
				return;
			}
			this.done = true;
			parent.prepareNextPage(parent.nextPageFunction.apply(this.page));
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Operators.onErrorDropped(t, parent.actual.currentContext());
				return;
			}
			this.done = true;
			parent.errorContent(t);
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.ACTUAL) return parent.actual;
			if (key == Attr.PARENT) return parent;
			if (key == Attr.PREFETCH) return 1;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return null;
		}
	}

}

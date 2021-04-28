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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
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

	@Override
	public int getPrefetch() {
		return 0;
	}

	@Nullable
	@Override
	public Object scanUnsafe(Attr key) {
		if (key == Attr.PREFETCH) return 0;
		if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

		return SourceProducer.super.scanUnsafe(key);
	}

	static final class PageMain<P, T> implements InnerProducer<T> {

		final CoreSubscriber<? super T> actual;
		final Function<? super P, Mono<P>> nextPageFunction;
		final Function<? super P, ? extends Publisher<T>> pageContentFunction;

		@Nullable
		Subscription pageContentSubscription;

		@Nullable
		PageSubscriber<P, T> pageSubscription;

		boolean done;
		boolean cancelled;

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
				this.pageSubscription = next;
				if (this.cancelled) {
					return;
				}
			}
			pageMono.subscribe(next);

			if (CONTENT_REQUESTED.get(this) > 0) {
				next.requestPageOnce();
			}
		}

		void nextPage(P page) {
			if (cancelled) {
				return;
			}
			//if we get a next page, it means we have at least outstanding request of 1
			Publisher<? extends T> pageContentPublisher = pageContentFunction.apply(page);
			pageContentPublisher.subscribe(new PageContentSubscriber<>(this, page));
		}

		void noMorePages() {
			this.done = true;
			actual.onComplete();
		}

		void nextContent(T content) {
			if (contentRequested != Long.MAX_VALUE) {
				CONTENT_REQUESTED.decrementAndGet(this);
			}
			actual.onNext(content);
		}

		void innerError(Throwable error) {
			this.done = true;
			actual.onError(error);
		}

		@Override
		public void request(long n) {
			if (Operators.validate(n)) {
				Operators.addCap(CONTENT_REQUESTED, this, n);

				long currentRequest = CONTENT_REQUESTED.get(this);

				PageSubscriber<P, T> prepNextPage;
				Subscription pageContentSub;
				boolean cancelled;
				synchronized (this) {
					prepNextPage = this.pageSubscription;
					pageContentSub = this.pageContentSubscription;
					cancelled = this.cancelled;
				}

				if (cancelled) {
					return;
				}

				if (prepNextPage != null) {
					prepNextPage.requestPageOnce();
					return;
				}
				if (pageContentSub != null) {
					long contentToRequest = currentRequest == Long.MAX_VALUE ? Long.MAX_VALUE : n;
					pageContentSub.request(contentToRequest);
				}
			}
		}

		@Override
		public void cancel() {
			PageSubscriber<P, T> prepNextPage;
			Subscription pageContentSub;
			synchronized (this) {
				this.cancelled = true;
				prepNextPage = this.pageSubscription;
				pageContentSub = pageContentSubscription;
				this.pageSubscription = null;
				this.pageContentSubscription = null;
			}

			if (prepNextPage != null) {
				prepNextPage.s.cancel();
			}
			if (pageContentSub != null) {
				pageContentSub.cancel();
			}
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.CANCELLED) return cancelled;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return contentRequested;
			if (key == Attr.PARENT) return Scannable.from(null);
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.ASYNC;

			return InnerProducer.super.scanUnsafe(key);
		}
	}

	static final class PageSubscriber<P, T> extends AtomicBoolean implements InnerConsumer<P> {

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

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				long previouslyRequested = PageMain.CONTENT_REQUESTED.get(this.parent);
				synchronized (parent) {
					if (parent.cancelled) {
						s.cancel();
						return;
					}
					parent.pageContentSubscription = null;
					parent.pageSubscription = this;
				}
				if (previouslyRequested > 0) {
					requestPageOnce();
				}
			}
		}

		void requestPageOnce() {
			if (compareAndSet(false, true)) {
				this.s.request(1);
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
			if (key == Attr.CANCELLED) return parent.cancelled;
			if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return get() ? 1L : 0L;
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
				if (parent.cancelled) {
					s.cancel();
					return;
				}
				parent.pageContentSubscription = s;
				parent.pageSubscription = null;
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
			parent.innerError(t);
		}

		@Nullable
		@Override
		public Object scanUnsafe(Attr key) {
			if (key == Attr.TERMINATED) return done;
			if (key == Attr.CANCELLED) return parent.cancelled;
			if (key == Attr.ACTUAL) return parent.actual;
			if (key == Attr.PARENT) return parent;
			if (key == Attr.PREFETCH) return 1;
			if (key == Attr.RUN_STYLE) return Attr.RunStyle.SYNC;

			return null;
		}
	}

}

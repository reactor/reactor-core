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

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
class FluxFromPaginatedTest {

	private static class Page {

		@NonNull
		final String pageId;
		@NonNull
		final String nextPageId;

		Page(String pageId, String nextPageId) {
			this.pageId = pageId;
			this.nextPageId = nextPageId;
		}

		List<String> content() {
			return IntStream.range(1, 4)
				.mapToObj(i -> i + pageId)
				.collect(Collectors.toList());
		}

		static Mono<Page> clientFetchPage(@Nullable String pageId) {
			if (pageId == null || pageId.trim().isEmpty()) {
				return Mono.empty();
			}
			return Mono.justOrEmpty(PAGES.get(pageId))
			           .switchIfEmpty(Mono.error(new IllegalArgumentException("Unknown page " + pageId)));
		}

		@Override
		public String toString() {
			return "Page{" +
					"pageId='" + pageId + '\'' +
					", nextPageId='" + nextPageId + '\'' +
					'}';
		}
	}

	private static final Map<String, Page> PAGES = new HashMap<>();

	@BeforeEach
	void initPages() {
		PAGES.clear();
		PAGES.put("A", new Page("A", "B"));
		PAGES.put("B", new Page("B", "C"));
		PAGES.put("C", new Page("C", "D"));
		PAGES.put("D", new Page("D", ""));
	}

	@Test
	void unboundedRequest() {
		Flux.fromPaginated(
				"A",
				Page::clientFetchPage,
				Page::content,
				p -> p.nextPageId
		).as(StepVerifier::create)
				.expectNext("1A", "2A", "3A")
				.expectNext("1B", "2B", "3B")
				.expectNext("1C", "2C", "3C")
				.expectNext("1D", "2D", "3D")
				.verifyComplete();
	}

	@Test
	void boundedRequestsSubscribeToPagesEarlyButRequestsAsNecessary() {
		final AtomicInteger pageSub = new AtomicInteger();
		final AtomicInteger pageReq = new AtomicInteger();

		Flux<String> fluxPage = Flux.fromPaginated(
			"A",
			pid -> "A".equals(pid) ? Page.clientFetchPage(pid) :
				Page.clientFetchPage(pid)
				.doOnSubscribe(s -> pageSub.incrementAndGet())
				.doOnRequest(r -> pageReq.incrementAndGet()),
			Page::content,
			p -> p.nextPageId
		);

		fluxPage.as(f -> StepVerifier.create(f, 0))
				.expectSubscription()
				.thenRequest(2)
				.expectNext("1A", "2A")
				.then(() -> assertThat(pageSub)
						.as("page 1 incomplete")
						.hasValue(0)
						.hasValue(pageReq.get()))
				.thenRequest(1)
				.expectNext("3A")
				.then(() -> {
					assertThat(pageSub).as("p2 subscribed after p1 complete").hasValue(1);
					assertThat(pageReq).as("p2 not requested yet").hasValue(0);
				})
				.thenRequest(1)
				.assertNext(v -> {
					assertThat(v).isEqualTo("1B");
					assertThat(pageSub).as("second page subscribed").hasValue(1);
					assertThat(pageReq).as("second page requested").hasValue(1);
				})
				.thenRequest(8) //exactly enough to get to the end of last meaningful page and trigger subscription to the last page
				.expectNext("2B", "3B")
				.assertNext(v -> {
					assertThat(v).isEqualTo("1C");
					assertThat(pageSub).as("third page subscribed").hasValue(2);
					assertThat(pageReq).as("third page requested").hasValue(2);
				})
				.expectNext("2C", "3C")
				.assertNext(v -> {
					assertThat(v).isEqualTo("1D");
					assertThat(pageSub).as("fourth page subscribed").hasValue(3);
					assertThat(pageReq).as("fourth page requested").hasValue(3);
				})
				.expectNext("2D", "3D")
				.expectComplete()
				//we expect that an exact request for total content will actually subscribe to the Mono.empty() (without requesting it)
				//which will complete fast
				.verify(Duration.ofSeconds(5));

		assertThat(pageSub).as("last page was subscribed").hasValue(4);
		assertThat(pageReq).as("last page was subscribed but not requested").hasValue(3);
	}

	@Test
	void requestingExactlyPageContentNumberWillNotTriggerNextPage() {
		final AtomicInteger nextPageRequested = new AtomicInteger();
		FluxFromPaginated<Page, String, String> fluxFromPaginated = new FluxFromPaginated<>(
			"A",
			pid -> pid.equals("A") ? Page.clientFetchPage(pid)
				: Page.clientFetchPage(pid).doOnRequest(r -> nextPageRequested.incrementAndGet()),
			Page::content,
			p -> p.nextPageId
		);

		fluxFromPaginated.as(f -> StepVerifier.create(f, 0))
				.expectSubscription()
				.thenRequest(3)
				.expectNext("1A", "2A", "3A")
				.then(() -> assertThat(nextPageRequested).as("second page not requested yet").hasValue(0))
				.expectNoEvent(Duration.ofMillis(100))
				.thenRequest(1)
				.then(() -> assertThat(nextPageRequested).as("second page now requested").hasValue(1))
				.expectNext("1B")
		        .expectNoEvent(Duration.ofSeconds(1))
				.thenRequest(Long.MAX_VALUE)
				.expectNext("2B", "3B", "1C", "2C", "3C", "1D", "2D", "3D")
				.verifyComplete();
	}

	@Test
	void requestingExactlyPageContentWillSubscribeNextPageThusCompleteEarly() {
		AtomicBoolean requested = new AtomicBoolean();
		FluxFromPaginated<Page, String, String> fluxFromPaginated = new FluxFromPaginated<>(
			"A",
			//this triggers immediate completion once we've consumed the first page
			pid -> pid.equals("A") ? Page.clientFetchPage(pid)
				: Mono.<Page>empty().doOnRequest(r -> requested.set(true)),
			Page::content,
			p -> p.nextPageId
		);

		fluxFromPaginated.as(f -> StepVerifier.create(f, 3))
				.expectNext("1A", "2A", "3A")
				.verifyComplete();

		assertThat(requested).as("empty last page mono not requested").isFalse();
	}

	@Test
	void errorInPageFetching() {
		PAGES.remove("B");

		FluxFromPaginated<Page, String, String> fluxFromPaginated = new FluxFromPaginated<>(
			"A",
			pid -> Page.clientFetchPage(pid), //will trigger error
			Page::content,
			p -> p.nextPageId
		);

		fluxFromPaginated.as(StepVerifier::create)
				.expectNext("1A", "2A", "3A")
				.verifyErrorMessage("Unknown page B");
	}

	@Test
	void errorInPageFetching_withRequestExactlyPageSize() {
		PAGES.remove("B");

		FluxFromPaginated<Page, String, String> fluxFromPaginated = new FluxFromPaginated<>(
			"A",
			pid -> Page.clientFetchPage(pid), //will trigger error
			Page::content,
			p -> p.nextPageId
		);

		fluxFromPaginated.as(f -> StepVerifier.create(f, 3))
				.expectNext("1A", "2A", "3A")
				.verifyErrorMessage("Unknown page B");
	}

	@Test
	void pageFetchCanSeeMainContext() {
		AtomicReference<String> forcedPageOnce = new AtomicReference<>("D");
		FluxFromPaginated<Page, String, String> fluxFromPaginated = new FluxFromPaginated<>(
			"A",
			pid -> Mono.deferContextual(ctx -> {
				if ("A".equals(pid)) {
					//normal behavior for the first page
					return Page.clientFetchPage(pid);
				}
				//next we force the next page from context
				AtomicReference<String> nextPageId = ctx.getOrDefault("forceNextPage", new AtomicReference<>(null));
				if (nextPageId.get() == null) {
					//this behaves "normally" and will iterate all the subsequent PAGES
					return Page.clientFetchPage(pid);
				}
				else {
					//we force once, and go back to normal behavior
					String forcedId = nextPageId.getAndSet(null);
					return Page.clientFetchPage(forcedId);
				}
			}),
			Page::content,
			p -> p.nextPageId
		);

		StepVerifier.create(fluxFromPaginated, StepVerifierOptions.create().withInitialContext(Context.of("forceNextPage", new AtomicReference("C"))))
				.expectNext("1A", "2A", "3A")
				.expectNext("1C", "2C", "3C")
				.expectNext("1D", "2D", "3D")
				.verifyComplete();
	}

	@Test
	void pageMainCanChangeThreadsDependingOnPageFetch() {
		Set<String> threadNames = new HashSet<>();
		Flux<Integer> fluxPaging = new FluxFromPaginated<Integer, Integer, Integer>(
			0,
			i -> {
				Mono<Integer> base;
				switch (i) {
					case 0:
						base = Mono.just(0).publishOn(Schedulers.parallel());
						break;
					case 1:
						base = Mono.just(1);
						break;
					case 2:
						base = Mono.just(2);
						break;
					default:
						base = Mono.empty();
						break;
				}
				return base
					.doOnNext(v -> threadNames.add(Thread.currentThread().getName()));
			},
			i -> Arrays.asList(i * 10, i * 10 + 1),
			i -> i + 1
		);

		StepVerifier.create(fluxPaging)
				.expectNext(0, 1, 10, 11, 20, 21)
				.verifyComplete();

		threadNames.remove(Thread.currentThread().getName());
		assertThat(threadNames)
				.hasSize(1)
				.allSatisfy(tn -> assertThat(tn).startsWith("parallel-"));
	}


	@Test
	void scanPublisher() {
		FluxFromPaginated<Object, Integer, Object> fluxFromPaginated = new FluxFromPaginated<>(1, o -> Mono.empty(), o -> Collections.emptyList(), p -> 1);

		assertThat(fluxFromPaginated.scan(Scannable.Attr.PARENT)).as("PARENT").isSameAs(Scannable.from(null));
		assertThat(fluxFromPaginated.scan(Scannable.Attr.ACTUAL)).as("ACTUAL").isSameAs(Scannable.from(null));
		assertThat(fluxFromPaginated.scan(Scannable.Attr.RUN_STYLE)).as("RUN_STYLE").isEqualTo(Scannable.Attr.RunStyle.ASYNC);
		assertThat(fluxFromPaginated.scan(Scannable.Attr.PREFETCH)).as("PREFETCH").isZero();
	}

	@Test
	void scanMain() {
		AssertSubscriber<Object> actual = AssertSubscriber.create(0);
		FluxFromPaginated.PaginatedCoordinator<Object, Integer, Object>
				main = new FluxFromPaginated.PaginatedCoordinator<>(actual, o -> Mono.empty(), o -> Collections.emptyList(), p -> 1);
		actual.onSubscribe(main);

		assertThat(main.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).as("REQUESTED_FROM_DOWNSTREAM pre-request").isEqualTo(0);
		actual.request(123);
		assertThat(main.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).as("REQUESTED_FROM_DOWNSTREAM").isEqualTo(123);

		assertThat(main.scan(Scannable.Attr.PARENT)).as("PARENT").isSameAs(Scannable.from(null));
		assertThat(main.scan(Scannable.Attr.ACTUAL)).as("ACTUAL").isSameAs(actual);
		assertThat(main.scan(Scannable.Attr.RUN_STYLE)).as("RUN_STYLE").isEqualTo(Scannable.Attr.RunStyle.ASYNC);

		main.cancel();
		assertThat(main.scan(Scannable.Attr.CANCELLED)).as("CANCELLED").isTrue();
	}

	@Test
	void scanMainTerminatedCases() {
		AssertSubscriber<Object> actual = AssertSubscriber.create(0);
		FluxFromPaginated.PaginatedCoordinator<Object, Integer, Object>
				main = new FluxFromPaginated.PaginatedCoordinator<>(actual, o -> Mono.empty(), o -> Collections.emptyList(), p -> 1);
		actual.onSubscribe(main);

		main.done = false;
		assertThat(main.scan(Scannable.Attr.TERMINATED)).as("TERMINATED reset #1").isFalse();

		main.noMorePages();
		assertThat(main.scan(Scannable.Attr.TERMINATED)).as("TERMINATED after noMorePages").isTrue();

		main.done = false;
		assertThat(main.scan(Scannable.Attr.TERMINATED)).as("TERMINATED reset #2").isFalse();

		main.innerError(new IllegalStateException("boom"));
		assertThat(main.scan(Scannable.Attr.TERMINATED)).as("TERMINATED after innerError").isTrue();
	}

	@Test
	void scanPageSubscriber() {
		CoreSubscriber<Object> actual = AssertSubscriber.create(0);
		FluxFromPaginated.PaginatedCoordinator<Object, Integer, Object>
				parent = new FluxFromPaginated.PaginatedCoordinator<>(actual, o -> Mono.empty(), o -> Collections.emptyList(), p -> 1);

		parent.prepareNextPage(Mono.never());

		FluxFromPaginated.PageSubscriber<Object, Object> pageSubscriber = parent.pageSubscription;

		assertThat(pageSubscriber.scan(Scannable.Attr.PARENT)).as("PARENT").isSameAs(parent);
		assertThat(pageSubscriber.scan(Scannable.Attr.ACTUAL)).as("ACTUAL").isSameAs(actual);
		assertThat(pageSubscriber.scan(Scannable.Attr.RUN_STYLE)).as("RUN_STYLE").isEqualTo(Scannable.Attr.RunStyle.SYNC);

		assertThat(pageSubscriber.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM))
				.as("REQUESTED_FROM_DOWNSTREAM initial")
				.isZero();

		parent.request(123);

		assertThat(pageSubscriber.scan(Scannable.Attr.REQUESTED_FROM_DOWNSTREAM)).as("REQUESTED_FROM_DOWNSTREAM").isOne();
	}

	@Test
	void scanPageSubscriberTerminatedCases() {
		CoreSubscriber<Object> actual = AssertSubscriber.create(0);
		FluxFromPaginated.PaginatedCoordinator<Object, Integer, Object>
				parent = new FluxFromPaginated.PaginatedCoordinator<>(actual, o -> Mono.empty(), o -> Collections.emptyList(), p -> 1);
		//we'll use a misbehaving TestPublisher to separately send onNext / onComplete / onError signals to terminate the subscriber
		TestPublisher<Object> pagePublisher = TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		parent.prepareNextPage(pagePublisher.mono());
		FluxFromPaginated.PageSubscriber<Object, Object> pageSubscriber = parent.pageSubscription;

		parent.request(123);
		pagePublisher.assertWasRequested();
		assertThat(pageSubscriber.scan(Scannable.Attr.TERMINATED)).as("not terminated 1").isFalse();

		pagePublisher.next("example");
		assertThat(pageSubscriber.scan(Scannable.Attr.TERMINATED)).as("onNext terminates").isTrue();

		pageSubscriber.done = false;
		assertThat(pageSubscriber.scan(Scannable.Attr.TERMINATED)).as("not terminated 2").isFalse();

		pagePublisher.complete();
		assertThat(pageSubscriber.scan(Scannable.Attr.TERMINATED)).as("onComplete terminates").isTrue();

		pageSubscriber.done = false;
		assertThat(pageSubscriber.scan(Scannable.Attr.TERMINATED)).as("not terminated 3").isFalse();

		pagePublisher.error(new IllegalStateException("intends to terminate pageSubscriber"));
		assertThat(pageSubscriber.scan(Scannable.Attr.TERMINATED)).as("onError terminates").isTrue();
	}

	@Test
	void scanPageContentSubscriber() {
		//FIXME
	}




//	static private final class Page {
//
//		Optional<Integer> nextPage;
//		List<Integer> values;
//
//		public Page(@Nullable Integer nextPageId, List<Integer> values) {
//			this.nextPage = Optional.ofNullable(nextPageId);
//			this.values = values;
//		}
//	}
//
//	static private final Mono<Page> clientRequest(Optional<Integer> pageId) {
//		if (pageId.isPresent()) {
//			int id = pageId.get();
//			if (id == 0) {
//				return Mono.just(new Page(1, Arrays.asList(1, 2, 3)));
//			}
//			if (id == 8) {
//				return Mono.just(new Page(null, IntStream.range(80, 86)
//					.boxed().collect(Collectors.toList())));
//			}
//			return Mono.just(new Page(id + 1, IntStream.range(10 * id, (10 * id) + id)
//				.boxed().collect(Collectors.toList())));
//		}
//		return Mono.empty();
//	}
//
//
//	private static void testFromPaginated() {
//		fromPaginated(Optional.of(0),
//			opt -> clientRequest(opt),
//			page -> page.values,
//			page -> page.nextPage);
//	}
//
//	static class Page2 {
//
//		final String url;
//		final String nextPageUrl;
//		final List<Integer> values;
//
//		Page2(String url, @Nullable String nextPageUrl, List<Integer> values) {
//			this.url = url;
//			this.nextPageUrl = nextPageUrl == null ? "" : nextPageUrl;
//			this.values = values;
//		}
//	}
//
//	static private final Mono<Page2> clientRequest2(String url) {
//		if (url == null || url.isEmpty()) {
//			return Mono.empty();
//		}
//
//		int id = Integer.parseInt(url.replaceAll("page", ""));
//		if (id == 8) {
//			return Mono.just(new Page2(url, null, IntStream.range(80, 86)
//				.boxed().collect(Collectors.toList())));
//		}
//		return Mono.just(new Page2(url, "page" + (id + 1), IntStream.range(10 * id, (10 * id) + id)
//			.boxed().collect(Collectors.toList())));
//	}
//
//	private static void testFromPaginated2() {
//		String organization = "reactor";
//		Flux<Integer> unpaginated = Flux.fromPaginated(
//			"https://api.github.com/orgs/"+ organization + "/repos?per_page=3",
//			Flux::clientRequest2,
//			page -> page.values,
//			page -> page.nextPageUrl
//		);
//	}

}
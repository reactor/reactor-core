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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import reactor.test.StepVerifier;
import reactor.test.StepVerifierOptions;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
class FluxPagingTest {

	private static class Page {

		@NonNull
		final String pageId;
		@NonNull
		final String nextPageId;

		Page(String pageId, String nextPageId) {
			this.pageId = pageId;
			this.nextPageId = nextPageId;
		}

		Flux<String> content() {
			return Flux.range(1, 3)
			           .map(i -> i + pageId);
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
		Flux.paging(PAGES.get("A"),
				Page::content,
				p -> Page.clientFetchPage(p.nextPageId)
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

		Flux<String> fluxPage = Flux.paging(PAGES.get("A"),
				Page::content, p -> Page.clientFetchPage(p.nextPageId)
						.doOnSubscribe(s -> pageSub.incrementAndGet())
						.doOnRequest(r -> pageReq.incrementAndGet())
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
		FluxPaging<Page, String> fluxPaging = new FluxPaging<>(PAGES.get("A"),
				Page::content, p -> Page.clientFetchPage(p.nextPageId)
				         .doOnRequest(r -> nextPageRequested.incrementAndGet())
		);

		fluxPaging.as(f -> StepVerifier.create(f, 0))
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
		FluxPaging<Page, String> fluxPaging = new FluxPaging<>(PAGES.get("A"),
				//this triggers immediate completion once we've consumed the first page
				Page::content, p -> Mono.<Page>empty().doOnRequest(r -> requested.set(true))
		);

		fluxPaging.as(f -> StepVerifier.create(f, 3))
				.expectNext("1A", "2A", "3A")
				.verifyComplete();

		assertThat(requested).as("empty last page mono not requested").isFalse();
	}

	@Test
	void errorInPageFetching() {
		PAGES.remove("B");

		FluxPaging<Page, String> fluxPaging = new FluxPaging<>(PAGES.get("A"),
				Page::content, p -> Page.clientFetchPage(p.nextPageId) //will trigger error
		);

		fluxPaging.as(StepVerifier::create)
				.expectNext("1A", "2A", "3A")
				.verifyErrorMessage("Unknown page B");
	}

	@Test
	void errorInPageFetching_withRequestExactlyPageSize() {
		PAGES.remove("B");

		FluxPaging<Page, String> fluxPaging = new FluxPaging<>(PAGES.get("A"),
				Page::content, p -> Page.clientFetchPage(p.nextPageId) //will trigger error
		);

		fluxPaging.as(f -> StepVerifier.create(f, 3))
				.expectNext("1A", "2A", "3A")
				.verifyErrorMessage("Unknown page B");
	}

	@Test
	void errorInPageContent() {
		FluxPaging<Page, String> fluxPaging = new FluxPaging<>(PAGES.get("A"),
				p -> {
					if (p.pageId.equals("B")) {
						return p.content().concatWith(Mono.error(new IllegalStateException("Error in page content")));
					}
					return p.content();
				}, p -> Page.clientFetchPage(p.nextPageId) //will trigger error
		);

		fluxPaging.as(StepVerifier::create)
				.expectNext("1A", "2A", "3A")
				.expectNext("1B", "2B", "3B")
				.verifyErrorMessage("Error in page content");
	}

	@Test
	void errorInPageContent_withRequestExactlyPageSize() {
		FluxPaging<Page, String> fluxPaging = new FluxPaging<>(PAGES.get("A"),
				p -> {
					if (p.pageId.equals("B")) {
						return p.content().concatWith(Mono.error(new IllegalStateException("Error in page content")));
					}
					return p.content();
				}, p -> Page.clientFetchPage(p.nextPageId) //will trigger error
		);

		fluxPaging.as(publisher -> StepVerifier.create(publisher, 6))
				.expectNext("1A", "2A", "3A")
				.expectNext("1B", "2B", "3B")
				.verifyErrorMessage("Error in page content");
	}

	@Test
	void pageFetchCanSeeMainContext() {
		FluxPaging<Page, String> fluxPaging = new FluxPaging<>(
				PAGES.get("A"),
				Page::content, p -> Mono.deferContextual(ctx -> {
					String nextPageId = ctx.getOrDefault("forceNextPage", p.nextPageId);
					if (Objects.equals(nextPageId, p.pageId)) {
						return Mono.empty();
					}
					else {
						return Mono.justOrEmpty(PAGES.get(nextPageId));
					}
				})
		);

		StepVerifier.create(fluxPaging, StepVerifierOptions.create().withInitialContext(Context.of("forceNextPage", "D")))
				.expectNext("1A", "2A", "3A")
				.expectNext("1D", "2D", "3D")
				.verifyComplete();
	}

	@Test
	void pageContentCanSeeMainContext() {
		FluxPaging<Page, String> fluxPaging = new FluxPaging<>(
				PAGES.get("A"),
				p -> Flux.deferContextual(ctx -> {
					String additionalValue = ctx.getOrDefault("prependValue", null);
					if (additionalValue != null) {
						return Mono.just(additionalValue).concatWith(p.content());
					}
					return p.content();
				}), p -> Mono.justOrEmpty(PAGES.get(p.nextPageId))
		);

		StepVerifier.create(fluxPaging, StepVerifierOptions.create().withInitialContext(Context.of("prependValue", "ADD")))
				.expectNext("ADD", "1A", "2A", "3A")
				.expectNext("ADD", "1B", "2B", "3B")
				.expectNext("ADD", "1C", "2C", "3C")
				.expectNext("ADD", "1D", "2D", "3D")
				.verifyComplete();
	}

}
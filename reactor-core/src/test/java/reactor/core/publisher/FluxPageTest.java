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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import reactor.test.StepVerifier;
import reactor.util.annotation.Nullable;

/**
 * @author Simon Baslé
 */
class FluxPageTest {

	private static class Page {

		final String pageId;
		final String nextPageId;

		Page(String pageId, String nextPageId) {
			this.pageId = pageId;
			this.nextPageId = nextPageId;
		}

		Flux<String> content() {
			return Flux.range(1, 3)
					.map(i -> i + pageId);
		}

		@Nullable
		String nextPageId() {
			return this.nextPageId;
		}

		static Mono<Page> clientFetchPage(@Nullable String pageId) {
			if (pageId == null) {
				return Mono.empty();
			}
			return Mono.justOrEmpty(PAGES.get(pageId));
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
	void sketchOutApi() {
		FluxPage<Page, String> fluxPage = new FluxPage<>(() -> PAGES.get("A"),
				p -> Page.clientFetchPage(p.nextPageId),
				Page::content);

		fluxPage.as(StepVerifier::create)
		        .expectNext("1A", "2A", "3A")
		        .expectNext("1B", "2B", "3B")
		        .expectNext("1C", "2C", "3C")
		        .expectNext("1D", "2D", "3D")
		        .verifyComplete();
	}

}
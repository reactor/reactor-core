/*
 * Copyright (c) 2025 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.adapter;

import java.util.concurrent.Flow;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;

class JdkFlowAdapterTest {

	@Test
	void shouldPropagateContextInAdaptedFlux() {
		String key = "key";
		Flux<String> flux = Flux.deferContextual(ctx -> {
			String value = ctx.get(key);
			assertThat(value).isEqualTo("present");
			return Flux.just("Hello");
		});

		Flow.Publisher<String> flowPublisher =
				JdkFlowAdapter.publisherToFlowPublisher(flux);

		Flux<String> reAdapted = JdkFlowAdapter.flowPublisherToFlux(flowPublisher);

		reAdapted.contextWrite(ctx -> ctx.put(key, "present")).blockLast();
	}

	@Test
	void shouldPropagateContextInAdaptedMono() {
		String key = "key";
		Mono<String> mono = Mono.deferContextual(ctx -> {
			String value = ctx.get(key);
			assertThat(value).isEqualTo("present");
			return Mono.just("Hello");
		});

		Flow.Publisher<String> flowPublisher =
				JdkFlowAdapter.publisherToFlowPublisher(mono);

		Mono<String> reAdapted =
				Mono.from(JdkFlowAdapter.flowPublisherToFlux(flowPublisher));

		reAdapted.contextWrite(ctx -> ctx.put(key, "present")).block();
	}
}

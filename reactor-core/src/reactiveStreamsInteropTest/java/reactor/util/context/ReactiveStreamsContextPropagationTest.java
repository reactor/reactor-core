/*
 * Copyright (c) 2019-Present Pivotal Software Inc, All Rights Reserved.
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

package reactor.util.context;

import io.reactivex.Flowable;
import junitparams.JUnitParamsRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitParamsRunner.class)
public class ReactiveStreamsContextPropagationTest {

	//this test is here to demonstrate a limitation of the transformDeferred Context propagation:
	//if the topmost SUBSCRIBER is a core subscriber, itself propagating to a non-reactor Subscriber,
	//the outer Context will be lost.
	@Test
	public void transformDeferredWithFlowableWrappingCoreLosesContext() {
		Flux<String> source = Flux.deferWithContext(Flux::just)
		                          .map(ctx -> ctx.getOrDefault("foo", "NO CONTEXT"));
		Context c = Context.of("foo", "bar");

		String extracted = source.transformDeferred(f -> Flowable.fromPublisher(f.map(v -> "EXTRACTED " + v+ " FROM FLUX")))
		                         .subscriberContext(c)
		                         .blockLast();

		assertThat(extracted).isEqualTo("EXTRACTED NO CONTEXT FROM FLUX");

		Mono<String> monoSource = Mono.subscriberContext()
				.map(ctx -> ctx.getOrDefault("foo", "NO CONTEXT"));

		extracted = monoSource.transformDeferred(f -> Flowable.fromPublisher(f.map(v -> "EXTRACTED " + v + " FROM MONO")))
		                      .subscriberContext(c)
		                      .block();

		assertThat(extracted).isEqualTo("EXTRACTED NO CONTEXT FROM MONO");
	}

	@Test
	public void transformDeferredWithFlowableIsolatedWorks() {
		Flux<String> source = Flux.deferWithContext(Flux::just)
		                          .map(ctx -> ctx.getOrDefault("foo", "NO CONTEXT"));
		Context c = Context.of("foo", "bar");

		String extracted = source.transformDeferred(f -> f.map(v -> "EXTRACTED " + v + " FROM FLUX"))
		                         .transformDeferred(Flowable::fromPublisher)
		                         .subscriberContext(c)
		                         .blockLast();

		assertThat(extracted).isEqualTo("EXTRACTED bar FROM FLUX");

		Mono<String> sourceMono = Mono.subscriberContext()
		                              .map(ctx -> ctx.getOrDefault("foo", "NO CONTEXT"));

		extracted = sourceMono.transformDeferred(f -> f.map(v -> "EXTRACTED " + v + " FROM MONO"))
		                         .transformDeferred(Flowable::fromPublisher)
		                         .subscriberContext(c)
		                         .block();

		assertThat(extracted).isEqualTo("EXTRACTED bar FROM MONO");
	}

}
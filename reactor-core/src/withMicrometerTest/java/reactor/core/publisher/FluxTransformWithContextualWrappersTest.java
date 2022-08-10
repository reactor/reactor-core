/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.core.publisher;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import reactor.WithMicrometerTestExecutionListener;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;
import reactor.util.function.FunctionalWrappers;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
class FluxTransformWithContextualWrappersTest {

	private static List<String> events ;

	@BeforeEach
	void init() {
		events = new ArrayList<>();
	}

	static Mono<Integer> requestWithLog(Integer id) {
		events.add("executing requestWithLog function for id " + id + " with tl=" + WithMicrometerTestExecutionListener.STRING_THREAD_LOCAL.get());

		return Mono.just(id)
			.filter(v -> {
				events.add("sync filter on " + id + " inside requestWithLog with tl=" + WithMicrometerTestExecutionListener.STRING_THREAD_LOCAL.get());
				return true;
			})
			.publishOn(Schedulers.parallel())
			.map(v -> {
				events.add("async mapping " + id + " inside requestWithLog with tl=" + WithMicrometerTestExecutionListener.STRING_THREAD_LOCAL.get());
				return 100 + v;
			});
	}

	static Mono<Integer> requestWithDeepLog(Integer id, FunctionalWrappers contextual) {
		events.add("executing requestWithDeepLog function for id " + id + " with tl=" + WithMicrometerTestExecutionListener.STRING_THREAD_LOCAL.get());

		return Mono.just(id)
			.filter(contextual.predicate(v -> {
				events.add("sync contextualized filter on " + id + " inside requestWithDeepLog with tl=" + WithMicrometerTestExecutionListener.STRING_THREAD_LOCAL.get());
				return true;
			}))
			.publishOn(Schedulers.parallel())
			.map(contextual.function(ignore -> {
				events.add("async contextualized mapping " + id + " inside requestWithDeepLog with tl=" + WithMicrometerTestExecutionListener.STRING_THREAD_LOCAL.get());
				return id - 59;
			}));
	}

	@Test
	void withJustTlAccessorNaturalCaptureAtScopeSubscription() {
		Flux<Integer> source = Flux.just(1)
			.publishOn(Schedulers.single());

		source
			.transformWithWrappers((self, contextual) -> self
				.doOnNext(contextual.consumer(v -> events.add("wrapped doOnNext, tl=" + WithMicrometerTestExecutionListener.STRING_THREAD_LOCAL.get())))
				.doOnNext(v -> events.add("UNWRAPPED doOnNext, tl=" + WithMicrometerTestExecutionListener.STRING_THREAD_LOCAL.get()))
				//this demonstrates an important limitation: if the pre-existing flatMapping function doesn't account for the contextual FunctionalWrappers
				//and has an async aspect, then TL will not be visible to the async parts
				.flatMap(contextual.function(FluxTransformWithContextualWrappersTest::requestWithLog))
				//this demonstrates how to fix the above, assuming the pre-existing flatmapping function can be changed to use a FunctionalWrappers
				.flatMap(v -> requestWithDeepLog(v, contextual))
			)
			.blockLast();

		assertThat(events).containsExactly(
			"wrapped doOnNext, tl=withJustTlAccessorNaturalCaptureAtScopeSubscription()",
			"UNWRAPPED doOnNext, tl=null",
			"executing requestWithLog function for id 1 with tl=withJustTlAccessorNaturalCaptureAtScopeSubscription()",
			"sync filter on 1 inside requestWithLog with tl=null",
			"async mapping 1 inside requestWithLog with tl=null",
			"executing requestWithDeepLog function for id 101 with tl=null",
			"sync contextualized filter on 101 inside requestWithDeepLog with tl=withJustTlAccessorNaturalCaptureAtScopeSubscription()",
			"async contextualized mapping 101 inside requestWithDeepLog with tl=withJustTlAccessorNaturalCaptureAtScopeSubscription()"
		);
	}

	@Test
	void whenSubscribedInAnotherThreadWithoutCapture() {
		Flux<Integer> source = Flux.just(1)
			.publishOn(Schedulers.single());

		source
			.transformWithWrappers((self, contextual) -> self
				.doOnNext(contextual.consumer(v -> events.add("wrapped doOnNext, tl=" + WithMicrometerTestExecutionListener.STRING_THREAD_LOCAL.get())))
				.doOnNext(v -> events.add("UNWRAPPED doOnNext, tl=" + WithMicrometerTestExecutionListener.STRING_THREAD_LOCAL.get()))
				//this demonstrates an important limitation: if the pre-existing flatMapping function doesn't account for the contextual FunctionalWrappers
				//and has an async aspect, then TL will not be visible to the async parts
				.flatMap(contextual.function(FluxTransformWithContextualWrappersTest::requestWithLog))
				//this demonstrates how to fix the above, assuming the pre-existing flatmapping function can be changed to use a FunctionalWrappers
				.flatMap(v -> requestWithDeepLog(v, contextual))
			)
			.subscribeOn(Schedulers.parallel())
			.blockLast();

		assertThat(events).containsExactly(
			"wrapped doOnNext, tl=null",
			"UNWRAPPED doOnNext, tl=null",
			"executing requestWithLog function for id 1 with tl=null",
			"sync filter on 1 inside requestWithLog with tl=null",
			"async mapping 1 inside requestWithLog with tl=null",
			"executing requestWithDeepLog function for id 101 with tl=null",
			"sync contextualized filter on 101 inside requestWithDeepLog with tl=null",
			"async contextualized mapping 101 inside requestWithDeepLog with tl=null"
		);
	}

	@Test
	void whenSubscribedInAnotherThreadWithContextCapture() {
		Flux<Integer> source = Flux.just(1)
			.publishOn(Schedulers.single());

		source
			.transformWithWrappers((self, contextual) -> self
				.doOnNext(contextual.consumer(v -> events.add("wrapped doOnNext, tl=" + WithMicrometerTestExecutionListener.STRING_THREAD_LOCAL.get())))
				.doOnNext(v -> events.add("UNWRAPPED doOnNext, tl=" + WithMicrometerTestExecutionListener.STRING_THREAD_LOCAL.get()))
				//this demonstrates an important limitation: if the pre-existing flatMapping function doesn't account for the contextual FunctionalWrappers
				//and has an async aspect, then TL will not be visible to the async parts
				.flatMap(contextual.function(FluxTransformWithContextualWrappersTest::requestWithLog))
				//this demonstrates how to fix the above, assuming the pre-existing flatmapping function can be changed to use a FunctionalWrappers
				.flatMap(v -> requestWithDeepLog(v, contextual))
			)
			.subscribeOn(Schedulers.parallel())
			.contextCapture()
			.blockLast();

		assertThat(events).containsExactly(
			"wrapped doOnNext, tl=whenSubscribedInAnotherThreadWithContextCapture()",
			"UNWRAPPED doOnNext, tl=null",
			"executing requestWithLog function for id 1 with tl=whenSubscribedInAnotherThreadWithContextCapture()",
			"sync filter on 1 inside requestWithLog with tl=null",
			"async mapping 1 inside requestWithLog with tl=null",
			"executing requestWithDeepLog function for id 101 with tl=null",
			"sync contextualized filter on 101 inside requestWithDeepLog with tl=whenSubscribedInAnotherThreadWithContextCapture()",
			"async contextualized mapping 101 inside requestWithDeepLog with tl=whenSubscribedInAnotherThreadWithContextCapture()"
		);
	}

	@Test
	void whenSubscribedInAnotherThreadWithContextCaptureAndOverwrittenContextValue() {
		Flux<Integer> source = Flux.just(1)
			.publishOn(Schedulers.single());

		source
			.transformWithWrappers((self, contextual) -> self
				.doOnNext(contextual.consumer(v -> events.add("wrapped doOnNext, tl=" + WithMicrometerTestExecutionListener.STRING_THREAD_LOCAL.get())))
				.doOnNext(v -> events.add("UNWRAPPED doOnNext, tl=" + WithMicrometerTestExecutionListener.STRING_THREAD_LOCAL.get()))
				//this demonstrates an important limitation: if the pre-existing flatMapping function doesn't account for the contextual FunctionalWrappers
				//and has an async aspect, then TL will not be visible to the async parts
				.flatMap(contextual.function(FluxTransformWithContextualWrappersTest::requestWithLog))
				//this demonstrates how to fix the above, assuming the pre-existing flatmapping function can be changed to use a FunctionalWrappers
				.flatMap(v -> requestWithDeepLog(v, contextual))
			)
			.subscribeOn(Schedulers.parallel())
			.contextWrite(Context.of(WithMicrometerTestExecutionListener.STRING_TL_KEY, "overwriteTl()"))
			.contextCapture()
			.blockLast();

		assertThat(events).containsExactly(
			"wrapped doOnNext, tl=overwriteTl()",
			"UNWRAPPED doOnNext, tl=null",
			"executing requestWithLog function for id 1 with tl=overwriteTl()",
			"sync filter on 1 inside requestWithLog with tl=null",
			"async mapping 1 inside requestWithLog with tl=null",
			"executing requestWithDeepLog function for id 101 with tl=null",
			"sync contextualized filter on 101 inside requestWithDeepLog with tl=overwriteTl()",
			"async contextualized mapping 101 inside requestWithDeepLog with tl=overwriteTl()"
		);
	}

}
/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.test.subscriber;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.util.context.Context;

import static org.assertj.core.api.Assertions.assertThat;

class TestSubscriberSpecTest {

	@Test
	void smokeInstanceOfTest() {
		TestSubscriberBuilder baseSpec = TestSubscriber.builder().initialRequest(123L).requireFusion(Fuseable.ANY);

		assertThat(baseSpec.build())
				.as("base")
				.isInstanceOf(CoreSubscriber.class)
				.isNotInstanceOf(Fuseable.ConditionalSubscriber.class);

		assertThat(baseSpec.buildConditional(v -> true))
				.as("conditional")
				.isInstanceOf(CoreSubscriber.class)
				.isInstanceOf(Fuseable.ConditionalSubscriber.class);
	}

	@Test
	void defaultInitialRequestIsUnbounded() {
		TestSubscriberBuilder spec = TestSubscriber.builder();

		assertThat(spec.initialRequest).as("default in spec").isEqualTo(Long.MAX_VALUE);
		assertThat(spec.build().initialRequest).as("consistent in subscriber").isEqualTo(Long.MAX_VALUE);
	}

	@Test
	void initialRequestMethodsMutateSpec() {
		TestSubscriberBuilder spec = TestSubscriber.builder();

		spec.initialRequest(123L);

		assertThat(spec.initialRequest).as("mutated initialRequest").isEqualTo(123L);
		assertThat(spec.build().initialRequest).as("consistent in subscriber").isEqualTo(123L);

		spec.initialRequestUnbounded();

		assertThat(spec.initialRequest).as("mutated initialRequestUnbounded").isEqualTo(Long.MAX_VALUE);
	}

	@Test
	void defaultContextIsEmpty() {
		TestSubscriberBuilder spec = TestSubscriber.builder();

		assertThat(spec.context).as("default in spec").isEqualTo(Context.empty());
		assertThat(spec.build().currentContext()).as("consistent in subscriber").isSameAs(spec.context);
	}

	@Test
	void contextPutMethodsMutateSpec() {
		TestSubscriberBuilder spec = TestSubscriber.builder()
				.contextPut("test", 1);

		assertThat(spec.context).as("mutated contextPut")
				.usingRecursiveComparison()
				.isEqualTo(Context.of("test", 1));
		assertThat(spec.build().currentContext()).as("consistent in subscriber").isSameAs(spec.context);

		spec.contextPutAll(Context.of("example", true));

		assertThat(spec.context)
				.as("mutated contextPutAll")
				.usingRecursiveComparison()
				.isEqualTo(Context.of("test", 1, "example", true));
	}

	@Test
	void defaultFusionIsDisabled() {
		TestSubscriberBuilder spec = TestSubscriber.builder();

		assertThat(spec.requestedFusionMode).as("default fusion modes in spec").isEqualTo(spec.expectedFusionMode).isZero();
		final TestSubscriber<Object> subscriber = spec.build();
		assertThat(subscriber.requestedFusionMode).as("consistent in subscriber").isEqualTo(subscriber.expectedFusionMode).isZero();
	}

	@Test
	void defaultFusionIsNotRequired() {
		TestSubscriberBuilder spec = TestSubscriber.builder();

		assertThat(spec.fusionRequirement).as("default fusion requirement in spec").isEqualTo(TestSubscriber.FusionRequirement.NONE);
		final TestSubscriber<Object> subscriber = spec.build();
		assertThat(subscriber.fusionRequirement).as("consistent in subscriber").isEqualTo(spec.fusionRequirement);
	}

	@Test
	void fusionMethodsMutateSpec() {
		TestSubscriberBuilder spec = TestSubscriber.builder();

		spec.requireFusion(Fuseable.ANY, Fuseable.ASYNC);

		assertThat(spec.fusionRequirement).as("fusion requirement").isEqualTo(TestSubscriber.FusionRequirement.FUSEABLE);
		assertThat(spec.requestedFusionMode).as("fusion mode requested").isEqualTo(Fuseable.ANY);
		assertThat(spec.expectedFusionMode).as("fusion mode expected").isEqualTo(Fuseable.ASYNC);

		spec.requireFusion(Fuseable.NONE);

		assertThat(spec.fusionRequirement).as("fusion requirement once NONE").isEqualTo(TestSubscriber.FusionRequirement.NONE);
		assertThat(spec.requestedFusionMode).as("fusion mode requested once NONE").isEqualTo(Fuseable.NONE);
		assertThat(spec.expectedFusionMode).as("fusion mode expected once NONE").isEqualTo(Fuseable.NONE);
	}

	@Test
	void subscriberCreatedCopiesSpecConfig() {
		TestSubscriberBuilder spec = TestSubscriber.builder()
				.initialRequest(123L)
				.contextPut("example", "foo")
				.requireFusion(Fuseable.ANY, Fuseable.ASYNC);

		TestSubscriber<String> testSubscriber = spec.build();

		spec = spec
				.initialRequest(456)
				.contextPut("example", "bar")
				.requireFusion(Fuseable.ASYNC, Fuseable.SYNC);

		assertThat(testSubscriber.initialRequest).as("initial request").isEqualTo(123L);
		assertThat(testSubscriber.context.stream().flatMap(e -> Stream.of(e.getKey(), e.getValue()))).as("context").containsExactly("example", "foo");
		assertThat(testSubscriber.requestedFusionMode).as("requestedFusionMode").isEqualTo(Fuseable.ANY);
		assertThat(testSubscriber.expectedFusionMode).as("expectedFusionMode").isEqualTo(Fuseable.ASYNC);
		assertThat(testSubscriber.fusionRequirement).as("fusionRequirement").isEqualTo(TestSubscriber.FusionRequirement.FUSEABLE);
	}
}
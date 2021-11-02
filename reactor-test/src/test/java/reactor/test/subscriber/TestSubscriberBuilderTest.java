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

class TestSubscriberBuilderTest {

	@Test
	void smokeInstanceOfTest() {
		TestSubscriberBuilder baseBuilder = TestSubscriber.builder().initialRequest(123L).requireFusion(Fuseable.ANY);

		assertThat(baseBuilder.build())
				.as("base")
				.isInstanceOf(CoreSubscriber.class)
				.isNotInstanceOf(Fuseable.ConditionalSubscriber.class);

		assertThat(baseBuilder.buildConditional(v -> true))
				.as("conditional")
				.isInstanceOf(CoreSubscriber.class)
				.isInstanceOf(Fuseable.ConditionalSubscriber.class);
	}

	@Test
	void defaultInitialRequestIsUnbounded() {
		TestSubscriberBuilder builder = TestSubscriber.builder();

		assertThat(builder.initialRequest).as("default in builder").isEqualTo(Long.MAX_VALUE);

		DefaultTestSubscriber<?> concreteTestSubscriber = (DefaultTestSubscriber<?>) builder.build();
		assertThat(concreteTestSubscriber.initialRequest).as("consistent in subscriber").isEqualTo(Long.MAX_VALUE);
	}

	@Test
	void initialRequestMethodsMutateBuilder() {
		TestSubscriberBuilder builder = TestSubscriber.builder();

		builder.initialRequest(123L);

		assertThat(builder.initialRequest).as("mutated initialRequest").isEqualTo(123L);

		DefaultTestSubscriber<?> concreteTestSubscriber = (DefaultTestSubscriber<?>) builder.build();
		assertThat(concreteTestSubscriber.initialRequest).as("consistent in subscriber").isEqualTo(123L);

		builder.initialRequestUnbounded();

		assertThat(builder.initialRequest).as("mutated initialRequestUnbounded").isEqualTo(Long.MAX_VALUE);
	}

	@Test
	void defaultContextIsEmpty() {
		TestSubscriberBuilder builder = TestSubscriber.builder();

		assertThat(builder.context).as("default in builder").isEqualTo(Context.empty());
		assertThat(builder.build().currentContext()).as("consistent in subscriber").isSameAs(builder.context);
	}

	@Test
	void contextPutMethodsMutateBuilder() {
		TestSubscriberBuilder builder = TestSubscriber.builder()
				.contextPut("test", 1);

		assertThat(builder.context).as("mutated contextPut")
				.usingRecursiveComparison()
				.isEqualTo(Context.of("test", 1));
		assertThat(builder.build().currentContext()).as("consistent in subscriber").isSameAs(builder.context);

		builder.contextPutAll(Context.of("example", true));

		assertThat(builder.context)
				.as("mutated contextPutAll")
				.usingRecursiveComparison()
				.isEqualTo(Context.of("test", 1, "example", true));
	}

	@Test
	void defaultFusionIsDisabled() {
		TestSubscriberBuilder builder = TestSubscriber.builder();

		assertThat(builder.requestedFusionMode).as("default fusion modes in builder").isEqualTo(builder.expectedFusionMode).isZero();
		final DefaultTestSubscriber<Object> concreteTestSubscriber = (DefaultTestSubscriber<Object>) builder.build();
		assertThat(concreteTestSubscriber.requestedFusionMode).as("consistent in subscriber").isEqualTo(concreteTestSubscriber.expectedFusionMode).isZero();
	}

	@Test
	void defaultFusionIsNotRequired() {
		TestSubscriberBuilder builder = TestSubscriber.builder();

		assertThat(builder.fusionRequirement).as("default fusion requirement in builder").isEqualTo(DefaultTestSubscriber.FusionRequirement.NONE);
		final DefaultTestSubscriber<Object> concreteTestSubscriber = (DefaultTestSubscriber<Object>) builder.build();
		assertThat(concreteTestSubscriber.fusionRequirement).as("consistent in subscriber").isEqualTo(builder.fusionRequirement);
	}

	@Test
	void fusionMethodsMutateBuilder() {
		TestSubscriberBuilder builder = TestSubscriber.builder();

		builder.requireFusion(Fuseable.ANY, Fuseable.ASYNC);

		assertThat(builder.fusionRequirement).as("fusion requirement").isEqualTo(DefaultTestSubscriber.FusionRequirement.FUSEABLE);
		assertThat(builder.requestedFusionMode).as("fusion mode requested").isEqualTo(Fuseable.ANY);
		assertThat(builder.expectedFusionMode).as("fusion mode expected").isEqualTo(Fuseable.ASYNC);

		builder.requireFusion(Fuseable.NONE);

		assertThat(builder.fusionRequirement).as("fusion requirement once NONE").isEqualTo(DefaultTestSubscriber.FusionRequirement.NONE);
		assertThat(builder.requestedFusionMode).as("fusion mode requested once NONE").isEqualTo(Fuseable.NONE);
		assertThat(builder.expectedFusionMode).as("fusion mode expected once NONE").isEqualTo(Fuseable.NONE);
	}

	@Test
	void subscriberCreatedCopiesBuilderConfig() {
		TestSubscriberBuilder builder = TestSubscriber.builder()
				.initialRequest(123L)
				.contextPut("example", "foo")
				.requireFusion(Fuseable.ANY, Fuseable.ASYNC);

		final DefaultTestSubscriber<Object> concreteTestSubscriber = (DefaultTestSubscriber<Object>) builder.build();

		TestSubscriberBuilder mutatedBuilder = builder
				.initialRequest(456)
				.contextPut("example", "bar")
				.requireFusion(Fuseable.ASYNC, Fuseable.SYNC);

		assertThat(builder.initialRequest)
			.as("original builder mutated")
			.isEqualTo(mutatedBuilder.initialRequest)
			.isEqualTo(456L);

		assertThat(concreteTestSubscriber.initialRequest).as("initial request").isEqualTo(123L);
		assertThat(concreteTestSubscriber.context.stream().flatMap(e -> Stream.of(e.getKey(), e.getValue()))).as("context").containsExactly("example", "foo");
		assertThat(concreteTestSubscriber.requestedFusionMode).as("requestedFusionMode").isEqualTo(Fuseable.ANY);
		assertThat(concreteTestSubscriber.expectedFusionMode).as("expectedFusionMode").isEqualTo(Fuseable.ASYNC);
		assertThat(concreteTestSubscriber.fusionRequirement).as("fusionRequirement").isEqualTo(DefaultTestSubscriber.FusionRequirement.FUSEABLE);
	}
}
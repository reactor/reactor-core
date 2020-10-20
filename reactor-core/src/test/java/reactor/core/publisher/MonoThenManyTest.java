/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import org.junit.jupiter.api.Test;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Stephane Maldini
 */
public class MonoThenManyTest {

	@Test
	public void testThenManySameType() {
		Flux<String> test = Mono.just("A")
		                        .thenMany(Flux.just("C", "D"));

		AssertSubscriber<String> ts = AssertSubscriber.create();
		test.subscribe(ts);
		ts.assertValues("C", "D");
		ts.assertComplete();
	}

	@Test
	public void testThenManyFusion() {
		Flux<Integer> test = Mono.just("A")
		                         .thenMany(Flux.just("C", "D"))
		                         .thenMany(Flux.just(1, 2));

		assertThat(test).isInstanceOf(FluxConcatArray.class);
		FluxConcatArray<Integer> s = (FluxConcatArray<Integer>) test;

		assertThat(s.array).hasSize(3);

		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		test.subscribe(ts);
		ts.assertValues(1, 2);
		ts.assertComplete();
	}

	@Test
	public void testThenManyDifferentType() {
		Flux<String> test = Mono.just(1)
		                        .thenMany(Flux.just("C", "D"));

		AssertSubscriber<String> ts = AssertSubscriber.create();
		test.subscribe(ts);
		ts.assertValues("C", "D");
		ts.assertComplete();
	}
}

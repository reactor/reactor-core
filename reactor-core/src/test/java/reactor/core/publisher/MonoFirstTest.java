/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import java.time.Duration;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoFirstTest {

	@Test(timeout = 5000)
	public void allEmpty() {
		Assert.assertNull(Mono.first(Mono.empty(),
				Mono.delay(Duration.ofMillis(250))
				    .ignoreElement())
		                      .block());
	}

	@Test(timeout = 5000)
	public void someEmpty() {
		Assert.assertNull(Mono.first(Mono.empty(), Mono.delay(Duration.ofMillis(250)))
		                      .block());
	}

	@Test//(timeout = 5000)
	public void all2NonEmpty() {
		Assert.assertEquals(Integer.MIN_VALUE,
				Mono.first(Mono.delay(Duration.ofMillis(150))
				               .map(i -> Integer.MIN_VALUE), Mono.delay(Duration.ofMillis(250)))
				    .block());
	}

	@Test
	public void pairWise() {
		Mono<Integer> f = Mono.first(Mono.just(1), Mono.just(2))
		                      .or(Mono.just(3));

		Assert.assertTrue(f instanceof MonoFirst);
		MonoFirst<Integer> s = (MonoFirst<Integer>) f;
		Assert.assertTrue(s.array != null);
		Assert.assertTrue(s.array.length == 3);

		f.subscribeWith(AssertSubscriber.create())
		 .assertValues(1)
		 .assertComplete();
	}

	@Test(timeout = 5000)
	public void allEmptyIterable() {
		Assert.assertNull(Mono.first(Arrays.asList(Mono.empty(),
				Mono.delay(Duration.ofMillis(250))
				    .ignoreElement()))
		                      .block());
	}

	@Test(timeout = 5000)
	public void someEmptyIterable() {
		Assert.assertNull(Mono.first(Arrays.asList(Mono.empty(),
				Mono.delay(Duration.ofMillis(250))))
		                      .block());
	}

	@Test//(timeout = 5000)
	public void all2NonEmptyIterable() {
		Assert.assertEquals(Integer.MIN_VALUE,
				Mono.first(Mono.delay(Duration.ofMillis(150))
				               .map(i -> Integer.MIN_VALUE), Mono.delay(Duration.ofMillis(250)))
				    .block());
	}

	@Test
	public void pairWiseIterable() {
		Mono<Integer> f = Mono.first(Arrays.asList(Mono.just(1), Mono.just(2)))
		                      .or(Mono.just(3));

		Assert.assertTrue(f instanceof MonoFirst);
		MonoFirst<Integer> s = (MonoFirst<Integer>) f;
		Assert.assertTrue(s.array != null);
		Assert.assertTrue(s.array.length == 2);

		f.subscribeWith(AssertSubscriber.create())
		 .assertValues(1)
		 .assertComplete();
	}


	@Test
	public void firstMonoJust() {
		MonoProcessor<Integer> mp = MonoProcessor.create();
		StepVerifier.create(Mono.first(Mono.just(1), Mono.just(2))
		                        .subscribeWith(mp))
		            .then(() -> assertThat(mp.isError()).isFalse())
		            .then(() -> assertThat(mp.isSuccess()).isTrue())
		            .then(() -> assertThat(mp.isTerminated()).isTrue())
		            .expectNext(1)
		            .verifyComplete();
	}

	Mono<Integer> scenario_fastestSource() {
		return Mono.first(Mono.delay(Duration.ofSeconds(4))
		                      .map(s -> 1),
				Mono.delay(Duration.ofSeconds(3))
				    .map(s -> 2));
	}

	@Test
	public void fastestSource() {
		StepVerifier.withVirtualTime(this::scenario_fastestSource)
		            .thenAwait(Duration.ofSeconds(4))
		            .expectNext(2)
		            .verifyComplete();
	}

	@Test
	public void scanOperator(){
		MonoFirst<Integer> test = new MonoFirst<>(Mono.just(1), Mono.just(2));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}

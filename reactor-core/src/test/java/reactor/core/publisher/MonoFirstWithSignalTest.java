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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static java.lang.Integer.MIN_VALUE;
import static java.time.Duration.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.publisher.Mono.delay;
import static reactor.core.publisher.Mono.firstWithSignal;

public class MonoFirstWithSignalTest {

	@Test
	@Timeout(5)
	public void allEmpty() {
		assertThat(Mono.firstWithSignal(Mono.empty(),
				Mono.delay(Duration.ofMillis(250))
				    .ignoreElement())
		                      .block()).isNull();
	}

	@Test
	@Timeout(5)
	public void someEmpty() {
		assertThat(Mono.firstWithSignal(Mono.empty(), Mono.delay(Duration.ofMillis(250)))
		                      .block()).isNull();
	}

	@Test//(timeout = 5000)
	public void all2NonEmpty() {
		assertThat(firstWithSignal(delay(ofMillis(150))
				.map(i -> MIN_VALUE), delay(ofMillis(250)))
				.block()).isEqualTo(MIN_VALUE);
	}

	@Test
	public void pairWise() {
		Mono<Integer> f = Mono.firstWithSignal(Mono.just(1), Mono.just(2))
		                      .or(Mono.just(3));

		assertThat(f).isInstanceOf(MonoFirstWithSignal.class);
		MonoFirstWithSignal<Integer> s = (MonoFirstWithSignal<Integer>) f;
		assertThat(s.array).isNotNull();
		assertThat(s.array).hasSize(3);

		f.subscribeWith(AssertSubscriber.create())
		 .assertValues(1)
		 .assertComplete();
	}

	@Test
	@Timeout(5)
	public void allEmptyIterable() {
		assertThat(Mono.firstWithSignal(Arrays.asList(Mono.empty(),
				Mono.delay(Duration.ofMillis(250))
				    .ignoreElement()))
		                      .block()).isNull();
	}

	@Test
	@Timeout(5)
	public void someEmptyIterable() {
		assertThat(Mono.firstWithSignal(Arrays.asList(Mono.empty(),
				Mono.delay(Duration.ofMillis(250))))
		                      .block()).isNull();
	}

	@Test//(timeout = 5000)
	public void all2NonEmptyIterable() {
		assertThat(Mono.firstWithSignal(Mono.delay(Duration.ofMillis(150))
				.map(i -> Integer.MIN_VALUE), Mono.delay(Duration.ofMillis(250)))
				.block()).isEqualTo(Integer.MIN_VALUE);
	}

	@Test
	public void pairWiseIterable() {
		Mono<Integer> f = Mono.firstWithSignal(Arrays.asList(Mono.just(1), Mono.just(2)))
		                      .or(Mono.just(3));

		assertThat(f).isInstanceOf(MonoFirstWithSignal.class);
		MonoFirstWithSignal<Integer> s = (MonoFirstWithSignal<Integer>) f;
		assertThat(s.array).isNotNull();
		assertThat(s.array).hasSize(2);

		f.subscribeWith(AssertSubscriber.create())
		 .assertValues(1)
		 .assertComplete();
	}


	@Test
	public void firstMonoJust() {
		StepVerifier.create(Mono.firstWithSignal(Mono.just(1), Mono.just(2)))
		            .expectNext(1)
		            .verifyComplete();
	}

	Mono<Integer> scenario_fastestSource() {
		return Mono.firstWithSignal(Mono.delay(Duration.ofSeconds(4))
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
		@SuppressWarnings("unchecked") MonoFirstWithSignal<Integer>
				test = new MonoFirstWithSignal<>(Mono.just(1), Mono.just(2));

		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}

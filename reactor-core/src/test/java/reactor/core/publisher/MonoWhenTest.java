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

import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoWhenTest {


	@Test
	public void pairWise() {
		Mono<Void> f = Mono.just(1)
		                   .and(Mono.just("test2"));

		assertThat(f).isInstanceOf(MonoWhen.class);
		MonoWhen s = (MonoWhen) f;
		assertThat(s.sources).isNotNull();
		assertThat(s.sources).hasSize(2);

		f.subscribeWith(AssertSubscriber.create())
		 .assertComplete()
		.assertNoValues();
	}


	@Test
	public void allEmptyPublisherIterable() {
		Mono<Void> test = Mono.when(Arrays.asList(Mono.empty(), Flux.empty()));
		StepVerifier.create(test)
		            .verifyComplete();
	}

	@Test
	public void allEmptyPublisher() {
		Mono<Void> test = Mono.when(Mono.empty(), Flux.empty());
		StepVerifier.create(test)
		            .verifyComplete();
	}

	@Test
	public void someEmptyPublisher() {
		Mono<Void> test = Mono.when(Mono.just(1), Flux.empty());
		StepVerifier.create(test)
		            .verifyComplete();
	}

	@Test
	public void noEmptyPublisher() {
		Mono<Void> test = Mono.when(Mono.just(1), Flux.just(3));
		StepVerifier.create(test)
		            .verifyComplete();
	}

	@Test
	public void noSourcePublisher() {
		Mono<Void> test = Mono.when();
		StepVerifier.create(test)
		            .verifyComplete();
	}

	@Test
	public void oneSourcePublisher() {
		Mono<Void> test = Mono.when(Flux.empty());
		StepVerifier.create(test)
		            .verifyComplete();
	}

	@Test
	public void allEmptyPublisherDelay() {
		Mono<Void> test = Mono.whenDelayError(Mono.empty(), Flux.empty());
		StepVerifier.create(test)
		            .verifyComplete();
	}

	@Test
	public void noSourcePublisherDelay() {
		Mono<Void> test = Mono.whenDelayError();
		StepVerifier.create(test)
		            .verifyComplete();
	}

	@Test
	public void oneSourcePublisherDelay() {
		Mono<Void> test = Mono.whenDelayError(Flux.empty());
		StepVerifier.create(test)
		            .verifyComplete();
	}

	@Test
	public void whenIterableDelayErrorPublishersVoidCombinesErrors() {
		Exception boom1 = new NullPointerException("boom1");
		Exception boom2 = new IllegalArgumentException("boom2");

		Iterable<Publisher<Void>> voidPublishers = Arrays.asList(
				Mono.<Void>empty(),
				Mono.<Void>error(boom1),
				Mono.<Void>error(boom2));

		StepVerifier.create(Mono.whenDelayError(voidPublishers))
		            .verifyErrorMatches(e -> e.getMessage().equals("Multiple exceptions") &&
				            e.getSuppressed()[0] == boom1 &&
				            e.getSuppressed()[1] == boom2);
	}

	@Test
	public void whenIterablePublishersVoidDoesntCombineErrors() {
		Exception boom1 = new NullPointerException("boom1");
		Exception boom2 = new IllegalArgumentException("boom2");

		Iterable<Publisher<Void>> voidPublishers = Arrays.asList(
				Mono.<Void>empty(),
				Mono.<Void>error(boom1),
				Mono.<Void>error(boom2));

		StepVerifier.create(Mono.when(voidPublishers))
		            .verifyErrorMatches(e -> e == boom1);
	}

	@Test
	public void scanOperator() {
		MonoWhen s = new MonoWhen(true);
		assertThat(s.scan(Scannable.Attr.DELAY_ERROR)).as("delayError").isTrue();
		assertThat(s.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}
}

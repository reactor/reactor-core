/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.Arrays;

import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.test.StepVerifier;

public class MonoWhenTest {

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
}

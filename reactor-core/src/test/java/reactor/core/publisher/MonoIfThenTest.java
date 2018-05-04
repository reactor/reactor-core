/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

import org.junit.Test;
import reactor.test.StepVerifier;

/**
 * @author Simon Baslé
 */
public class MonoIfThenTest {

	@Test
	public void ifThenElseTrue() {
		StepVerifier.create(Mono.just(true)
		                        .ifThen(Mono.just(1),
				                        Mono.just(10)))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void ifThenElseFalse() {
		StepVerifier.create(Mono.just(false)
		                        .ifThen(Mono.just(1),
				                        Mono.just(10)))
		            .expectNext(10)
		            .verifyComplete();
	}

	@Test
	public void ifThenElseEmpty() {
		StepVerifier.create(Mono.<Boolean>empty()
				.ifThen(Mono.just(1),
						Mono.just(10)))
		            .expectNext(10)
		            .verifyComplete();
	}

	@Test
	public void ifThenElsePredicateTrue() {
		StepVerifier.create(Mono.just("A")
		                        .ifThen(s -> s.length() == 1,
				                        Mono.just(1),
				                        Mono.just(10)))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void ifThenElsePredicateFalse() {
		StepVerifier.create(Mono.just("AA")
		                        .ifThen(s -> s.length() == 1,
				                        Mono.just(1),
				                        Mono.just(10)))
		            .expectNext(10)
		            .verifyComplete();
	}

	@Test
	public void ifThenElsePredicateEmpty() {
		StepVerifier.create(Mono.<String>empty()
				.ifThen(s -> s.length() == 1,
						Mono.just(1),
						Mono.just(10)))
		            .expectNext(10)
		            .verifyComplete();
	}

	@Test
	public void ifThenTrue() {
		StepVerifier.create(Mono.just(true)
		                        .ifThen(Mono.just(1)))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void ifThenFalse() {
		StepVerifier.create(Mono.just(false)
		                        .ifThen(Mono.just(1)))
		            .verifyComplete();
	}

	@Test
	public void ifThenEmpty() {
		StepVerifier.create(Mono.<String>empty()
				.ifThen(Mono.just(1)))
		            .verifyComplete();
	}
	
	@Test
	public void ifThenNonBooleanValue() {
		StepVerifier.create(Mono.just("foo")
		                        .ifThen(Mono.just(1)))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void ifThenPredicateTrue() {
		StepVerifier.create(Mono.just("A")
		                        .ifThen(s -> s.length() == 1,
				                        Mono.just(1)))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void ifThenPredicateFalse() {
		StepVerifier.create(Mono.just("AA")
		                        .ifThen(s -> s.length() == 1,
				                        Mono.just(1)))
		            .verifyComplete();
	}

	@Test
	public void ifThenPredicateEmpty() {
		StepVerifier.create(Mono.<String>empty()
				.ifThen(s -> s.length() == 1,
						Mono.just(1)))
		            .verifyComplete();
	}
	
	@Test
	public void ifThenManyElseTrue() {
		StepVerifier.create(Mono.just(true)
		                        .ifThenMany(Flux.range(1, 2),
				                        Flux.range(10, 2)))
		            .expectNext(1, 2)
		            .verifyComplete();
	}

	@Test
	public void ifThenManyElseFalse() {
		StepVerifier.create(Mono.just(false)
		                        .ifThenMany(Flux.range(1, 2),
				                        Flux.range(10, 2)))
		            .expectNext(10, 11)
		            .verifyComplete();
	}

	@Test
	public void ifThenManyElseEmpty() {
		StepVerifier.create(Mono.<Boolean>empty()
				.ifThenMany(Flux.range(1, 2),
						Flux.range(10, 2)))
		            .expectNext(10, 11)
		            .verifyComplete();
	}

	@Test
	public void ifThenManyElsePredicateTrue() {
		StepVerifier.create(Mono.just("A")
		                        .ifThenMany(s -> s.length() == 1,
				                        Flux.range(1, 2),
				                        Flux.range(10, 2)))
		            .expectNext(1, 2)
		            .verifyComplete();
	}

	@Test
	public void ifThenManyElsePredicateFalse() {
		StepVerifier.create(Mono.just("AA")
		                        .ifThenMany(s -> s.length() == 1,
				                        Flux.range(1, 2),
				                        Flux.range(10, 2)))
		            .expectNext(10, 11)
		            .verifyComplete();
	}

	@Test
	public void ifThenManyElsePredicateEmpty() {
		StepVerifier.create(Mono.<String>empty()
				.ifThenMany(s -> s.length() == 1,
						Flux.range(1, 2),
						Flux.range(10, 2)))
		            .expectNext(10, 11)
		            .verifyComplete();
	}

	@Test
	public void ifThenManyTrue() {
		StepVerifier.create(Mono.just(true)
		                        .ifThenMany(Flux.range(1, 2)))
		            .expectNext(1, 2)
		            .verifyComplete();
	}

	@Test
	public void ifThenManyFalse() {
		StepVerifier.create(Mono.just(false)
		                        .ifThenMany(Flux.range(1, 2)))
		            .verifyComplete();
	}

	@Test
	public void ifThenManyEmpty() {
		StepVerifier.create(Mono.<String>empty()
				.ifThenMany(Flux.range(1, 2)))
		            .verifyComplete();
	}

	@Test
	public void ifThenManyNonBooleanValue() {
		StepVerifier.create(Mono.just("foo")
		                        .ifThenMany(Flux.range(1, 2)))
		            .expectNext(1, 2)
		            .verifyComplete();
	}

	@Test
	public void ifThenManyPredicateTrue() {
		StepVerifier.create(Mono.just("A")
		                        .ifThenMany(s -> s.length() == 1,
				                        Flux.range(1, 2)))
		            .expectNext(1, 2)
		            .verifyComplete();
	}

	@Test
	public void ifThenManyPredicateFalse() {
		StepVerifier.create(Mono.just("AA")
		                        .ifThenMany(s -> s.length() == 1,
				                        Flux.range(1, 2)))
		            .verifyComplete();
	}

	@Test
	public void ifThenManyPredicateEmpty() {
		StepVerifier.create(Mono.<String>empty()
				.ifThenMany(s -> s.length() == 1,
						Flux.range(1, 2)))
		            .verifyComplete();
	}

}

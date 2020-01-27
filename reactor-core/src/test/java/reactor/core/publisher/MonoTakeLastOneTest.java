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

import java.util.NoSuchElementException;
import java.util.function.Predicate;

import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple3;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoTakeLastOneTest {

	@Test
	public void emptyThrowsNoSuchElement() {
		StepVerifier.create(Flux.empty()
		                        .hide()
		                        .last())
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(NoSuchElementException.class)
		                                                    .hasMessage("Flux#last() didn't observe any onNext signal"));
	}

	@Test
	public void emptyCallableThrowsNoSuchElement() {
		StepVerifier.create(Flux.empty()
		                        .last())
		            .verifyErrorSatisfies(e -> assertThat(e).isInstanceOf(NoSuchElementException.class)
		                                                    .hasMessage("Flux#last() didn't observe any onNext signal from Callable flux"));
	}

	@Test
	public void fallback() {
		StepVerifier.create(Flux.empty()
		                        .last(1))
		            .expectNext(1)
		            .verifyComplete();
	}

	@Test
	public void error() {
		StepVerifier.create(Flux.error(new Exception("test"))
		                        .last())
		            .verifyErrorMessage("test");
	}

	@Test
	public void errorHide() {
		StepVerifier.create(Flux.error(new Exception("test"))
		                        .hide()
		                        .last())
		            .verifyErrorMessage("test");
	}

	@Test
	public void errorDefault() {
		StepVerifier.create(Flux.error(new Exception("test"))
		                        .last("blah"))
		            .verifyErrorMessage("test");
	}

	@Test
	public void errorHideDefault() {
		StepVerifier.create(Flux.error(new Exception("test"))
		                        .hide()
		                        .last("blah"))
		            .verifyErrorMessage("test");
	}

	@Test
	public void normal() {
		StepVerifier.create(Flux.range(1, 100)
		                        .last())
		            .expectNext(100)
		            .verifyComplete();
	}

	@Test
	public void normal2() {
		StepVerifier.create(Flux.range(1, 100)
		                        .last(-1))
		            .expectNext(100)
		            .verifyComplete();
	}


	@Test
	public void normal3() {
		StepVerifier.create(Mono.fromCallable(() -> 100)
		                        .flux()
		                        .last(-1))
		            .expectNext(100)
		            .verifyComplete();
	}

	@Test
	public void normalHide() {
		StepVerifier.create(Flux.range(1, 100)
		                        .hide()
		                        .last())
		            .expectNext(100)
		            .verifyComplete();
	}

	@Test
	public void norma2() {
		StepVerifier.create(Flux.just(100)
		                        .last(-1))
		            .expectNext(100)
		            .verifyComplete();
	}

	@Test
	public void defaultUsingZip() {

		UnicastProcessor<String> processor1 = UnicastProcessor.create();
		FluxSink<String> sink1 = processor1.sink();
		UnicastProcessor<String> processor2 = UnicastProcessor.create();
		FluxSink<String> sink2 = processor2.sink();
		UnicastProcessor<String> processor3 = UnicastProcessor.create();
		FluxSink<String> sink3 = processor3.sink();

		StepVerifier.create(
						Flux.zip(
								processor1.last("Missing Value1"),
								processor2.last("Missing Value2"),
								processor3.last("Missing Value3")
						)
				)
				.then(() -> {
					sink2.next("3");
					sink3.next("1");
					sink1.complete();
					sink2.complete();
					sink3.complete();
				})
				.expectNextMatches(objects -> objects.getT1().equals("Missing Value1") && objects.getT2().equals("3") && objects.getT3().equals("1"))
				.verifyComplete();
	}

	@Test
	public void scanTakeLastOneSubscriber() {
		CoreSubscriber<String>
				actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoTakeLastOne.TakeLastOneSubscriber<String> test = new MonoTakeLastOne.TakeLastOneSubscriber<>(
				actual, "foo", true);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);

		//terminated is detected via state HAS_VALUE
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.complete("bar");
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}
}

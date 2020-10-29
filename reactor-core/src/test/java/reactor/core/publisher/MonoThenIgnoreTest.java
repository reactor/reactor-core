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

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoThenIgnoreTest {

	@Test
	public void normal() {
		StepVerifier.create(Mono.just(1)
		                        .thenEmpty(Flux.empty()))
		            .verifyComplete();
	}

	Publisher<Void> scenario(){
		return Mono.just(1)
		    .thenEmpty(Mono.delay(Duration.ofSeconds(123)).then());
	}

	@Test
	public void normal3() {
		StepVerifier.create(Mono.just(1)
		                        .then())
		            .verifyComplete();
	}

	@Test
	public void chained() {
		StepVerifier.create(Mono.just(0)
		                        .then(Mono.just(1))
		                        .then(Mono.just(2)))
		            .expectNext(2)
		            .verifyComplete();
	}


	@Test
    public void thenReturn() {
	    StepVerifier.create(Mono.just(0).thenReturn(2))
                    .expectNext(2)
                    .verifyComplete();
    }

	@Test
	public void normalTime() {
		StepVerifier.withVirtualTime(this::scenario)
		            .thenAwait(Duration.ofSeconds(123))
		            .verifyComplete();
	}

	@Test
	public void cancel() {
		TestPublisher<String> cancelTester = TestPublisher.create();

		StepVerifier.create(cancelTester.flux()
										.then())
					.thenCancel()
					.verify();

		cancelTester.assertCancelled();
	}

	@Test
	public void scanThenAcceptInner() {
		CoreSubscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoIgnoreThen.ThenIgnoreMain<String> main = new MonoIgnoreThen.ThenIgnoreMain<>(actual, new Publisher[0], Mono.just("foo"));

		MonoIgnoreThen.ThenAcceptInner<String> test = new MonoIgnoreThen.ThenAcceptInner<>(main);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	@Test
	public void scanThenIgnoreInner() {
		CoreSubscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoIgnoreThen.ThenIgnoreMain<String> main = new MonoIgnoreThen.ThenIgnoreMain<>(actual, new Publisher[0], Mono.just("foo"));

		MonoIgnoreThen.ThenIgnoreInner test = new MonoIgnoreThen.ThenIgnoreInner(main);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(main);

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}

	//see https://github.com/reactor/reactor-core/issues/661
	@Test
	public void fluxThenMonoAndShift() {
		StepVerifier.create(Flux.just("Good Morning", "Hello")
		                        .then(Mono.just("Good Afternoon"))
		                        .then(Mono.just("Bye")))
		            .expectNext("Bye")
		            .verifyComplete();
	}

	//see https://github.com/reactor/reactor-core/issues/661
	@Test
	public void monoThenMonoAndShift() {
		StepVerifier.create(Mono.just("Good Morning")
		                        .then(Mono.just("Good Afternoon"))
		                        .then(Mono.just("Bye")))
		            .expectNext("Bye")
		            .verifyComplete();
	}
}

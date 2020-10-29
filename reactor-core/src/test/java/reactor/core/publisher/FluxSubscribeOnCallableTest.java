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

import java.io.IOException;
import java.time.Duration;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.CoreSubscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxSubscribeOnCallableTest {

	@Test
	public void error() {
		StepVerifier.create(Flux.error(new RuntimeException("forced failure"))
		                        .subscribeOn(Schedulers.single()))
		            .verifyErrorMessage("forced failure");
	}

	@Test
	public void errorHide() {
		StepVerifier.create(Flux.error(new RuntimeException("forced failure"))
		                        .hide()
		                        .subscribeOn(Schedulers.single()))
		            .verifyErrorMessage("forced failure");
	}

	@Test
	public void callableReturnsNull() {
		StepVerifier.create(Mono.empty()
		                        .flux()
		                        .subscribeOn(Schedulers.single()))
		            .verifyComplete();
	}

	@Test
	public void callableReturnsNull2() {
		StepVerifier.create(Mono.fromCallable(() -> null)
		                        .flux()
		                        .subscribeOn(Schedulers.single()), 0)
		            .verifyComplete();
	}

	@Test
	public void callableReturnsNull3() {
		StepVerifier.create(Mono.fromCallable(() -> null)
		                        .flux()
		                        .subscribeOn(Schedulers.single()), 1)
		            .verifyComplete();
	}

	@Test
	public void normal() {
		StepVerifier.create(Mono.fromCallable(() -> 1)
		                        .flux()
		                        .subscribeOn(Schedulers.single()))
		            .expectNext(1)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void normalBackpressured() {
		StepVerifier.withVirtualTime(() -> Mono.fromCallable(() -> 1)
		                                       .flux()
		                                       .subscribeOn(Schedulers.single()), 0)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(1))
		            .thenRequest(1)
		            .thenAwait()
		            .expectNext(1)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void callableReturnsNullFused() {
		StepVerifier.create(Mono.empty()
		                        .flux()
		                        .subscribeOn(Schedulers.single()))
		            .expectFusion(Fuseable.ASYNC)
		            .verifyComplete();
	}

	@Test
	public void callableReturnsNullFused2() {
		StepVerifier.create(Mono.fromCallable(() -> null)
		                        .flux()
		                        .subscribeOn(Schedulers.single())
				.doOnNext(v -> System.out.println(v)), 1)
		            .expectFusion(Fuseable.ASYNC)
		            .thenRequest(1)
		            .verifyComplete();
	}

	@Test
	public void callableReturnsNullFused3() {
		StepVerifier.create(Mono.fromCallable(() -> null)
		                        .flux()
		                        .subscribeOn(Schedulers.single()), 0)
		            .expectFusion(Fuseable.ASYNC)
		            .verifyComplete();
	}

	@Test
	public void normalFused() {
		StepVerifier.create(Mono.fromCallable(() -> 1)
		                        .flux()
		                        .subscribeOn(Schedulers.single()))
		            .expectFusion(Fuseable.ASYNC)
		            .expectNext(1)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void normalBackpressuredFused() {
		StepVerifier.withVirtualTime(() -> Mono.fromCallable(() -> 1)
		                                       .flux()
		                                       .subscribeOn(
				Schedulers.single()), 0)
		            .expectFusion(Fuseable.ASYNC)
		            .thenAwait()
		            .consumeSubscriptionWith(s -> {
		            	assertThat(FluxSubscribeOnCallable
					            .CallableSubscribeOnSubscription.class.cast(s)
			            .size()).isEqualTo(1);
		            })
		            .thenRequest(1)
		            .thenAwait()
		            .expectNext(1)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void normalBackpressuredFusedCancelled() {
		StepVerifier.withVirtualTime(() -> Mono.fromCallable(() -> 1)
		                                       .flux()
		                                       .subscribeOn(
				Schedulers.single()), 0)
		            .expectFusion(Fuseable.ASYNC)
		            .thenAwait()
		            .thenCancel()
		            .verify();
	}

	@Test
	public void callableThrows() {
		StepVerifier.create(Mono.fromCallable(() -> {
			throw new IOException("forced failure");
		})
		                        .flux()
		                        .subscribeOn(Schedulers.single()))
		            .expectErrorMatches(e -> e instanceof IOException
				            && e.getMessage().equals("forced failure"))
		            .verify();
	}

	@Test
	public void scanOperator() {
		FluxSubscribeOnCallable test = new FluxSubscribeOnCallable<>(() -> "foo", Schedulers.immediate());

		assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(Schedulers.immediate());
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
	}

	@Test
    public void scanMainSubscriber() {
        CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxSubscribeOnCallable.CallableSubscribeOnSubscription<Integer> test =
        		new FluxSubscribeOnCallable.CallableSubscribeOnSubscription<Integer>(actual, () -> 1, Schedulers.single());

        Assertions.assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
        Assertions.assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(Schedulers.single());
        Assertions.assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
        test.value = 1;
        Assertions.assertThat(test.scan(Scannable.Attr.BUFFERED)).isEqualTo(1);

        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
        test.cancel();
        Assertions.assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
    }
}

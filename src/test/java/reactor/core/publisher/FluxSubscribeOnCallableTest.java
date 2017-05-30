/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
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
import org.junit.Test;
import org.reactivestreams.Subscriber;
import reactor.core.Fuseable;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxSubscribeOnCallableTest {

	@Test
	public void callableReturnsNull() {
		StepVerifier.create(Mono.empty()
		                        .flux()
		                        .subscribeOn(Schedulers.single()))
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
    public void scanMainSubscriber() {
        Subscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
        FluxSubscribeOnCallable.CallableSubscribeOnSubscription<Integer> test =
        		new FluxSubscribeOnCallable.CallableSubscribeOnSubscription<Integer>(actual, () -> 1, Schedulers.single());

        Assertions.assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);
        test.value = 1;
        Assertions.assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(1);

        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
        test.cancel();
        Assertions.assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
    }
}
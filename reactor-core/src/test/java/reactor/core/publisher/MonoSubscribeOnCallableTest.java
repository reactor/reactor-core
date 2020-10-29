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

import org.junit.jupiter.api.Test;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoSubscribeOnCallableTest {

	@Test
	public void normal() {
		StepVerifier.create(Mono.fromCallable(() -> 1).subscribeOn(Schedulers.single()))
		            .expectNext(1)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void normalBackpressured() {
		StepVerifier.withVirtualTime( () ->
				Mono.fromCallable(() -> 1).subscribeOn(Schedulers.single()), 0)
		            .expectSubscription()
		            .expectNoEvent(Duration.ofSeconds(1))
		            .thenRequest(1)
		            .thenAwait()
		            .expectNext(1)
		            .expectComplete()
		            .verify();
	}

	@Test
	public void callableThrows() {
		StepVerifier.create(Mono.fromCallable(() -> {
			throw new IOException("forced failure");
		}).subscribeOn(Schedulers.single()))
		            .expectErrorMatches(e -> e instanceof IOException
				            && e.getMessage().equals("forced failure"))
		            .verify();
	}

	@Test
	public void callableNull() {
		StepVerifier.create(Mono.fromCallable(() -> null).subscribeOn(Schedulers.single()))
		            .expectComplete()
		            .verify();
	}

	@Test
	public void callableNullBackpressured() {
		StepVerifier.create(
				Mono.fromCallable(() -> null).subscribeOn(Schedulers.single()), 0)
		            .expectSubscription()
		            .expectComplete()
		            .verify();
	}

	@Test
	public void scanOperator() {
		MonoSubscribeOnCallable<String> test = new MonoSubscribeOnCallable<>(() -> "foo", Schedulers.immediate());

		assertThat(test.scan(Scannable.Attr.RUN_ON)).isSameAs(Schedulers.immediate());
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.ASYNC);
	}
}

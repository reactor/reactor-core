/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoTakeLastOneTest {

	@Test
	public void empty() {
		StepVerifier.create(Flux.empty()
		                        .last())
		            .verifyComplete();
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
	public void scanTakeLastOneSubscriber() {
		Subscriber<String> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoTakeLastOne.TakeLastOneSubscriber<String> test = new MonoTakeLastOne.TakeLastOneSubscriber<>(
				actual, "foo", true);
		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

		//terminated is detected via state HAS_VALUE
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		test.complete("bar");
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}
}
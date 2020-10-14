/*
 * Copyright (c) 2020-Present Pivotal Software Inc, All Rights Reserved.
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
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxConcatMapNoPrefetchTest extends AbstractFluxConcatMapTest {

	@Override
	int implicitPrefetchValue() {
		return 0;
	}

	@Test
	public void noRequestBeforeOnCompleteWithZeroPrefetch() {
		AtomicBoolean firstCompleted = new AtomicBoolean(false);
		Flux
				.<Integer, Integer>generate(() -> 0, (i, sink) -> {
					switch (i) {
						case 0:
							sink.next(1);
							return 1;
						case 1:
							assertThat(firstCompleted).isTrue();
							sink.next(2);
							return 2;
						default:
							sink.complete();
							return -1;
					}
				})
				.concatMap(
						it -> {
							switch (it) {
								case 1:
									return Mono.delay(Duration.ofMillis(50))
									           .then(Mono.fromRunnable(() -> {
										           firstCompleted.set(true);
									           }))
									           .thenReturn(it);
								default:
									return Mono.just(it);
							}
						},
						0
				)
				.as(StepVerifier::create)
				.expectNext(1, 2)
				.expectComplete()
				.verify(Duration.ofSeconds(5));

		assertThat(firstCompleted).isTrue();
	}

	@Test
	public void scanOperator(){
		Flux<Integer> parent = Flux.just(1, 2);
		FluxConcatMapNoPrefetch<Integer, String> test = new FluxConcatMapNoPrefetch<>(parent, i -> Flux.just(i.toString()) , FluxConcatMap.ErrorMode.END);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isZero();
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanConcatMapNoPrefetchDelayError() {
		CoreSubscriber<Integer> actual = new LambdaSubscriber<>(null, e -> {}, null, null);
		FluxConcatMapNoPrefetch.FluxConcatMapNoPrefetchSubscriber<Integer, Integer> test =
				new FluxConcatMapNoPrefetch.FluxConcatMapNoPrefetchSubscriber<>(actual, Flux::just, FluxConcatMap.ErrorMode.END);

		Subscription parent = Operators.emptySubscription();
		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isZero();
		assertThat(test.scan(Scannable.Attr.DELAY_ERROR)).isTrue();
		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(actual);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.onComplete();
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}
}

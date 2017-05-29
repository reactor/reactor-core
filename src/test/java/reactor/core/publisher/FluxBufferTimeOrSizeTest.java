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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class FluxBufferTimeOrSizeTest {

	Flux<List<Integer>> scenario_bufferWithTimeoutAccumulateOnTimeOrSize() {
		return Flux.range(1, 6)
		           .delayElements(Duration.ofMillis(300))
		           .bufferTimeout(5, Duration.ofMillis(2000));
	}

	@Test
	public void bufferWithTimeoutAccumulateOnTimeOrSize() {
		StepVerifier.withVirtualTime(this::scenario_bufferWithTimeoutAccumulateOnTimeOrSize)
		            .thenAwait(Duration.ofMillis(1500))
		            .assertNext(s -> assertThat(s).containsExactly(1, 2, 3, 4, 5))
		            .thenAwait(Duration.ofMillis(2000))
		            .assertNext(s -> assertThat(s).containsExactly(6))
		            .verifyComplete();
	}

	Flux<List<Integer>> scenario_bufferWithTimeoutAccumulateOnTimeOrSize2() {
		return Flux.range(1, 6)
		           .delayElements(Duration.ofMillis(300))
		           .bufferTimeout(5, Duration.ofMillis(2000));
	}

	@Test
	public void bufferWithTimeoutAccumulateOnTimeOrSize2() {
		StepVerifier.withVirtualTime(this::scenario_bufferWithTimeoutAccumulateOnTimeOrSize2)
		            .thenAwait(Duration.ofMillis(1500))
		            .assertNext(s -> assertThat(s).containsExactly(1, 2, 3, 4, 5))
		            .thenAwait(Duration.ofMillis(2000))
		            .assertNext(s -> assertThat(s).containsExactly(6))
		            .verifyComplete();
	}

	@Test
	public void scanSubscriber() {
		Subscriber<List<String>> actual = new LambdaSubscriber<>(null, e -> {}, null, null);

		FluxBufferTimeOrSize.BufferTimeoutSubscriber<String, List<String>> test = new FluxBufferTimeOrSize.BufferTimeoutSubscriber<String, List<String>>(
						actual, 123, 1000, Schedulers.elastic().createWorker(), ArrayList::new);

		Subscription subscription = Operators.emptySubscription();
		test.onSubscribe(subscription);

		test.requested = 3L;
		test.index = 100;

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(subscription);
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.LongAttr.REQUESTED_FROM_DOWNSTREAM)).isEqualTo(3L);
		assertThat(test.scan(Scannable.IntAttr.CAPACITY)).isEqualTo(123);
		assertThat(test.scan(Scannable.IntAttr.BUFFERED)).isEqualTo(23);

		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();

		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();
	}

	@Test
	public void scanSubscriberCancelled() {
		Subscriber<List<String>> actual = new LambdaSubscriber<>(null, e -> {}, null, null);

		FluxBufferTimeOrSize.BufferTimeoutSubscriber<String, List<String>> test = new FluxBufferTimeOrSize.BufferTimeoutSubscriber<String, List<String>>(
						actual, 123, 1000, Schedulers.elastic().createWorker(), ArrayList::new);

		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();

		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
	}
}
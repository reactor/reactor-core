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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoStreamCollectorTest {

	@Test
	public void collectToList() {
		Mono<List<Integer>> source = Flux.range(1, 5).collect(Collectors.toList());

		for (int i = 0; i < 5; i++) {
			AssertSubscriber<List<Integer>> ts = AssertSubscriber.create();
			source.subscribe(ts);

			ts.assertValues(Arrays.asList(1, 2, 3, 4, 5))
			.assertNoError()
			.assertComplete();
		}
	}

	@Test
	public void collectToSet() {
		Mono<Set<Integer>> source = Flux.just(1).repeat(5).collect(Collectors.toSet());

		for (int i = 0; i < 5; i++) {
			AssertSubscriber<Set<Integer>> ts = AssertSubscriber.create();
			source.subscribe(ts);

			ts.assertValues(Collections.singleton(1))
			.assertNoError()
			.assertComplete();
		}
	}

	@Test
	public void scanStreamCollectorSubscriber() {
		Subscriber<List<String>> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		Collector<String, Integer, List<String>> collector = (Collector<String, Integer, List<String>>) Collectors.<String>toList();
		MonoStreamCollector.StreamCollectorSubscriber<String, Integer, List<String>> test = new MonoStreamCollector.StreamCollectorSubscriber<>(
				actual,
				1,
				collector.accumulator(),
				collector.finisher());
		Subscription parent = Operators.emptySubscription();

		test.onSubscribe(parent);

		assertThat(test.scan(Scannable.IntAttr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);

		assertThat(test.scan(Scannable.ScannableAttr.PARENT)).isSameAs(parent);
		assertThat(test.scan(Scannable.ScannableAttr.ACTUAL)).isSameAs(actual);

		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isFalse();
		test.onError(new IllegalStateException("boom"));
		assertThat(test.scan(Scannable.BooleanAttr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.BooleanAttr.CANCELLED)).isTrue();
	}

}

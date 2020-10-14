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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.function.Supplier;
import java.util.logging.Level;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.ParallelCollect.ParallelCollectSubscriber;
import reactor.test.StepVerifier;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;

public class ParallelCollectTest {

	@Test
	public void collect() {
		Supplier<List<Integer>> as = () -> new ArrayList<>();

		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Flux.range(1, 10)
		    .parallel()
		    .collect(as, (a, b) -> a.add(b))
		    .sequential()
		    .flatMapIterable(v -> v)
		    .log("ParallelCollectTest#collect", Level.FINE)
		    .subscribe(ts);

		ts.assertContainValues(new HashSet<>(Arrays.asList(1,
				2,
				3,
				4,
				5,
				6,
				7,
				8,
				9,
				10)))
		  .assertNoError()
		  .assertComplete();
	}

	@Test
	public void failInitial() {
		Supplier<List<Integer>> as = () -> {
			throw new RuntimeException("test");
		};

		StepVerifier.create(Flux.range(1, 10)
		                        .parallel(3)
		                        .collect(as, List::add))
		            .verifyErrorMessage("test");
	}

	@Test
	public void failCombination() {
		StepVerifier.create(Flux.range(1, 10)
		                        .parallel(3)
		                        .collect(() -> 0, (a, b) -> {
			                        throw new RuntimeException("test");
		                        }))
		            .verifyErrorMessage("test");
	}

	@Test
	public void testPrefetch() {
		assertThat(Flux.range(1, 10)
		               .parallel(3)
		               .collect(ArrayList::new, List::add)
		               .getPrefetch()).isEqualTo(Integer.MAX_VALUE);
	}

	@Test
	public void parallelism() {
		ParallelFlux<Integer> source = Flux.range(1, 4).parallel(3);
		ParallelCollect<Integer, List<Integer>> test = new ParallelCollect<>(source, ArrayList::new, List::add);

		assertThat(test.parallelism())
				.isEqualTo(3)
				.isEqualTo(source.parallelism());
	}

	@Test
	public void scanOperator() {
		ParallelFlux<Integer> source = Flux.range(1, 4).parallel(3);
		ParallelCollect<Integer, List<Integer>> test = new ParallelCollect<>(source, ArrayList::new, List::add);

		assertThat(test.scan(Scannable.Attr.PARENT)).isSameAs(source);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);
	}

	@Test
	public void scanSubscriber() {
		CoreSubscriber<List<Integer>> subscriber = new LambdaSubscriber<>(null, e -> {}, null, null);
		ParallelCollectSubscriber<Integer, List<Integer>> test = new ParallelCollectSubscriber<>(
				subscriber, new ArrayList<>(), List::add);
		Subscription s = Operators.emptySubscription();
		test.onSubscribe(s);

		assertThat(test.scan(Scannable.Attr.ACTUAL)).isSameAs(subscriber);
		assertThat(test.scan(Scannable.Attr.PREFETCH)).isEqualTo(Integer.MAX_VALUE);
		assertThat(test.scan(Scannable.Attr.RUN_STYLE)).isSameAs(Scannable.Attr.RunStyle.SYNC);

		assertThat(test.scan(Scannable.Attr.TERMINATED)).isFalse();
		test.complete(Collections.emptyList());
		assertThat(test.scan(Scannable.Attr.TERMINATED)).isTrue();

		assertThat(test.scan(Scannable.Attr.CANCELLED)).isFalse();
		test.cancel();
		assertThat(test.scan(Scannable.Attr.CANCELLED)).isTrue();
	}
}

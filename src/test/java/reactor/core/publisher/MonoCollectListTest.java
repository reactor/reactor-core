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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Scannable;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

public class MonoCollectListTest {

	@Test
	public void aFluxCanBeSorted(){
		List<Integer> vals = Flux.just(43, 32122, 422, 321, 43, 443311)
		                         .collectSortedList()
		                         .block();

		assertThat(vals).containsExactly(43, 43, 321, 422, 32122, 443311);
	}

	@Test
	public void aFluxCanBeSorted2(){
		List<Integer> vals = Flux.just(1, 2, 3, 4)
		                         .collectSortedList(Comparator.reverseOrder())
		                         .block();

		assertThat(vals).containsExactly(4,3,2,1);
	}

	@Test
	public void aFluxCanBeSorted3(){
		StepVerifier.create(Flux.just(43, 32122, 422, 321, 43, 443311)
		                        .sort(Comparator.reverseOrder()))
		            .expectNext(443311, 32122, 422, 321, 43, 43)
		            .verifyComplete();
	}

	@Test
	public void aFluxCanBeSorted4(){
		StepVerifier.create(Flux.just(43, 32122, 422, 321, 43, 443311)
		                        .sort())
		            .expectNext(43, 43, 321, 422, 32122, 443311)
		            .verifyComplete();
	}

	@Test
	public void collectListOne() {
		StepVerifier.create(Flux.just(1)
		                        .collectList())
		            .assertNext(d -> assertThat(d).containsExactly(1))
	                .verifyComplete();

	}

	@Test
	public void collectListEmpty() {
		StepVerifier.create(Flux.empty()
		                        .collectList())
		            .assertNext(d -> assertThat(d).isEmpty())
	                .verifyComplete();

	}

	@Test
	public void collectListCallable() {
		StepVerifier.create(Mono.fromCallable(() -> 1)
		                        .flux()
		                        .collectList())
		            .assertNext(d -> assertThat(d).containsExactly(1))
	                .verifyComplete();

	}

	@Test
	public void scanBufferAllSubscriber() {
		Subscriber<List<String>> actual = new LambdaMonoSubscriber<>(null, e -> {}, null, null);
		MonoCollectList.MonoBufferAllSubscriber<String, List<String>> test = new MonoCollectList.MonoBufferAllSubscriber<String, List<String>>(
				actual, new ArrayList<>());
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